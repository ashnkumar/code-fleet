"""The runner, driven against a stub server with a scripted brain.

Nothing here opens a socket, reads an API key, or runs an SDK session. That is
the claim of section 6.3 — coordination is the server's, the runner is a
protocol client — and these tests are what makes the claim falsifiable: swap the
executor for `ScriptedExecutor` and the whole runner-side protocol still runs.
"""

from __future__ import annotations

import ast
import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

import codefleet.runner as runner_module
from codefleet.config import Settings
from codefleet.models import ErrorKind
from codefleet.runner import Runner, ScriptedExecutor, ScriptedWrite

AGENT_ID = "a_stub00000001"
TARGET = "linkstash/api.py"

ASSIGNED_TASK = {
    "id": "T1",
    "title": "Add a health route to the dispatcher",
    "description": "Add GET /health to the handle dispatcher.",
    "priority": 5,
    "file_scope": [TARGET],
    "attempts": 1,
    "deadline": "2026-07-31T18:14:22.117Z",
    "blocked_on_path": None,
}


class StubServer:
    """The smallest thing that speaks section 5.1.

    It records every call it received, including the `X-Agent-Epoch` header, so a
    test can assert on the protocol rather than on the runner's internals.
    """

    def __init__(self, *, lease: str = "allow", stale_heartbeats: int = 0) -> None:
        self.calls: list[tuple[str, str, str | None]] = []
        self.completions: list[dict[str, Any]] = []
        self.registrations = 0
        self.epoch = 0
        self.lease = lease
        self.stale_heartbeats = stale_heartbeats
        self.assignment_pending = True
        self.completed = asyncio.Event()

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handle), base_url="http://stub")

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append((request.method, path, request.headers.get("X-Agent-Epoch")))

        if path == "/agents/register":
            self.registrations += 1
            self.epoch += 1
            # A fresh registration means the previous incarnation's in-flight
            # task went back to pending, so it is on offer again.
            self.assignment_pending = True
            return httpx.Response(
                200,
                json={
                    "agent_id": AGENT_ID,
                    "epoch": self.epoch,
                    "status": "idle",
                    "heartbeat_interval_s": 5,
                    "poll_interval_s": 1,
                    "task_timeout_s": 600,
                    "server_time": "2026-07-31T18:04:22.117Z",
                },
            )

        if path.endswith("/heartbeat"):
            if self.stale_heartbeats > 0:
                self.stale_heartbeats -= 1
                return httpx.Response(409, json={"error": {"code": "stale_epoch", "message": ""}})
            return httpx.Response(200, json={"status": "idle", "epoch": self.epoch})

        if path.endswith("/assignment"):
            if not self.assignment_pending:
                return httpx.Response(204)
            self.assignment_pending = False
            return httpx.Response(200, json={"task": ASSIGNED_TASK})

        if path.endswith("/start"):
            return httpx.Response(200, json={"status": "running"})

        if path == "/leases/acquire":
            return self._lease(json.loads(request.content))

        if path == "/changes":
            return httpx.Response(202, json={})

        if path.endswith("/complete"):
            self.completions.append(json.loads(request.content))
            self.completed.set()
            return httpx.Response(200, json={"status": "succeeded", "attempts": 1})

        if request.method == "DELETE":
            return httpx.Response(204)

        raise AssertionError(f"unexpected {request.method} {path}")

    def _lease(self, body: dict[str, Any]) -> httpx.Response:
        if self.lease == "error":
            return httpx.Response(500, json={"error": {"code": "boom", "message": "no"}})
        if self.lease == "deny":
            return httpx.Response(
                200,
                json={
                    "decision": "deny",
                    "denied": [
                        {
                            "path": body["paths"][0],
                            "holder_agent_id": "a_other",
                            "holder_agent_name": "runner-2",
                            "holder_task_id": "T3",
                            "reason": "held",
                        }
                    ],
                    "message": f"{body['paths'][0]} is held by runner-2 for task T3.",
                },
            )
        return httpx.Response(200, json={"decision": "allow", "granted": body["paths"]})

    # -- assertions helpers ------------------------------------------------

    def sequence(self) -> list[str]:
        """Calls in order, minus the heartbeats and the empty assignment polls."""
        return [
            f"{method} {path}" for method, path, _ in self.calls if not path.endswith("/heartbeat")
        ]


def settings_for(tmp_path: Path) -> Settings:
    return Settings(
        poll_interval=0.01,
        heartbeat_interval=0.02,
        task_timeout=5.0,
        run_dir=tmp_path / "runs",
    )


async def drive(
    stub: StubServer,
    tmp_path: Path,
    *,
    executor: Any = None,
    timeout: float = 5.0,
) -> Runner:
    """Run one runner until the stub sees a completion, then shut it down."""
    workdir = tmp_path / "work"
    workdir.mkdir(exist_ok=True)
    client = stub.client()
    runner = Runner(
        "runner-1",
        "http://stub",
        workdir,
        settings_for(tmp_path),
        executor if executor is not None else ScriptedExecutor(),
        client=client,
    )
    main = asyncio.create_task(runner.run_forever())
    try:
        await asyncio.wait_for(stub.completed.wait(), timeout)
    finally:
        await runner.shutdown()
        await asyncio.wait_for(main, timeout)
        await client.aclose()
    return runner


def assert_ordered(expected: list[str], actual: list[str]) -> None:
    """`expected` appears in `actual` in that order, with anything in between."""
    remaining = list(actual)
    for wanted in expected:
        assert wanted in remaining, f"{wanted} missing after {actual}"
        remaining = remaining[remaining.index(wanted) + 1 :]


# ---------------------------------------------------------------------------


async def test_completes_an_assignment_in_protocol_order(tmp_path: Path) -> None:
    stub = StubServer()
    await drive(stub, tmp_path)

    assert_ordered(
        [
            "POST /agents/register",
            f"GET /agents/{AGENT_ID}/assignment",
            "POST /tasks/T1/start",
            "POST /leases/acquire",
            "POST /changes",
            "POST /tasks/T1/complete",
        ],
        stub.sequence(),
    )

    (report,) = stub.completions
    assert report["ok"] is True
    assert report["agent_id"] == AGENT_ID
    assert report["files_written"] == [TARGET]
    assert (tmp_path / "work" / TARGET).exists()


async def test_every_call_after_registration_carries_the_epoch(tmp_path: Path) -> None:
    stub = StubServer()
    await drive(stub, tmp_path)

    for method, path, epoch in stub.calls:
        if path == "/agents/register":
            continue
        assert epoch == "1", f"{method} {path} carried epoch {epoch!r}"


async def test_deregisters_on_shutdown(tmp_path: Path) -> None:
    stub = StubServer()
    await drive(stub, tmp_path)

    assert ("DELETE", f"/agents/{AGENT_ID}", "1") in stub.calls


async def test_reregisters_when_the_epoch_is_fenced(tmp_path: Path) -> None:
    stub = StubServer(stale_heartbeats=1)
    await drive(stub, tmp_path)

    assert stub.registrations == 2
    # The work is reported under the epoch the server last handed out, never the
    # stale one — that is what stops a zombie writing to someone else's task.
    assert stub.completions[-1]["ok"] is True
    completes = [epoch for _, path, epoch in stub.calls if path.endswith("/complete")]
    assert completes == ["2"]


async def test_a_denied_lease_is_reported_as_a_veto(tmp_path: Path) -> None:
    stub = StubServer(lease="deny")
    await drive(stub, tmp_path)

    (report,) = stub.completions
    assert report["ok"] is False
    assert report["error_kind"] == ErrorKind.VETO
    assert report["blocked_on_path"] == TARGET
    assert report["files_written"] == []
    # The veto happened before the write, not after it. That is the whole claim.
    assert not (tmp_path / "work" / TARGET).exists()
    assert "POST /changes" not in stub.sequence()


async def test_write_coordination_failure_fails_closed(tmp_path: Path) -> None:
    stub = StubServer(lease="error")
    await drive(stub, tmp_path)

    (report,) = stub.completions
    assert report["ok"] is False
    assert report["error_kind"] == ErrorKind.INFRA
    assert "500" in report["error"]
    assert not (tmp_path / "work" / TARGET).exists()


async def test_a_crashing_executor_is_reported_rather_than_lost(tmp_path: Path) -> None:
    async def explode(**_: Any) -> None:
        raise RuntimeError("the session blew up")

    stub = StubServer()
    await drive(stub, tmp_path, executor=explode)

    (report,) = stub.completions
    assert report["ok"] is False
    assert report["error_kind"] == ErrorKind.INFRA
    assert "the session blew up" in report["error"]


async def test_scripted_executor_stops_at_the_first_denied_write(tmp_path: Path) -> None:
    """A denial is final: nothing after it in the script is attempted."""
    executor = ScriptedExecutor(
        lambda task: [ScriptedWrite(TARGET), ScriptedWrite("linkstash/store.py")]
    )
    stub = StubServer(lease="deny")
    await drive(stub, tmp_path, executor=executor)

    acquisitions = [call for call in stub.sequence() if call == "POST /leases/acquire"]
    assert len(acquisitions) == 1
    assert not (tmp_path / "work" / "linkstash" / "store.py").exists()


# ---------------------------------------------------------------------------
# The architectural claim, enforced mechanically
# ---------------------------------------------------------------------------


def _imports(tree: ast.AST, *, module_level_only: bool) -> set[str]:
    nodes = tree.body if module_level_only else list(ast.walk(tree))  # type: ignore[attr-defined]
    names: set[str] = set()
    for node in nodes:
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


@pytest.fixture
def runner_source() -> ast.Module:
    return ast.parse(Path(runner_module.__file__).read_text(encoding="utf-8"))


def test_runner_holds_no_coordination_logic(runner_source: ast.Module) -> None:
    """No store, no scheduler, anywhere in the module — not even lazily."""
    everywhere = _imports(runner_source, module_level_only=False)
    assert "codefleet.store" not in everywhere
    assert "codefleet.scheduler" not in everywhere


def test_runner_needs_no_sdk_to_import(runner_source: ast.Module) -> None:
    """`codefleet.session` is the only module allowed to reach the SDK.

    The runner does not import it at module scope either, so a `Runner` can be
    constructed — and every coordination test driven — on a machine with no
    `claude_agent_sdk` installed.
    """
    at_import = _imports(runner_source, module_level_only=True)
    assert "claude_agent_sdk" not in at_import
    assert "codefleet.session" not in at_import


def test_runner_is_constructible_without_touching_the_network(tmp_path: Path) -> None:
    runner = Runner("runner-9", "http://nowhere:1", tmp_path, settings_for(tmp_path))
    assert runner.agent_id is None
    assert runner.epoch == 0
