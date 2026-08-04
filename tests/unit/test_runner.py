"""The runner, driven against a stub server with a scripted brain.

Nothing here opens a socket, reads an API key, or runs an SDK session. That is
the claim of section 6.3 — coordination is the server's, the runner is a
protocol client — and these tests are what makes the claim falsifiable: swap the
executor for `ScriptedExecutor` and the whole runner-side protocol still runs.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httpx
import pytest

import codefleet.runner as runner_module
from codefleet import cli
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

    def __init__(
        self,
        *,
        lease: str = "allow",
        stale_heartbeats: int = 0,
        faults: Sequence[str] = (),
    ) -> None:
        self.calls: list[tuple[str, str, str | None]] = []
        self.completions: list[dict[str, Any]] = []
        self.registrations = 0
        self.epoch = 0
        self.lease = lease
        self.stale_heartbeats = stale_heartbeats
        # Path suffixes that answer 500 once each: one entry is a blip, several
        # of the same suffix is a server that stays broken.
        self.faults = list(faults)
        self.assignment_pending = True
        self.completed = asyncio.Event()

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handle), base_url="http://stub")

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append((request.method, path, request.headers.get("X-Agent-Epoch")))

        if self._faulty(path):
            if path.endswith("/start"):
                # The server never moved the task on, so it is still on offer —
                # which is what makes a retried poll meaningful.
                self.assignment_pending = True
            return httpx.Response(500, json={"error": {"code": "boom", "message": "transient"}})

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

    def _faulty(self, path: str) -> bool:
        for suffix in self.faults:
            if path.endswith(suffix):
                self.faults.remove(suffix)
                return True
        return False

    def _lease(self, body: dict[str, Any]) -> httpx.Response:
        if self.lease == "error":
            return httpx.Response(500, json={"error": {"code": "boom", "message": "no"}})
        if self.lease in {"deny", "deny-last"}:
            # `deny-last` is the interesting shape: a request for several paths
            # refused over one that is not the first.
            held = body["paths"][-1] if self.lease == "deny-last" else body["paths"][0]
            return httpx.Response(
                200,
                json={
                    "decision": "deny",
                    "denied": [
                        {
                            "path": held,
                            "holder_agent_id": "a_other",
                            "holder_agent_name": "runner-2",
                            "holder_task_id": "T3",
                            "reason": "held",
                        }
                    ],
                    "message": f"{held} is held by runner-2 for task T3.",
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


async def test_a_denial_carries_the_path_the_server_actually_refused(tmp_path: Path) -> None:
    """A multi-path write refused over its *second* path is blocked on that path.

    `session.make_pre_write_hook` records `decision.path` as the file the task is
    blocked on and falls back to the first path asked for when the decision names
    none. The server then widens the task's `file_scope` with it before the
    retry — so a runner that reads the server's `denied[].path` only into the
    prose puts a file nobody contended into the next attempt's scope, and the
    retry can collide over it.
    """
    denials: list[Any] = []
    other = "linkstash/models.py"

    async def multi_path_write(
        *, task: Any, workdir: Path, on_pre_write: Any, on_post_write: Any
    ) -> Any:
        # One tool call asking for two files, the way MultiEdit does.
        decision = await on_pre_write([other, TARGET], "MultiEdit")
        denials.append(decision)
        # Exactly what the real PreToolUse hook does with the answer.
        blocked = decision.path if decision.path in (other, TARGET) else other
        return runner_module._outcome(
            ok=False,
            error=f"blocked: {blocked} is held by another agent",
            error_kind=ErrorKind.VETO,
            blocked_on_path=blocked,
        )

    stub = StubServer(lease="deny-last")
    await drive(stub, tmp_path, executor=multi_path_write)

    (decision,) = denials
    assert decision.allow is False
    assert decision.path == TARGET
    assert TARGET in (decision.message or "")

    (report,) = stub.completions
    assert report["error_kind"] == ErrorKind.VETO
    assert report["blocked_on_path"] == TARGET


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


def test_runner_is_constructible_without_touching_the_network(tmp_path: Path) -> None:
    runner = Runner("runner-9", "http://nowhere:1", tmp_path, settings_for(tmp_path))
    assert runner.agent_id is None
    assert runner.epoch == 0


# The AST tests that used to live here are gone: tests/unit/test_boundaries.py
# makes the same claims about this module with a stricter walker, and two
# implementations of one boundary is one of them silently rotting.


# ---------------------------------------------------------------------------
# Staying up
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("endpoint", ["/heartbeat", "/assignment", "/start"])
async def test_a_transient_server_error_does_not_shrink_the_fleet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, endpoint: str
) -> None:
    """One 500 is weather. A runner that dies of it is a slot lost for the run.

    This is the failure the heartbeat and stale-agent machinery exists to make
    survivable, so the runner has to be around to be swept — not gone.
    """
    monkeypatch.setattr(runner_module, "RETRY_BACKOFF_S", (0.0,) * 5)
    stub = StubServer(faults=[endpoint])

    await drive(stub, tmp_path, timeout=2.0)

    (report,) = stub.completions
    assert report["ok"] is True
    assert not stub.faults, f"the stub never got to fail {endpoint}"


async def test_a_server_that_never_answers_ends_the_runner_loudly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The retry budget is bounded: a permanent fault still ends the runner.

    Riding out a blip must not become a slot spinning forever against a server it
    will never reach again, and the exception has to reach the caller so the
    command that started the fleet can say the fleet is gone.
    """
    monkeypatch.setattr(runner_module, "RETRY_BACKOFF_S", (0.0, 0.0))
    stub = StubServer(faults=["/assignment"] * 10)
    client = stub.client()
    runner = Runner("runner-1", "http://stub", tmp_path, settings_for(tmp_path), client=client)

    with (
        caplog.at_level(logging.WARNING, logger="codefleet.runner"),
        pytest.raises(httpx.HTTPStatusError),
    ):
        await asyncio.wait_for(runner.run_forever(), 5.0)
    await client.aclose()

    assert "giving up" in caplog.text
    assert "HTTP 500" in caplog.text
    # Tried the budget, then stopped: three attempts, not ten and not one.
    polls = [call for call in stub.calls if call[1].endswith("/assignment")]
    assert len(polls) == 3


async def test_the_init_frame_is_reported_rather_than_discarded(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The frame is the record that a session ran under the posture we asked for."""
    frame = {
        "session_id": "sess_abc",
        "cwd": str((tmp_path / "work").resolve()),
        "model": "claude-haiku-4-5-20251001",
        "permissionMode": "dontAsk",
        "tools": ["Read", "Write", "Edit"],
    }

    async def executor(**_: Any) -> Any:
        return runner_module._outcome(ok=True, summary="done", init_frame=frame)

    stub = StubServer()
    with caplog.at_level(logging.INFO, logger="codefleet.runner"):
        await drive(stub, tmp_path, executor=executor)

    assert "claude-haiku-4-5-20251001" in caplog.text
    assert "permission_mode=dontAsk" in caplog.text
    assert "tools=3" in caplog.text
    assert "not the shared tree" not in caplog.text


async def test_a_session_that_ran_outside_the_shared_tree_is_flagged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Writes outside the coordinated tree are writes no lease could have vetoed."""

    async def executor(**_: Any) -> Any:
        return runner_module._outcome(
            ok=True, summary="done", init_frame={"cwd": str(tmp_path / "elsewhere")}
        )

    stub = StubServer()
    with caplog.at_level(logging.WARNING, logger="codefleet.runner"):
        await drive(stub, tmp_path, executor=executor)

    assert "not the shared tree" in caplog.text


# ---------------------------------------------------------------------------
# What `codefleet run` does with a runner that died
#
# These belong to cli.py rather than runner.py, but the subject is a dead
# runner: the two defects are the same defect seen from either end.
# ---------------------------------------------------------------------------


def _dead_fleet(tmp_path: Path, failure: BaseException) -> list[tuple[Runner, asyncio.Task[None]]]:
    async def die() -> None:
        raise failure

    runner = Runner("runner-1", "http://nowhere:1", tmp_path, settings_for(tmp_path))
    return [(runner, asyncio.create_task(die(), name="runner-1-main"))]


async def test_a_dead_runner_is_reported_not_discarded(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fleet = _dead_fleet(tmp_path, RuntimeError("the fleet lost this one"))

    await cli._stop_runners(fleet)

    printed = capsys.readouterr().out
    assert "runner-1" in printed
    assert "the fleet lost this one" in printed


async def test_the_wait_for_a_finished_run_ends_when_the_last_runner_dies(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """`run_finished` can only arrive while somebody is still running the run.

    Waiting on it with no runner left is a hang with an empty screen, which is
    indistinguishable from a fleet that is merely slow.
    """

    async def never_finishes() -> None:
        await asyncio.Event().wait()

    fleet = _dead_fleet(tmp_path, RuntimeError("boom"))

    await asyncio.wait_for(
        cli._wait_while_the_fleet_lives(never_finishes(), fleet, timeout=None), 2.0
    )

    assert "every runner has stopped" in capsys.readouterr().out
    await cli._stop_runners(fleet)
