"""API tests: the whole server, over HTTP, against a temp-file database.

These run the real app — real tick loop, real scheduler, real SQLite — with
`httpx.ASGITransport` in place of a socket. No network, no API key, no runner
process: every agent here is a few lines of test code speaking the same HTTP
contract the real runner speaks, which is the point of the split (spec 6.3).

Because the tick loop is live, most assertions are about what the fleet settles
on rather than what happens on the next line, hence `eventually`. Where a state
is deliberately stable — a task inside its backoff window, a dependent with an
unmet dependency — the assertion is immediate.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from codefleet.config import Settings
from codefleet.server import create_app

BASE_URL = "http://codefleet.test"


@pytest.fixture
async def app(tmp_path: Path, request: pytest.FixtureRequest) -> AsyncIterator[FastAPI]:
    """The real app on a throwaway database.

    A test that needs the server configured differently passes the difference as
    an indirect parameter, so the settings a test runs against are visible on the
    test rather than smuggled in through the environment.
    """
    overrides: dict[str, Any] = getattr(request, "param", {})
    settings = Settings(
        db=tmp_path / "codefleet.db",
        workdir=tmp_path / "repo",
        # Fast enough that the reconciliation sweep is not what the tests wait on.
        tick_interval=0.02,
        allow_reset=True,
        **overrides,
    )
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        yield application


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as http:
        yield http


# -- helpers ----------------------------------------------------------------


@dataclass(slots=True)
class Runner:
    """A fake runner: the HTTP contract without the SDK session behind it."""

    client: AsyncClient
    name: str
    agent_id: str
    epoch: int

    @property
    def headers(self) -> dict[str, str]:
        return {"X-Agent-Epoch": str(self.epoch)}

    async def assignment(self) -> dict[str, Any] | None:
        response = await self.client.get(
            f"/agents/{self.agent_id}/assignment", headers=self.headers
        )
        if response.status_code == 204:
            return None
        assert response.status_code == 200, response.text
        return response.json()["task"]

    async def await_assignment(self) -> dict[str, Any]:
        return await eventually(self.assignment)

    async def start(self, task_id: str) -> None:
        response = await self.client.post(
            f"/tasks/{task_id}/start", json={"agent_id": self.agent_id}, headers=self.headers
        )
        assert response.status_code == 200, response.text

    async def acquire(self, task_id: str, *paths: str) -> dict[str, Any]:
        response = await self.client.post(
            "/leases/acquire",
            json={"agent_id": self.agent_id, "task_id": task_id, "paths": list(paths)},
            headers=self.headers,
        )
        assert response.status_code == 200, response.text
        return response.json()

    async def complete(self, task_id: str, **result: Any) -> dict[str, Any]:
        body = {"agent_id": self.agent_id, "ok": True} | result
        response = await self.client.post(
            f"/tasks/{task_id}/complete", json=body, headers=self.headers
        )
        assert response.status_code == 200, response.text
        return response.json()


async def register(client: AsyncClient, name: str) -> Runner:
    response = await client.post(
        "/agents/register", json={"name": name, "workdir": "/tmp/demo-repo", "pid": 4242}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    return Runner(client=client, name=name, agent_id=body["agent_id"], epoch=body["epoch"])


async def create_tasks(client: AsyncClient, *tasks: dict[str, Any]) -> list[str]:
    response = await client.post("/tasks", json={"tasks": list(tasks)})
    assert response.status_code == 201, response.text
    return response.json()["created"]


def task_spec(task_id: str, **overrides: Any) -> dict[str, Any]:
    return {
        "id": task_id,
        "title": f"task {task_id}",
        "description": "do the thing",
    } | overrides


async def eventually[T](check: Callable[[], Awaitable[T | None]], *, timeout: float = 5.0) -> T:
    """Poll until the server settles on something truthy, or fail loudly."""
    deadline = monotonic() + timeout
    while True:
        result = await check()
        if result:
            return result
        if monotonic() > deadline:
            raise AssertionError(f"condition did not hold within {timeout}s")
        await asyncio.sleep(0.01)


async def get_task(client: AsyncClient, task_id: str) -> dict[str, Any]:
    response = await client.get(f"/tasks/{task_id}")
    assert response.status_code == 200, response.text
    return response.json()["task"]


async def event_types(client: AsyncClient) -> list[str]:
    response = await client.get("/events", params={"since": 0, "limit": 500})
    assert response.status_code == 200, response.text
    return [event["type"] for event in response.json()["events"]]


async def assigned_pair(client: AsyncClient) -> tuple[Runner, str, Runner, str]:
    """Two agents, each holding one of two tasks with disjoint declared scopes.

    The scheduler co-schedules them precisely because their declared scopes do
    not overlap — which is the setup for a veto: the declaration was a guess.
    """
    first = await register(client, "runner-1")
    second = await register(client, "runner-2")
    await create_tasks(
        client,
        task_spec("T3", file_scope=["linkstash/api.py"]),
        task_spec("T4", file_scope=["linkstash/middleware.py"]),
    )
    first_task = await first.await_assignment()
    second_task = await second.await_assignment()
    return first, first_task["id"], second, second_task["id"]


async def read_sse_frames(
    app: FastAPI, query: str, *, count: int, timeout: float = 5.0
) -> list[dict[str, str]]:
    """Drive the ASGI app directly to read a live stream.

    `httpx.ASGITransport` runs the app to completion and buffers the body, which
    for an endpoint that never completes means it never returns. So the test
    speaks ASGI: collect body chunks until `count` frames have arrived, then hang
    up the way a real client would.
    """
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "headers": [],
        "scheme": "http",
        "path": "/events/stream",
        "raw_path": b"/events/stream",
        "query_string": query.encode(),
        "server": ("codefleet.test", 80),
        "client": ("test", 1234),
        "root_path": "",
    }
    received: list[str] = []
    enough = asyncio.Event()

    async def receive() -> dict[str, Any]:
        await enough.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            assert message["status"] == 200, message
        elif message["type"] == "http.response.body":
            received.append(message["body"].decode())
            if len(parse_sse("".join(received))) >= count:
                enough.set()

    streaming = asyncio.create_task(app(scope, receive, send))
    try:
        await asyncio.wait_for(enough.wait(), timeout)
    finally:
        streaming.cancel()
        with suppress(asyncio.CancelledError):
            await streaming
    return parse_sse("".join(received))


def parse_sse(payload: str) -> list[dict[str, str]]:
    """Split an SSE body into frames. Comments (`: ping`) are not frames."""
    frames: list[dict[str, str]] = []
    for block in payload.split("\n\n"):
        lines = [line for line in block.splitlines() if line and not line.startswith(":")]
        if not lines:
            continue
        fields = dict(line.split(": ", 1) for line in lines)  # type: ignore[misc]
        frames.append(fields)
    return frames


# -- registration, heartbeat, assignment ------------------------------------


async def test_register_heartbeat_and_assignment_round_trip(client: AsyncClient) -> None:
    runner = await register(client, "runner-1")
    assert runner.epoch == 1

    beat = await client.post(f"/agents/{runner.agent_id}/heartbeat", headers=runner.headers)
    assert beat.status_code == 200
    assert beat.json() == {"status": "idle", "epoch": 1}

    assert await runner.assignment() is None

    await create_tasks(client, task_spec("T1", priority=5, file_scope=["linkstash/api.py"]))
    task = await runner.await_assignment()
    assert task["id"] == "T1"
    assert task["attempts"] == 1
    assert task["file_scope"] == ["linkstash/api.py"]
    assert task["deadline"].endswith("Z")

    await runner.start("T1")
    assert (await get_task(client, "T1"))["status"] == "running"

    # Assignment is the claim: the agent is busy the moment the server decided,
    # not when the runner got around to polling.
    agents = (await client.get("/agents")).json()["agents"]
    assert [(agent["status"], agent["current_task_id"]) for agent in agents] == [("busy", "T1")]

    types = await event_types(client)
    assert types[:2] == ["fleet_started", "agent_registered"]
    assert "task_assigned" in types and "task_started" in types


async def test_stale_epoch_is_rejected(client: AsyncClient) -> None:
    runner = await register(client, "runner-1")

    response = await client.post(
        f"/agents/{runner.agent_id}/heartbeat", headers={"X-Agent-Epoch": "99"}
    )
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "stale_epoch"
    assert error["detail"]["current_epoch"] == runner.epoch

    # Re-registering under the same name is the recovery path, and it is the same row.
    again = await register(client, "runner-1")
    assert again.agent_id == runner.agent_id
    assert again.epoch == runner.epoch + 1
    assert await event_types(client) == ["fleet_started", "agent_registered", "agent_online"]


# -- leases -----------------------------------------------------------------


async def test_lease_is_granted_once_and_the_denial_names_the_holder(
    client: AsyncClient,
) -> None:
    holder, holder_task, requester, requester_task = await assigned_pair(client)

    granted = await holder.acquire(holder_task, "linkstash/api.py")
    assert granted == {"decision": "allow", "granted": ["linkstash/api.py"]}

    denied = await requester.acquire(requester_task, "linkstash/api.py")
    assert denied["decision"] == "deny"
    assert denied["denied"] == [
        {
            "path": "linkstash/api.py",
            "holder_agent_id": holder.agent_id,
            "holder_agent_name": holder.name,
            "holder_task_id": holder_task,
            "reason": "held",
        }
    ]
    assert holder.name in denied["message"] and holder_task in denied["message"]

    # Re-acquiring what you already hold is an allow, not a conflict.
    assert (await holder.acquire(holder_task, "linkstash/api.py"))["decision"] == "allow"


async def test_denied_lease_leaves_no_row_behind(client: AsyncClient) -> None:
    holder, holder_task, requester, requester_task = await assigned_pair(client)

    await holder.acquire(holder_task, "linkstash/api.py")
    denied = await requester.acquire(requester_task, "linkstash/middleware.py", "linkstash/api.py")
    assert denied["decision"] == "deny"

    # All-or-nothing: the path the requester *could* have taken was rolled back
    # with the one it could not, or the request would be hold-and-wait.
    leases = (await client.get("/leases")).json()["leases"]
    assert [(lease["path"], lease["task_id"]) for lease in leases] == [
        ("linkstash/api.py", holder_task)
    ]


# -- completion, cascade, veto ----------------------------------------------


async def test_success_cascades_to_the_dependent(client: AsyncClient) -> None:
    runner = await register(client, "runner-1")
    await create_tasks(
        client,
        task_spec("T3", priority=5, file_scope=["linkstash/api.py"]),
        task_spec("T5", priority=5, file_scope=["tests/test_api.py"], depends_on=["T3"]),
    )

    dependent = await get_task(client, "T5")
    assert dependent["status"] == "pending"
    assert dependent["runnable"] is False
    assert dependent["unmet_dependencies"] == ["T3"]

    task = await runner.await_assignment()
    assert task["id"] == "T3", "an unmet dependency must not be assignable"
    await runner.start("T3")
    reported = await runner.complete(
        "T3", ok=True, summary="added the health route", files_written=["linkstash/api.py"]
    )
    assert reported["status"] == "succeeded"
    assert reported["unblocked"] == ["T5"]

    assert "task_unblocked" in await event_types(client)
    # Nothing pushes work at a runner: the next tick simply finds T5 runnable.
    assert (await runner.await_assignment())["id"] == "T5"

    # Leases and the agent are both released by the same transaction.
    assert (await client.get("/leases")).json()["leases"] == []


async def test_a_duplicate_completion_report_changes_nothing(
    app: FastAPI, client: AsyncClient
) -> None:
    """Spec 5.1: a report for an attempt already recorded is a no-op `200`.

    The in-memory memo covers the ordinary case, a runner redelivering a report
    seconds after the first one landed. It does not survive a restart, and the
    durable check is what makes the guarantee real: without it a redelivered
    success adds its tokens and cost to the task and to the agent all over again,
    and cascades to the dependents a second time.
    """
    runner = await register(client, "runner-1")
    await create_tasks(client, task_spec("T1", file_scope=["linkstash/api.py"]))
    await runner.await_assignment()
    await runner.start("T1")

    result = {"summary": "done", "input_tokens": 100, "output_tokens": 10, "cost_usd": 0.01}
    assert (await runner.complete("T1", **result))["status"] == "succeeded"

    # What a restarted server looks like: the row still remembers, the memo does not.
    app.state.fleet.completions.clear()
    assert await runner.complete("T1", **result) == {
        "status": "succeeded",
        "attempts": 1,
        "duplicate": True,
    }

    task = await get_task(client, "T1")
    assert (task["input_tokens"], task["output_tokens"], task["cost_usd"]) == (100, 10, 0.01)
    agent = (await client.get("/agents")).json()["agents"][0]
    assert (agent["tasks_succeeded"], agent["input_tokens"]) == (1, 100)
    assert (await event_types(client)).count("task_succeeded") == 1


async def test_veto_requeues_the_task_and_widens_its_file_scope(client: AsyncClient) -> None:
    runner = await register(client, "runner-1")
    await create_tasks(client, task_spec("T4", file_scope=["linkstash/middleware.py"]))
    await runner.await_assignment()
    await runner.start("T4")

    ledger = await client.post(
        "/changes",
        json={
            "agent_id": runner.agent_id,
            "task_id": "T4",
            "path": "linkstash/middleware.py",
            "tool": "Write",
        },
        headers=runner.headers,
    )
    assert ledger.status_code == 202

    reported = await runner.complete(
        "T4",
        ok=False,
        error="Blocked: linkstash/api.py is held by runner-2.",
        error_kind="veto",
        blocked_on_path="linkstash/api.py",
        files_written=["linkstash/middleware.py"],
    )
    assert reported["status"] == "pending"
    assert reported["attempts"] == 1
    assert reported["backoff_until"].endswith("Z")

    task = await get_task(client, "T4")
    assert task["status"] == "pending"
    # Spec 4.5 step 8: the denied path is now a scheduling fact, so the retry is
    # not co-scheduled with whoever holds it.
    assert task["file_scope"] == ["linkstash/middleware.py", "linkstash/api.py"]
    assert task["blocked_on_path"] == "linkstash/api.py"
    assert task["runnable"] is False, "backoff must hold the retry back"
    assert "task_requeued" in await event_types(client)

    # Partial work survives a requeue, and the ledger is how you find out about it.
    changes = (await client.get("/tasks/T4")).json()["changes"]
    assert [(change["path"], change["tool"]) for change in changes] == [
        ("linkstash/middleware.py", "Write")
    ]

    # A duplicate report of the same attempt is a no-op, not a second requeue.
    assert await runner.complete("T4", ok=False, error_kind="veto") == reported
    assert (await get_task(client, "T4"))["attempts"] == 1


async def test_exhausted_attempts_fail_the_task_and_block_its_dependents(
    client: AsyncClient,
) -> None:
    runner = await register(client, "runner-1")
    await create_tasks(
        client,
        task_spec("T1", max_attempts=1),
        task_spec("T2", depends_on=["T1"]),
    )
    await runner.await_assignment()
    reported = await runner.complete("T1", ok=False, error="it broke", error_kind="agent_error")
    assert reported == {"status": "failed", "attempts": 1, "error_kind": "attempts_exhausted"}

    async def blocked() -> dict[str, Any] | None:
        task = await get_task(client, "T2")
        return task if task["status"] == "blocked_upstream" else None

    assert (await eventually(blocked))["error"] == "dependency T1 did not succeed"
    assert "task_blocked_upstream" in await event_types(client)


@pytest.mark.parametrize("app", [{"max_attempts": 1}], indirect=True)
async def test_the_configured_attempt_cap_reaches_the_tasks_that_did_not_name_one(
    client: AsyncClient,
) -> None:
    """`CODEFLEET_MAX_ATTEMPTS` is a documented knob, so something has to read it.

    A graph is written once and run against differently-configured fleets; a task
    that names its own cap means it, and one that does not takes the fleet's.
    """
    await create_tasks(client, task_spec("T1"), task_spec("T2", max_attempts=3))
    assert (await get_task(client, "T1"))["max_attempts"] == 1
    assert (await get_task(client, "T2"))["max_attempts"] == 3

    runner = await register(client, "runner-1")
    await runner.await_assignment()
    reported = await runner.complete("T1", ok=False, error="it broke", error_kind="agent_error")
    assert reported["status"] == "failed", "one attempt was the whole budget"


@pytest.mark.parametrize("app", [{"stale_after": 0.3}], indirect=True)
async def test_a_stale_agent_loses_its_leases_and_its_task(client: AsyncClient) -> None:
    runner = await register(client, "runner-1")
    await create_tasks(client, task_spec("T1", file_scope=["linkstash/api.py"]))
    await runner.await_assignment()
    await runner.start("T1")
    await runner.acquire("T1", "linkstash/api.py")

    async def offline() -> dict[str, Any] | None:
        agents = (await client.get("/agents")).json()["agents"]
        return agents[0] if agents[0]["status"] == "offline" else None

    agent = await eventually(offline)
    assert agent["stale"] is True
    # The epoch bump is what stops the agent coming back and writing to a task it
    # no longer owns.
    assert agent["epoch"] == runner.epoch + 1

    # Released in the same transaction as the requeue: this is what unblocks
    # whoever the dead agent was denying.
    assert (await client.get("/leases")).json()["leases"] == []
    task = await get_task(client, "T1")
    assert task["status"] == "pending"
    assert task["assigned_agent_id"] is None
    assert task["attempts"] == 1, "a lost assignment still costs an attempt"

    types = await event_types(client)
    assert {"agent_offline", "lease_released", "task_requeued"} <= set(types)


async def test_cancelling_a_task_fences_the_agent_holding_it(client: AsyncClient) -> None:
    runner = await register(client, "runner-1")
    await create_tasks(client, task_spec("T1", file_scope=["linkstash/api.py"]))
    await runner.await_assignment()
    await runner.start("T1")
    await runner.acquire("T1", "linkstash/api.py")

    cancelled = await client.post("/tasks/T1/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["task"]["status"] == "cancelled"
    assert (await client.get("/leases")).json()["leases"] == []

    # There is no "your task was taken away" flag: the epoch bump is the signal,
    # and the zombie meets it on whatever call it makes next.
    stale = await client.post(f"/agents/{runner.agent_id}/heartbeat", headers=runner.headers)
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "stale_epoch"


async def test_fleet_idle_and_run_finished_are_emitted_once_per_run(
    client: AsyncClient,
) -> None:
    runner = await register(client, "runner-1")
    await create_tasks(client, task_spec("T1"))
    await runner.await_assignment()
    await runner.complete("T1", ok=True, summary="done")

    async def finished() -> list[str] | None:
        types = await event_types(client)
        return types if "run_finished" in types else None

    types = await eventually(finished)
    assert types.count("fleet_idle") == 1
    assert types.index("fleet_idle") < types.index("run_finished")

    # A second batch reopens the run, so the pair fires again when it drains.
    await create_tasks(client, task_spec("T2"))
    await runner.await_assignment()
    await runner.complete("T2", ok=True, summary="done")

    async def finished_twice() -> list[str] | None:
        types = await event_types(client)
        return types if types.count("run_finished") == 2 else None

    assert (await eventually(finished_twice)).count("fleet_idle") == 2


# -- graphs -----------------------------------------------------------------


async def test_a_cyclic_graph_is_rejected_whole(client: AsyncClient) -> None:
    response = await client.post(
        "/tasks",
        json={
            "tasks": [
                task_spec("T1", depends_on=["T2"]),
                task_spec("T2", depends_on=["T1"]),
                task_spec("T3"),
            ]
        },
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_graph"

    # Atomic: the acyclic third task did not land either.
    listing = (await client.get("/tasks")).json()
    assert listing["total"] == 0
    assert "task_created" not in await event_types(client)


# -- conflicts --------------------------------------------------------------


async def test_conflicts_resolve_when_the_requester_later_succeeds(
    client: AsyncClient,
) -> None:
    holder, holder_task, requester, requester_task = await assigned_pair(client)
    await holder.acquire(holder_task, "linkstash/api.py")
    await requester.acquire(requester_task, "linkstash/api.py")

    conflicts = (await client.get("/conflicts")).json()["conflicts"]
    assert len(conflicts) == 1
    conflict = conflicts[0]
    assert conflict["path"] == "linkstash/api.py"
    assert conflict["holder_agent_name"] == holder.name
    assert conflict["requester_task_id"] == requester_task
    assert conflict["resolved"] is False

    assert (await client.get("/conflicts", params={"resolved": True})).json()["conflicts"] == []

    await requester.complete(requester_task, ok=True, summary="succeeded on retry")

    resolved = (await client.get("/conflicts", params={"resolved": True})).json()["conflicts"]
    assert [conflict["requester_task_id"] for conflict in resolved] == [requester_task]
    assert resolved[0]["requester_status"] == "succeeded"


# -- stream and health ------------------------------------------------------


async def test_sse_replays_from_since_zero_in_id_order(app: FastAPI, client: AsyncClient) -> None:
    await register(client, "runner-1")
    await create_tasks(client, task_spec("T1"), task_spec("T2", depends_on=["T1"]))

    frames = await read_sse_frames(app, "since=0", count=5)
    ids = [int(frame["id"]) for frame in frames]
    assert ids == sorted(ids) == list(range(1, len(ids) + 1))

    first = frames[0]
    assert first["event"] == "fleet_started"
    payload = json.loads(first["data"])
    assert payload["id"] == 1
    assert payload["type"] == "fleet_started"
    assert payload["at"].endswith("Z")

    # `event:` carries the EventType so a client can subscribe selectively.
    assert [frame["event"] for frame in frames[1:4]] == [
        "agent_registered",
        "task_created",
        "task_created",
    ]

    # A reconnect hands back the last id it saw and gets only what followed.
    tail = await read_sse_frames(app, f"since={ids[1]}", count=1)
    assert int(tail[0]["id"]) == ids[2]


async def test_health_works_on_an_empty_database(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["db"] == "ok"
    assert body["version"]
    assert body["uptime_s"] >= 0

    state = (await client.get("/state")).json()
    assert state["tasks"] == [] and state["agents"] == [] and state["leases"] == []
    assert state["counters"]["tasks"]["pending"] == 0
