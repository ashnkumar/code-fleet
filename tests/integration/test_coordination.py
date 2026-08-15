"""The whole fleet, coordinating, with the model swapped out.

This is the test spec 6.3 says the design owes: a real coordination server — real
tick loop, real pure scheduler, real SQLite, a real socket — driven by real
`codefleet.runner.Runner` instances whose only difference from production is that
they follow a script instead of running a Claude session. No API key, no model
call, no spend, no network beyond loopback. Assignment, the dependency cascade,
the write veto, stale-agent recovery, attempt exhaustion and `blocked_upstream`
propagation are all exercised here, and none of them can be, because a
`ScriptedExecutor` has no idea what a dependency is.

The fleet runs **once**, in `_grand_tour`, and almost every test below is a
question asked of the record that run left behind. That shape is deliberate: the
claim in spec §1 is that one append-only table is the complete account of a run.
If these questions could not be answered from `GET /events`, the claim would be
false.

Two implementation notes. The server runs on a real port rather than through
`httpx.ASGITransport`, because that transport buffers a response until the
handler returns and `/events/stream` never returns — and the stream is one of the
things under test. And the stale-agent case registers a bare HTTP agent instead
of a `Runner`: the failure being modeled is a process that died, and a process
that died does not heartbeat, which is exactly what a live `Runner` cannot be
made to stop doing.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn

from codefleet.cli import _await_run_finished
from codefleet.config import Settings
from codefleet.dashboard import stream_events
from codefleet.models import EventType, Task, TaskStatus
from codefleet.runner import Runner, ScriptedExecutor, ScriptedWrite
from codefleet.server import create_app

# -- the graph ---------------------------------------------------------------

# The file two tasks end up fighting over. Only one of them declared it.
CONTESTED = "service/api.py"

HOLD = "T_HOLD"  # declares the contested file and keeps it for its whole run
GRAB = "T_GRAB"  # declares something else, then reaches for the contested file
SEED = "T_SEED"  # the third of the opening trio, and the cascade's dependency
CASCADE = "T_CASCADE"  # must not start before SEED succeeds
CRASH = "T_CRASH"  # every attempt blows up, so it exhausts its attempts
STRANDED = "T_STRANDED"  # depends on CRASH, so it can only ever be blocked
DOOMED = "T_DOOMED"  # long enough to still be running when it is cancelled

WRITE_PAUSE = 0.3
GRAB_PAUSE = 0.5  # the grabber reaches for the contested file after the holder took it
HOLD_PAUSE = 2.0  # and the holder is still working on it when that happens
DOOMED_PAUSE = 5.0  # long enough that the cancel lands mid-session, not after it

RUN_TIMEOUT = 90.0

GRAPH: list[dict[str, Any]] = [
    {
        "id": HOLD,
        "title": "Add a health route",
        "description": "Owns the contested file for the length of its session.",
        "priority": 5,
        "file_scope": [CONTESTED],
    },
    {
        "id": GRAB,
        "title": "Add request-logging middleware and enable it",
        "description": "Declares middleware.py, then has to register it in api.py.",
        "priority": 5,
        "file_scope": ["service/middleware.py"],
    },
    {
        "id": SEED,
        "title": "Introduce a Settings object",
        "description": "Independent work, and what the cascade waits on.",
        "priority": 5,
        "file_scope": ["service/config.py"],
    },
    {
        "id": CASCADE,
        "title": "Load settings from the environment",
        "description": "Cannot start until the Settings object exists.",
        "priority": 4,
        "file_scope": ["service/config_env.py"],
        "depends_on": [SEED],
    },
    {
        "id": CRASH,
        "title": "Port the storage layer",
        "description": "Every attempt at this one dies in the session.",
        "priority": 2,
        "file_scope": ["service/storage.py"],
        # Two rather than the default three: each retry costs a backoff, and the
        # third attempt would prove nothing the second does not.
        "max_attempts": 2,
    },
    {
        "id": STRANDED,
        "title": "Use the ported storage layer",
        "description": "Downstream of a task that can never succeed.",
        "priority": 2,
        "file_scope": ["service/storage_client.py"],
        "depends_on": [CRASH],
    },
    {
        "id": DOOMED,
        "title": "Rewrite the slow path",
        "description": "Still working when the operator changes their mind.",
        "priority": 1,
        "file_scope": ["service/slow.py"],
    },
]


def plan(task: Task) -> list[ScriptedWrite]:
    """What a scripted agent 'does' for each task in the graph.

    The interesting entry is `GRAB`: it writes the file it declared and then
    writes one it did not, which is the realistic failure mode — `file_scope` is
    a guess a human wrote before the agent read the code, and agents follow the
    code.
    """
    if task.id == CRASH:
        # An executor that raises is how a real session blowing up reaches the
        # server: the runner reports `infra`, which is retryable, so this task
        # burns both its attempts and lands in `failed`.
        raise RuntimeError("simulated session crash")

    writes = [ScriptedWrite(path, pause_before=WRITE_PAUSE) for path in task.file_scope]
    if task.id == HOLD:
        writes.append(ScriptedWrite(CONTESTED, tool="Edit", pause_before=HOLD_PAUSE))
    if task.id == GRAB:
        writes.append(ScriptedWrite(CONTESTED, tool="Edit", pause_before=GRAB_PAUSE))
    if task.id == DOOMED:
        writes.append(ScriptedWrite("service/slow.py", tool="Edit", pause_before=DOOMED_PAUSE))
    return writes


# -- the record --------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RunRecord:
    """Everything one fleet run left behind, read back over the public API."""

    events: list[dict[str, Any]]
    tasks: dict[str, dict[str, Any]]
    agents: list[dict[str, Any]]
    conflicts: list[dict[str, Any]]
    leases: list[dict[str, Any]]
    tailed: list[dict[str, Any]]  # what one long-lived SSE subscriber saw, live
    replayed: list[dict[str, Any]]  # the same stream, replayed from since=0 afterward


def select(
    events: list[dict[str, Any]],
    *,
    type: EventType | None = None,
    task_id: str | None = None,
    agent_id: str | None = None,
) -> list[dict[str, Any]]:
    return [
        event
        for event in events
        if (type is None or event["type"] == type)
        and (task_id is None or event["task_id"] == task_id)
        and (agent_id is None or event["agent_id"] == agent_id)
    ]


def only(
    events: list[dict[str, Any]],
    *,
    type: EventType | None = None,
    task_id: str | None = None,
    agent_id: str | None = None,
) -> dict[str, Any]:
    """The single matching event, or a failure that says what was there instead."""
    matches = select(events, type=type, task_id=task_id, agent_id=agent_id)
    assert len(matches) == 1, f"expected one {type} for {task_id}, got {len(matches)}: {matches}"
    return matches[0]


# -- the run -----------------------------------------------------------------


@pytest.fixture(scope="module")
def run(tmp_path_factory: pytest.TempPathFactory) -> RunRecord:
    """One fleet run, shared by every assertion in this file.

    Synchronous on purpose: `asyncio.run` puts the server, the runners and the
    SSE subscribers in one loop for the run's whole lifetime, with no dependence
    on how the test framework happens to scope its own.
    """
    root = tmp_path_factory.mktemp("grand-tour")
    return asyncio.run(_grand_tour(root))


async def _grand_tour(root: Path) -> RunRecord:
    workdir = root / "workspace"
    workdir.mkdir()
    settings = _settings(root, workdir)

    async with _serving(settings) as base_url:
        tailed: list[dict[str, Any]] = []
        tail = asyncio.create_task(_tail(base_url, tailed, until=EventType.RUN_FINISHED))

        async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
            created = await client.post("/tasks", json={"tasks": GRAPH})
            assert created.status_code == 201, created.text

            fleet = _fleet(base_url, workdir, settings, count=3)
            loops = [asyncio.create_task(r.run_forever(), name=r.name) for r in fleet]
            try:
                await _cancel_once_running(client, DOOMED)
                await _await_tail(tail, tailed)
            finally:
                for runner in fleet:
                    await runner.shutdown()
                await asyncio.gather(*loops, return_exceptions=True)
                tail.cancel()

            # Runners deregister on the way out, which is the only `agent_offline`
            # this run produces, so the event log is read after they are gone.
            await _eventually(
                lambda: _all_offline(client), what="every runner to deregister", timeout=10.0
            )
            logged = (await client.get("/events", params={"since": 0, "limit": 5000})).json()
            tasks = (await client.get("/tasks", params={"limit": 500})).json()["tasks"]
            agents = (await client.get("/agents")).json()["agents"]
            conflicts = (await client.get("/conflicts")).json()["conflicts"]
            leases = (await client.get("/leases")).json()["leases"]

        replayed = await _replay(base_url, until_id=logged["events"][-1]["id"])

    return RunRecord(
        events=logged["events"],
        tasks={task["id"]: task for task in tasks},
        agents=agents,
        conflicts=conflicts,
        leases=leases,
        tailed=tailed,
        replayed=replayed,
    )


def _settings(root: Path, workdir: Path, **overrides: Any) -> Settings:
    defaults: dict[str, Any] = {
        "db": root / "codefleet.db",
        "workdir": workdir,
        "run_dir": root / "runs",
        "host": "127.0.0.1",
        "port": _free_port(),
        "runners": 3,
        # Tight enough that the tests wait on the fleet rather than on timers,
        # and loose enough that a busy CI box is not mistaken for a dead runner.
        "tick_interval": 0.05,
        "poll_interval": 0.05,
        "heartbeat_interval": 0.25,
        "stale_after": 10.0,
        "task_timeout": 120.0,
    }
    return Settings(**(defaults | overrides))


def _fleet(base_url: str, workdir: Path, settings: Settings, *, count: int) -> list[Runner]:
    return [
        Runner(
            name=f"runner-{index + 1}",
            base_url=base_url,
            workdir=workdir,
            settings=settings,
            executor=ScriptedExecutor(plan),
        )
        for index in range(count)
    ]


async def _cancel_once_running(client: httpx.AsyncClient, task_id: str) -> None:
    """Revoke a task from under a live session, which is what fences its runner."""
    await _eventually(
        lambda: _has_status(client, task_id, TaskStatus.RUNNING),
        what=f"{task_id} to start running",
    )
    response = await client.post(f"/tasks/{task_id}/cancel")
    assert response.status_code == 200, response.text


async def _await_tail(tail: asyncio.Task[None], tailed: list[dict[str, Any]]) -> None:
    try:
        await asyncio.wait_for(asyncio.shield(tail), timeout=RUN_TIMEOUT)
    except TimeoutError:
        seen = [event["type"] for event in tailed[-12:]]
        raise AssertionError(
            f"the fleet did not reach run_finished within {RUN_TIMEOUT}s; last events: {seen}"
        ) from None


# -- server, streams, polling ------------------------------------------------


@asynccontextmanager
async def _serving(settings: Settings) -> AsyncIterator[str]:
    config = uvicorn.Config(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_level="error",
        access_log=False,
    )
    server = uvicorn.Server(config)
    serving = asyncio.create_task(server.serve(), name="codefleet-test-server")
    while not server.started:
        if serving.done():
            await serving  # surfaces whatever stopped it from binding
        await asyncio.sleep(0.01)
    try:
        yield settings.base_url
    finally:
        server.should_exit = True
        await serving


def _free_port(host: str = "127.0.0.1") -> int:
    with socket.socket() as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


async def _tail(base_url: str, sink: list[dict[str, Any]], *, until: EventType) -> None:
    """One subscriber, opened before there is any work, kept until the run ends."""
    async with (
        httpx.AsyncClient(base_url=base_url, timeout=None) as client,
        aclosing(stream_events(client, since=0)) as stream,
    ):
        async for event in stream:
            sink.append(event)
            if event["type"] == until:
                return


async def _replay(base_url: str, *, until_id: int) -> list[dict[str, Any]]:
    """The same endpoint, after the fact, from the beginning of time."""
    collected: list[dict[str, Any]] = []

    async def read() -> None:
        async with (
            httpx.AsyncClient(base_url=base_url, timeout=None) as client,
            aclosing(stream_events(client, since=0)) as stream,
        ):
            async for event in stream:
                collected.append(event)
                if event["id"] >= until_id:
                    return

    await asyncio.wait_for(read(), timeout=30.0)
    return collected


async def _eventually[T](
    check: Callable[[], Awaitable[T | None]], *, what: str, timeout: float = 30.0
) -> T:
    """Poll until the fleet settles on something, or fail saying what was awaited."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = await check()
        if result:
            return result
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"waited {timeout}s for {what}")
        await asyncio.sleep(0.02)


async def _has_status(client: httpx.AsyncClient, task_id: str, status: TaskStatus) -> bool:
    response = await client.get(f"/tasks/{task_id}")
    assert response.status_code == 200, response.text
    return response.json()["task"]["status"] == status


async def _all_offline(client: httpx.AsyncClient) -> bool:
    agents = (await client.get("/agents")).json()["agents"]
    return bool(agents) and all(agent["status"] == "offline" for agent in agents)


# -- assignment --------------------------------------------------------------


def test_three_runners_take_three_tasks_at_once(run: RunRecord) -> None:
    """A tick produces as many assignments as there are idle agents, not one."""
    opening = select(run.events, type=EventType.TASK_ASSIGNED)[:3]
    assert len({event["task_id"] for event in opening}) == 3, opening
    assert len({event["agent_id"] for event in opening}) == 3, opening

    first_result = select(run.events, type=EventType.TASK_SUCCEEDED)[0]
    assert max(event["id"] for event in opening) < first_result["id"], (
        "the three opening tasks were not in flight together: one of them finished "
        "before the last was even assigned"
    )


def test_every_task_reached_a_terminal_state(run: RunRecord) -> None:
    assert {task_id: task["status"] for task_id, task in run.tasks.items()} == {
        HOLD: TaskStatus.SUCCEEDED,
        GRAB: TaskStatus.SUCCEEDED,
        SEED: TaskStatus.SUCCEEDED,
        CASCADE: TaskStatus.SUCCEEDED,
        CRASH: TaskStatus.FAILED,
        STRANDED: TaskStatus.BLOCKED_UPSTREAM,
        DOOMED: TaskStatus.CANCELLED,
    }


def test_the_run_drained_and_said_so(run: RunRecord) -> None:
    """`fleet_idle` is an edge, not a condition: once per drained batch."""
    assert len(select(run.events, type=EventType.FLEET_IDLE)) == 1
    finished = only(run.events, type=EventType.RUN_FINISHED)
    assert finished["payload"]["tasks"] == len(GRAPH)
    assert finished["payload"]["succeeded"] == 4
    assert not run.leases, f"leases outlived the run: {run.leases}"


# -- dependency cascade ------------------------------------------------------


def test_a_dependent_waits_for_its_dependency_and_then_runs_unasked(run: RunRecord) -> None:
    succeeded = only(run.events, type=EventType.TASK_SUCCEEDED, task_id=SEED)
    assignments = select(run.events, type=EventType.TASK_ASSIGNED, task_id=CASCADE)
    assert assignments, "the dependent was never scheduled at all"
    assert assignments[0]["id"] > succeeded["id"], (
        "the dependent was assigned before its dependency succeeded, which is the "
        "failure a derived runnability join exists to prevent"
    )

    unblocked = only(run.events, type=EventType.TASK_UNBLOCKED, task_id=CASCADE)
    assert unblocked["payload"]["unblocked_by"] == SEED
    assert succeeded["id"] < unblocked["id"] < assignments[0]["id"]


# -- the veto ----------------------------------------------------------------


def test_exactly_one_agent_gets_the_contested_file(run: RunRecord) -> None:
    """The mutual exclusion is a primary key, so a race cannot end in a tie."""
    grants = [
        event
        for event in select(run.events, type=EventType.LEASE_ACQUIRED)
        if event["payload"]["path"] == CONTESTED
    ]
    holder = only(run.events, type=EventType.LEASE_DENIED)
    assert grants[0]["task_id"] == HOLD
    assert holder["task_id"] == GRAB
    assert holder["payload"] == {
        "path": CONTESTED,
        "holder_agent_id": grants[0]["agent_id"],
        "holder_agent_name": _agent_name(run, grants[0]["agent_id"]),
        "holder_task_id": HOLD,
        "reason": "held",
    }
    # The write never landed, so the loser recorded no change against that path.
    changed = [
        event
        for event in select(run.events, type=EventType.FILE_CHANGED, task_id=GRAB)
        if event["payload"]["path"] == CONTESTED and event["id"] < holder["id"]
    ]
    assert not changed, "a vetoed write was still written to the ledger"


def test_the_vetoed_task_is_rescheduled_against_reality(run: RunRecord) -> None:
    """Spec 4.5 step 8: the denied path becomes a scheduling fact, not a coin flip."""
    grab = run.tasks[GRAB]
    assert CONTESTED in grab["file_scope"], (
        "the denied path was not folded into the task's scope, so the retry could be "
        "co-scheduled with the holder all over again"
    )
    assert grab["attempts"] >= 2
    assert grab["status"] == TaskStatus.SUCCEEDED

    requeued = only(run.events, type=EventType.TASK_REQUEUED, task_id=GRAB)
    assert requeued["payload"]["reason"] == "veto"

    released = next(
        event
        for event in select(run.events, type=EventType.LEASE_RELEASED, task_id=HOLD)
        if event["payload"]["path"] == CONTESTED
    )
    retry = select(run.events, type=EventType.TASK_ASSIGNED, task_id=GRAB)[1]
    assert requeued["id"] < released["id"] < retry["id"], (
        "the vetoed task was retried before the holder let go of the file"
    )


def test_a_conflict_is_a_projection_of_the_denial(run: RunRecord) -> None:
    assert len(run.conflicts) == 1
    conflict = run.conflicts[0]
    assert conflict["path"] == CONTESTED
    assert conflict["requester_task_id"] == GRAB
    assert conflict["holder_task_id"] == HOLD
    # Resolution is computed from the requester's current status, not stamped.
    assert conflict["resolved"] is True


# -- failure propagation -----------------------------------------------------


def test_attempts_exhaustion_fails_a_task_and_blocks_its_dependents(run: RunRecord) -> None:
    crash = run.tasks[CRASH]
    assert crash["attempts"] == 2
    assert crash["status"] == TaskStatus.FAILED
    assert crash["error_kind"] == "attempts_exhausted"

    blocked = only(run.events, type=EventType.TASK_BLOCKED_UPSTREAM, task_id=STRANDED)
    assert blocked["payload"]["failed_ancestor_id"] == CRASH
    assert run.tasks[STRANDED]["attempts"] == 0, (
        "a task blocked by a failed ancestor should never have been run at all"
    )


def test_cancelling_a_running_task_fences_its_runner(run: RunRecord) -> None:
    """One mechanism covers revocation: the epoch bump, seen on the next call."""
    cancelled = only(run.events, type=EventType.TASK_CANCELLED, task_id=DOOMED)
    holder = cancelled["agent_id"]
    assert holder is not None

    recovered = [
        event
        for event in select(run.events, type=EventType.AGENT_ONLINE)
        if event["agent_id"] == holder and event["id"] > cancelled["id"]
    ]
    assert recovered, (
        "the runner whose task was cancelled never re-registered, so it never "
        "discovered that its session belonged to nobody"
    )
    assert not [
        event
        for event in select(run.events, type=EventType.FILE_CHANGED, task_id=DOOMED)
        if event["id"] > cancelled["id"]
    ], "the fenced runner kept writing after its task was taken away"


# -- the record itself -------------------------------------------------------


def test_every_event_type_is_reachable(run: RunRecord) -> None:
    """Spec 3.8: an enum member nothing emits is vocabulary, not behavior."""
    seen = {event["type"] for event in run.events}
    missing = sorted(str(member) for member in EventType if member not in seen)
    assert not missing, f"never emitted in a full run: {', '.join(missing)}"


def test_the_replayed_stream_is_the_live_tail(run: RunRecord) -> None:
    """Replay and live tail are the same query, so they cannot disagree."""
    assert run.tailed, "the live subscriber saw nothing"
    assert run.tailed[-1]["type"] == EventType.RUN_FINISHED
    assert len(run.replayed) >= len(run.tailed)
    assert run.replayed[: len(run.tailed)] == run.tailed
    assert [event["id"] for event in run.tailed] == list(
        range(run.tailed[0]["id"], run.tailed[0]["id"] + len(run.tailed))
    ), "the stream skipped an event id, so a reconnecting client would lose one"


def _agent_name(run: RunRecord, agent_id: str) -> str:
    return next(agent["name"] for agent in run.agents if agent["id"] == agent_id)


# -- stale agents ------------------------------------------------------------


async def test_a_silent_agent_loses_its_leases_and_its_task(tmp_path: Path) -> None:
    """The recovery path, from the one angle a live `Runner` cannot reach.

    The agent that goes quiet here is a bare HTTP client rather than a `Runner`,
    because the failure being modeled is a process that died: it holds a task
    and a lease, and then simply stops calling. A `Runner` heartbeats from its own
    asyncio task and would keep doing so.
    """
    workdir = tmp_path / "workspace"
    workdir.mkdir()
    settings = _settings(tmp_path, workdir, stale_after=1.0, heartbeat_interval=0.15)
    path = "service/api.py"

    async with (
        _serving(settings) as base_url,
        httpx.AsyncClient(base_url=base_url, timeout=10.0) as client,
    ):
        zombie = await _register(client, "runner-zombie")
        created = await client.post(
            "/tasks",
            json={
                "tasks": [
                    {
                        "id": "T_ORPHAN",
                        "title": "Work its agent will not live to finish",
                        "description": "Held by an agent that stops answering.",
                        "file_scope": [path],
                    }
                ]
            },
        )
        assert created.status_code == 201, created.text

        assigned = await _eventually(
            lambda: _assignment(client, zombie), what="the zombie to be assigned work"
        )
        assert assigned["id"] == "T_ORPHAN"
        await _post(client, zombie, "/tasks/T_ORPHAN/start", {"agent_id": zombie["agent_id"]})
        decision = await _post(
            client,
            zombie,
            "/leases/acquire",
            {"agent_id": zombie["agent_id"], "task_id": "T_ORPHAN", "paths": [path]},
        )
        assert decision["decision"] == "allow"

        # From here the zombie says nothing at all.
        survivor = _fleet(base_url, workdir, settings, count=1)[0]
        survivor.name = "runner-survivor"
        loop = asyncio.create_task(survivor.run_forever(), name=survivor.name)
        try:
            await _eventually(
                lambda: _has_status(client, "T_ORPHAN", TaskStatus.SUCCEEDED),
                what="the requeued task to be finished by someone else",
            )
        finally:
            await survivor.shutdown()
            await loop

        events = (await client.get("/events", params={"since": 0, "limit": 5000})).json()["events"]
        task = (await client.get("/tasks/T_ORPHAN")).json()["task"]
        leases = (await client.get("/leases")).json()["leases"]

    offline = only(events, type=EventType.AGENT_OFFLINE, agent_id=zombie["agent_id"])
    assert offline["payload"]["reason"] == "stale"
    assert offline["payload"]["released_paths"] == [path]

    released = only(events, type=EventType.LEASE_RELEASED, agent_id=zombie["agent_id"])
    requeued = only(events, type=EventType.TASK_REQUEUED, agent_id=zombie["agent_id"])
    assert released["payload"] == {"path": path, "reason": "agent_stale"}
    assert [released["id"], offline["id"], requeued["id"]] == [
        released["id"],
        released["id"] + 1,
        released["id"] + 2,
    ], (
        "the lease release, the offline mark and the requeue are not contiguous in the "
        "log, so they did not happen in one transaction — and a window where the task is "
        "runnable while its file is still held is exactly the collision this prevents"
    )

    assert task["assigned_agent_id"] != zombie["agent_id"]
    assert not leases


async def _register(client: httpx.AsyncClient, name: str) -> dict[str, Any]:
    response = await client.post(
        "/agents/register", json={"name": name, "workdir": "/tmp/workspace", "pid": 4242}
    )
    assert response.status_code == 200, response.text
    return response.json()


async def _post(
    client: httpx.AsyncClient, agent: dict[str, Any], path: str, body: dict[str, Any]
) -> dict[str, Any]:
    response = await client.post(path, json=body, headers={"X-Agent-Epoch": str(agent["epoch"])})
    assert response.status_code == 200, response.text
    return response.json()


async def _assignment(client: httpx.AsyncClient, agent: dict[str, Any]) -> dict[str, Any] | None:
    response = await client.get(
        f"/agents/{agent['agent_id']}/assignment",
        headers={"X-Agent-Epoch": str(agent["epoch"])},
    )
    if response.status_code == 204:
        return None
    assert response.status_code == 200, response.text
    return response.json()["task"]


# -- successive runs against one server --------------------------------------


async def test_a_second_run_waits_for_its_own_run_finished(tmp_path: Path) -> None:
    """A server outlives the run that used it, and so does its event log.

    `codefleet run` learns that the fleet has drained by watching for
    `run_finished` on the SSE stream. Every `run_finished` the server ever
    emitted is still in that stream, so a second run has to say where it started
    — otherwise it stops on the *previous* run's record before its own fleet has
    done anything, and reports that run's vetoes as its own.

    The CLI helper is called directly rather than through `_run_fleet`, which
    reads back its summary over a synchronous client: that is correct against the
    separate server process `codefleet run` really talks to, and would deadlock
    against this one, which shares the test's event loop.
    """
    workdir = tmp_path / "workspace"
    workdir.mkdir()
    settings = _settings(tmp_path, workdir)

    async with (
        _serving(settings) as base_url,
        httpx.AsyncClient(base_url=base_url, timeout=10.0) as client,
    ):
        for batch in ("first", "second"):
            since = (await client.get("/state")).json()["last_event_id"]
            created = await client.post(
                "/tasks",
                json={
                    "tasks": [
                        {
                            "id": batch,
                            "title": f"the {batch} batch",
                            "description": "one scripted write, one runner",
                            "file_scope": [f"service/{batch}.py"],
                        }
                    ]
                },
            )
            assert created.status_code == 201, created.text

            runner = _fleet(base_url, workdir, settings, count=1)[0]
            loop = asyncio.create_task(runner.run_forever(), name=runner.name)
            try:
                await _await_run_finished(base_url, since, RUN_TIMEOUT)
            finally:
                await runner.shutdown()
                await asyncio.gather(loop, return_exceptions=True)

            assert await _has_status(client, batch, TaskStatus.SUCCEEDED), (
                f"the {batch} run reported itself finished while {batch} was still "
                "unfinished, so it stopped on an earlier run's run_finished"
            )
