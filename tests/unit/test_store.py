"""Store tests.

The lease tests are the point of this file. Exclusion is claimed to be a schema
constraint rather than application logic, so the tests race real concurrent
callers at one path and count rows, not calls.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from codefleet.models import (
    AgentStatus,
    ErrorKind,
    EventType,
    TaskStatus,
    utcnow,
)
from codefleet.store import GraphError, Store, TaskSpec


@pytest.fixture
async def store(tmp_path: Path) -> AsyncIterator[Store]:
    store = await Store.open(tmp_path / "codefleet.db")
    try:
        yield store
    finally:
        await store.close()


def spec(task_id: str | None = None, **overrides: object) -> TaskSpec:
    fields: dict[str, object] = {
        "id": task_id,
        "title": f"task {task_id}",
        "description": "do the thing",
    }
    fields.update(overrides)
    return TaskSpec(**fields)


async def make_agent(store: Store, name: str) -> str:
    agent = await store.register_agent(name, workdir="/tmp/demo-repo", pid=1234)
    return agent.id


# -- schema and round-tripping ---------------------------------------------


async def test_open_sets_wal_and_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "codefleet.db"
    store = await Store.open(path)
    await store.emit(EventType.FLEET_STARTED)
    # A write-ahead log beside the database is the observable form of WAL mode.
    assert path.with_name(path.name + "-wal").exists()
    await store.close()

    # Re-opening an existing database must not fight the existing schema.
    reopened = await Store.open(path)
    assert await reopened.list_tasks() == []
    await reopened.close()


async def test_task_round_trips_every_column(store: Store) -> None:
    [task_id] = await store.create_tasks(
        [spec("T1", priority=5, file_scope=["a.py", "b.py"], max_attempts=2)]
    )
    agent_id = await make_agent(store, "runner-1")
    deadline = utcnow() + timedelta(seconds=30)

    updated = await store.update_task(
        task_id,
        status=TaskStatus.RUNNING,
        assigned_agent_id=agent_id,
        attempts=1,
        backoff_until=deadline,
        result_summary="halfway",
        error="none yet",
        error_kind=ErrorKind.VETO,
        blocked_on_path="a.py",
        input_tokens=8421,
        output_tokens=613,
        cost_usd=0.0094,
        duration_ms=15612,
        session_id="sess_abc",
        assigned_at=deadline,
        started_at=deadline,
        completed_at=deadline,
    )

    fetched = await store.get_task(task_id)
    assert fetched == updated
    assert fetched.status is TaskStatus.RUNNING
    assert fetched.error_kind is ErrorKind.VETO
    assert fetched.file_scope == ("a.py", "b.py")
    assert fetched.priority == 5
    assert fetched.max_attempts == 2
    assert fetched.cost_usd == pytest.approx(0.0094)
    assert fetched.created_at.utcoffset() == timedelta(0)
    # Storage keeps milliseconds, and update_task returns what it wrote rather than
    # what it was handed, so the two agree exactly.
    assert fetched.backoff_until == updated.backoff_until
    assert deadline - fetched.backoff_until < timedelta(milliseconds=1)


async def test_agent_round_trips_every_column(store: Store) -> None:
    agent = await store.register_agent("runner-1", workdir="/tmp/demo-repo", pid=99)
    assigned_at = utcnow()
    updated = await store.update_agent(
        agent.id,
        status=AgentStatus.BUSY,
        current_task_id="T1",
        last_assigned_at=assigned_at,
        tasks_succeeded=2,
        tasks_failed=1,
        input_tokens=10,
        output_tokens=20,
        cost_usd=1.5,
    )
    fetched = await store.get_agent(agent.id)
    assert fetched == updated
    assert fetched.status is AgentStatus.BUSY
    assert fetched.pid == 99
    assert fetched.current_task_id == "T1"
    assert fetched.last_assigned_at == updated.last_assigned_at
    assert assigned_at - fetched.last_assigned_at < timedelta(milliseconds=1)
    assert fetched.registered_at.tzinfo is not None


async def test_get_task_and_agent_return_none_when_absent(store: Store) -> None:
    assert await store.get_task("nope") is None
    assert await store.get_agent("nope") is None


# -- task creation ----------------------------------------------------------


async def test_create_tasks_generates_ids_and_keeps_supplied_ones(store: Store) -> None:
    ids = await store.create_tasks([spec("T1"), spec()])
    assert ids[0] == "T1"
    assert ids[1].startswith("t_")
    assert len(await store.list_tasks()) == 2


async def test_a_spec_without_an_attempt_cap_inherits_the_fleets(store: Store) -> None:
    """The store holds no policy, so the fleet's cap arrives as an argument."""
    await store.create_tasks([spec("T1"), spec("T2", max_attempts=5)], default_max_attempts=1)
    assert (await store.get_task("T1")).max_attempts == 1
    assert (await store.get_task("T2")).max_attempts == 5


async def test_create_tasks_stores_edges_both_ways(store: Store) -> None:
    await store.create_tasks(
        [spec("T1"), spec("T2", depends_on=["T1"]), spec("T3", depends_on=["T1", "T2"])]
    )
    assert await store.dependencies_of("T3") == ["T1", "T2"]
    assert await store.dependents_of("T1") == ["T2", "T3"]
    assert await store.dependencies_of("T1") == []


async def test_create_tasks_can_depend_on_an_already_stored_task(store: Store) -> None:
    await store.create_tasks([spec("T1")])
    await store.create_tasks([spec("T2", depends_on=["T1"])])
    assert await store.dependencies_of("T2") == ["T1"]


async def test_create_tasks_rejects_a_cycle_and_inserts_nothing(store: Store) -> None:
    batch = [
        spec("T1", depends_on=["T3"]),
        spec("T2", depends_on=["T1"]),
        spec("T3", depends_on=["T2"]),
    ]
    with pytest.raises(GraphError, match="cycle"):
        await store.create_tasks(batch)
    assert await store.list_tasks() == []
    assert await store.events_since(0) == []


async def test_create_tasks_validates_the_batch_against_the_stored_graph(store: Store) -> None:
    await store.create_tasks([spec("T1"), spec("T2", depends_on=["T1"])])
    # Acyclic against the stored graph but cyclic among the new nodes: both halves
    # of the union have to be checked together.
    with pytest.raises(GraphError, match="cycle"):
        await store.create_tasks(
            [spec("T3", depends_on=["T2", "T4"]), spec("T4", depends_on=["T3"])]
        )
    assert {t.id for t in await store.list_tasks()} == {"T1", "T2"}
    assert await store.dependents_of("T2") == []


async def test_create_tasks_rejects_self_dependency(store: Store) -> None:
    with pytest.raises(GraphError, match="cycle"):
        await store.create_tasks([spec("T1", depends_on=["T1"])])
    assert await store.list_tasks() == []


async def test_create_tasks_rejects_a_dangling_dependency(store: Store) -> None:
    with pytest.raises(GraphError, match="unknown task ghost"):
        await store.create_tasks([spec("T1"), spec("T2", depends_on=["ghost"])])
    assert await store.list_tasks() == []


async def test_create_tasks_rejects_duplicate_ids(store: Store) -> None:
    with pytest.raises(GraphError, match="duplicate task id"):
        await store.create_tasks([spec("T1"), spec("T1")])
    await store.create_tasks([spec("T1")])
    with pytest.raises(GraphError, match="already exists"):
        await store.create_tasks([spec("T1")])
    assert len(await store.list_tasks()) == 1


async def test_create_tasks_emits_one_task_created_each(store: Store) -> None:
    await store.create_tasks([spec("T1"), spec("T2", depends_on=["T1"])])
    events = await store.events_since(0)
    assert [e.type for e in events] == [EventType.TASK_CREATED, EventType.TASK_CREATED]
    assert events[1].task_id == "T2"
    assert events[1].payload["depends_on"] == ["T1"]


# -- task listing and updates ----------------------------------------------


async def test_list_tasks_filters_orders_and_pages(store: Store) -> None:
    await store.create_tasks(
        [spec("low", priority=1), spec("high", priority=5), spec("mid", priority=3)]
    )
    await store.update_task("mid", status=TaskStatus.SUCCEEDED)

    assert [t.id for t in await store.list_tasks()] == ["high", "mid", "low"]
    assert [t.id for t in await store.list_tasks(status=TaskStatus.PENDING)] == ["high", "low"]
    assert [t.id for t in await store.list_tasks(limit=1)] == ["high"]
    assert [t.id for t in await store.list_tasks(limit=1, offset=2)] == ["low"]


async def test_update_task_stamps_updated_at(store: Store) -> None:
    [task_id] = await store.create_tasks([spec("T1")])
    before = await store.get_task(task_id)
    await asyncio.sleep(0.002)  # stored timestamps have millisecond resolution
    updated = await store.update_task(task_id, status=TaskStatus.ASSIGNED)
    assert updated.updated_at > before.updated_at
    assert updated.created_at == before.created_at


async def test_update_task_refuses_a_hand_written_updated_at(store: Store) -> None:
    await store.create_tasks([spec("T1")])
    with pytest.raises(ValueError, match="stamped by the store"):
        await store.update_task("T1", updated_at=utcnow())


async def test_update_task_rejects_unknown_fields_and_naive_datetimes(store: Store) -> None:
    await store.create_tasks([spec("T1")])
    with pytest.raises(ValueError, match="unknown task field"):
        await store.update_task("T1", blocked_by="T2")
    with pytest.raises(ValueError, match="naive datetime"):
        await store.update_task("T1", completed_at=datetime(2026, 7, 31, 18, 0, 0))
    with pytest.raises(KeyError):
        await store.update_task("ghost", status=TaskStatus.FAILED)


async def test_widen_file_scope_appends_once(store: Store) -> None:
    await store.create_tasks([spec("T4", file_scope=["linkstash/middleware.py"])])
    await store.widen_file_scope("T4", "linkstash/api.py")
    await store.widen_file_scope("T4", "linkstash/api.py")
    task = await store.get_task("T4")
    assert task.file_scope == ("linkstash/middleware.py", "linkstash/api.py")


# -- agents -----------------------------------------------------------------


async def test_register_agent_reuses_the_row_and_bumps_epoch(store: Store) -> None:
    first = await store.register_agent("runner-1", workdir="/tmp/a", pid=1)
    await store.update_agent(
        first.id, tasks_succeeded=3, status=AgentStatus.BUSY, current_task_id="T1"
    )

    second = await store.register_agent("runner-1", workdir="/tmp/b", pid=2)
    assert second.id == first.id
    assert second.epoch == first.epoch + 1
    assert second.tasks_succeeded == 3  # lifetime counters survive a restart
    assert second.status is AgentStatus.IDLE
    assert second.current_task_id is None
    assert second.workdir == "/tmp/b"
    assert second.registered_at == first.registered_at
    assert len(await store.list_agents()) == 1


async def test_bump_epoch_and_heartbeat(store: Store) -> None:
    agent = await store.register_agent("runner-1", workdir="/tmp/a", pid=1)
    assert await store.bump_epoch(agent.id) == agent.epoch + 1

    await asyncio.sleep(0.002)  # stored timestamps have millisecond resolution
    beaten = await store.heartbeat(agent.id)
    assert beaten.last_heartbeat_at > agent.last_heartbeat_at
    assert beaten.epoch == agent.epoch + 1


async def test_heartbeat_emits_nothing(store: Store) -> None:
    agent = await store.register_agent("runner-1", workdir="/tmp/a", pid=1)
    await store.heartbeat(agent.id)
    assert await store.events_since(0) == []


# -- leases -----------------------------------------------------------------


async def test_acquire_grants_and_emits(store: Store) -> None:
    agent_id = await make_agent(store, "runner-1")
    decision = await store.acquire_leases(agent_id=agent_id, task_id="T3", paths=["a.py", "b.py"])
    assert decision.allowed
    assert decision.granted == ["a.py", "b.py"]
    assert [lease.path for lease in await store.list_leases()] == ["a.py", "b.py"]

    acquired = await store.events_since(0, types=[EventType.LEASE_ACQUIRED])
    assert [e.payload["path"] for e in acquired] == ["a.py", "b.py"]
    assert acquired[0].task_id == "T3"


async def test_reacquiring_your_own_lease_is_an_allow(store: Store) -> None:
    agent_id = await make_agent(store, "runner-1")
    await store.acquire_leases(agent_id=agent_id, task_id="T3", paths=["a.py"])
    again = await store.acquire_leases(agent_id=agent_id, task_id="T3", paths=["a.py", "c.py"])

    assert again.allowed
    assert again.granted == ["a.py", "c.py"]
    assert len(await store.list_leases()) == 2
    # Re-acquisition is not a state change, so only the new path emits.
    acquired = await store.events_since(0, types=[EventType.LEASE_ACQUIRED])
    assert [e.payload["path"] for e in acquired] == ["a.py", "c.py"]


async def test_denial_names_the_holder(store: Store) -> None:
    holder = await make_agent(store, "runner-2")
    other = await make_agent(store, "runner-1")
    await store.acquire_leases(agent_id=holder, task_id="T3", paths=["linkstash/api.py"])

    decision = await store.acquire_leases(agent_id=other, task_id="T4", paths=["linkstash/api.py"])
    assert not decision.allowed
    assert decision.granted == []
    [denied] = decision.denied
    assert denied.path == "linkstash/api.py"
    assert denied.holder_agent_id == holder
    assert denied.holder_agent_name == "runner-2"
    assert denied.holder_task_id == "T3"
    assert denied.reason == "held"

    [event] = await store.events_since(0, types=[EventType.LEASE_DENIED])
    assert event.task_id == "T4"
    assert event.agent_id == other
    assert event.payload["holder_agent_name"] == "runner-2"


async def test_acquire_is_all_or_nothing(store: Store) -> None:
    holder = await make_agent(store, "runner-2")
    other = await make_agent(store, "runner-1")
    await store.acquire_leases(agent_id=holder, task_id="T3", paths=["b.py"])

    decision = await store.acquire_leases(
        agent_id=other, task_id="T4", paths=["a.py", "b.py", "c.py"]
    )
    assert not decision.allowed
    assert [d.path for d in decision.denied] == ["b.py"]
    # a.py and c.py were free; taking them would be hold-and-wait.
    assert [lease.path for lease in await store.list_leases()] == ["b.py"]
    acquired = await store.events_since(0, types=[EventType.LEASE_ACQUIRED])
    assert [e.payload["path"] for e in acquired] == ["b.py"]


async def test_concurrent_acquirers_produce_exactly_one_grant(store: Store) -> None:
    contenders = 8
    agents = [await make_agent(store, f"runner-{i}") for i in range(contenders)]
    decisions = await asyncio.gather(
        *(
            store.acquire_leases(agent_id=agent_id, task_id=f"T{i}", paths=["linkstash/api.py"])
            for i, agent_id in enumerate(agents)
        )
    )

    assert sum(d.allowed for d in decisions) == 1
    assert sum(not d.allowed for d in decisions) == contenders - 1
    assert len(await store.list_leases()) == 1

    acquired = await store.events_since(0, types=[EventType.LEASE_ACQUIRED])
    denied = await store.events_since(0, types=[EventType.LEASE_DENIED])
    assert len(acquired) == 1
    assert len(denied) == contenders - 1
    winner = acquired[0].task_id
    assert {e.payload["holder_task_id"] for e in denied} == {winner}


async def test_exclusion_holds_across_connections(tmp_path: Path) -> None:
    """The lock in `Store` is not what makes this safe — the primary key is.

    Two connections to the same file, so nothing in one process is serializing
    them: `ON CONFLICT(path) DO NOTHING` still yields exactly one holder.
    """
    path = tmp_path / "codefleet.db"
    one = await Store.open(path)
    two = await Store.open(path)
    try:
        first, second = await asyncio.gather(
            one.acquire_leases(agent_id="a_one", task_id="T1", paths=["linkstash/api.py"]),
            two.acquire_leases(agent_id="a_two", task_id="T2", paths=["linkstash/api.py"]),
        )
        assert [first.allowed, second.allowed].count(True) == 1
        assert len(await one.list_leases()) == 1
    finally:
        await one.close()
        await two.close()


async def test_release_by_task(store: Store) -> None:
    agent_id = await make_agent(store, "runner-1")
    await store.acquire_leases(agent_id=agent_id, task_id="T3", paths=["a.py", "b.py"])
    await store.acquire_leases(agent_id=agent_id, task_id="T9", paths=["z.py"])

    released = await store.release_leases_for_task("T3", reason="task_succeeded")
    assert released == ["a.py", "b.py"]
    assert [lease.path for lease in await store.list_leases()] == ["z.py"]

    events = await store.events_since(0, types=[EventType.LEASE_RELEASED])
    assert [e.payload["reason"] for e in events] == ["task_succeeded"] * 2
    assert {e.task_id for e in events} == {"T3"}
    assert await store.release_leases_for_task("T3", reason="task_succeeded") == []


async def test_release_by_agent(store: Store) -> None:
    stale = await make_agent(store, "runner-2")
    healthy = await make_agent(store, "runner-1")
    await store.acquire_leases(agent_id=stale, task_id="T3", paths=["a.py", "b.py"])
    await store.acquire_leases(agent_id=healthy, task_id="T4", paths=["c.py"])

    released = await store.release_leases_for_agent(stale, reason="agent_stale")
    assert released == ["a.py", "b.py"]
    assert [lease.path for lease in await store.list_leases()] == ["c.py"]

    # The whole point: the path the dead agent held is now acquirable.
    decision = await store.acquire_leases(agent_id=healthy, task_id="T4", paths=["a.py"])
    assert decision.allowed


async def test_a_lease_released_in_a_failing_transaction_stays_held(store: Store) -> None:
    agent_id = await make_agent(store, "runner-1")
    await store.acquire_leases(agent_id=agent_id, task_id="T3", paths=["a.py"])

    with pytest.raises(RuntimeError):
        async with store.transaction():
            await store.release_leases_for_task("T3", reason="task_succeeded")
            raise RuntimeError("the caller's own step failed")

    assert [lease.path for lease in await store.list_leases()] == ["a.py"]
    assert await store.events_since(0, types=[EventType.LEASE_RELEASED]) == []


# -- ledgers ----------------------------------------------------------------


async def test_record_change_writes_ledger_and_event(store: Store) -> None:
    agent_id = await make_agent(store, "runner-1")
    await store.record_change(
        agent_id=agent_id, task_id="T4", path="linkstash/middleware.py", tool="Write"
    )
    await store.record_change(agent_id=agent_id, task_id="T5", path="README.md", tool="Edit")

    everything = await store.list_changes()
    assert [c.path for c in everything] == ["linkstash/middleware.py", "README.md"]
    assert everything[0].tool == "Write"
    assert everything[0].at.tzinfo is not None
    assert [c.path for c in await store.list_changes("T5")] == ["README.md"]

    changed = await store.events_since(0, types=[EventType.FILE_CHANGED])
    assert [e.payload["tool"] for e in changed] == ["Write", "Edit"]


async def test_emit_rejects_an_unknown_type(store: Store) -> None:
    with pytest.raises(ValueError, match="not a valid EventType"):
        await store.emit("heartbeat")


async def test_events_have_monotonic_ids_and_page(store: Store) -> None:
    for i in range(10):
        await store.emit(EventType.FLEET_IDLE, index=i)

    ids = [e.id for e in await store.events_since(0)]
    assert ids == sorted(ids)
    assert len(set(ids)) == 10
    assert await store.last_event_id() == ids[-1]

    first_page = await store.events_since(0, limit=4)
    assert [e.payload["index"] for e in first_page] == [0, 1, 2, 3]
    second_page = await store.events_since(first_page[-1].id, limit=4)
    assert [e.payload["index"] for e in second_page] == [4, 5, 6, 7]
    tail = await store.events_since(second_page[-1].id)
    assert [e.payload["index"] for e in tail] == [8, 9]
    assert await store.events_since(await store.last_event_id()) == []


async def test_events_filter_by_type(store: Store) -> None:
    await store.emit(EventType.FLEET_STARTED)
    await store.emit(EventType.TASK_CREATED, task_id="T1")
    await store.emit(EventType.FLEET_IDLE)

    picked = await store.events_since(0, types=[EventType.FLEET_STARTED, EventType.FLEET_IDLE])
    assert [e.type for e in picked] == [EventType.FLEET_STARTED, EventType.FLEET_IDLE]


async def test_emitted_event_round_trips(store: Store) -> None:
    agent_id = await make_agent(store, "runner-1")
    emitted = await store.emit(
        EventType.LEASE_DENIED, task_id="T4", agent_id=agent_id, path="a.py", holder="runner-2"
    )
    [stored] = await store.events_since(0)
    assert stored == emitted
    assert stored.at.tzinfo is not None
    assert stored.payload == {"path": "a.py", "holder": "runner-2"}


async def test_last_event_id_is_zero_on_an_empty_database(store: Store) -> None:
    assert await store.last_event_id() == 0


# -- snapshot, reset --------------------------------------------------------


async def test_fleet_state_snapshots_everything(store: Store) -> None:
    await store.create_tasks([spec("T1"), spec("T2", depends_on=["T1"])])
    agent_id = await make_agent(store, "runner-1")
    await store.update_task("T1", status=TaskStatus.RUNNING, assigned_agent_id=agent_id)
    await store.acquire_leases(agent_id=agent_id, task_id="T1", paths=["a.py"])

    state = await store.fleet_state(stale_after_s=20.0)
    assert state.stale_after_s == 20.0
    assert {t.id for t in state.tasks} == {"T1", "T2"}
    assert state.dependencies == (("T2", "T1"),)
    assert state.dependents_of("T1") == ("T2",)
    assert [a.id for a in state.agents] == [agent_id]
    assert [lease.path for lease in state.leases] == ["a.py"]
    assert state.task_by_id("T1").status is TaskStatus.RUNNING


async def test_reset_truncates_everything_including_event_ids(store: Store) -> None:
    await store.create_tasks([spec("T1"), spec("T2", depends_on=["T1"])])
    agent_id = await make_agent(store, "runner-1")
    await store.acquire_leases(agent_id=agent_id, task_id="T1", paths=["a.py"])
    await store.record_change(agent_id=agent_id, task_id="T1", path="a.py", tool="Write")

    await store.reset()

    assert await store.list_tasks() == []
    assert await store.list_agents() == []
    assert await store.list_leases() == []
    assert await store.list_changes() == []
    assert await store.events_since(0) == []
    assert await store.last_event_id() == 0
    assert await store.dependents_of("T1") == []

    await store.create_tasks([spec("T1")])
    assert (await store.events_since(0))[0].id == 1
