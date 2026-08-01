"""Scheduler rules, one test per rule.

Every test here builds a `FleetState` literally and calls one pure function.
There is no database, no event loop, no fixture and no mock in this file, and
there is not supposed to be: if a scheduling rule ever needs one of those to be
exercised, the rule has leaked out of the scheduler.

`task()` and `agent()` below only fill in the fields the models require but the
rules do not care about (titles, workdirs), so each test shows exactly the state
its rule depends on and nothing else.
"""

from __future__ import annotations

import ast
from datetime import UTC, datetime, timedelta
from pathlib import Path

from codefleet.models import Agent, AgentStatus, ErrorKind, FleetState, Lease, Task, TaskStatus
from codefleet.scheduler import (
    Assign,
    BlockDownstream,
    EmitFleetIdle,
    FailTask,
    MarkAgentOffline,
    RequeueTask,
    backoff_delay,
    idle_agents,
    is_runnable,
    runnable_tasks,
    schedule,
)

NOW = datetime(2026, 7, 31, 18, 0, 0, tzinfo=UTC)
EARLIER = NOW - timedelta(minutes=5)


def task(
    task_id: str,
    *,
    status: TaskStatus = TaskStatus.PENDING,
    priority: int = 3,
    file_scope: tuple[str, ...] = (),
    created_at: datetime = EARLIER,
    attempts: int = 0,
    max_attempts: int = 3,
    backoff_until: datetime | None = None,
    assigned_agent_id: str | None = None,
) -> Task:
    return Task(
        id=task_id,
        title=task_id,
        description=f"do {task_id}",
        status=status,
        priority=priority,
        file_scope=file_scope,
        created_at=created_at,
        attempts=attempts,
        max_attempts=max_attempts,
        backoff_until=backoff_until,
        assigned_agent_id=assigned_agent_id,
    )


def agent(
    name: str,
    *,
    status: AgentStatus = AgentStatus.IDLE,
    last_heartbeat_at: datetime = NOW,
    last_assigned_at: datetime | None = None,
) -> Agent:
    return Agent(
        id=f"a_{name}",
        name=name,
        status=status,
        workdir="/tmp/demo-repo",
        last_heartbeat_at=last_heartbeat_at,
        last_assigned_at=last_assigned_at,
    )


def assignments(decisions: list[object]) -> list[Assign]:
    return [d for d in decisions if isinstance(d, Assign)]


# ---------------------------------------------------------------------------
# Assignment (spec 4.2)
# ---------------------------------------------------------------------------


def test_one_tick_assigns_as_many_pairs_as_there_are_idle_agents() -> None:
    state = FleetState(
        tasks=(task("T1"), task("T2"), task("T3"), task("T4")),
        agents=(agent("runner-1"), agent("runner-2"), agent("runner-3")),
    )

    assert assignments(schedule(state, NOW)) == [
        Assign("T1", "a_runner-1"),
        Assign("T2", "a_runner-2"),
        Assign("T3", "a_runner-3"),
    ]


def test_runnable_order_is_priority_then_created_at_then_id() -> None:
    state = FleetState(
        tasks=(
            task("T1", priority=1, created_at=EARLIER),
            task("T2", priority=5, created_at=NOW),
            task("T3", priority=5, created_at=EARLIER),
            # Same priority and same instant as T3: the id breaks the tie.
            task("T0", priority=5, created_at=EARLIER),
        ),
    )

    assert [t.id for t in runnable_tasks(state, NOW)] == ["T0", "T3", "T2", "T1"]


def test_scope_conflict_skips_the_task_and_keeps_scanning() -> None:
    """A contended high-priority task must not head-of-line-block the queue."""
    state = FleetState(
        tasks=(
            task("T1", status=TaskStatus.RUNNING, file_scope=("api.py",)),
            task("T2", priority=5, file_scope=("api.py",)),
            task("T3", priority=1, file_scope=("readme.md",)),
        ),
        agents=(agent("runner-1"),),
    )

    assert assignments(schedule(state, NOW)) == [Assign("T3", "a_runner-1")]


def test_scopes_assigned_earlier_in_the_same_tick_are_busy_too() -> None:
    state = FleetState(
        tasks=(
            task("T1", priority=5, file_scope=("api.py", "db.py")),
            task("T2", priority=4, file_scope=("db.py",)),
            task("T3", priority=3, file_scope=("cli.py",)),
        ),
        agents=(agent("runner-1"), agent("runner-2")),
    )

    assert assignments(schedule(state, NOW)) == [
        Assign("T1", "a_runner-1"),
        Assign("T3", "a_runner-2"),
    ]


def test_a_leased_path_is_busy_scope_even_with_no_task_declaring_it() -> None:
    """Leases are acquired lazily, so they cover files nobody declared."""
    state = FleetState(
        tasks=(
            task("T1", status=TaskStatus.RUNNING, file_scope=("middleware.py",)),
            task("T2", file_scope=("api.py",)),
        ),
        agents=(agent("runner-2"),),
        leases=(Lease(path="api.py", agent_id="a_runner-1", task_id="T1"),),
    )

    assert assignments(schedule(state, NOW)) == []


def test_a_task_with_no_declared_scope_never_conflicts() -> None:
    state = FleetState(
        tasks=(
            task("T1", status=TaskStatus.RUNNING, file_scope=("api.py",)),
            task("T2", file_scope=()),
        ),
        agents=(agent("runner-2"),),
        leases=(Lease(path="api.py", agent_id="a_runner-1", task_id="T1"),),
    )

    assert assignments(schedule(state, NOW)) == [Assign("T2", "a_runner-2")]


# ---------------------------------------------------------------------------
# Agent selection (spec 4.2 step 4)
# ---------------------------------------------------------------------------


def test_longest_idle_agent_is_chosen_first() -> None:
    state = FleetState(
        agents=(
            agent("runner-1", last_assigned_at=NOW - timedelta(seconds=5)),
            agent("runner-2", last_assigned_at=NOW - timedelta(minutes=9)),
            # Never assigned anything: idle for its whole life, so it goes first.
            agent("runner-3", last_assigned_at=None),
        ),
    )

    assert [a.name for a in idle_agents(state, NOW)] == [
        "runner-3",
        "runner-2",
        "runner-1",
    ]


def test_busy_and_stale_agents_are_not_assignable() -> None:
    state = FleetState(
        tasks=(task("T1"),),
        agents=(
            agent("runner-1", status=AgentStatus.BUSY),
            agent("runner-2", status=AgentStatus.OFFLINE),
            agent("runner-3", last_heartbeat_at=NOW - timedelta(seconds=21)),
        ),
        stale_after_s=20.0,
    )

    assert assignments(schedule(state, NOW)) == []


# ---------------------------------------------------------------------------
# Runnability (spec 4.1)
# ---------------------------------------------------------------------------


def test_a_task_whose_dependency_has_not_succeeded_is_not_runnable() -> None:
    state = FleetState(
        tasks=(task("T1", status=TaskStatus.RUNNING), task("T2")),
        dependencies=(("T2", "T1"),),
        agents=(agent("runner-1"),),
    )

    assert is_runnable(state, state.task_by_id("T2"), NOW) is False
    assert assignments(schedule(state, NOW)) == []


def test_a_task_becomes_runnable_once_every_dependency_has_succeeded() -> None:
    state = FleetState(
        tasks=(
            task("T1", status=TaskStatus.SUCCEEDED),
            task("T2", status=TaskStatus.SUCCEEDED),
            task("T3"),
        ),
        dependencies=(("T3", "T1"), ("T3", "T2")),
        agents=(agent("runner-1"),),
    )

    assert assignments(schedule(state, NOW)) == [Assign("T3", "a_runner-1")]


def test_a_task_whose_dependency_failed_is_never_runnable() -> None:
    state = FleetState(
        tasks=(task("T1", status=TaskStatus.FAILED), task("T2")),
        dependencies=(("T2", "T1"),),
        agents=(agent("runner-1"),),
    )

    assert is_runnable(state, state.task_by_id("T2"), NOW) is False


def test_backoff_in_the_future_suppresses_assignment() -> None:
    state = FleetState(
        tasks=(task("T1", attempts=1, backoff_until=NOW + timedelta(seconds=2)),),
        agents=(agent("runner-1"),),
    )

    assert assignments(schedule(state, NOW)) == []
    assert assignments(schedule(state, NOW + timedelta(seconds=2))) == [Assign("T1", "a_runner-1")]


# ---------------------------------------------------------------------------
# Failure propagation (spec 4.3)
# ---------------------------------------------------------------------------


def test_dependents_of_a_failed_task_are_blocked_upstream() -> None:
    state = FleetState(
        tasks=(task("T1", status=TaskStatus.FAILED), task("T2")),
        dependencies=(("T2", "T1"),),
    )

    assert BlockDownstream("T2", "T1") in schedule(state, NOW)


def test_blocking_is_transitive_and_names_the_original_ancestor() -> None:
    state = FleetState(
        tasks=(
            task("T1", status=TaskStatus.CANCELLED),
            task("T2"),
            task("T3"),
        ),
        dependencies=(("T2", "T1"), ("T3", "T2")),
    )

    decisions = schedule(state, NOW)

    assert BlockDownstream("T2", "T1") in decisions
    assert BlockDownstream("T3", "T1") in decisions


def test_an_already_blocked_task_is_not_reported_again() -> None:
    """Ticking is continuous, so a settled fact must not re-fire every 500ms."""
    state = FleetState(
        tasks=(
            task("T1", status=TaskStatus.FAILED),
            task("T2", status=TaskStatus.BLOCKED_UPSTREAM),
            task("T3"),
        ),
        dependencies=(("T2", "T1"), ("T3", "T2")),
    )

    decisions = schedule(state, NOW)

    assert [d for d in decisions if isinstance(d, BlockDownstream)] == [BlockDownstream("T3", "T1")]


def test_a_succeeded_dependent_of_a_failed_task_is_left_alone() -> None:
    state = FleetState(
        tasks=(task("T1", status=TaskStatus.FAILED), task("T2", status=TaskStatus.SUCCEEDED)),
        dependencies=(("T2", "T1"),),
    )

    assert [d for d in schedule(state, NOW) if isinstance(d, BlockDownstream)] == []


# ---------------------------------------------------------------------------
# Stale agents (spec 4.4)
# ---------------------------------------------------------------------------


def test_a_stale_agent_goes_offline_and_its_assigned_task_is_requeued() -> None:
    state = FleetState(
        tasks=(task("T1", status=TaskStatus.ASSIGNED, attempts=1, assigned_agent_id="a_runner-1"),),
        agents=(
            agent(
                "runner-1", status=AgentStatus.BUSY, last_heartbeat_at=NOW - timedelta(seconds=21)
            ),
        ),
        stale_after_s=20.0,
    )

    decisions = schedule(state, NOW)

    assert decisions[0] == MarkAgentOffline("a_runner-1")
    assert decisions[1] == RequeueTask("T1", "agent_stale", NOW + timedelta(seconds=2))


def test_a_stale_agents_running_task_is_requeued_too() -> None:
    """The reference only recovered `assigned`, which is a millisecond-wide window."""
    state = FleetState(
        tasks=(task("T1", status=TaskStatus.RUNNING, attempts=1, assigned_agent_id="a_runner-1"),),
        agents=(
            agent(
                "runner-1", status=AgentStatus.BUSY, last_heartbeat_at=NOW - timedelta(seconds=21)
            ),
        ),
        stale_after_s=20.0,
    )

    assert RequeueTask("T1", "agent_stale", NOW + timedelta(seconds=2)) in schedule(state, NOW)


def test_a_stale_agent_out_of_attempts_fails_its_task_instead_of_requeueing() -> None:
    state = FleetState(
        tasks=(
            task(
                "T1",
                status=TaskStatus.RUNNING,
                attempts=3,
                max_attempts=3,
                assigned_agent_id="a_runner-1",
            ),
        ),
        agents=(
            agent(
                "runner-1", status=AgentStatus.BUSY, last_heartbeat_at=NOW - timedelta(seconds=21)
            ),
        ),
        stale_after_s=20.0,
    )

    decisions = schedule(state, NOW)
    failures = [d for d in decisions if isinstance(d, FailTask)]

    assert [d for d in decisions if isinstance(d, RequeueTask)] == []
    assert len(failures) == 1
    assert failures[0].task_id == "T1"
    assert failures[0].error_kind is ErrorKind.ATTEMPTS_EXHAUSTED


def test_a_task_freed_by_a_stale_agent_releases_its_scope_in_the_same_tick() -> None:
    """The requeue and the assignment apply in one transaction, so the scope of
    the abandoned task is not busy any more."""
    state = FleetState(
        tasks=(
            task(
                "T1",
                status=TaskStatus.RUNNING,
                attempts=1,
                assigned_agent_id="a_runner-1",
                file_scope=("api.py",),
            ),
            task("T2", file_scope=("api.py",)),
        ),
        agents=(
            agent(
                "runner-1", status=AgentStatus.BUSY, last_heartbeat_at=NOW - timedelta(seconds=21)
            ),
            agent("runner-2"),
        ),
        leases=(Lease(path="api.py", agent_id="a_runner-1", task_id="T1"),),
        stale_after_s=20.0,
    )

    assert assignments(schedule(state, NOW)) == [Assign("T2", "a_runner-2")]


def test_an_agent_already_offline_is_not_marked_offline_again() -> None:
    state = FleetState(
        agents=(
            agent(
                "runner-1", status=AgentStatus.OFFLINE, last_heartbeat_at=NOW - timedelta(minutes=5)
            ),
        ),
        stale_after_s=20.0,
    )

    assert [d for d in schedule(state, NOW) if isinstance(d, MarkAgentOffline)] == []


# ---------------------------------------------------------------------------
# Attempt caps and backoff (spec 4.7)
# ---------------------------------------------------------------------------


def test_a_pending_task_at_its_attempt_cap_fails_rather_than_waiting_forever() -> None:
    state = FleetState(
        tasks=(task("T1", attempts=3, max_attempts=3),),
        agents=(agent("runner-1"),),
    )

    decisions = schedule(state, NOW)

    assert (
        FailTask("T1", ErrorKind.ATTEMPTS_EXHAUSTED, "no attempts left (3 of 3 attempts used)")
        in decisions
    )
    assert assignments(decisions) == []


def test_failing_a_task_at_its_cap_blocks_its_dependents_in_the_same_tick() -> None:
    state = FleetState(
        tasks=(task("T1", attempts=3, max_attempts=3), task("T2")),
        dependencies=(("T2", "T1"),),
    )

    assert BlockDownstream("T2", "T1") in schedule(state, NOW)


def test_backoff_is_linear_then_capped() -> None:
    assert backoff_delay(1) == timedelta(seconds=2)
    assert backoff_delay(3) == timedelta(seconds=6)
    assert backoff_delay(100) == timedelta(seconds=30)


# ---------------------------------------------------------------------------
# Fleet idle (spec 4.8)
# ---------------------------------------------------------------------------


def test_fleet_is_idle_when_every_task_reached_a_terminal_state() -> None:
    state = FleetState(
        tasks=(
            task("T1", status=TaskStatus.SUCCEEDED),
            task("T2", status=TaskStatus.FAILED),
            task("T3", status=TaskStatus.BLOCKED_UPSTREAM),
        ),
        agents=(agent("runner-1"),),
    )

    assert EmitFleetIdle() in schedule(state, NOW)


def test_fleet_is_not_idle_while_a_task_is_pending_or_running() -> None:
    pending = FleetState(tasks=(task("T1", status=TaskStatus.PENDING),))
    running = FleetState(tasks=(task("T1", status=TaskStatus.RUNNING),))

    assert EmitFleetIdle() not in schedule(pending, NOW)
    assert EmitFleetIdle() not in schedule(running, NOW)


def test_a_task_that_is_pending_but_unassignable_still_counts_as_work() -> None:
    """Waiting on a backoff is not idleness — the fleet has work coming."""
    state = FleetState(
        tasks=(task("T1", attempts=1, backoff_until=NOW + timedelta(seconds=30)),),
        agents=(agent("runner-1"),),
    )

    decisions = schedule(state, NOW)

    assert assignments(decisions) == []
    assert EmitFleetIdle() not in decisions


def test_the_last_task_failing_this_tick_makes_the_fleet_idle() -> None:
    state = FleetState(
        tasks=(task("T1", attempts=3, max_attempts=3), task("T2")),
        dependencies=(("T2", "T1"),),
    )

    decisions = schedule(state, NOW)

    assert isinstance(decisions[-1], EmitFleetIdle)


# ---------------------------------------------------------------------------
# Purity
# ---------------------------------------------------------------------------


def test_the_same_snapshot_produces_the_same_decisions() -> None:
    state = FleetState(
        tasks=(
            task("T1", priority=5, file_scope=("api.py",)),
            task("T2", priority=5, file_scope=("api.py",)),
            task("T3", status=TaskStatus.FAILED),
            task("T4"),
            task("T5", status=TaskStatus.ASSIGNED, attempts=1, assigned_agent_id="a_runner-3"),
        ),
        dependencies=(("T4", "T3"),),
        agents=(
            agent("runner-1"),
            agent("runner-2", last_assigned_at=NOW - timedelta(seconds=1)),
            agent(
                "runner-3", status=AgentStatus.BUSY, last_heartbeat_at=NOW - timedelta(seconds=60)
            ),
        ),
        stale_after_s=20.0,
    )

    assert schedule(state, NOW) == schedule(state, NOW)


def test_the_scheduler_imports_nothing_that_could_do_io() -> None:
    """The purity claim of section 6.3, enforced rather than asserted in prose."""
    source = Path(__file__).parents[2] / "src" / "codefleet" / "scheduler.py"
    tree = ast.parse(source.read_text())

    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    assert imported == {"__future__", "collections", "dataclasses", "datetime", "codefleet.models"}
    for forbidden in ("codefleet.store", "httpx", "asyncio", "fastapi", "aiosqlite"):
        assert forbidden not in imported
