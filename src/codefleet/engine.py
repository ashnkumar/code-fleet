"""The coordination state machine: the tick loop, and every transition it makes.

`codefleet.scheduler` decides and `codefleet.server` speaks HTTP; this is what
sits between them. Nothing here imports a web framework, which is the point of
the seam — the rules below are about rows and events, not about requests.

Three things are worth knowing before reading on:

* **The tick loop is the heart.** It wakes on an `asyncio.Event` set by any write
  that could create readiness, or on `tick_interval` as a reconciliation sweep.
  Each pass snapshots the fleet, calls the pure `schedule()`, and applies the
  returned decisions **in order, inside one transaction**. The order is the
  scheduler's contract: leases released by step 1 are what make step 5's
  assignments legal.
* **`epoch` is the only revocation mechanism.** Anything that takes a task away
  from an agent — a stale requeue, an operator cancel, a deadline sweep — bumps
  that agent's epoch, so the agent's next call returns `409 stale_epoch` and it
  re-registers. There is no second "your task was cancelled" flag to keep in
  sync with the first.
* **A snapshot is not the present.** The scheduler decides from a snapshot taken
  in an earlier transaction, so anything applied from it is re-checked against
  the rows as they are now: a task that completed in between is not resurrected,
  and a batch that arrived in between does not get declared idle.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from codefleet.config import Settings
from codefleet.models import (
    AgentStatus,
    ErrorKind,
    EventType,
    FleetState,
    Task,
    TaskStatus,
    isoformat,
    utcnow,
)
from codefleet.scheduler import (
    Assign,
    BlockDownstream,
    Decision,
    EmitFleetIdle,
    FailTask,
    MarkAgentOffline,
    RequeueTask,
    backoff_delay,
    schedule,
)
from codefleet.store import Store

logger = logging.getLogger("codefleet.engine")

# Reasons that end up in `lease_released` / `task_requeued` payloads. They are the
# operator's explanation of why work moved, so they are named once here.
REQUEUE_REREGISTERED = "agent_reregistered"
REQUEUE_DEREGISTERED = "agent_deregistered"
REQUEUE_DEADLINE = "deadline_exceeded"
RELEASE_TASK_TERMINAL = "task_terminal"
RELEASE_TASK_REQUEUED = "task_requeued"

# Fleet-sized data: a few hundred tasks and a few thousand events. This cap
# exists so a runaway query is bounded, not because paging is expected.
SCAN_LIMIT = 10_000


@dataclass(slots=True)
class ServerState:
    """Everything the process owns that is not in SQLite.

    The three in-memory fields are all edge detectors over durable state, not
    state of their own: which one-shot fleet events have already been emitted for
    the current batch of work, and which completion reports have already been
    applied.
    """

    settings: Settings
    store: Store
    wake: asyncio.Event
    started_at: datetime
    fleet_idle_emitted: bool = False
    run_finished_emitted: bool = False
    # Idempotence for `POST /tasks/{id}/complete`, keyed (task, agent, attempt).
    # The window this closes is a runner retrying a report it already delivered,
    # which is seconds wide and inside one server process — so an in-memory memo
    # is the honest size of the mechanism. What a restart forgets, the durable
    # state still knows: a duplicate for an attempt that has since gone terminal
    # or been requeued is refused from the row itself, not from this dict.
    completions: dict[tuple[str, str, int], dict[str, Any]] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# The tick loop
# ---------------------------------------------------------------------------


async def tick(server: ServerState) -> None:
    """One reconciliation pass: sweep deadlines, schedule, apply.

    The deadline sweep runs first and in its own transaction so the scheduler
    sees the tasks it freed. Everything the scheduler decides is then applied in
    one transaction, in the order it returned — a partial or reordered
    application would co-schedule tasks that must not be co-scheduled.
    """
    now = utcnow()
    state = await server.store.fleet_state(stale_after_s=server.settings.stale_after)
    if await _sweep_deadlines(server, state, now):
        state = await server.store.fleet_state(stale_after_s=server.settings.stale_after)
    await apply_decisions(server, state, schedule(state, now), now)


async def tick_loop(server: ServerState) -> None:
    """Fast reactive path, slow safety net (spec 4.2).

    The wake event is cleared *before* the pass so that a write landing mid-tick
    schedules another one rather than being swallowed by it.
    """
    while True:
        with suppress(TimeoutError):
            await asyncio.wait_for(server.wake.wait(), timeout=server.settings.tick_interval)
        server.wake.clear()
        try:
            await tick(server)
        except asyncio.CancelledError:
            raise
        except Exception:
            # A failed pass must not take the fleet down with it: the next sweep
            # re-derives everything from the database. Loudly logged, never hidden.
            logger.exception("tick failed")


async def _sweep_deadlines(server: ServerState, state: FleetState, now: datetime) -> bool:
    """Spec 4.7: `assigned_at + task_timeout + grace` requeues even if nobody reports.

    Heartbeat staleness catches a runner that died. This catches one that is
    wedged — still heartbeating, no longer making progress — which is a different
    failure with a different clock.
    """
    limit = timedelta(seconds=server.settings.task_timeout + server.settings.lease_grace_s)
    overdue = [
        task
        for task in state.tasks
        if task.status.is_active and task.assigned_at is not None and now - task.assigned_at > limit
    ]
    if not overdue:
        return False

    store = server.store
    async with store.transaction():
        for stale in overdue:
            task = await store.get_task(stale.id)
            if task is None or not task.status.is_active:
                continue
            holder_id = task.assigned_agent_id
            if holder_id is not None:
                await fence_agent(store, holder_id, reason=REQUEUE_DEADLINE)
            await requeue(
                store,
                task,
                reason=REQUEUE_DEADLINE,
                backoff_until=now + backoff_delay(task.attempts),
            )
    server.wake.set()
    return True


async def apply_decisions(
    server: ServerState, state: FleetState, decisions: list[Decision], now: datetime
) -> None:
    """Turn decisions into rows and events, in order, in one transaction."""
    if _has_live_work(state):
        # A new batch of work reopens the run, so the one-shot fleet events fire
        # again when it drains.
        server.fleet_idle_emitted = False
        server.run_finished_emitted = False

    if not decisions:
        return

    store = server.store
    async with store.transaction():
        for decision in decisions:
            match decision:
                case MarkAgentOffline(agent_id=agent_id):
                    await _apply_mark_offline(store, agent_id, now)
                case RequeueTask(task_id=task_id, reason=reason, backoff_until=backoff_until):
                    task = await store.get_task(task_id)
                    if task is not None and task.status.is_active:
                        await requeue(store, task, reason=reason, backoff_until=backoff_until)
                case FailTask(task_id=task_id, error_kind=error_kind, error=error):
                    await _apply_fail(store, task_id, error_kind, error, now)
                case BlockDownstream(task_id=task_id, failed_ancestor_id=ancestor_id):
                    await _apply_block(store, task_id, ancestor_id)
                case Assign(task_id=task_id, agent_id=agent_id):
                    await _apply_assign(server, task_id, agent_id, now)
                case EmitFleetIdle():
                    await _apply_fleet_idle(server, state)
                case _:
                    # A decision the scheduler emits and this loop ignores is the
                    # worst failure mode here, because the fleet keeps running and
                    # simply never does the thing. Adding a `Decision` member with
                    # no arm now fails the tick instead of passing silently.
                    raise TypeError(f"no apply step for decision {decision!r}")
        await _maybe_finish_run(server)


async def _apply_mark_offline(store: Store, agent_id: str, now: datetime) -> None:
    agent = await store.get_agent(agent_id)
    if agent is None or agent.status is AgentStatus.OFFLINE:
        return
    # The epoch bump is the fence: whatever this agent does next gets a 409.
    await store.update_agent(
        agent_id, status=AgentStatus.OFFLINE, current_task_id=None, epoch=agent.epoch + 1
    )
    released = await store.release_leases_for_agent(agent_id, "agent_stale")
    await store.emit(
        EventType.AGENT_OFFLINE,
        agent_id=agent_id,
        name=agent.name,
        reason="stale",
        last_heartbeat_at=isoformat(agent.last_heartbeat_at),
        silent_for_s=round((now - agent.last_heartbeat_at).total_seconds(), 3),
        released_paths=released,
    )


async def _apply_fail(
    store: Store, task_id: str, error_kind: ErrorKind, error: str, now: datetime
) -> None:
    task = await store.get_task(task_id)
    if task is None or task.status.is_terminal:
        return
    await store.release_leases_for_task(task_id, RELEASE_TASK_TERMINAL)
    await store.update_task(
        task_id,
        status=TaskStatus.FAILED,
        error=error,
        error_kind=error_kind,
        completed_at=now,
    )
    if task.assigned_agent_id is not None:
        await free_agent(store, task.assigned_agent_id, task_id)
    await store.emit(
        EventType.TASK_FAILED,
        task_id=task_id,
        agent_id=task.assigned_agent_id,
        attempt=task.attempts,
        error=error,
        error_kind=error_kind,
    )


async def _apply_block(store: Store, task_id: str, ancestor_id: str) -> None:
    task = await store.get_task(task_id)
    if task is None or task.status.is_terminal:
        return
    await store.update_task(
        task_id,
        status=TaskStatus.BLOCKED_UPSTREAM,
        error=f"dependency {ancestor_id} did not succeed",
    )
    await store.emit(
        EventType.TASK_BLOCKED_UPSTREAM, task_id=task_id, failed_ancestor_id=ancestor_id
    )


async def _apply_assign(server: ServerState, task_id: str, agent_id: str, now: datetime) -> None:
    """Assignment is the claim (spec D4): status, owner, attempt and agent all move here.

    The snapshot the scheduler decided from was read a moment ago, so both rows
    are re-checked against the transaction — a task that completed in between is
    skipped rather than resurrected.
    """
    store = server.store
    task = await store.get_task(task_id)
    agent = await store.get_agent(agent_id)
    if task is None or task.status is not TaskStatus.PENDING:
        return
    if agent is None or agent.status is not AgentStatus.IDLE:
        return
    task = await store.update_task(
        task_id,
        status=TaskStatus.ASSIGNED,
        assigned_agent_id=agent_id,
        assigned_at=now,
        attempts=task.attempts + 1,
        backoff_until=None,
    )
    await store.update_agent(
        agent_id, status=AgentStatus.BUSY, current_task_id=task_id, last_assigned_at=now
    )
    await store.emit(
        EventType.TASK_ASSIGNED,
        task_id=task_id,
        agent_id=agent_id,
        agent_name=agent.name,
        attempt=task.attempts,
        deadline=isoformat(task_deadline(task, server.settings)),
        file_scope=list(task.file_scope),
    )


async def _apply_fleet_idle(server: ServerState, state: FleetState) -> None:
    """`fleet_idle` is an edge, not a condition.

    The scheduler reports the condition on every idle tick, which is the right
    shape for a pure function; turning that into the one event of spec 4.8 is the
    server's job, and an empty database is not an idle fleet — it is a fleet with
    nothing to do yet.

    The condition is re-derived here rather than read off `state`: the snapshot
    was taken in an earlier transaction, and a `POST /tasks` that commits in the
    gap would otherwise be declared idle from a snapshot that never saw it.
    """
    if server.fleet_idle_emitted or not state.tasks:
        return
    current = await server.store.fleet_state(stale_after_s=server.settings.stale_after)
    if _has_live_work(current):
        return
    server.fleet_idle_emitted = True
    await server.store.emit(EventType.FLEET_IDLE, **fleet_counters(current))


async def _maybe_finish_run(server: ServerState) -> None:
    if not server.fleet_idle_emitted or server.run_finished_emitted:
        return
    agents = await server.store.list_agents()
    if any(agent.status is AgentStatus.BUSY for agent in agents):
        return
    tasks = await server.store.list_tasks(limit=SCAN_LIMIT)
    if any(_is_live(task) for task in tasks):
        # Work that arrived after this tick's snapshot. The run is not over; the
        # next tick reopens it and this fires when *that* batch drains.
        return
    server.run_finished_emitted = True
    await server.store.emit(
        EventType.RUN_FINISHED,
        tasks=len(tasks),
        succeeded=sum(1 for task in tasks if task.status is TaskStatus.SUCCEEDED),
        failed=sum(1 for task in tasks if task.status is TaskStatus.FAILED),
        blocked_upstream=sum(1 for task in tasks if task.status is TaskStatus.BLOCKED_UPSTREAM),
        cancelled=sum(1 for task in tasks if task.status is TaskStatus.CANCELLED),
        cost_usd=round(sum(task.cost_usd for task in tasks), 6),
        input_tokens=sum(task.input_tokens for task in tasks),
        output_tokens=sum(task.output_tokens for task in tasks),
    )


# ---------------------------------------------------------------------------
# Transitions the tick loop and the HTTP surface share
# ---------------------------------------------------------------------------


async def requeue(store: Store, task: Task, *, reason: str, backoff_until: datetime) -> None:
    """Back to `pending`, leases released in the same transaction that moves it.

    `attempts` is not touched: it counts transitions into `assigned`, so a lost
    assignment already cost an attempt and a requeue must not charge a second.
    """
    await store.release_leases_for_task(task.id, RELEASE_TASK_REQUEUED)
    await store.update_task(
        task.id,
        status=TaskStatus.PENDING,
        assigned_agent_id=None,
        assigned_at=None,
        started_at=None,
        backoff_until=backoff_until,
    )
    await store.emit(
        EventType.TASK_REQUEUED,
        task_id=task.id,
        agent_id=task.assigned_agent_id,
        reason=reason,
        attempts=task.attempts,
        backoff_until=isoformat(backoff_until),
    )


async def reclaim(store: Store, agent_id: str, *, reason: str) -> None:
    """Take back everything an agent was holding: leases first, then its work."""
    await store.release_leases_for_agent(agent_id, reason)
    for task in await _active_tasks_of(store, agent_id):
        await requeue(
            store, task, reason=reason, backoff_until=utcnow() + backoff_delay(task.attempts)
        )


async def free_agent(store: Store, agent_id: str, task_id: str) -> None:
    """Return an agent to `idle`, but only if it is still holding this task.

    Only for the paths where the runner is the one reporting: it has finished with
    the task and is about to poll for the next one. Anything that takes a task
    *away* from a runner uses `fence_agent` instead — the runner does not know
    yet, so it is not idle.
    """
    agent = await store.get_agent(agent_id)
    if agent is None or agent.current_task_id != task_id:
        return
    await store.update_agent(agent_id, status=AgentStatus.IDLE, current_task_id=None)


async def fence_agent(store: Store, agent_id: str, *, reason: str) -> None:
    """Revoke an incarnation: bump the epoch, then park the agent (spec 4.4 step 1).

    The epoch bump is the only revocation mechanism, and it is asynchronous by
    nature — the runner is inside a session and learns about it on whichever call
    it makes next, up to a heartbeat interval later. `offline` is what carries
    that fact into scheduling: an agent that is fenced but `idle` is one the very
    next tick can assign to, which spends an innocent task's attempt on a runner
    that will 409 and abandon it (and spec 4.7 never refunds an attempt). It comes
    back through re-registration, the one event that proves the runner stopped.
    """
    agent = await store.get_agent(agent_id)
    if agent is None:
        return
    await store.bump_epoch(agent_id)
    await store.update_agent(agent_id, status=AgentStatus.OFFLINE, current_task_id=None)
    await store.emit(EventType.AGENT_OFFLINE, agent_id=agent_id, name=agent.name, reason=reason)


async def _active_tasks_of(store: Store, agent_id: str) -> list[Task]:
    tasks: list[Task] = []
    for status in (TaskStatus.ASSIGNED, TaskStatus.RUNNING):
        tasks += [
            task
            for task in await store.list_tasks(status=status, limit=SCAN_LIMIT)
            if task.assigned_agent_id == agent_id
        ]
    return tasks


# ---------------------------------------------------------------------------
# Derived values both layers read
# ---------------------------------------------------------------------------


def _is_live(task: Task) -> bool:
    """Queued or in flight — the scheduler's own definition of work remaining."""
    return task.status is TaskStatus.PENDING or task.status.is_active


def _has_live_work(state: FleetState) -> bool:
    return any(_is_live(task) for task in state.tasks)


def task_deadline(task: Task, settings: Settings) -> datetime:
    started = task.assigned_at or utcnow()
    return started + timedelta(seconds=settings.task_timeout)


def fleet_counters(state: FleetState) -> dict[str, Any]:
    tasks_by_status = {status.value: 0 for status in TaskStatus}
    for task in state.tasks:
        tasks_by_status[task.status.value] += 1
    agents_by_status = {status.value: 0 for status in AgentStatus}
    for agent in state.agents:
        agents_by_status[agent.status.value] += 1
    return {
        "tasks": tasks_by_status,
        "agents": agents_by_status,
        "leases": len(state.leases),
    }
