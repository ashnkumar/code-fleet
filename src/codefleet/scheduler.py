"""The whole coordination policy, as one pure function.

`schedule(state, now)` looks at a frozen snapshot of the fleet and returns the
list of things that should happen. It decides; it never does. Nothing in this
module opens a socket, reads a clock, or touches SQLite — which is why every
rule in section 4 of the spec can be tested with three lines of setup and no
database, and why there is exactly one place to read to find out how work is
handed out.

The returned list is **ordered, and the order is part of the contract.** The
caller applies it top to bottom inside a single transaction:

    1. MarkAgentOffline   — a runner stopped heartbeating
    2. RequeueTask / FailTask — the work that runner was holding
    3. FailTask           — pending tasks that have run out of attempts
    4. BlockDownstream    — dependents of anything permanently failed
    5. Assign             — new work, placed against the state left by 1-4
    6. EmitFleetIdle      — nothing is left to do

Step 5 assumes steps 1-4 have already applied, because they do: a lease held by
an agent that just went offline is released by the same transaction, so this
function does not treat that lease's path as busy. Applying the list out of
order, or partially, will co-schedule tasks that should not be co-scheduled.

Two things deliberately do *not* live here. Emitting `fleet_idle` only on the
first idle tick is the caller's job — this function reports the condition, not
the edge. And the wall-clock deadline sweep of spec section 4.7 needs a task
timeout that `FleetState` does not carry, so it stays server-side.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from codefleet.models import Agent, AgentStatus, ErrorKind, FleetState, Task, TaskStatus

__all__ = [
    "Assign",
    "BlockDownstream",
    "Decision",
    "EmitFleetIdle",
    "FailTask",
    "MarkAgentOffline",
    "RequeueTask",
    "backoff_delay",
    "idle_agents",
    "is_runnable",
    "runnable_tasks",
    "schedule",
]

BACKOFF_STEP_S = 2.0
BACKOFF_MAX_S = 30.0

REQUEUE_AGENT_STALE = "agent_stale"

# Sorts before every real timestamp, so an agent that has never been assigned
# anything is the longest-idle agent there is.
_NEVER_ASSIGNED = datetime.min.replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# Decisions
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Assign:
    """Give this task to this agent. Assignment is the claim (spec D4)."""

    task_id: str
    agent_id: str


@dataclass(frozen=True, slots=True)
class MarkAgentOffline:
    """The agent's heartbeat is older than `stale_after_s`. Release its leases."""

    agent_id: str


@dataclass(frozen=True, slots=True)
class RequeueTask:
    """Back to `pending`, not before `backoff_until`."""

    task_id: str
    reason: str
    backoff_until: datetime


@dataclass(frozen=True, slots=True)
class FailTask:
    """Terminal. Its dependents will be blocked in the same pass."""

    task_id: str
    error_kind: ErrorKind
    error: str


@dataclass(frozen=True, slots=True)
class BlockDownstream:
    """A dependency failed permanently, so this task can never become runnable."""

    task_id: str
    failed_ancestor_id: str


@dataclass(frozen=True, slots=True)
class EmitFleetIdle:
    """No task is pending, assigned, or running once this tick applies."""


Decision = Assign | MarkAgentOffline | RequeueTask | FailTask | BlockDownstream | EmitFleetIdle


# ---------------------------------------------------------------------------
# The rules
# ---------------------------------------------------------------------------


def backoff_delay(attempts: int) -> timedelta:
    """`min(2s * attempts, 30s)`, no jitter.

    Jitter exists to desynchronize independent retriers. There is one scheduler
    in one process here, so there is nothing to desynchronize — and a
    deterministic backoff is a testable backoff.
    """
    return timedelta(seconds=min(BACKOFF_STEP_S * max(attempts, 1), BACKOFF_MAX_S))


def is_runnable(state: FleetState, task: Task, now: datetime) -> bool:
    """Spec section 4.1. Blockedness is a join, never a stored flag."""
    if task.status is not TaskStatus.PENDING:
        return False
    if task.attempts >= task.max_attempts:
        return False
    if task.backoff_until is not None and task.backoff_until > now:
        return False
    return all(
        _status_of(state, dependency_id) is TaskStatus.SUCCEEDED
        for dependency_id in state.dependencies_of(task.id)
    )


def runnable_tasks(state: FleetState, now: datetime) -> list[Task]:
    """Runnable tasks, best first: highest priority, then oldest, then by id.

    The trailing id makes the ordering total, so two tasks created in the same
    millisecond still have a defined order and the tests do not need a frozen
    clock to be deterministic.
    """
    ready = [task for task in state.tasks if is_runnable(state, task, now)]
    ready.sort(key=lambda task: (-task.priority, task.created_at, task.id))
    return ready


def idle_agents(state: FleetState, now: datetime) -> list[Agent]:
    """Assignable agents, longest-idle first, so work spreads across the fleet."""
    free = [
        agent
        for agent in state.agents
        if agent.status is AgentStatus.IDLE and not _is_stale(agent, now, state.stale_after_s)
    ]
    free.sort(key=lambda agent: (agent.last_assigned_at or _NEVER_ASSIGNED, agent.name))
    return free


def schedule(state: FleetState, now: datetime) -> list[Decision]:
    """Decide everything this tick should do. Pure: same snapshot, same list."""
    decisions: list[Decision] = []

    stale_agent_ids = {
        agent.id for agent in state.agents if _is_stale(agent, now, state.stale_after_s)
    }
    decisions.extend(MarkAgentOffline(agent_id) for agent_id in sorted(stale_agent_ids))

    # Work the dead agents were holding. Requeued and failed alike, these tasks
    # stop occupying their file scope the moment this tick applies.
    requeued_ids: set[str] = set()
    failed_ids: set[str] = set()
    for task in _sorted_tasks(state):
        if not task.status.is_active or task.assigned_agent_id not in stale_agent_ids:
            continue
        if task.attempts >= task.max_attempts:
            decisions.append(_exhausted(task, "assigned agent went offline"))
            failed_ids.add(task.id)
        else:
            decisions.append(
                RequeueTask(
                    task_id=task.id,
                    reason=REQUEUE_AGENT_STALE,
                    backoff_until=now + backoff_delay(task.attempts),
                )
            )
            requeued_ids.add(task.id)

    # A pending task at its attempt cap can never become runnable again. Fail it
    # now rather than leaving it in the queue as sediment that makes the fleet
    # look busy forever.
    for task in _sorted_tasks(state):
        if task.status is TaskStatus.PENDING and task.attempts >= task.max_attempts:
            decisions.append(_exhausted(task, "no attempts left"))
            failed_ids.add(task.id)

    permanently_failed = failed_ids | {
        task.id for task in state.tasks if task.status in (TaskStatus.FAILED, TaskStatus.CANCELLED)
    }
    blocks = _block_downstream(state, permanently_failed)
    decisions.extend(blocks)

    busy_scope = _busy_scope(state, stale_agent_ids, requeued_ids | failed_ids)
    assignments = _assignments(state, now, busy_scope)
    decisions.extend(assignments)

    remaining = {
        task.id
        for task in state.tasks
        if task.status is TaskStatus.PENDING or task.status.is_active
    }
    remaining -= failed_ids
    remaining -= {block.task_id for block in blocks}
    if not remaining:
        decisions.append(EmitFleetIdle())

    return decisions


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _sorted_tasks(state: FleetState) -> list[Task]:
    """Sweeps iterate in id order so the decision list does not depend on how
    the snapshot happened to be assembled."""
    return sorted(state.tasks, key=lambda task: task.id)


def _status_of(state: FleetState, task_id: str) -> TaskStatus | None:
    task = state.task_by_id(task_id)
    return task.status if task is not None else None


def _is_stale(agent: Agent, now: datetime, stale_after_s: float) -> bool:
    if agent.status is AgentStatus.OFFLINE:
        return False  # already marked; marking it again would emit a duplicate
    return (now - agent.last_heartbeat_at).total_seconds() > stale_after_s


def _exhausted(task: Task, cause: str) -> FailTask:
    return FailTask(
        task_id=task.id,
        error_kind=ErrorKind.ATTEMPTS_EXHAUSTED,
        error=f"{cause} ({task.attempts} of {task.max_attempts} attempts used)",
    )


def _block_downstream(state: FleetState, permanently_failed: set[str]) -> list[BlockDownstream]:
    """Walk the dependents of every failed task and name the ancestor that did it.

    Already-blocked dependents are walked *through* without being re-reported,
    so a task added downstream of an old failure still gets blocked, and a task
    blocked on a previous tick does not generate an event every tick after.
    """
    blocked: dict[str, str] = {}
    seen: set[str] = set()

    for ancestor_id in sorted(permanently_failed):
        queue = deque([ancestor_id])
        while queue:
            current_id = queue.popleft()
            for dependent_id in sorted(state.dependents_of(current_id)):
                if dependent_id in seen:
                    continue
                seen.add(dependent_id)
                dependent = state.task_by_id(dependent_id)
                if dependent is None or dependent_id in permanently_failed:
                    continue
                if dependent.status is TaskStatus.BLOCKED_UPSTREAM:
                    queue.append(dependent_id)
                    continue
                if dependent.status.is_terminal:
                    continue
                blocked[dependent_id] = ancestor_id
                queue.append(dependent_id)

    return [
        BlockDownstream(task_id=task_id, failed_ancestor_id=ancestor_id)
        for task_id, ancestor_id in sorted(blocked.items())
    ]


def _busy_scope(
    state: FleetState, offline_agent_ids: set[str], released_task_ids: set[str]
) -> set[str]:
    """Paths that are spoken for: declared scopes of in-flight tasks, plus leases.

    Scopes and leases belonging to work this tick is tearing down are excluded —
    the same transaction that applies these decisions releases them.
    """
    busy = {
        path
        for task in state.tasks
        if task.status.is_active and task.id not in released_task_ids
        for path in task.file_scope
    }
    busy.update(
        lease.path
        for lease in state.leases
        if lease.agent_id not in offline_agent_ids and lease.task_id not in released_task_ids
    )
    return busy


def _assignments(state: FleetState, now: datetime, busy_scope: set[str]) -> list[Assign]:
    """Greedy scan: best runnable task to longest-idle agent, skipping collisions.

    A task whose scope overlaps something in flight is skipped and the scan
    continues. Waiting for it instead would let one contended high-priority task
    head-of-line-block every task behind it.
    """
    claimed = set(busy_scope)
    available = deque(idle_agents(state, now))
    assignments: list[Assign] = []

    for task in runnable_tasks(state, now):
        if not available:
            break
        scope = set(task.file_scope)
        if scope & claimed:
            continue
        assignments.append(Assign(task_id=task.id, agent_id=available.popleft().id))
        claimed |= scope

    return assignments
