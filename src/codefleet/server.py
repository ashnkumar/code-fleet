"""The coordination server: the HTTP surface, the SSE stream, the app factory.

This is the only process that writes coordination state, which is what makes the
rest of the design simple. Runners hold no policy: they register, heartbeat, read
the assignment the server already made, and report. The scheduler holds no state:
it is handed a snapshot and returns a list of decisions. What sits between those
two — the tick loop and every transition it makes — is `codefleet.engine`; this
module is the wire: request in, transition, response and event out.

Two things are worth knowing before reading on:

* **A denied lease is a 200.** Denial is a normal coordination outcome, not a
  transport error, and the hook on the other end has to be able to tell those two
  apart: an HTTP failure means the server is unreachable and the write must fail
  closed, a denial means another agent holds the file.
* **`X-Agent-Epoch` on every runner call is the fence.** Anything that takes a
  task away from an agent bumps that agent's epoch, so the next call it makes —
  whichever one that is — returns `409 stale_epoch` and it re-registers. There is
  no second "your task was cancelled" flag to keep in sync with the first.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from importlib.metadata import version
from time import monotonic
from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, Header, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from codefleet.config import Settings, get_settings
from codefleet.engine import (
    RELEASE_TASK_TERMINAL,
    REQUEUE_DEREGISTERED,
    REQUEUE_REREGISTERED,
    SCAN_LIMIT,
    ServerState,
    fence_agent,
    fleet_counters,
    free_agent,
    reclaim,
    requeue,
    task_deadline,
    tick_loop,
)
from codefleet.models import (
    RETRYABLE_ERROR_KINDS,
    Agent,
    AgentStatus,
    ErrorKind,
    Event,
    EventType,
    FleetState,
    Task,
    TaskResult,
    TaskStatus,
    isoformat,
    utcnow,
)
from codefleet.scheduler import backoff_delay, is_runnable
from codefleet.store import GraphError, Store, TaskSpec

VERSION = version("codefleet")

# Fleet-sized data: a few hundred tasks and a few thousand events. This cap
# exists so a runaway query is bounded, not because paging is expected.
CONFLICT_SCAN_LIMIT = 10_000

SSE_POLL_S = 0.25
SSE_PING_INTERVAL_S = 15.0
SSE_BATCH = 500


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------


class RegisterRequest(BaseModel):
    name: str
    workdir: str
    pid: int | None = None


class StartRequest(BaseModel):
    agent_id: str


class LeaseRequest(BaseModel):
    agent_id: str
    task_id: str
    paths: list[str]
    tool: str | None = None


class ChangeRequest(BaseModel):
    agent_id: str
    task_id: str
    path: str
    tool: str


class TaskGraphRequest(BaseModel):
    """A whole graph, always. One task and fifty take the same path."""

    tasks: list[TaskSpec]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ApiError(Exception):
    """An error with a wire shape: `{"error": {"code", "message", "detail"}}`."""

    def __init__(self, status: int, code: str, message: str, **detail: Any) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.detail = detail

    def response(self) -> JSONResponse:
        return JSONResponse(
            status_code=self.status,
            content={"error": {"code": self.code, "message": self.message, "detail": self.detail}},
        )


def _not_found(noun: str, ident: str) -> ApiError:
    return ApiError(404, f"unknown_{noun}", f"no such {noun}: {ident}", id=ident)


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


def _server(request: Request) -> ServerState:
    """The engine's state, hung off the app by `create_app`'s lifespan."""
    return request.app.state.fleet


ServerDep = Annotated[ServerState, Depends(_server)]


async def _requesting_agent_id(request: Request) -> str:
    """Which agent is making this call.

    Runner endpoints identify the caller either in the path (`/agents/{id}/...`)
    or in the body (`{"agent_id": ...}`), and the epoch check has to work for
    both — otherwise it would be written out four times and one of them would
    eventually be forgotten. Reading the body here is safe: FastAPI has already
    read and cached it before dependencies are solved.
    """
    from_path = request.path_params.get("agent_id")
    if from_path is not None:
        return str(from_path)
    body = await request.body()
    if body:
        payload = json.loads(body)
        if isinstance(payload, dict) and isinstance(payload.get("agent_id"), str):
            return payload["agent_id"]
    raise ApiError(422, "missing_agent_id", "no agent_id in the path or the request body")


async def require_agent(
    request: Request,
    server: ServerDep,
    x_agent_epoch: Annotated[int, Header(description="The agent's current fencing token.")],
) -> Agent:
    """Fence the caller against its own past.

    A runner whose task was taken away — stale requeue, operator cancel, deadline
    sweep — had its epoch bumped by whatever took it. The mismatch here is how it
    finds out, on whichever call it makes next.
    """
    agent_id = await _requesting_agent_id(request)
    agent = await server.store.get_agent(agent_id)
    if agent is None:
        raise _not_found("agent", agent_id)
    if agent.epoch != x_agent_epoch:
        raise ApiError(
            409,
            "stale_epoch",
            f"agent {agent.name} is at epoch {agent.epoch}, not {x_agent_epoch}; re-register",
            agent_id=agent.id,
            current_epoch=agent.epoch,
            presented_epoch=x_agent_epoch,
        )
    return agent


AgentDep = Annotated[Agent, Depends(require_agent)]

router = APIRouter()


# ---------------------------------------------------------------------------
# Runner-facing endpoints
# ---------------------------------------------------------------------------


@router.post("/agents/register")
async def register(body: RegisterRequest, server: ServerDep) -> dict[str, Any]:
    """Upsert an agent by slot name and bump its epoch.

    An agent is an identity, not a process (spec D5): the row survives restarts so
    lifetime counters mean something, and the epoch identifies the incarnation.
    Whatever the previous incarnation was holding is reclaimed here, in the same
    transaction, because a runner that re-registers mid-task has abandoned it.
    """
    store = server.store
    settings = server.settings
    async with store.transaction():
        previous = next((a for a in await store.list_agents() if a.name == body.name), None)
        agent = await store.register_agent(body.name, workdir=body.workdir, pid=body.pid)
        if previous is not None:
            await reclaim(store, agent.id, reason=REQUEUE_REREGISTERED)
        await store.emit(
            EventType.AGENT_REGISTERED if previous is None else EventType.AGENT_ONLINE,
            agent_id=agent.id,
            name=agent.name,
            epoch=agent.epoch,
            workdir=agent.workdir,
            pid=agent.pid,
        )
    server.wake.set()
    return {
        "agent_id": agent.id,
        "epoch": agent.epoch,
        "status": agent.status,
        "heartbeat_interval_s": settings.heartbeat_interval,
        "poll_interval_s": settings.poll_interval,
        "task_timeout_s": settings.task_timeout,
        "server_time": isoformat(utcnow()),
    }


@router.post("/agents/{agent_id}/heartbeat")
async def heartbeat(agent_id: str, server: ServerDep, agent: AgentDep) -> dict[str, Any]:
    """Liveness only. Heartbeats emit no event (spec 3.8) — except the one that
    brings an agent the sweep had given up on back online."""
    store = server.store
    async with store.transaction():
        current = await store.heartbeat(agent.id)
        if current.status is AgentStatus.OFFLINE:
            current = await store.update_agent(agent.id, status=AgentStatus.IDLE)
            await store.emit(
                EventType.AGENT_ONLINE,
                agent_id=agent.id,
                name=current.name,
                epoch=current.epoch,
                reason="heartbeat",
            )
            server.wake.set()
    return {"status": current.status, "epoch": current.epoch}


@router.delete("/agents/{agent_id}", status_code=204)
async def deregister(agent_id: str, server: ServerDep, agent: AgentDep) -> Response:
    """Graceful shutdown: the opposite of a stale agent, handled the same way."""
    store = server.store
    async with store.transaction():
        await store.update_agent(agent.id, status=AgentStatus.OFFLINE, current_task_id=None)
        await reclaim(store, agent.id, reason=REQUEUE_DEREGISTERED)
        await store.emit(
            EventType.AGENT_OFFLINE,
            agent_id=agent.id,
            name=agent.name,
            reason=REQUEUE_DEREGISTERED,
        )
    server.wake.set()
    return Response(status_code=204)


@router.get("/agents/{agent_id}/assignment", response_model=None)
async def get_assignment(
    agent_id: str, server: ServerDep, agent: AgentDep
) -> dict[str, Any] | Response:
    """Read the decision the scheduler already made and committed.

    There is no claim step here and no compare-and-swap: one scheduler in one
    process assigned this task inside one transaction, so the runner cannot lose
    a race it was never in.
    """
    if agent.current_task_id is None:
        return Response(status_code=204)
    task = await server.store.get_task(agent.current_task_id)
    if task is None or task.assigned_agent_id != agent.id or not task.status.is_active:
        return Response(status_code=204)
    return {"task": _assignment_view(task, server.settings)}


@router.post("/tasks/{task_id}/start")
async def start_task(
    task_id: str, body: StartRequest, server: ServerDep, agent: AgentDep
) -> dict[str, Any]:
    store = server.store
    async with store.transaction():
        task = await _require_task(store, task_id)
        _require_owner(task, agent)
        if task.status is TaskStatus.RUNNING:
            return {"status": task.status}
        task = await store.update_task(task_id, status=TaskStatus.RUNNING, started_at=utcnow())
        await store.emit(
            EventType.TASK_STARTED,
            task_id=task.id,
            agent_id=agent.id,
            attempt=task.attempts,
        )
    return {"status": task.status}


@router.post("/leases/acquire")
async def acquire_leases(body: LeaseRequest, server: ServerDep, agent: AgentDep) -> dict[str, Any]:
    """The veto. Always `200` — a denial is a coordination outcome, not an error.

    All-or-nothing across `paths`: taking some of what was asked for would be
    hold-and-wait, the one ingredient of deadlock this design otherwise lacks.
    """
    store = server.store
    async with store.transaction():
        task = await _require_task(store, body.task_id)
        _require_owner(task, agent)
        decision = await store.acquire_leases(
            agent_id=agent.id, task_id=body.task_id, paths=body.paths
        )
    if decision.allowed:
        return {"decision": "allow", "granted": decision.granted}
    first = decision.denied[0]
    return {
        "decision": "deny",
        "denied": [denied.model_dump() for denied in decision.denied],
        "message": (
            f"{first.path} is held by {first.holder_agent_name} for task {first.holder_task_id}."
        ),
    }


@router.post("/changes", status_code=202)
async def record_change(body: ChangeRequest, server: ServerDep, agent: AgentDep) -> dict[str, Any]:
    """The observational ledger, written from `PostToolUse`. Never vetoes anything."""
    await server.store.record_change(
        agent_id=agent.id, task_id=body.task_id, path=body.path, tool=body.tool
    )
    return {}


@router.post("/tasks/{task_id}/complete")
async def complete_task(
    task_id: str, result: TaskResult, server: ServerDep, agent: AgentDep
) -> dict[str, Any]:
    """Record one attempt and decide what happens to the task next.

    Success cascades (spec 4.3). Failure either requeues with backoff or, at the
    attempt cap, goes terminal and lets the next tick block its dependents. A
    veto additionally folds the denied path into `file_scope` (spec 4.5 step 8),
    which is what stops the retry from being a coin flip.
    """
    store = server.store
    now = utcnow()
    async with store.transaction():
        task = await _require_task(store, task_id)
        memo_key = (task_id, agent.id, task.attempts)
        recorded = server.completions.get(memo_key)
        if recorded is not None:
            return recorded
        _require_owner(task, agent)
        if task.status.is_terminal:
            # A duplicate the memo above could not answer: it was lost with the
            # process, or this is a second delivery of the same report. The
            # attempt is already recorded, and re-applying it would double the
            # token and cost counters and fire the cascade a second time.
            # Requeued attempts need no equivalent guard — a requeue clears
            # `assigned_agent_id`, so `_require_owner` has already rejected them.
            return {"status": task.status, "attempts": task.attempts, "duplicate": True}

        task = await store.update_task(
            task_id,
            input_tokens=task.input_tokens + result.input_tokens,
            output_tokens=task.output_tokens + result.output_tokens,
            cost_usd=task.cost_usd + result.cost_usd,
            duration_ms=result.duration_ms,
            session_id=result.session_id,
        )
        await store.update_agent(
            agent.id,
            input_tokens=agent.input_tokens + result.input_tokens,
            output_tokens=agent.output_tokens + result.output_tokens,
            cost_usd=agent.cost_usd + result.cost_usd,
            tasks_succeeded=agent.tasks_succeeded + (1 if result.ok else 0),
            tasks_failed=agent.tasks_failed + (0 if result.ok else 1),
        )

        if result.ok:
            response = await _complete_succeeded(store, task, agent, result, now)
        else:
            response = await _complete_failed(store, task, agent, result, now)
        await free_agent(store, agent.id, task_id)

    server.completions[memo_key] = response
    server.wake.set()
    return response


async def _complete_succeeded(
    store: Store, task: Task, agent: Agent, result: TaskResult, now: datetime
) -> dict[str, Any]:
    await store.release_leases_for_task(task.id, RELEASE_TASK_TERMINAL)
    task = await store.update_task(
        task.id,
        status=TaskStatus.SUCCEEDED,
        result_summary=result.summary,
        error=None,
        error_kind=None,
        completed_at=now,
    )
    await store.emit(
        EventType.TASK_SUCCEEDED,
        task_id=task.id,
        agent_id=agent.id,
        attempt=task.attempts,
        summary=result.summary,
        files_written=list(result.files_written),
        cost_usd=result.cost_usd,
        duration_ms=result.duration_ms,
    )
    unblocked = await _cascade_unblocked(store, task.id)
    return {"status": task.status, "attempts": task.attempts, "unblocked": unblocked}


async def _complete_failed(
    store: Store, task: Task, agent: Agent, result: TaskResult, now: datetime
) -> dict[str, Any]:
    error_kind = result.error_kind or ErrorKind.AGENT_ERROR
    if error_kind is ErrorKind.VETO and result.blocked_on_path:
        # Spec 4.5 step 8: the loop-closer. A path this task was denied is a
        # scheduling fact from now on, so the retry runs after the holder, not
        # alongside it.
        await store.widen_file_scope(task.id, result.blocked_on_path)

    retryable = error_kind in RETRYABLE_ERROR_KINDS and task.attempts < task.max_attempts
    if retryable:
        backoff_until = now + backoff_delay(task.attempts)
        task = await store.update_task(
            task.id,
            error=result.error,
            error_kind=error_kind,
            blocked_on_path=result.blocked_on_path,
        )
        await requeue(store, task, reason=error_kind.value, backoff_until=backoff_until)
        return {
            "status": TaskStatus.PENDING,
            "attempts": task.attempts,
            "backoff_until": isoformat(backoff_until),
        }

    exhausted = task.attempts >= task.max_attempts
    final_kind = ErrorKind.ATTEMPTS_EXHAUSTED if exhausted else error_kind
    await store.release_leases_for_task(task.id, RELEASE_TASK_TERMINAL)
    task = await store.update_task(
        task.id,
        status=TaskStatus.FAILED,
        error=result.error,
        error_kind=final_kind,
        blocked_on_path=result.blocked_on_path,
        completed_at=now,
    )
    await store.emit(
        EventType.TASK_FAILED,
        task_id=task.id,
        agent_id=agent.id,
        attempt=task.attempts,
        error=result.error,
        error_kind=final_kind,
        blocked_on_path=result.blocked_on_path,
    )
    # Dependents are blocked by the next tick, which already knows how to walk
    # the graph transitively from every permanently failed task.
    return {"status": task.status, "attempts": task.attempts, "error_kind": final_kind}


# ---------------------------------------------------------------------------
# Operator-facing endpoints
# ---------------------------------------------------------------------------


@router.post("/tasks", status_code=201)
async def create_tasks(body: TaskGraphRequest, server: ServerDep) -> dict[str, Any]:
    created = await server.store.create_tasks(
        body.tasks, default_max_attempts=server.settings.max_attempts
    )
    server.wake.set()
    return {"created": created}


@router.get("/tasks")
async def list_tasks(
    server: ServerDep,
    status: TaskStatus | None = None,
    limit: int = Query(default=100, ge=1, le=SCAN_LIMIT),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """Task rows plus the two derived fields nothing stores: `runnable` and
    `unmet_dependencies` (spec 4.1 — blockedness is a join).

    Queue order and paging come from `store.list_tasks`, which is the same
    ordering the scheduler is handed and the same one the index is built for; the
    snapshot is here only for the derived fields. Sorting the snapshot instead
    would be a second copy of the queue order that has to agree with the first.
    """
    now = utcnow()
    store = server.store
    async with store.transaction():
        # One transaction, so the page cannot contain a task the snapshot behind
        # `runnable` and `unmet_dependencies` never saw.
        state = await store.fleet_state(stale_after_s=server.settings.stale_after)
        page = await store.list_tasks(status=status, limit=limit, offset=offset)
    total = sum(1 for task in state.tasks if status is None or task.status is status)
    return {
        "tasks": [_task_view(task, state, now) for task in page],
        "total": total,
    }


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, server: ServerDep) -> dict[str, Any]:
    store = server.store
    now = utcnow()
    state = await store.fleet_state(stale_after_s=server.settings.stale_after)
    task = state.task_by_id(task_id)
    if task is None:
        raise _not_found("task", task_id)
    return {
        "task": _task_view(task, state, now),
        "dependencies": await store.dependencies_of(task_id),
        "dependents": await store.dependents_of(task_id),
        "changes": [_jsonable(change) for change in await store.list_changes(task_id)],
    }


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, server: ServerDep) -> dict[str, Any]:
    """Cancel is a revocation, so it fences the holder (spec 4.4).

    That is the whole "tell the runner its task was taken away" mechanism: the
    zombie's next call — heartbeat, lease, completion — returns `409 stale_epoch`
    and it aborts the session and re-registers. Until it does it stays `offline`,
    because a runner that has not yet heard it was fenced is not a runner that
    can be given work.
    """
    store = server.store
    async with store.transaction():
        task = await _require_task(store, task_id)
        if task.status.is_terminal:
            raise ApiError(
                409,
                "task_terminal",
                f"task {task_id} is already {task.status}",
                status=task.status.value,
            )
        holder_id = task.assigned_agent_id
        if holder_id is not None:
            # Spec 4.4 order: fence the incarnation *before* anything it was
            # holding becomes available again, so there is no instant in which
            # the work is free and the runner is still assignable.
            await fence_agent(store, holder_id, reason=ErrorKind.CANCELLED.value)
        await store.release_leases_for_task(task_id, ErrorKind.CANCELLED.value)
        task = await store.update_task(
            task_id,
            status=TaskStatus.CANCELLED,
            error_kind=ErrorKind.CANCELLED,
            error="cancelled by operator",
            completed_at=utcnow(),
        )
        await store.emit(EventType.TASK_CANCELLED, task_id=task_id, agent_id=holder_id)
    server.wake.set()
    return {"task": _jsonable(task)}


@router.post("/tasks/{task_id}/retry")
async def retry_task(task_id: str, server: ServerDep) -> dict[str, Any]:
    """Operator override: a failed task gets its attempts back, and everything it
    stranded downstream returns to `pending`."""
    store = server.store
    async with store.transaction():
        task = await _require_task(store, task_id)
        if task.status is not TaskStatus.FAILED:
            raise ApiError(
                409,
                "not_failed",
                f"task {task_id} is {task.status}; only a failed task can be retried",
                status=task.status.value,
            )
        task = await store.update_task(
            task_id,
            status=TaskStatus.PENDING,
            attempts=0,
            assigned_agent_id=None,
            assigned_at=None,
            started_at=None,
            completed_at=None,
            backoff_until=None,
            error=None,
            error_kind=None,
        )
        await store.emit(
            EventType.TASK_REQUEUED, task_id=task_id, reason="operator_retry", attempts=0
        )
        unblocked = await _unblock_downstream(store, task_id)
    server.wake.set()
    return {"task": _jsonable(task), "unblocked": unblocked}


@router.get("/agents")
async def list_agents(server: ServerDep) -> dict[str, Any]:
    now = utcnow()
    agents = await server.store.list_agents()
    return {"agents": [_agent_view(agent, now, server.settings.stale_after) for agent in agents]}


@router.get("/leases")
async def list_leases(server: ServerDep) -> dict[str, Any]:
    now = utcnow()
    agents = {agent.id: agent.name for agent in await server.store.list_agents()}
    return {
        "leases": [
            _jsonable(lease)
            | {
                "agent_name": agents.get(lease.agent_id, lease.agent_id),
                "age_s": round((now - lease.acquired_at).total_seconds(), 3),
            }
            for lease in await server.store.list_leases()
        ]
    }


@router.get("/conflicts")
async def list_conflicts(server: ServerDep, resolved: bool | None = None) -> dict[str, Any]:
    """A projection, not a table (spec 3.6).

    A conflict *is* a `lease_denied` event; whether it is resolved is a property
    of the requester task's current status, computed from the thing that actually
    determines it rather than stamped by whoever remembered to stamp it.
    """
    store = server.store
    denials = await store.events_since(0, limit=CONFLICT_SCAN_LIMIT, types=[EventType.LEASE_DENIED])
    conflicts: list[dict[str, Any]] = []
    for event in denials:
        requester = await store.get_task(event.task_id) if event.task_id else None
        is_resolved = requester is not None and requester.status is TaskStatus.SUCCEEDED
        if resolved is not None and resolved is not is_resolved:
            continue
        conflicts.append(
            {
                "event_id": event.id,
                "at": isoformat(event.at),
                "path": event.payload.get("path"),
                "requester_task_id": event.task_id,
                "requester_agent_id": event.agent_id,
                "requester_status": requester.status if requester else None,
                "holder_agent_id": event.payload.get("holder_agent_id"),
                "holder_agent_name": event.payload.get("holder_agent_name"),
                "holder_task_id": event.payload.get("holder_task_id"),
                "reason": event.payload.get("reason"),
                "resolved": is_resolved,
            }
        )
    return {"conflicts": conflicts}


@router.get("/events")
async def list_events(
    server: ServerDep,
    since: int = Query(default=0, ge=0),
    limit: int = Query(default=500, ge=1, le=SCAN_LIMIT),
    type: Annotated[list[EventType] | None, Query()] = None,
) -> dict[str, Any]:
    events = await server.store.events_since(since, limit=limit, types=type)
    return {
        "events": [_jsonable(event) for event in events],
        "last_event_id": events[-1].id if events else since,
    }


@router.get("/events/stream")
async def stream_events(server: ServerDep, since: int = Query(default=0, ge=0)) -> Response:
    """Replay then tail, over one cursor.

    Replay and live tail are the same code path — the tail is just a replay that
    has caught up — which is why a dropped client reconnects by handing back the
    last id it saw.
    """
    return StreamingResponse(
        _sse_frames(server.store, since),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store",
            "Connection": "keep-alive",
            # Tells nginx and friends not to buffer, which would defeat the point.
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/state")
async def get_state(server: ServerDep) -> dict[str, Any]:
    """One snapshot: what the dashboard fetches before subscribing to the stream."""
    now = utcnow()
    store = server.store
    state = await store.fleet_state(stale_after_s=server.settings.stale_after)
    stale_after = server.settings.stale_after
    return {
        "tasks": [_task_view(task, state, now) for task in state.tasks],
        "agents": [_agent_view(agent, now, stale_after) for agent in state.agents],
        "leases": [_jsonable(lease) for lease in state.leases],
        "counters": fleet_counters(state),
        "last_event_id": await store.last_event_id(),
        "server_time": isoformat(now),
    }


@router.get("/health")
async def health(server: ServerDep) -> dict[str, Any]:
    # Touching the events table is the check: it proves the file is open and
    # readable rather than proving the process is running, which we already know.
    await server.store.last_event_id()
    return {
        "ok": True,
        "version": VERSION,
        "db": "ok",
        "uptime_s": round((utcnow() - server.started_at).total_seconds(), 3),
    }


@router.post("/reset")
async def reset(server: ServerDep) -> dict[str, Any]:
    if not server.settings.allow_reset:
        raise ApiError(403, "reset_forbidden", "set CODEFLEET_ALLOW_RESET=1 to allow /reset")
    await server.store.reset()
    server.completions.clear()
    server.fleet_idle_emitted = False
    server.run_finished_emitted = False
    await server.store.emit(EventType.FLEET_STARTED, version=VERSION, reason="reset")
    return {"ok": True}


async def _cascade_unblocked(store: Store, task_id: str) -> list[str]:
    """Spec 4.3: nothing is mutated, because nothing needs to be.

    A dependent that was `pending` is still `pending` — it just became runnable.
    The event exists so the run is legible, not because anything reads it back.
    """
    unblocked: list[str] = []
    for dependent_id in await store.dependents_of(task_id):
        dependent = await store.get_task(dependent_id)
        if dependent is None or dependent.status is not TaskStatus.PENDING:
            continue
        remaining = [
            dependency_id
            for dependency_id in await store.dependencies_of(dependent_id)
            if not await _has_succeeded(store, dependency_id)
        ]
        if remaining:
            continue
        await store.emit(EventType.TASK_UNBLOCKED, task_id=dependent_id, unblocked_by=task_id)
        unblocked.append(dependent_id)
    return unblocked


async def _unblock_downstream(store: Store, task_id: str) -> list[str]:
    """Undo `blocked_upstream` transitively, for `POST /tasks/{id}/retry`."""
    returned: list[str] = []
    frontier = [task_id]
    while frontier:
        current_id = frontier.pop()
        for dependent_id in await store.dependents_of(current_id):
            dependent = await store.get_task(dependent_id)
            if dependent is None or dependent.status is not TaskStatus.BLOCKED_UPSTREAM:
                continue
            await store.update_task(
                dependent_id, status=TaskStatus.PENDING, error=None, error_kind=None
            )
            await store.emit(EventType.TASK_UNBLOCKED, task_id=dependent_id, unblocked_by=task_id)
            returned.append(dependent_id)
            frontier.append(dependent_id)
    return returned


async def _has_succeeded(store: Store, task_id: str) -> bool:
    task = await store.get_task(task_id)
    return task is not None and task.status is TaskStatus.SUCCEEDED


async def _require_task(store: Store, task_id: str) -> Task:
    task = await store.get_task(task_id)
    if task is None:
        raise _not_found("task", task_id)
    return task


def _require_owner(task: Task, agent: Agent) -> None:
    if task.assigned_agent_id != agent.id:
        raise ApiError(
            409,
            "not_owner",
            f"task {task.id} is not assigned to {agent.name}",
            task_id=task.id,
            assigned_agent_id=task.assigned_agent_id,
        )


# ---------------------------------------------------------------------------
# SSE
# ---------------------------------------------------------------------------


async def _sse_frames(store: Store, since: int) -> AsyncIterator[str]:
    """`id:` is the event id, `event:` is the EventType, `data:` is the whole row.

    Polling the events table is what makes replay and live tail literally the same
    query. A pub/sub fan-out would be a second delivery path to keep consistent
    with the table, for a stream whose latency budget is a quarter of a second.
    """
    cursor = since
    last_frame_at = monotonic()
    while True:
        batch = await store.events_since(cursor, limit=SSE_BATCH)
        for event in batch:
            cursor = event.id or cursor
            yield _sse_frame(event)
            last_frame_at = monotonic()
        if batch:
            continue  # drain a backlog without sleeping between pages
        if monotonic() - last_frame_at >= SSE_PING_INTERVAL_S:
            # Keeps intermediaries from closing a stream that is merely quiet.
            yield ": ping\n\n"
            last_frame_at = monotonic()
        await asyncio.sleep(SSE_POLL_S)


def _sse_frame(event: Event) -> str:
    data = json.dumps(_jsonable(event), separators=(",", ":"))
    return f"id: {event.id}\nevent: {event.type.value}\ndata: {data}\n\n"


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


def _jsonable(model: BaseModel) -> dict[str, Any]:
    """`model_dump`, with this codebase's timestamp convention (`...Z`, not `+00:00`)."""
    return {
        key: isoformat(value) if isinstance(value, datetime) else value
        for key, value in model.model_dump().items()
    }


def _task_view(task: Task, state: FleetState, now: datetime) -> dict[str, Any]:
    unmet = [
        dependency_id
        for dependency_id in state.dependencies_of(task.id)
        if (dependency := state.task_by_id(dependency_id)) is None
        or dependency.status is not TaskStatus.SUCCEEDED
    ]
    return _jsonable(task) | {
        "runnable": is_runnable(state, task, now),
        "unmet_dependencies": sorted(unmet),
    }


def _agent_view(agent: Agent, now: datetime, stale_after: float) -> dict[str, Any]:
    return _jsonable(agent) | {
        "stale": (now - agent.last_heartbeat_at).total_seconds() > stale_after
    }


def _assignment_view(task: Task, settings: Settings) -> dict[str, Any]:
    """What a runner needs to execute one task, and nothing else."""
    return {
        "id": task.id,
        "title": task.title,
        "description": task.description,
        "priority": task.priority,
        "file_scope": list(task.file_scope),
        "attempts": task.attempts,
        "deadline": isoformat(task_deadline(task, settings)),
        "blocked_on_path": task.blocked_on_path,
    }


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        store = await Store.open(settings.db)
        server = ServerState(
            settings=settings, store=store, wake=asyncio.Event(), started_at=utcnow()
        )
        app.state.fleet = server
        await store.emit(
            EventType.FLEET_STARTED,
            version=VERSION,
            workdir=str(settings.workdir),
            tick_interval_s=settings.tick_interval,
        )
        ticker = asyncio.create_task(tick_loop(server), name="codefleet-tick")
        try:
            yield
        finally:
            ticker.cancel()
            with suppress(asyncio.CancelledError):
                await ticker
            await store.close()

    app = FastAPI(
        title="CodeFleet",
        version=VERSION,
        summary="Coordination server for a fleet of parallel Claude coding agents.",
        lifespan=lifespan,
    )
    app.include_router(router)

    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return exc.response()

    @app.exception_handler(GraphError)
    async def _graph_error(_: Request, exc: GraphError) -> JSONResponse:
        return ApiError(400, "invalid_graph", str(exc)).response()

    @app.exception_handler(RequestValidationError)
    async def _invalid_request(_: Request, exc: RequestValidationError) -> JSONResponse:
        return ApiError(
            422,
            "invalid_request",
            "request body or parameters failed validation",
            errors=json.loads(json.dumps(exc.errors(), default=str)),
        ).response()

    return app


app = create_app(get_settings())
