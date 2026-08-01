"""Shared types.

Every module in CodeFleet speaks these. They are the contract between the
server, the scheduler, and the runners, and several of them are literally HTTP
request and response bodies — so changing one changes the wire format.

Timestamps are timezone-aware UTC everywhere. Naive datetimes are rejected at
the boundary rather than quietly compared against aware ones, which is the bug
the reference shipped.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator


def utcnow() -> datetime:
    """The only clock in the codebase. Tests monkeypatch nothing else."""
    return datetime.now(UTC)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _require_aware(v: Any) -> Any:
    """Reject naive datetimes; normalize aware ones to UTC."""
    if isinstance(v, str):
        text = v.replace("Z", "+00:00")
        v = datetime.fromisoformat(text)
    if isinstance(v, datetime):
        if v.tzinfo is None:
            raise ValueError("naive datetime rejected; pass a timezone-aware value")
        return v.astimezone(UTC)
    return v


UtcDatetime = Annotated[datetime, BeforeValidator(_require_aware)]


def isoformat(dt: datetime) -> str:
    """Serialize to the sortable, sqlite3-readable form used in the database."""
    return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class TaskStatus(StrEnum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED_UPSTREAM = "blocked_upstream"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL_TASK_STATUSES

    @property
    def is_active(self) -> bool:
        """Occupying an agent right now."""
        return self in (TaskStatus.ASSIGNED, TaskStatus.RUNNING)


_TERMINAL_TASK_STATUSES = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.BLOCKED_UPSTREAM,
        TaskStatus.CANCELLED,
    }
)


class AgentStatus(StrEnum):
    IDLE = "idle"
    BUSY = "busy"
    OFFLINE = "offline"


class ErrorKind(StrEnum):
    AGENT_ERROR = "agent_error"
    VETO = "veto"
    TIMEOUT = "timeout"
    BUDGET = "budget"
    INFRA = "infra"
    ATTEMPTS_EXHAUSTED = "attempts_exhausted"
    CANCELLED = "cancelled"


RETRYABLE_ERROR_KINDS = frozenset(
    {ErrorKind.AGENT_ERROR, ErrorKind.VETO, ErrorKind.TIMEOUT, ErrorKind.INFRA}
)


class EventType(StrEnum):
    TASK_CREATED = "task_created"
    TASK_ASSIGNED = "task_assigned"
    TASK_STARTED = "task_started"
    TASK_SUCCEEDED = "task_succeeded"
    TASK_FAILED = "task_failed"
    TASK_REQUEUED = "task_requeued"
    TASK_UNBLOCKED = "task_unblocked"
    TASK_BLOCKED_UPSTREAM = "task_blocked_upstream"
    TASK_CANCELLED = "task_cancelled"
    LEASE_ACQUIRED = "lease_acquired"
    LEASE_DENIED = "lease_denied"
    LEASE_RELEASED = "lease_released"
    FILE_CHANGED = "file_changed"
    AGENT_REGISTERED = "agent_registered"
    AGENT_ONLINE = "agent_online"
    AGENT_OFFLINE = "agent_offline"
    FLEET_STARTED = "fleet_started"
    FLEET_IDLE = "fleet_idle"
    RUN_FINISHED = "run_finished"


class WriteTool(StrEnum):
    """The tools the PreToolUse hook gates. The matcher is built from these."""

    WRITE = "Write"
    EDIT = "Edit"
    MULTI_EDIT = "MultiEdit"
    NOTEBOOK_EDIT = "NotebookEdit"


WRITE_TOOL_MATCHER = "|".join(t.value for t in WriteTool)


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------


class Task(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: new_id("t"))
    title: str
    description: str
    status: TaskStatus = TaskStatus.PENDING
    priority: int = Field(default=3, ge=1, le=5)
    file_scope: tuple[str, ...] = ()
    assigned_agent_id: str | None = None
    attempts: int = 0
    max_attempts: int = 3
    backoff_until: UtcDatetime | None = None
    result_summary: str | None = None
    error: str | None = None
    error_kind: ErrorKind | None = None
    blocked_on_path: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    duration_ms: int | None = None
    session_id: str | None = None
    created_at: UtcDatetime = Field(default_factory=utcnow)
    updated_at: UtcDatetime = Field(default_factory=utcnow)
    assigned_at: UtcDatetime | None = None
    started_at: UtcDatetime | None = None
    completed_at: UtcDatetime | None = None

    @field_validator("file_scope", mode="before")
    @classmethod
    def _normalize_scope(cls, v: Any) -> tuple[str, ...]:
        if v is None:
            return ()
        if isinstance(v, str):
            raise TypeError("file_scope must be a sequence of paths, not a string")
        # Deduplicate while preserving order, so equality is stable.
        return tuple(dict.fromkeys(str(p) for p in v))


class Agent(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: new_id("a"))
    name: str
    status: AgentStatus = AgentStatus.IDLE
    epoch: int = 1
    current_task_id: str | None = None
    workdir: str
    pid: int | None = None
    last_heartbeat_at: UtcDatetime = Field(default_factory=utcnow)
    last_assigned_at: UtcDatetime | None = None
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    registered_at: UtcDatetime = Field(default_factory=utcnow)
    updated_at: UtcDatetime = Field(default_factory=utcnow)


class Lease(BaseModel):
    model_config = ConfigDict(frozen=True)

    path: str
    agent_id: str
    task_id: str
    acquired_at: UtcDatetime = Field(default_factory=utcnow)


class FileChangeRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int | None = None
    task_id: str
    agent_id: str
    path: str
    tool: str
    at: UtcDatetime = Field(default_factory=utcnow)


class Event(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int | None = None
    at: UtcDatetime = Field(default_factory=utcnow)
    type: EventType
    task_id: str | None = None
    agent_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Transient DTOs
# ---------------------------------------------------------------------------


class TaskResult(BaseModel):
    """What one runner attempt produced. Also the body of POST /tasks/{id}/complete."""

    agent_id: str
    ok: bool
    summary: str | None = None
    error: str | None = None
    error_kind: ErrorKind | None = None
    blocked_on_path: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    duration_ms: int = 0
    session_id: str | None = None
    files_written: tuple[str, ...] = ()


class FleetState(BaseModel):
    """The frozen snapshot handed to the pure scheduler.

    Constructed literally in scheduler tests — no database, no event loop.
    """

    model_config = ConfigDict(frozen=True)

    tasks: tuple[Task, ...] = ()
    dependencies: tuple[tuple[str, str], ...] = ()  # (task_id, depends_on_id)
    agents: tuple[Agent, ...] = ()
    leases: tuple[Lease, ...] = ()
    stale_after_s: float = 20.0

    def task_by_id(self, task_id: str) -> Task | None:
        return next((t for t in self.tasks if t.id == task_id), None)

    def dependencies_of(self, task_id: str) -> tuple[str, ...]:
        return tuple(dep for tid, dep in self.dependencies if tid == task_id)

    def dependents_of(self, task_id: str) -> tuple[str, ...]:
        return tuple(tid for tid, dep in self.dependencies if dep == task_id)
