"""Persistence.

The only module in CodeFleet that speaks SQL. Everything else — the server, the
scheduler's apply loop, the CLI — goes through this class, so the invariants that
matter can be stated once and enforced in one place:

* **Exclusion is the schema, not application logic.** `acquire_leases` is an
  `INSERT ... ON CONFLICT(path) DO NOTHING` and the decision is the rowcount. No
  read-then-write window exists for two callers to race through.
* **All-or-nothing.** A lease request either takes every path it asked for or
  takes none. Partial acquisition would be hold-and-wait, which is the one
  ingredient of deadlock this design otherwise lacks.
* **`updated_at` is stamped here.** `update_task` and `update_agent` set it and
  reject a caller that tries to; no call site anywhere touches it by hand.
* **`emit()` is the only door into `events`.** It validates the type against the
  enum, so an event type that no `EventType` member covers cannot be written.

Which events the store emits, and which it does not: the store emits the events
whose transaction it owns end to end — `task_created`, `lease_acquired`,
`lease_denied`, `lease_released`, `file_changed`. Status transitions
(`task_assigned`, `task_started`, `agent_offline`, ...) are policy decisions made
by the server, which emits them via `emit()` inside the same transaction as the
`update_task`/`update_agent` that carries them out.

Concurrency: one connection, one `asyncio.Lock`, `BEGIN IMMEDIATE` for writes and
`busy_timeout` for anything that still contends. Coroutines sharing a connection
would otherwise interleave statements inside each other's transactions, so the
lock is what makes `transaction()` mean what it says. It is re-entrant per
asyncio task, which is what lets the server compose store calls — a requeue that
releases leases, updates the task, and emits three events — into one atomic unit.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import aiosqlite
from pydantic import BaseModel, Field, field_validator

from codefleet.models import (
    Agent,
    AgentStatus,
    Event,
    EventType,
    FileChangeRecord,
    FleetState,
    Lease,
    Task,
    TaskStatus,
    isoformat,
    new_id,
    utcnow,
)

# Long enough to ride out a checkpoint under WAL, short enough that a genuine
# deadlock surfaces as an error instead of a hang. Nothing here holds a write
# transaction across an await that can block.
BUSY_TIMEOUT_MS = 5_000

_SCHEMA_PATH = Path(__file__).with_name("schema.sql")

# The task/agent tables carry exactly the model's fields, so the column lists are
# derived rather than repeated. A drift between model and DDL fails the round-trip
# test immediately instead of silently dropping a column.
_TASK_COLUMNS: tuple[str, ...] = tuple(Task.model_fields)
_AGENT_COLUMNS: tuple[str, ...] = tuple(Agent.model_fields)

# Tables in an order safe to delete under foreign keys: children before parents.
_TABLES_CHILD_FIRST = (
    "events",
    "file_changes",
    "file_leases",
    "task_dependencies",
    "tasks",
    "agents",
)


class GraphError(ValueError):
    """A task batch was rejected as a whole: duplicate id, dangling dependency, or cycle."""


class TaskSpec(BaseModel):
    """What a caller supplies to create one task. The body of `POST /tasks`, per item.

    `id` is optional so a YAML graph can name its own nodes (`T1`) and express edges
    between them before the server has assigned anything. `max_attempts` is optional
    for a different reason: unset means "whatever the fleet is configured for", which
    is what makes `CODEFLEET_MAX_ATTEMPTS` a real knob rather than a documented one.
    """

    id: str | None = None
    title: str
    description: str
    priority: int = Field(default=3, ge=1, le=5)
    file_scope: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()
    max_attempts: int | None = Field(default=None, ge=1)

    @field_validator("file_scope", "depends_on", mode="before")
    @classmethod
    def _sequence_not_string(cls, v: Any) -> Any:
        if v is None:
            return ()
        if isinstance(v, str):
            raise TypeError("expected a sequence of strings, got a string")
        return tuple(dict.fromkeys(str(item) for item in v))


class DeniedPath(BaseModel):
    """One path a lease request could not take, and who is holding it."""

    path: str
    holder_agent_id: str
    holder_agent_name: str
    holder_task_id: str
    reason: str = "held"


class LeaseDecision(BaseModel):
    """The outcome of one `POST /leases/acquire`.

    `denied` non-empty means nothing was taken: the decision is all-or-nothing, so
    `granted` and `denied` are never both populated.
    """

    granted: list[str] = Field(default_factory=list)
    denied: list[DeniedPath] = Field(default_factory=list)

    @property
    def allowed(self) -> bool:
        return not self.denied


def _encode(column: str, value: Any) -> Any:
    """Python value to SQLite value for one named column."""
    if column == "file_scope":
        return json.dumps(list(value))
    if column == "payload":
        return json.dumps(value)
    if isinstance(value, datetime):
        return isoformat(value)
    return value


def _insert_sql(table: str, columns: Sequence[str]) -> str:
    placeholders = ", ".join("?" * len(columns))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"


def _row_to_task(row: aiosqlite.Row) -> Task:
    data = dict(row)
    data["file_scope"] = json.loads(data["file_scope"])
    return Task.model_validate(data)


def _row_to_agent(row: aiosqlite.Row) -> Agent:
    return Agent.model_validate(dict(row))


def _row_to_event(row: aiosqlite.Row) -> Event:
    data = dict(row)
    data["payload"] = json.loads(data["payload"])
    return Event.model_validate(data)


def _require_acyclic(nodes: Iterable[str], edges: Iterable[tuple[str, str]]) -> None:
    """Kahn's algorithm over the whole graph. Raises `GraphError` if anything cycles.

    Edges point task -> dependency; a node is ready once every task depending on it
    has been emitted. Whatever is left when the frontier empties is in a cycle.
    """
    indegree = dict.fromkeys(nodes, 0)
    dependents: dict[str, list[str]] = {node: [] for node in indegree}
    for task_id, depends_on_id in edges:
        dependents[depends_on_id].append(task_id)
        indegree[task_id] += 1

    frontier = [node for node, degree in indegree.items() if degree == 0]
    settled = 0
    while frontier:
        node = frontier.pop()
        settled += 1
        for dependent in dependents[node]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                frontier.append(dependent)

    if settled != len(indegree):
        cyclic = sorted(node for node, degree in indegree.items() if degree > 0)
        raise GraphError(f"dependency cycle among tasks: {', '.join(cyclic)}")


class Store:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db
        self._lock = asyncio.Lock()
        self._holder: asyncio.Task[Any] | None = None
        self._in_transaction = False

    @classmethod
    async def open(cls, path: Path) -> Store:
        path.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None turns off sqlite3's implicit transaction management so
        # that BEGIN IMMEDIATE below is the only thing opening a write transaction.
        db = await aiosqlite.connect(path, isolation_level=None)
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        await db.execute("PRAGMA foreign_keys = ON")
        await db.execute("PRAGMA synchronous = NORMAL")
        await db.executescript(_SCHEMA_PATH.read_text())
        return cls(db)

    async def close(self) -> None:
        await self._db.close()

    # -- transactions -------------------------------------------------------

    @asynccontextmanager
    async def _exclusive(self) -> AsyncIterator[aiosqlite.Connection]:
        """Sole use of the connection for the duration, re-entrant per asyncio task."""
        current = asyncio.current_task()
        if self._holder is current:
            yield self._db
            return
        async with self._lock:
            self._holder = current
            try:
                yield self._db
            finally:
                self._holder = None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        """`BEGIN IMMEDIATE` ... `COMMIT`, rolling back on any exception.

        Nesting joins the enclosing transaction rather than starting a second one, so
        a caller can wrap several store calls into one atomic unit and each of those
        calls can still be atomic when called alone.
        """
        if self._in_transaction and self._holder is asyncio.current_task():
            yield self._db
            return
        async with self._exclusive() as db:
            # IMMEDIATE takes the write lock up front: a transaction that reads, then
            # decides, then writes must not discover a writer arrived in between.
            await db.execute("BEGIN IMMEDIATE")
            self._in_transaction = True
            try:
                yield db
            except BaseException:
                await db.rollback()
                raise
            else:
                await db.commit()
            finally:
                self._in_transaction = False

    async def _fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[aiosqlite.Row]:
        async with self._exclusive() as db, db.execute(sql, params) as cursor:
            return list(await cursor.fetchall())

    async def _fetchone(self, sql: str, params: Sequence[Any] = ()) -> aiosqlite.Row | None:
        async with self._exclusive() as db, db.execute(sql, params) as cursor:
            return await cursor.fetchone()

    # -- snapshot -----------------------------------------------------------

    async def fleet_state(self, *, stale_after_s: float) -> FleetState:
        """One consistent snapshot for the pure scheduler.

        Read inside a transaction so the tasks, the edges, the agents and the leases
        all describe the same instant — a scheduler deciding from a torn read could
        assign a task whose lease it never saw.

        `stale_after_s` is passed in rather than read from the environment because
        the store holds no policy: the app that owns this store was built with a
        `Settings`, and a second reading of the environment here would let the
        scheduler's idea of a dead agent drift from the one `GET /agents` reports.
        """
        async with self.transaction():
            tasks = [_row_to_task(row) for row in await self._fetchall("SELECT * FROM tasks")]
            deps = [
                (row["task_id"], row["depends_on_id"])
                for row in await self._fetchall(
                    "SELECT task_id, depends_on_id FROM task_dependencies"
                )
            ]
            agents = [_row_to_agent(row) for row in await self._fetchall("SELECT * FROM agents")]
            leases = [
                Lease.model_validate(dict(row))
                for row in await self._fetchall("SELECT * FROM file_leases")
            ]
        return FleetState(
            tasks=tuple(tasks),
            dependencies=tuple(deps),
            agents=tuple(agents),
            leases=tuple(leases),
            stale_after_s=stale_after_s,
        )

    # -- tasks --------------------------------------------------------------

    async def create_tasks(
        self, specs: list[TaskSpec], *, default_max_attempts: int = 3
    ) -> list[str]:
        """Insert a whole graph, or nothing.

        Validated against the batch *and* the existing database: caller-supplied ids
        must be fresh, every `depends_on` must resolve, and the union graph must be
        acyclic. Emits one `task_created` per task in the same transaction, so a task
        row without its creation event is not a reachable state.

        `default_max_attempts` is the fleet setting a spec that named no cap inherits.
        The store holds no policy, so it is passed in rather than read here.
        """
        if not specs:
            return []

        async with self.transaction() as db:
            existing_ids = {row["id"] for row in await self._fetchall("SELECT id FROM tasks")}

            tasks: list[Task] = []
            batch_ids: set[str] = set()
            for spec in specs:
                task_id = spec.id or new_id("t")
                if task_id in batch_ids:
                    raise GraphError(f"duplicate task id in batch: {task_id}")
                if task_id in existing_ids:
                    raise GraphError(f"task id already exists: {task_id}")
                batch_ids.add(task_id)
                tasks.append(
                    Task(
                        id=task_id,
                        title=spec.title,
                        description=spec.description,
                        priority=spec.priority,
                        file_scope=spec.file_scope,
                        max_attempts=spec.max_attempts or default_max_attempts,
                    )
                )

            known = existing_ids | batch_ids
            new_edges: list[tuple[str, str]] = []
            for task, spec in zip(tasks, specs, strict=True):
                for depends_on_id in spec.depends_on:
                    if depends_on_id not in known:
                        raise GraphError(f"task {task.id} depends on unknown task {depends_on_id}")
                    new_edges.append((task.id, depends_on_id))

            existing_edges = [
                (row["task_id"], row["depends_on_id"])
                for row in await self._fetchall(
                    "SELECT task_id, depends_on_id FROM task_dependencies"
                )
            ]
            _require_acyclic(known, [*existing_edges, *new_edges])

            await db.executemany(
                _insert_sql("tasks", _TASK_COLUMNS),
                [[_encode(c, getattr(task, c)) for c in _TASK_COLUMNS] for task in tasks],
            )
            await db.executemany(
                "INSERT INTO task_dependencies (task_id, depends_on_id) VALUES (?, ?)",
                new_edges,
            )
            for task in tasks:
                await self.emit(
                    EventType.TASK_CREATED,
                    task_id=task.id,
                    title=task.title,
                    priority=task.priority,
                    file_scope=list(task.file_scope),
                    depends_on=[dep for tid, dep in new_edges if tid == task.id],
                )

        return [task.id for task in tasks]

    async def get_task(self, task_id: str) -> Task | None:
        row = await self._fetchone("SELECT * FROM tasks WHERE id = ?", (task_id,))
        return _row_to_task(row) if row else None

    async def list_tasks(
        self,
        *,
        status: TaskStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Task]:
        """Queue order: the ordering the scheduler cares about, and the index we built."""
        sql = "SELECT * FROM tasks"
        params: list[Any] = []
        if status is not None:
            sql += " WHERE status = ?"
            params.append(TaskStatus(status).value)
        sql += " ORDER BY priority DESC, created_at ASC, id ASC LIMIT ? OFFSET ?"
        params += [limit, offset]
        return [_row_to_task(row) for row in await self._fetchall(sql, params)]

    async def update_task(self, task_id: str, **fields: Any) -> Task:
        """Apply a partial update and stamp `updated_at`. Returns the stored row.

        The return value is re-read rather than the merged object: timestamps are
        stored at millisecond precision, so handing back the in-memory value would
        return something that never compares equal to a later `get_task`.
        """
        if "updated_at" in fields:
            raise ValueError("updated_at is stamped by the store, never by a call site")
        async with self.transaction() as db:
            current = await self._require_task(task_id)
            updated = self._merge(current, fields, _TASK_COLUMNS, "task")
            columns = [*fields, "updated_at"]
            assignments = ", ".join(f"{column} = ?" for column in columns)
            await db.execute(
                f"UPDATE tasks SET {assignments} WHERE id = ?",
                [*(_encode(c, getattr(updated, c)) for c in columns), task_id],
            )
            return await self._require_task(task_id)

    async def widen_file_scope(self, task_id: str, path: str) -> None:
        """Fold a vetoed path into the task's declared scope (SPEC 4.5 step 8).

        This is the loop-closer: on the next tick the scheduler will not co-schedule
        the retry with whoever holds that path, so the retry is not a coin flip.
        """
        async with self.transaction():
            task = await self._require_task(task_id)
            if path in task.file_scope:
                return
            await self.update_task(task_id, file_scope=(*task.file_scope, path))

    async def dependencies_of(self, task_id: str) -> list[str]:
        rows = await self._fetchall(
            "SELECT depends_on_id FROM task_dependencies WHERE task_id = ? ORDER BY depends_on_id",
            (task_id,),
        )
        return [row["depends_on_id"] for row in rows]

    async def dependents_of(self, task_id: str) -> list[str]:
        rows = await self._fetchall(
            "SELECT task_id FROM task_dependencies WHERE depends_on_id = ? ORDER BY task_id",
            (task_id,),
        )
        return [row["task_id"] for row in rows]

    # -- agents -------------------------------------------------------------

    async def register_agent(self, name: str, workdir: str, pid: int | None) -> Agent:
        """Upsert on `name`, bumping `epoch`.

        An agent is an identity, not a process: the row is keyed by slot name so
        lifetime counters survive a restart, and `epoch` identifies the incarnation.
        Bumping it is what fences a zombie runner out of a task it no longer owns.

        Releasing the previous incarnation's leases and requeueing its in-flight task
        are policy, and belong to the caller — which should wrap this call and those
        in one `transaction()`.
        """
        async with self.transaction() as db:
            row = await self._fetchone("SELECT * FROM agents WHERE name = ?", (name,))
            now = utcnow()
            if row is None:
                agent = Agent(
                    name=name,
                    workdir=workdir,
                    pid=pid,
                    status=AgentStatus.IDLE,
                    epoch=1,
                    last_heartbeat_at=now,
                    registered_at=now,
                    updated_at=now,
                )
                await db.execute(
                    _insert_sql("agents", _AGENT_COLUMNS),
                    [_encode(c, getattr(agent, c)) for c in _AGENT_COLUMNS],
                )
                return await self._require_agent(agent.id)

            existing = _row_to_agent(row)
            return await self.update_agent(
                existing.id,
                status=AgentStatus.IDLE,
                epoch=existing.epoch + 1,
                current_task_id=None,
                workdir=workdir,
                pid=pid,
                last_heartbeat_at=now,
            )

    async def get_agent(self, agent_id: str) -> Agent | None:
        row = await self._fetchone("SELECT * FROM agents WHERE id = ?", (agent_id,))
        return _row_to_agent(row) if row else None

    async def list_agents(self) -> list[Agent]:
        rows = await self._fetchall("SELECT * FROM agents ORDER BY name")
        return [_row_to_agent(row) for row in rows]

    async def update_agent(self, agent_id: str, **fields: Any) -> Agent:
        if "updated_at" in fields:
            raise ValueError("updated_at is stamped by the store, never by a call site")
        async with self.transaction() as db:
            current = await self._require_agent(agent_id)
            updated = self._merge(current, fields, _AGENT_COLUMNS, "agent")
            columns = [*fields, "updated_at"]
            assignments = ", ".join(f"{column} = ?" for column in columns)
            await db.execute(
                f"UPDATE agents SET {assignments} WHERE id = ?",
                [*(_encode(c, getattr(updated, c)) for c in columns), agent_id],
            )
            return await self._require_agent(agent_id)

    async def bump_epoch(self, agent_id: str) -> int:
        async with self.transaction():
            agent = await self._require_agent(agent_id)
            updated = await self.update_agent(agent_id, epoch=agent.epoch + 1)
            return updated.epoch

    async def heartbeat(self, agent_id: str) -> Agent:
        """Stamp liveness only. Heartbeats emit no event (SPEC 3.8) and change no status.

        Bringing an `offline` agent back to `idle` is a recovery decision with an
        `agent_online` event attached to it, and that belongs to the server.
        """
        return await self.update_agent(agent_id, last_heartbeat_at=utcnow())

    # -- leases -------------------------------------------------------------

    async def acquire_leases(
        self, *, agent_id: str, task_id: str, paths: list[str]
    ) -> LeaseDecision:
        """Take every path or none of them.

        The insert is the test: `ON CONFLICT(path) DO NOTHING` means a conflicting
        row already exists, and the rowcount says so without a read-then-write window
        for a second caller to slip through. A conflict against a lease this same
        task already holds is an allow — re-acquiring what you hold is idempotent,
        because leases are held for the whole task and one task writes a file more
        than once.

        Denial is a normal outcome, not an error: it returns, it does not raise.
        """
        wanted = list(dict.fromkeys(paths))
        if not wanted:
            return LeaseDecision()

        async with self.transaction() as db:
            acquired_at = isoformat(utcnow())
            inserted: list[str] = []
            contended: list[str] = []
            for path in wanted:
                cursor = await db.execute(
                    "INSERT INTO file_leases (path, agent_id, task_id, acquired_at) "
                    "VALUES (?, ?, ?, ?) ON CONFLICT(path) DO NOTHING",
                    (path, agent_id, task_id, acquired_at),
                )
                (inserted if cursor.rowcount else contended).append(path)

            denied: list[DeniedPath] = []
            for path in contended:
                holder = await self._holder_of(path)
                if holder.holder_task_id != task_id:
                    denied.append(holder)

            if not denied:
                for path in inserted:
                    await self.emit(
                        EventType.LEASE_ACQUIRED,
                        task_id=task_id,
                        agent_id=agent_id,
                        path=path,
                    )
                return LeaseDecision(granted=wanted)

            # Give back whatever this call took. Partial acquisition is hold-and-wait.
            await db.executemany(
                "DELETE FROM file_leases WHERE path = ?", [(path,) for path in inserted]
            )
            for holder in denied:
                await self.emit(
                    EventType.LEASE_DENIED,
                    task_id=task_id,
                    agent_id=agent_id,
                    path=holder.path,
                    holder_agent_id=holder.holder_agent_id,
                    holder_agent_name=holder.holder_agent_name,
                    holder_task_id=holder.holder_task_id,
                    reason=holder.reason,
                )
            return LeaseDecision(denied=denied)

    async def release_leases_for_task(self, task_id: str, reason: str) -> list[str]:
        """Release on terminal status or requeue, in the transaction that moved the task."""
        return await self._release("task_id", task_id, reason)

    async def release_leases_for_agent(self, agent_id: str, reason: str) -> list[str]:
        """Release when an agent is marked stale. This is what unblocks whoever it denied."""
        return await self._release("agent_id", agent_id, reason)

    async def list_leases(self) -> list[Lease]:
        rows = await self._fetchall("SELECT * FROM file_leases ORDER BY acquired_at, path")
        return [Lease.model_validate(dict(row)) for row in rows]

    # -- ledgers ------------------------------------------------------------

    async def record_change(self, *, agent_id: str, task_id: str, path: str, tool: str) -> None:
        """Append to the observational ledger and emit `file_changed`.

        Records intent: a write the agent made, including one it later reverts. That
        is the accepted cost of attributing changes per agent in a shared tree, where
        `git diff` cannot say who wrote what.
        """
        async with self.transaction() as db:
            await db.execute(
                "INSERT INTO file_changes (task_id, agent_id, path, tool, at) "
                "VALUES (?, ?, ?, ?, ?)",
                (task_id, agent_id, path, tool, isoformat(utcnow())),
            )
            await self.emit(
                EventType.FILE_CHANGED, task_id=task_id, agent_id=agent_id, path=path, tool=tool
            )

    async def list_changes(self, task_id: str | None = None) -> list[FileChangeRecord]:
        sql = "SELECT * FROM file_changes"
        params: list[Any] = []
        if task_id is not None:
            sql += " WHERE task_id = ?"
            params.append(task_id)
        sql += " ORDER BY id"
        return [
            FileChangeRecord.model_validate(dict(row)) for row in await self._fetchall(sql, params)
        ]

    async def emit(
        self,
        # `type` shadows the builtin, but it is the column name and the wire field.
        type: EventType,
        *,
        task_id: str | None = None,
        agent_id: str | None = None,
        **payload: Any,
    ) -> Event:
        """Append one event. The only writer of the `events` table.

        `EventType(type)` is the validation: an unknown type raises here rather than
        landing in the table for a consumer to trip over.
        """
        event_type = EventType(type)
        # Hand back the stored text rather than the datetime it came from: the column
        # keeps milliseconds, and the model parses that text back to an aware value,
        # so the returned event equals the one a consumer will read.
        at = isoformat(utcnow())
        async with (
            self.transaction() as db,
            db.execute(
                "INSERT INTO events (at, type, task_id, agent_id, payload) VALUES (?, ?, ?, ?, ?)",
                (at, event_type.value, task_id, agent_id, json.dumps(payload)),
            ) as cursor,
        ):
            return Event(
                id=cursor.lastrowid,
                at=at,
                type=event_type,
                task_id=task_id,
                agent_id=agent_id,
                payload=payload,
            )

    async def events_since(
        self, since: int, *, limit: int = 500, types: list[EventType] | None = None
    ) -> list[Event]:
        """Replay after `since`. The same code path serves the SSE tail and `GET /events`."""
        sql = "SELECT * FROM events WHERE id > ?"
        params: list[Any] = [since]
        if types:
            placeholders = ", ".join("?" * len(types))
            sql += f" AND type IN ({placeholders})"
            params += [EventType(t).value for t in types]
        sql += " ORDER BY id LIMIT ?"
        params.append(limit)
        return [_row_to_event(row) for row in await self._fetchall(sql, params)]

    async def last_event_id(self) -> int:
        row = await self._fetchone("SELECT COALESCE(MAX(id), 0) AS last FROM events")
        return int(row["last"])

    async def reset(self) -> None:
        """Truncate everything, including the autoincrement counters.

        Event ids restart at 1 so a re-run of the demo is byte-comparable with the
        previous one. There is nothing left for an old SSE cursor to point at.
        """
        async with self.transaction() as db:
            for table in _TABLES_CHILD_FIRST:
                await db.execute(f"DELETE FROM {table}")
            await db.execute("DELETE FROM sqlite_sequence WHERE name IN ('events', 'file_changes')")

    # -- internals ----------------------------------------------------------

    async def _require_task(self, task_id: str) -> Task:
        task = await self.get_task(task_id)
        if task is None:
            raise KeyError(f"no such task: {task_id}")
        return task

    async def _require_agent(self, agent_id: str) -> Agent:
        agent = await self.get_agent(agent_id)
        if agent is None:
            raise KeyError(f"no such agent: {agent_id}")
        return agent

    @staticmethod
    def _merge[M: BaseModel](
        current: M, fields: dict[str, Any], columns: Sequence[str], noun: str
    ) -> M:
        """Validate a partial update by re-validating the whole entity.

        Cheaper than a per-field schema and it means a bad enum value or a naive
        datetime is rejected before it reaches the database, not after.
        """
        unknown = set(fields) - set(columns)
        if unknown:
            raise ValueError(f"unknown {noun} field(s): {', '.join(sorted(unknown))}")
        if "id" in fields:
            raise ValueError(f"{noun} id is immutable")
        return type(current).model_validate(
            {**current.model_dump(), **fields, "updated_at": utcnow()}
        )

    async def _holder_of(self, path: str) -> DeniedPath:
        row = await self._fetchone(
            "SELECT l.path, l.agent_id, l.task_id, COALESCE(a.name, l.agent_id) AS agent_name "
            "FROM file_leases l LEFT JOIN agents a ON a.id = l.agent_id WHERE l.path = ?",
            (path,),
        )
        if row is None:
            raise KeyError(f"no lease on {path}")
        return DeniedPath(
            path=row["path"],
            holder_agent_id=row["agent_id"],
            holder_agent_name=row["agent_name"],
            holder_task_id=row["task_id"],
        )

    async def _release(self, column: str, value: str, reason: str) -> list[str]:
        """Delete every lease matching one holder column, emitting `lease_released` per path.

        `column` is a literal from the two callers below, never caller input.
        """
        async with self.transaction() as db:
            rows = await self._fetchall(
                f"SELECT path, agent_id, task_id FROM file_leases WHERE {column} = ? ORDER BY path",
                (value,),
            )
            if not rows:
                return []
            await db.execute(f"DELETE FROM file_leases WHERE {column} = ?", (value,))
            for row in rows:
                await self.emit(
                    EventType.LEASE_RELEASED,
                    task_id=row["task_id"],
                    agent_id=row["agent_id"],
                    path=row["path"],
                    reason=reason,
                )
            return [row["path"] for row in rows]
