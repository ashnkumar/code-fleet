# CodeFleet — specification

Status: implemented. This document is the contract the code is written against and the tests are
written from; where the two disagree, one of them is a bug.

---

## 1. What this is

CodeFleet is a coordination server for a fleet of parallel Claude coding agents working on one
shared checkout. The server owns the task graph, decides which agent gets which task, cascades
dependencies as tasks finish, and — the part that matters — **vetoes a file write in flight when
another agent already holds that file**. Runners are deliberately thin: register, heartbeat, poll
for an assignment, run one Claude Agent SDK session, report the result. They contain no
coordination logic at all.

### The problem

Running several coding agents at once against the same repository is easy to start and hard to
finish. Two failure modes dominate:

1. **Ordering.** Task B needs task A's output. Without a dependency graph the fleet either runs B
   too early against a half-finished tree, or you serialize everything and lose the parallelism you
   wanted.
2. **Collision.** Two agents open the same file, both read a stale version, both write. The last
   writer silently wins. Nothing errors. You find out later, from a test.

Most multi-agent demos ignore (2), or address it after the fact by detecting overlapping edits and
reporting them. Detection after the write has already landed is not much use — the damage is
already in the working tree.

CodeFleet handles (1) with a dependency graph the server evaluates on every state change, and (2)
with a per-file lease checked *before* the write happens, via a `PreToolUse` hook that can return
`permissionDecision: "deny"`. The denied agent is told which agent holds the file, stops cleanly,
and its task is requeued with a widened file scope so the scheduler will not co-schedule the two
again. The write never lands. That is conflict prevention, not conflict detection.

The secondary goal is legibility. Every state change is one row in one append-only events table.
That table drives an SSE stream, a terminal dashboard, and a replayable record of any run. If you
want to know what the fleet did, you read one table.

---

## 2. Components

| Component | Module | Responsibility |
| --- | --- | --- |
| Coordination server | `codefleet.server` | FastAPI app. The HTTP API, the SSE stream, the app factory — the wire: request in, transition, response and event out. One process. |
| Engine | `codefleet.engine` | The tick loop and every transition it applies. Sits between the scheduler and the server and imports no web framework: the rules here are about rows and events, not requests. |
| Store | `codefleet.store` | aiosqlite, WAL mode. All persistence. Every write in the system goes through here. |
| Scheduler | `codefleet.scheduler` | `schedule(state, now) -> list[Decision]`. Pure function. No I/O, no async, no database, no clock of its own. |
| Runner | `codefleet.runner` | Thin agent process. Registers, heartbeats, polls, runs one SDK session per task, reports. Thin is defined by what it may not import — neither `codefleet.store` nor `codefleet.scheduler` — not by a line budget; `tests/unit/test_boundaries.py` enforces it. |
| SDK session | `codefleet.session` | Builds `ClaudeAgentOptions`, installs the two hooks, drains the message stream, extracts usage. |
| CLI | `codefleet.cli` | `codefleet serve`, `codefleet run`, `codefleet watch`, `codefleet tasks`, `codefleet load`, `codefleet demo`, `codefleet doctor`, `codefleet reset`. |
| Dashboard | `codefleet.dashboard` | Rich terminal view. Consumes `GET /events/stream`. Read-only; holds no state the server does not have. |
| Models | `codefleet.models` | Entities, transient DTOs, and every enum the rest of the system is typed against — including the gated write-tool set the hook matcher is built from. |
| Configuration | `codefleet.config` | `Settings`. One environment-backed object, no config file; `.env.example` is its documentation. |
| Demo target | `examples/demo-repo` | `linkstash`, a ~130-line stdlib URL shortener with a passing test suite of its own. The codebase the fleet edits. |
| Demo graph | `examples/demo-tasks.yaml` | Five tasks shaped to force one real dependency cascade and one real write veto. |

### Diagram

```
   operator                ┌──────────────────────────────────────────────────────┐
   ────────                │   coordination server  —  FastAPI, single process    │
   codefleet run ─────────▶│                                                      │
   codefleet tasks         │   HTTP API ────────────▶ store: SQLite (WAL)         │
   codefleet watch         │       │                    tasks  task_deps  agents  │
        ▲                  │       │                    file_leases  file_changes │
        │ SSE              │       │                    events  (append-only)     │
        └──────────────────│  scheduler tick loop                                 │
                           │       │                                              │
                           │       └─ schedule(state, now) -> [Decision]          │
                           │            pure · no I/O · unit-tested with no DB    │
                           └───┬──────────────────┬──────────────────┬────────────┘
        assignment / veto /    │                  │                  │
        cascade over HTTP      │                  │                  │
                          ┌────▼─────┐       ┌────▼─────┐       ┌────▼─────┐
                          │ runner-1 │       │ runner-2 │       │ runner-3 │
                          └────┬─────┘       └────┬─────┘       └────┬─────┘
                               │  one Claude Agent SDK session per task
                               │
                               │  PreToolUse  <write tools> ──▶ POST /leases/acquire
                               │                               ──▶ allow  →  write proceeds
                               │                               ──▶ deny   →  write vetoed
                               │  PostToolUse <write tools> ──▶ POST /changes  (ledger)
                               │  <write tools> = Write|Edit|MultiEdit|NotebookEdit
                               ▼
                     ┌───────────────────────────────────────┐
                     │  ONE shared working tree              │
                     │  (examples/demo-repo by default)      │
                     └───────────────────────────────────────┘
```

### Interaction rules

- Runners talk to the server over HTTP only. They never touch the database.
- The server never calls a runner. All traffic is runner→server or operator→server.
- The scheduler is called by the engine's tick loop, in the server process, and returns decisions;
  the loop applies them inside a single transaction. The scheduler itself cannot write anything.
- The dashboard is a pure consumer of the SSE stream. Turning it off changes nothing.

---

## 3. Data model

SQLite. All timestamps are timezone-aware UTC, stored as ISO-8601 text with a `Z` suffix
(`2026-07-31T18:04:22.117Z`), which sorts lexicographically and is readable from the `sqlite3` CLI.
Every `datetime` in Python is aware; naive datetimes are rejected at the model boundary.

IDs are text. Tasks may carry a caller-supplied id (`T1`) so a YAML graph can express its own edges;
otherwise the server generates `t_<12 hex>`. Agents get `a_<12 hex>` at first registration and keep
it across restarts (§3.3, §4.4).

### 3.1 `tasks`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | TEXT PK | caller-supplied or `t_<hex>` |
| `title` | TEXT NOT NULL | |
| `description` | TEXT NOT NULL | becomes the agent prompt body |
| `status` | TEXT NOT NULL | `TaskStatus`, default `pending` |
| `priority` | INTEGER NOT NULL | 1–5, default 3, higher runs first |
| `file_scope` | TEXT (JSON array) | declared paths, relative POSIX. **Advisory scheduling hint, not a lock.** May be widened by the server after a veto. |
| `assigned_agent_id` | TEXT NULL | FK `agents.id` |
| `attempts` | INTEGER NOT NULL | incremented on each transition into `assigned` |
| `max_attempts` | INTEGER NOT NULL | default 3 |
| `backoff_until` | TEXT NULL | not schedulable before this instant |
| `result_summary` | TEXT NULL | |
| `error` | TEXT NULL | |
| `error_kind` | TEXT NULL | `ErrorKind` |
| `blocked_on_path` | TEXT NULL | the path that caused the last veto |
| `input_tokens` | INTEGER NOT NULL | cumulative across attempts, default 0 |
| `output_tokens` | INTEGER NOT NULL | cumulative, default 0 |
| `cost_usd` | REAL NOT NULL | cumulative, default 0.0 |
| `duration_ms` | INTEGER NULL | last attempt only |
| `session_id` | TEXT NULL | SDK session id of the last attempt |
| `created_at` | TEXT NOT NULL | |
| `updated_at` | TEXT NOT NULL | set by a single `store.touch()` helper; never by hand at call sites |
| `assigned_at` | TEXT NULL | |
| `started_at` | TEXT NULL | |
| `completed_at` | TEXT NULL | |

Indexes: `(status, priority DESC, created_at)`, `(assigned_agent_id)`.

### 3.2 `task_dependencies`

| Column | Type | Notes |
| --- | --- | --- |
| `task_id` | TEXT | FK `tasks.id`, ON DELETE CASCADE |
| `depends_on_id` | TEXT | FK `tasks.id` |

PK `(task_id, depends_on_id)`. This is the **only** stored representation of the graph. There is no
`blocked_by` column. Runnability is derived (§4.1). Cycles are rejected at insert time by a
topological check over the batch plus the existing graph; the insert is a single transaction, so a
graph either lands whole or not at all.

### 3.3 `agents`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | TEXT PK | `a_<hex>`, stable across restarts |
| `name` | TEXT UNIQUE NOT NULL | slot name, e.g. `runner-1` |
| `status` | TEXT NOT NULL | `AgentStatus` |
| `epoch` | INTEGER NOT NULL | incremented on every registration and on every forced requeue. Fencing token. |
| `current_task_id` | TEXT NULL | |
| `workdir` | TEXT NOT NULL | absolute path the runner actually passes as SDK `cwd`. Written, not decorative. |
| `pid` | INTEGER NULL | |
| `last_heartbeat_at` | TEXT NOT NULL | |
| `last_assigned_at` | TEXT NULL | when this slot last took work. The scheduler's tiebreak: idle agents are picked longest-idle first (§4.2 step 4). |
| `tasks_succeeded` | INTEGER NOT NULL | lifetime, survives restarts |
| `tasks_failed` | INTEGER NOT NULL | lifetime |
| `input_tokens` | INTEGER NOT NULL | lifetime |
| `output_tokens` | INTEGER NOT NULL | lifetime |
| `cost_usd` | REAL NOT NULL | lifetime |
| `registered_at` | TEXT NOT NULL | first registration |
| `updated_at` | TEXT NOT NULL | |

### 3.4 `file_leases`

| Column | Type | Notes |
| --- | --- | --- |
| `path` | TEXT PK | relative POSIX path inside the workdir. **The primary key is the mutual exclusion.** |
| `agent_id` | TEXT NOT NULL | |
| `task_id` | TEXT NOT NULL | |
| `acquired_at` | TEXT NOT NULL | |

Leases have no expiry of their own. They are released when the holding task reaches a terminal state
or is requeued, and when the holding agent is marked stale — both of which already happen in a
transaction that has to touch the task anyway. A third expiry clock would be a third thing that can
disagree with the other two.

Exclusion is enforced by the schema, not by application logic: acquisition is
`INSERT INTO file_leases ... ON CONFLICT(path) DO NOTHING` inside a transaction, and the decision is
the rowcount. Two racing acquisitions cannot both succeed.

### 3.5 `file_changes`

Observational ledger, written from `PostToolUse`. Never read by the scheduler.

| Column | Type |
| --- | --- |
| `id` | INTEGER PK AUTOINCREMENT |
| `task_id` | TEXT NOT NULL |
| `agent_id` | TEXT NOT NULL |
| `path` | TEXT NOT NULL |
| `tool` | TEXT NOT NULL (`Write` \| `Edit` \| `MultiEdit` \| `NotebookEdit`) |
| `at` | TEXT NOT NULL |

### 3.6 Conflicts are not a table

A conflict is the moment a lease acquisition was denied, and that moment is already one
`lease_denied` row in `events` carrying the path, the holder, and the requester. A second table
would be a denormalized copy of data the events table already owns, kept in sync by hand.

`GET /conflicts` is therefore a projection: select `lease_denied` events, join each requester task's
current status, and report `resolved` when that task has since succeeded. One source of truth, and
the resolution status is computed from the thing that actually determines it rather than stamped by
whoever remembered to stamp it.

### 3.7 `events`

| Column | Type | Notes |
| --- | --- | --- |
| `id` | INTEGER PK AUTOINCREMENT | monotonic. **This is the SSE cursor.** |
| `at` | TEXT NOT NULL | |
| `type` | TEXT NOT NULL | `EventType` |
| `task_id` | TEXT NULL | nullable — fleet-level events belong to no task |
| `agent_id` | TEXT NULL | nullable — server-originated events belong to no agent |
| `payload` | TEXT (JSON object) | typed on the dimension you group by, free-form underneath |

Append-only. Nothing updates or deletes a row here except `codefleet reset`. All events are written
through one `store.emit(type, **fields)` helper which validates `type` against the enum, so an
unknown event type is impossible and an unemitted enum member is caught by a test that asserts every
member appears at least once in the demo run.

### 3.8 Enums

**`TaskStatus`** — 7 values, every one written by some code path:

| Value | Meaning |
| --- | --- |
| `pending` | Created or requeued. Not necessarily runnable — unmet dependencies are derived, not stored. |
| `assigned` | Server has picked an agent. Lease running. The agent may not have polled yet. |
| `running` | The agent confirmed start. An SDK session is live. |
| `succeeded` | Terminal. |
| `failed` | Terminal. Attempts exhausted, or cancelled mid-flight upstream. |
| `blocked_upstream` | Terminal-until-intervention. A dependency failed permanently. |
| `cancelled` | Terminal. Operator asked. |

**`AgentStatus`** — 3 values: `idle`, `busy`, `offline`. `offline` is recoverable (§4.3).

**`ErrorKind`** — `agent_error` (the session reported `is_error`), `veto` (denied a lease it needed),
`timeout` (wall-clock exceeded), `budget` (SDK `max_budget_usd` / `max_turns` stop), `infra` (the
runner or the SDK blew up), `attempts_exhausted`, `cancelled`.

**`EventType`**:

```
task_created        task_assigned      task_started       task_succeeded
task_failed         task_requeued      task_unblocked     task_blocked_upstream
task_cancelled      lease_acquired     lease_denied       lease_released
file_changed        agent_registered   agent_online       agent_offline
fleet_started       fleet_idle         run_finished
```

Deliberately absent: `heartbeat`. Heartbeats are a liveness signal, not a state change; emitting one
per agent per interval would flood the table that the dashboard and the recording read from, at a
rate proportional to fleet size and unrelated to how much work is happening.

### 3.9 Transient DTOs (not persisted as tables)

`TaskResult` — what a runner returns from one session: `ok`, `summary`, `error`, `error_kind`,
`blocked_on_path`, `input_tokens`, `output_tokens`, `cost_usd`, `duration_ms`, `session_id`,
`files_written`. It is the contract between "the thing that executes" and "the thing that records",
and it is exactly the body of `POST /tasks/{id}/complete`.

`FleetState` — a frozen snapshot (`tasks`, `deps`, `agents`, `leases`, `now`) handed to the pure
scheduler. Constructed literally in unit tests.

### 3.10 Fields deliberately not modelled

The schema above is small on purpose. Each of the following is an obvious thing to put in a task or
agent row, and each is left out for a reason worth stating.

| Not modelled | Why |
| --- | --- |
| Embeddings of the task description (`semantic_text`, `dense_vector`, or similar) | Their only consumer would be a "find similar tasks" query. Semantic dedup of tasks is a non-goal (§8). An embedding column commits the schema, the write path and a model dependency to a feature nothing in the coordination path reads. |
| A dashboard definition format (saved objects, data views, NDJSON) | The dashboard is a client of `GET /events/stream`, not a document imported into a separate tool. A dashboard that ships as a definition file has to be re-created wherever it lands; one that reads the events table works the moment the server is up. |
| Planner artifacts — decomposition prompts, tool and agent declarations | Tasks come from a YAML file or `POST /tasks`. Task decomposition by an LLM is out of scope (§8, D17). |
| `blocked_by` on the task | It double-books `task_dependencies`. Two stored representations of one graph need something to keep them equal, and every write path that touches one and not the other is drift — the failure mode being a task whose `depends_on` is set, whose blocker list is empty, and which is therefore immediately eligible for assignment. Runnability is derived instead (§4.1). |
| `estimated_complexity` | Nothing would read it, and nothing here schedules on it — ordering is `(priority, created_at, id)`. A field whose validator quietly coerces an unrecognised value to a default is worse than no field: it accepts bad input and hands back a confident answer. |
| `labels` | Nothing would read it. Free-form tagging is a task-tracker feature; the scheduler needs `priority`, `file_scope` and the edge table. |
| `Agent.capabilities` | Capability routing is a non-goal (§8). Any idle agent can take any task, so the assigner matches on `status` and idleness alone, and there is nothing on the task side to match capabilities against. |
| `Agent.type` | One value. A single-member enum is a column that carries no information. |
| `Task.branch_name`, `Task.pr_url` | Git automation is a non-goal (§8). |
| `Agent.session_id` | A session belongs to an attempt, not to an agent. An agent outlives many sessions, so a session id parked on the agent row is overwritten by the next task and cannot identify the one you want. It lives on `tasks.session_id`. |
| `FileChange.commit_sha`, `lines_added`, `lines_removed`, `branch_name` | Line counts and commits belong to a `git diff` feature that does not exist here (§8). The ledger records which tool touched which path, at the instant it touched it. |
| Parallel `agent_ids` / `task_ids` / `file_paths` arrays on a conflict | Three unpaired lists lose the association — given two of each you cannot say who touched what. One row per (path, holder, requester) keeps it (§3.6). |
| A `ConflictStatus` lifecycle (`detected` / `resolving` / `resolved` / `escalated`) | Every state in a lifecycle needs something that drives the transition into it and something that acts on it. Neither exists here. Resolution is derived from the requester task's current status instead (§3.6). |
| `AgentStatus.paused`, `AgentStatus.error` | Unreachable: no code path would assign either. `idle` / `busy` / `offline` is the whole lifecycle, and an error is a property of a task attempt, not of an agent. |
| A required `agent_id` on events | It would make server-originated events unrepresentable, and there are plenty of them — `fleet_idle`, `task_created`, `task_blocked_upstream` belong to no agent. Nullable (§3.7). |

---

## 4. Coordination rules

Everything in this section is stated as a rule with a test attached to it. The scheduler is a pure
function, so most of these are testable with three lines of setup and no database.

### 4.1 Runnability

A task is **runnable** at instant `now` iff all of:

1. `status == pending`
2. every row in `task_dependencies` for it points at a task with `status == succeeded`
3. `attempts < max_attempts`
4. `backoff_until is null or backoff_until <= now`

There is no stored `blocked` flag and no `blocked` status. Blockedness is a join. At fleet-sized data
the join is free, and it removes an entire class of drift: a stored blocker list is a cached answer,
and any write path that updates the edges without updating the cache leaves a task with an unmet
dependency looking immediately eligible for assignment.

### 4.2 Assignment

The tick produces **as many assignments as there are idle agents**, not one.

```
def schedule(state: FleetState, now: datetime) -> list[Decision]
```

Algorithm, in order:

1. `runnable` = tasks satisfying §4.1.
2. Sort `runnable` by `(-priority, created_at, id)`. The trailing `id` makes the ordering total, so
   scheduler tests are deterministic without freezing the clock.
3. `busy_scope` = the union of (a) `file_scope` of every task in `assigned` or `running`, and
   (b) every `path` in `file_leases`.
4. `idle` = agents with `status == idle` and a fresh heartbeat, sorted by
   `(last_assigned_at ASC, name ASC)` — longest-idle first, so work spreads across the fleet.
5. Greedy scan of `runnable`. For each task: if its `file_scope` is disjoint from `busy_scope` and an
   idle agent remains, emit `Assign(task, agent)`, add its scope to `busy_scope`, consume the agent.
   If the scope intersects, **skip and keep scanning** — a scope-conflicted high-priority task must
   not head-of-line-block the rest of the queue.
6. Stop when agents run out.

`Assign` applies, in one `BEGIN IMMEDIATE` transaction: `tasks.status = assigned`,
`assigned_agent_id`, `assigned_at`, `attempts += 1`, `agents.status = busy`,
`current_task_id`, and `emit(task_assigned)`.

**Assignment is the claim.** There is no separate claim step and no compare-and-swap dance, because
there is exactly one scheduler in exactly one process and the transition happens inside one
transaction. A runner polling `GET /agents/{id}/assignment` is *reading* a decision that has already
been made and durably recorded — it cannot lose a race it was never in.

**Ticking.** The loop wakes on either (a) an `asyncio.Event` set by any write that could create
readiness — task created, task terminal, lease released, agent registered, agent went idle — or
(b) a 500 ms timer, as a reconciliation sweep. Fast reactive path, slow safety net.

**Why the tick is a batch, and why it lives in this process.** Assignment is a matching loop over all
idle agents, not a single pair per tick. A per-tick single assignment caps fleet throughput at one
task per interval regardless of how many runners are idle — a five-runner fleet needs five ticks
merely to become fully busy, and no amount of shortening the interval fixes the shape of that. The
interval and the body are two separate knobs and the body is the one that matters.

The loop is ordinary Python in the server process rather than a definition handed to an external
workflow engine. That keeps the policy callable from a unit test with no infrastructure standing
(§6.3), and it means there is no deploy step whose silent failure would leave the fleet running with
no scheduler at all.

### 4.3 Dependency cascade

On `task_succeeded`, in the same transaction:

1. Find every task with a `task_dependencies` row pointing at the completed task.
2. For each, count remaining dependencies whose status is not `succeeded`. If zero, `emit(task_unblocked)`.
3. Set the scheduler wakeup event.

No status is mutated by the cascade. `pending` tasks were already `pending`; they simply became
runnable. `task_unblocked` is emitted for observability, not because anything reads it back.

On `task_failed` or `task_cancelled`, transitively mark every downstream dependent
`blocked_upstream` and emit `task_blocked_upstream` with the failing ancestor id in the payload.
Failure has to cascade as deliberately as success does. If only the success path fires a cascade,
dependents of a failed task sit `pending` forever with no signal: they are never runnable, nothing
ever says so, and the fleet reads as idle-and-healthy while work is silently stranded. Naming the
failing ancestor is what makes that state diagnosable and what `retry` walks back.

`POST /tasks/{id}/retry` on a `failed` task resets it to `pending` with `attempts = 0`, and
recursively returns its `blocked_upstream` dependents to `pending`.

### 4.4 Stale agents, requeue, and held leases

- Heartbeat interval: 5 s (`CODEFLEET_HEARTBEAT_INTERVAL`).
- Stale threshold: 20 s — four missed beats (`CODEFLEET_STALE_AFTER`).

When the scheduler observes `now - last_heartbeat_at > stale_after` for an agent that is not already
`offline`, it emits `MarkOffline(agent)`. Applying it, in one transaction:

1. `agents.status = offline`, `current_task_id = null`, `agents.epoch += 1`.
2. **Release every lease held by that agent**, emitting `lease_released` with `reason="agent_stale"`
   for each. This is what unblocks whoever was denied by the dead agent.
3. Its task, if in `assigned` or `running`, goes back to `pending` (or to `failed` with
   `error_kind=attempts_exhausted` if `attempts >= max_attempts`), with `backoff_until` set.
4. `emit(agent_offline)`, `emit(task_requeued)`, set the wakeup event.

Note the requeue covers **both** `assigned` and `running`. A runner flips its task to `running` as
soon as it picks it up, so recovering only `assigned` tasks would leave the recovery window
milliseconds wide: in practice every task orphaned by a dying agent would sit `running` forever,
holding its leases, recoverable only by a human running a reset.

**Fencing.** Bumping `epoch` is what makes the requeue safe. Every runner→server call carries
`X-Agent-Epoch`. A zombie runner that comes back to life after its task was reassigned gets `409
Conflict` on its next heartbeat, lease acquisition, or completion report. It aborts the session and
re-registers. Without this, a stale-but-alive process could write to a task another agent now owns.

**Recovery.** An `offline` agent that heartbeats again returns to `idle` and emits `agent_online`.
Re-registering under the same `name` reuses the same row and bumps `epoch`, so lifetime counters
accumulate across restarts. Both halves matter. If a heartbeat only refreshed a timestamp without
restoring status, and the sweep that watches agents ignored the `offline` ones, the fleet would
shrink monotonically over a long run and no restart could grow it back.

**Two independent failure modes, two independent mechanisms.** Heartbeat staleness catches a *dead*
runner. It cannot catch a *wedged* one: the heartbeat loop and the work loop are separate asyncio
tasks by necessity, so a runner whose SDK session has hung goes on heartbeating cheerfully and looks
perfectly healthy to a liveness check. CodeFleet therefore adds a per-task wall-clock timeout (§4.7)
enforced on both sides — liveness and progress are different questions and need different answers.

### 4.5 File leases

| Property | Decision |
| --- | --- |
| Granularity | One lease per file path. Not per directory, not per declared scope. |
| Path normalization | Resolved against the workdir, symlinks resolved, converted to a relative POSIX path. Any path resolving outside the workdir is denied with `reason="outside_workdir"`. |
| Acquired | Lazily, at the *first write attempt* to that path, from `PreToolUse`. |
| Held | For the remainder of the task, not per-write. A half-applied edit that loses its file mid-task leaves the tree broken. |
| Released | On task terminal (`succeeded`/`failed`/`cancelled`) or requeue, in the same transaction as the status change. Also on agent stale. |
| Idempotence | Re-acquiring a lease you already hold is an allow, not a conflict. |
| Server unreachable | The hook **fails closed** — deny. Failing open reintroduces the exact collision the system exists to prevent. The task fails with `error_kind=infra` and is retryable. |

**The veto path**, end to end:

1. The agent calls `Write(file_path="linkstash/api.py", ...)`.
2. The runner's `PreToolUse` hook (matcher `Write|Edit|MultiEdit|NotebookEdit`) extracts the path(s)
   from `tool_input` and POSTs `/leases/acquire`.
3. The server attempts `INSERT ... ON CONFLICT(path) DO NOTHING` for each path in one transaction.
   All-or-nothing: if any path is held by another agent, no lease is taken and the whole request is
   denied. (Partial acquisition would create exactly the hold-and-wait condition we are avoiding.)
4. On denial the server emits `lease_denied` carrying the path, the holder and the requester, and
   returns the holder's agent name and task id.
5. The hook returns

   ```json
   {"hookSpecificOutput": {
      "hookEventName": "PreToolUse",
      "permissionDecision": "deny",
      "permissionDecisionReason": "linkstash/api.py is held by runner-2 for task T3. Do not retry this file or edit around it. Stop now and report that you are blocked on linkstash/api.py."
   }}
   ```

6. The write does not happen. The agent stops and reports being blocked — verified empirically.
7. The runner reports `ok=false, error_kind=veto, blocked_on_path="linkstash/api.py"`.
8. The server requeues the task with `attempts += 1`, `backoff_until = now + backoff`, **and widens
   `file_scope` to include the denied path**. On the next tick step 3 of §4.2 will no longer
   co-schedule it with the holder, so the retry runs after the holder finishes.

Step 8 is the loop-closer: it turns a one-off veto into a scheduling fact, so the retry is not a
coin flip.

**Why lazy leases cannot deadlock.** A denied write is an *immediate veto*, never a wait. No agent
ever holds a lease while waiting for another lease, so there is no hold-and-wait, no wait-for cycle,
and therefore no deadlock. The cost is wasted work, not a stall — and the attempt cap bounds it.

### 4.6 Conflict recording

A denial is one `lease_denied` event and nothing else (§3.6). `GET /conflicts` projects those events
against the requester task's current status, so a conflict reads as `resolved` exactly when the task
that was denied has since succeeded. There is no resolution workflow, no escalation state and no
approval queue: each of those is a state that needs an actor to enter it and an actor to leave it,
and here there is neither. A status nobody drives is a status that is always wrong.

### 4.7 Retries, attempt caps, and timeouts

| Rule | Value |
| --- | --- |
| `max_attempts` | 3 per task, per-task overridable |
| Attempt counted | on transition into `assigned` — so a lost assignment counts too |
| Backoff | `min(2s * attempts, 30s)`, deterministic, no jitter (single scheduler, no thundering herd) |
| Retryable kinds | `agent_error`, `veto`, `timeout`, `infra` |
| Non-retryable | `cancelled`; and any kind once `attempts >= max_attempts` → `failed` with `error_kind=attempts_exhausted` |
| Task wall clock | 600 s default (`CODEFLEET_TASK_TIMEOUT`), enforced by the runner |
| Server-side lease | `assigned_at + task_timeout + 60s` grace; expiry requeues even if the runner never reports |
| SDK-side guards | `max_turns=40`, `max_budget_usd` per task (`CODEFLEET_TASK_BUDGET_USD`, default 0.50) |

A vetoed task therefore cannot spin forever: at most `max_attempts` sessions, each bounded by wall
clock, turn count, and dollar budget. In the worst case it lands in `failed` with
`error_kind=attempts_exhausted` and `blocked_on_path` set, its dependents go `blocked_upstream`, and
the fleet drains cleanly instead of livelocking.

None of these bounds is optional, and each removes a state whose only exit is a human typing a
command. Without an attempt count, `failed` is terminal on the first stumble and a transient error
costs the whole task. Without backoff, a retry re-collides with the same holder immediately. Without
a wall clock, a wedged session is invisible to a liveness check that only watches heartbeats and the
task never ends at all.

### 4.8 Run completion

The fleet is **done** when no task is `pending`, `assigned`, or `running`. The scheduler emits
`fleet_idle` on the first tick where that holds. `codefleet run` exits when it sees `run_finished`
(emitted after `fleet_idle` once all runners have reported), printing a summary table and the exit
code `0` if every task succeeded, `1` otherwise.

---

## 5. External interfaces

Base URL `http://127.0.0.1:8099` by default. JSON everywhere. Errors are
`{"error": {"code": "...", "message": "...", "detail": {...}}}` with conventional status codes.

### 5.1 Runner-facing endpoints

All of these require the header `X-Agent-Epoch: <int>` except `register`. A mismatch returns `409`
with `code="stale_epoch"`, and the runner's contract on `409` is: abort the current session, discard
the result, re-register.

#### `POST /agents/register`

```json
→ {"name": "runner-1", "workdir": "/abs/path/to/demo-repo", "pid": 41823}
← 200 {"agent_id": "a_9f2c1d40aa31", "epoch": 3, "status": "idle",
       "heartbeat_interval_s": 5, "poll_interval_s": 1,
       "task_timeout_s": 600, "server_time": "2026-07-31T18:04:22.117Z"}
```

Upsert on `name`. Increments `epoch`. Releases any leases the previous incarnation held and requeues
its in-flight task. Emits `agent_registered` (first time) or `agent_online`.

#### `POST /agents/{agent_id}/heartbeat`

```json
→ {}
← 200 {"status": "idle", "epoch": 3}
```

There is no separate "your task was taken away" flag. Anything that removes a task from an agent —
operator cancel, stale requeue — bumps that agent's `epoch`, so the very next call the runner makes
returns `409 stale_epoch` and it aborts and re-registers. One mechanism covers both cases.

#### `DELETE /agents/{agent_id}`

Graceful deregistration. Sets `offline`, releases leases, requeues any in-flight task. `204`.

#### `GET /agents/{agent_id}/assignment`

```json
← 200 {"task": {"id": "T3", "title": "...", "description": "...",
                "priority": 5, "file_scope": ["linkstash/api.py"],
                "attempts": 1, "deadline": "2026-07-31T18:14:22.117Z",
                "blocked_on_path": null}}
← 204   (nothing assigned)
```

Pure read. Assignment already happened server-side.

#### `POST /tasks/{task_id}/start`

```json
→ {"agent_id": "a_9f2c1d40aa31"}
← 200 {"status": "running"}
← 409 {"error": {"code": "not_owner"}}
```

#### `POST /leases/acquire`

```json
→ {"agent_id": "a_9f...", "task_id": "T4", "paths": ["linkstash/api.py"], "tool": "Edit"}
← 200 {"decision": "allow", "granted": ["linkstash/api.py"]}
← 200 {"decision": "deny",
       "denied": [{"path": "linkstash/api.py",
                   "holder_agent_id": "a_1b...", "holder_agent_name": "runner-2",
                   "holder_task_id": "T3", "reason": "held"}],
       "message": "linkstash/api.py is held by runner-2 for task T3."}
```

Always `200` — a denial is a normal outcome, not an HTTP error. All-or-nothing across `paths`.

#### `POST /changes`

```json
→ {"agent_id": "a_9f...", "task_id": "T4", "path": "linkstash/middleware.py", "tool": "Write"}
← 202 {}
```

Fire-and-forget ledger write from `PostToolUse`. The hook does not block on the response beyond a
short timeout and never vetoes.

#### `POST /tasks/{task_id}/complete`

```json
→ {"agent_id": "a_9f...", "ok": false,
   "summary": null,
   "error": "Blocked: linkstash/api.py is held by runner-2.",
   "error_kind": "veto",
   "blocked_on_path": "linkstash/api.py",
   "input_tokens": 8421, "output_tokens": 613, "cost_usd": 0.0094,
   "duration_ms": 15612, "session_id": "sess_...",
   "files_written": ["linkstash/middleware.py"]}
← 200 {"status": "pending", "attempts": 2, "backoff_until": "2026-07-31T18:05:00.000Z"}
```

Idempotent on `(task_id, agent_id, attempt)`: a duplicate report for an attempt already recorded is a
no-op `200`.

### 5.2 Operator-facing endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/tasks` | Create a graph. Body: `{"tasks": [{title, description, priority?, file_scope?, depends_on?, max_attempts?, id?}, ...]}`. One task and fifty take the same path. Atomic: validates acyclicity and that every `depends_on` resolves, then inserts the batch or nothing. `201 {"created": ["T1", ...]}`. |
| `GET` | `/tasks` | `?status=&limit=&offset=`. Returns the task rows plus a derived `runnable` boolean and `unmet_dependencies` list. |
| `GET` | `/tasks/{id}` | One task, its dependencies, its dependents, and its file changes. |
| `POST` | `/tasks/{id}/cancel` | Any non-terminal → `cancelled`. Releases leases, tells the holding runner via the heartbeat response. |
| `POST` | `/tasks/{id}/retry` | `failed` → `pending`, `attempts=0`; recursively un-blocks `blocked_upstream` dependents. |
| `GET` | `/agents` | All agents with derived `stale` boolean. |
| `GET` | `/leases` | Current leases with holder and age. |
| `GET` | `/conflicts` | Projection over `lease_denied` events (§3.6). `?resolved=` filter. |
| `GET` | `/events` | `?since=<id>&limit=&type=`. Paged replay. |
| `GET` | `/events/stream` | SSE (§5.3). |
| `GET` | `/state` | One snapshot: tasks + agents + leases + counters + `last_event_id`. What the dashboard fetches on connect before subscribing. |
| `GET` | `/health` | `{"ok": true, "version": "...", "db": "ok", "uptime_s": ...}`. Works with an empty database and no agents. |
| `POST` | `/reset` | Truncates everything. `403` unless `CODEFLEET_ALLOW_RESET=1`. |

Every read endpoint returns JSON, and the CLI renders it. There is no data path that exists only as
formatted terminal text: a command whose output exists only as a rendered table has nothing to assert
on but its formatting, which is why such commands end up with no test at all.

### 5.3 SSE stream

`GET /events/stream?since=<event_id>`

Replays every event after `since`, then streams live. A dropped client reconnects by passing the
last id it saw back as `since`, so the integer `events.id` primary key is doing real work: it is the
cursor, and replay and live tail are the same code path.

```
id: 1043
event: lease_denied
data: {"id":1043,"at":"2026-07-31T18:05:07.441Z","type":"lease_denied",
       "task_id":"T4","agent_id":"a_9f2c1d40aa31",
       "payload":{"path":"linkstash/api.py","holder_agent_name":"runner-2","holder_task_id":"T3"}}

: ping
```

A `: ping` comment every 15 s keeps intermediaries from closing an idle stream. The `event:` field
carries the `EventType`, so a client can subscribe selectively.

Because the stream is a pure projection of an append-only table, a run is fully replayable:
`GET /events?since=0` after the fact reproduces the dashboard frame-for-frame, and the demo recording
is generated from a real run rather than staged.

### 5.4 Configuration

Environment, `CODEFLEET_` prefix, every variable listed in a committed `.env.example`.

| Variable | Default | Notes |
| --- | --- | --- |
| `CODEFLEET_HOST` / `CODEFLEET_PORT` | `127.0.0.1` / `8099` | |
| `CODEFLEET_DB` | `./codefleet.db` | |
| `CODEFLEET_WORKDIR` | `./examples/demo-repo` | The shared tree. It defaults to the target repo that ships with this one, never to the current directory — a first run points N `bypassPermissions` sessions at a known throwaway checkout rather than at whatever the operator happened to be standing in. |
| `CODEFLEET_RUNNERS` | `3` | |
| `CODEFLEET_MODEL` | `claude-haiku-4-5-20251001` | Overridable. |
| `CODEFLEET_TASK_TIMEOUT` | `600` | Seconds. |
| `CODEFLEET_TASK_BUDGET_USD` | `0.50` | SDK `max_budget_usd`. |
| `CODEFLEET_MAX_TURNS` | `40` | |
| `CODEFLEET_HEARTBEAT_INTERVAL` | `5` | Seconds. |
| `CODEFLEET_STALE_AFTER` | `20` | Seconds. |
| `CODEFLEET_POLL_INTERVAL` | `1` | Seconds; how often a runner asks for an assignment. |
| `CODEFLEET_TICK_INTERVAL` | `0.5` | Seconds; the reconciliation sweep. |
| `CODEFLEET_MAX_ATTEMPTS` | `3` | |
| `CODEFLEET_RUN_DIR` | `./runs` | Where `codefleet demo` puts each run's workspace, database and per-runner stderr, one timestamped directory per run; `codefleet run` writes its per-runner stderr straight into it. |
| `CODEFLEET_ALLOW_RESET` | unset | Guards `POST /reset`. |
| `ANTHROPIC_API_KEY` | — | Read by the SDK, not by CodeFleet. |

Every setting above is also a flag on the command it applies to — `codefleet serve --help` carries
the server and liveness knobs, `codefleet run --help` the fleet and session ones — with the single
exception of `CODEFLEET_RUN_DIR`, which is environment-only. Nothing is read from a config file the
code does not document.

---

## 6. The interesting decisions

### 6.1 The write veto is a `PreToolUse` hook returning `deny`

This is the load-bearing mechanism, and the two obvious alternatives were rejected on reasons that
can be re-checked against the SDK, not on taste.

**Rejected: `can_use_tool`.** It looks like the right API — a permission callback the host controls.
It is not usable here. It requires streaming-input mode (`ValueError` otherwise), and it is
*shadowed* the moment `permission_mode="bypassPermissions"` is set: the SDK emits
`CanUseToolShadowedWarning` whose text explicitly directs you to a `PreToolUse` hook instead. It is
also shadowed by any whole-tool entry in `allowed_tools` (`"Write"`, `"Write()"`, `"Write(*)"`), and
by `skills="all"`, which silently appends a bare `Skill` entry. Too many ways to disable it by
accident.

**Rejected: `permission_mode="acceptEdits"`.** It auto-accepts file edits and leaves everything else
on the standard permission path, which prompts (`ClaudeAgentOptions.permission_mode`,
claude-agent-sdk 0.2.128). An unattended runner has nobody to answer a prompt. It also buys none of
the machinery back: the veto still has to arrive as a `PreToolUse` deny either way, because that is
the only place the server's answer can reach the tool call before it runs. So `acceptEdits` is a
weaker posture for the same amount of code.

An earlier revision of this section claimed something stronger — that headless `acceptEdits` sessions
completed, reported success, and had written nothing at all. That does not reproduce against the
pinned SDK: `acceptEdits` writes, both through a bare `query()` and through this repository's own
`run_session` with its hooks installed. Nothing in the repository ever demonstrated the no-write
result, so the claim is withdrawn rather than re-explained.

**Chosen: `bypassPermissions` + a `PreToolUse` hook.** Empirically, a hook returning
`permissionDecision: "deny"` vetoes the write, and the denied agent stops cleanly and reports being
blocked rather than retrying or editing around it. A matching `PostToolUse` hook reliably yields
`tool_name` and `tool_input.file_path` for `Write`/`Edit`, which is the ledger.

The cost of this choice, stated plainly: **the coordination server is now the only safety boundary.**
`bypassPermissions` means the CLI's own permission rules are not in play, so a bug in the hook is a
bug in the only thing standing between an agent and the filesystem. That is why the hook fails
*closed* on an unreachable server, why path normalization rejects anything resolving outside the
workdir, and why the hook body does nothing but one loopback HTTP call with a hard timeout — a
`PreToolUse` hook that misses its deadline does not fall through to the tool; the CLI stops the turn
with "the tool call was not executed", which would look like a mysterious agent failure.

**The session is given an explicit tool set, and `Bash` is not in it.** `tools=` is pinned to `Read`,
`Write`, `Edit`, `MultiEdit`, `NotebookEdit`, `Glob`, `Grep` — the four write tools the matcher
gates, plus three that cannot mutate the tree. Note that this is `tools=`, not `allowed_tools=`:
under `bypassPermissions` an `allowed_tools` entry would only pre-approve a tool the session already
has, whereas `tools=` removes everything unnamed from the session entirely, which is the difference
between a permission list and a boundary.

`Bash` is excluded because it is an *ungated* write path — `sed -i`, `cat > f`, a formatter, a
codegen script, a test run that rewrites a fixture — and none of it reaches a `PreToolUse` matcher
keyed on tool names. Such a write takes no lease and lands in no `file_changes` row, so the veto
would hold for structured edits and silently not hold for shell ones, which is worse than not holding
at all: the ledger would under-report what the fleet touched while reading as complete. Gating `Bash`
instead would mean deciding which paths an arbitrary shell command writes, which is parsing arbitrary
shell, and being wrong once is exactly the collision this system exists to prevent. `Task` is out for
the same reason at one remove: a subagent gets its own tool set and would take `Bash` back with it.

The cost is real and is not hidden: **an agent cannot run anything inside a task** — not the tests it
just wrote, not a linter, not `git`. Tasks have to be expressible as edits, and verification is
something that happens to the tree afterwards; `codefleet demo` runs the target repository's own
suite itself, once, after the fleet drains.

Two smaller decisions fall out of this:

- **Matcher, not `if`.** Hooks are registered as
  `HookMatcher(matcher="Write|Edit|MultiEdit|NotebookEdit", hooks=[...])` rather than a match-all
  matcher with a Python-side tool-name filter. For that pattern the CLI does exact set membership on
  a `|`-split list; the intent is declarative and reviewable in one line.
- **`setting_sources=[]`.** The SDK default is `None`, which loads the *host user's* `~/.claude`
  settings, agents, skills, and `CLAUDE.md` — so the tool, agent and skill set in scope is whatever
  that particular developer happens to have installed, and a demo that does not pin it behaves
  differently on every reader's laptop. With `[]` the session sees the CLI built-ins and nothing
  else, and the `init` frame proves which (§6.4). Note the trap: setting `skills=` silently flips
  `setting_sources` back to
  `["user", "project"]`, so if skills are ever enabled, `setting_sources` must be passed explicitly
  in the same call.

### 6.2 Lazy per-file leases, not pre-declared scope locks

Two designs were available.

**Pre-declare and lock up front.** Take a lock on every path in `file_scope` at assignment. Pro:
a task that gets assigned is guaranteed to be able to finish; no wasted work. Con: `file_scope` is a
*guess written by a human before the agent read the code*. Agents follow the code. Locking a
declared scope up front means either (a) the agent hits a file it did not declare and is stuck, or
(b) you must let it widen its lock mid-task — which is hold-and-wait, which is a real deadlock
condition needing a real deadlock detector. And an over-declared scope serializes the fleet for no
reason.

**Acquire lazily at first write.** Pro: the lease set is exactly what the agent actually touched; no
guessing; and because a denial is an immediate veto rather than a wait, there is no hold-and-wait and
so no deadlock. Con: partial work. An agent can do fifteen minutes of correct editing and then be
vetoed on the sixteenth file, and everything it already wrote stays in the tree while its task goes
back to `pending`.

**Chosen: lazy, with declared scope demoted to a scheduling hint.** `file_scope` is used in §4.2
step 3 to avoid the obvious collisions cheaply — two tasks that both declare `config.py` are simply
not co-scheduled — while the lease catches the ones the declaration missed. The partial-work problem
is mitigated three ways: the veto arrives at the *first* write to the contended file, so the losing
agent stops early rather than late; the denied path is folded back into `file_scope` so the retry is
scheduled against reality (§4.5 step 8); and `max_attempts` bounds the total wasted work.

This is exactly what the demo graph exercises. T3 (`linkstash/api.py`) and T4 (`linkstash/middleware.py`)
declare disjoint scopes, so the scheduler runs them together — correctly, by its own rules. But T4
cannot finish without registering its middleware in `api.py`, which T3 holds. T4 is denied, backs
off, and succeeds on retry once T3 releases. The declared scope was a good-faith guess and it was
wrong; the lease is what made that safe.

The point of `file_scope` is that the *scheduler* reads it. A declared scope that only ever reaches
the agent as a sentence in its prompt is documentation, not coordination: it cannot stop two tasks
from being started against the same file, and it makes the fleet's behaviour depend on whether a
model chose to honour a request. Here it is an input to step 3 of the assignment algorithm, and the
lease is the enforcement behind it.

### 6.3 Dumb runner, smart server — and how to falsify it

The claim is that runners contain zero coordination logic. Claims like that rot unless something
enforces them.

**The falsifiable test.** The substitution is one seam wide: the *real* `Runner` takes an `Executor`,
and `ScriptedExecutor` executes a task by writing a scripted file and returning a scripted result
instead of running an SDK session. Everything else — register, heartbeat, poll, start, acquire
leases, report — is the shipping code path, not a re-implementation of it, which is what makes the
test falsifiable rather than a parallel universe that agrees with itself. **Every coordination test
in the suite runs that way, and passes.** Assignment ordering, dependency cascade, stale-agent
requeue, lease exclusion under concurrency, veto-and-retry, attempt exhaustion, `blocked_upstream`
propagation — all of it, with no API key, no network, and no `claude_agent_sdk` import.

The enforcement is mechanical, in `tests/unit/test_boundaries.py`: a test asserts that no module
under `codefleet/` other than `codefleet.session` imports `claude_agent_sdk`, and a second asserts
that `codefleet.runner` imports neither `codefleet.store` nor `codefleet.scheduler` — the import ban
is the checkable form of "holds no coordination logic and cannot reach the database". If coordination
logic ever leaks into the runner, a coordination test would have to import the SDK to exercise it,
and that test fails.

The corollary is that the scheduler is a pure function. `schedule(state, now) -> [Decision]` takes a
frozen snapshot and returns decisions; it cannot read a clock, open a socket, or touch SQLite. Its
tests construct `FleetState` literally and assert on the returned list. There is no fixture, no
event loop, and no mock database in any scheduler test.

**Rejected alternative: runners claim their own work.** A runner could `SELECT ... FOR UPDATE`-style
claim a pending task directly. That is fewer moving parts, but it pushes the priority ordering, the
dependency join, and the scope-disjointness check into every runner — three copies of the policy that
must agree — and it makes the "swap in a scripted executor" test meaningless, because the
scripted executor would have to reimplement the scheduler.

**Rejected alternative: one Claude session, many subagents.** Using the SDK's `agents` /
`Task`-spawning to fan out inside a single session is simpler and cheaper. It also makes the
coordination invisible — there is no queue to inspect, no state to resume, no way to add a runner
mid-run, and no place to stand between an agent and a file write. The whole point here is that the
coordination is externalized and legible.

### 6.4 Smaller decisions worth recording

- **`query()`, not `ClaudeSDKClient`.** `ClaudeSDKClient` offers a cooperative `interrupt()`, which
  is the tidier way to enforce a wall clock. It also requires streaming-input mode and a connect /
  disconnect lifecycle to get right. `query()` is one call, it is the path measured to work end to
  end, and the session is already bounded on three axes the SDK enforces itself (`max_turns`,
  `max_budget_usd`, and the model's own stopping). The wall clock is an `asyncio.wait_for` backstop
  around the generator. Honest cost: on that backstop path a cancellation can skip the SDK's
  terminate/kill escalation and orphan a CLI subprocess, so the runner reaps its child explicitly on
  timeout. This is the simpler thing that works; if the reaping proves unreliable, the upgrade path
  to `ClaudeSDKClient` is local to one module.
- **Token accounting from `ResultMessage.model_usage`.** `ResultMessage.usage` is an untyped
  passthrough of the *final* API call's usage plus an `iterations` array — measured at 136/82 tokens
  in a session whose true totals were 657/94. `model_usage` is the typed, cumulative,
  per-model record (`inputTokens`, `outputTokens`, `costUSD`), and its `costUSD` sum matches
  `total_cost_usd`. Also: the CLI emits one `AssistantMessage` per content block, all sharing a
  `message_id` and repeating the same `usage` dict, so summing per-message double-counts.
- **`SystemMessage(subtype="thinking_tokens")` is dropped at the runner.** In a trivial three-turn
  run, 21 of 32 messages were of that subtype. Persisting them would flood the events table the
  dashboard reads, and the SDK's internal stream buffers only 100 messages — a slow consumer
  throttles the agent.
- **`max_buffer_size` is raised above the 1 MiB default.** A single tool result containing a large
  file overflows a single NDJSON line and kills the session with a `CLIJSONDecodeError` that is not
  recoverable.
- **Per-runner `stderr` callback.** With `options.stderr=None` the child inherits the parent's
  stderr and three sessions interleave unattributed diagnostics. Each runner captures its own to a
  file under `runs/`.
- **Capture the `SystemMessage(subtype="init")` frame.** It is the authoritative record of
  `session_id`, `cwd`, `model`, `permissionMode`, and the exact tool/agent/skill set in scope — which
  is how a run proves it was hermetic.

---

## 7. Decision record

The questions a design like this has to answer, and how each was answered.

| # | Question | Decision | Reasoning |
| --- | --- | --- | --- |
| D1 | Is `blocked_by` stored or derived? | Derived from `task_dependencies`. | Storing the graph twice means storing one fact in two shapes that can disagree, and something has to keep them equal on every write. One edge table gives both semantics — declared edges and remaining blockers — and has nothing to drift against. |
| D2 | Is `blocked` a status or a predicate? | A predicate. `blocked_upstream` is a *different* thing and is a real status. | A status is a durable fact somebody has to act on. Blocked-by-dependency is neither: it is recomputed from the graph on every tick and clears itself when the dependency lands. Blocked-because-an-ancestor-failed is durable and does need operator action, so it earns a status. |
| D3 | Are `assigned` and `running` really two states? | Yes, both kept. | They fail differently: `assigned` but never started means the runner died between the decision and the poll; `running` means a session is live and may be wedged. Both are requeued, but the diagnostics differ. |
| D4 | Is assignment separate from claiming? | No. Assignment *is* the claim. | One scheduler, one process, one transaction. Adding a claim step would add a race that does not currently exist. |
| D5 | Is an agent an identity or a process instance? | An identity, keyed by `name`, with an `epoch` that identifies the instance. | Minting a fresh id per registration orphans every lifetime counter the moment a runner restarts, and restarts are routine. A name-keyed row makes "this slot has succeeded 40 tasks and spent $2" a true statement across the life of the fleet; `epoch` carries the per-instance part, which is what fences the zombie. |
| D6 | Is `offline` terminal? | No. Heartbeat or re-registration returns an agent to `idle`. | A terminal `offline` makes the fleet shrink monotonically: any runner that misses four beats is gone for good and restarting the process cannot return the capacity. Recovery has to be the same path as arrival. |
| D7 | Timezone policy? | All datetimes timezone-aware UTC; stored as ISO-8601 `...Z` text; naive datetimes rejected at the model boundary. | Mixing naive `datetime.utcnow()` with `Z`-suffixed strings makes comparing two timestamps a `TypeError` that only appears when a stored row meets a fresh one — late, in production, on the staleness path. One representation, enforced at the boundary, makes the whole class unrepresentable. |
| D8 | How are file changes observed — hooks or `git diff`? | `PostToolUse` hooks. | In one shared tree with N concurrent agents, `git diff` cannot attribute a change to an agent. Hooks give per-agent, per-tool attribution in real time. Accepted tradeoff: hooks record intent, including edits later reverted. |
| D9 | Does `FileChange` survive as an entity? | Yes, as a narrow `(task, agent, path, tool, at)` ledger. | Narrow and indexable, and it is the audit trail for "what did this run actually touch". Recording it once as a typed row — rather than also as an untyped blob inside an event payload — means one place to query and no second copy to keep honest. |
| D10 | Does `Conflict` persist, or is it a query? | Persists, as immutable rows. No resolution state machine. | The moment of denial is a fact worth keeping; a four-state lifecycle with nothing driving it is not. |
| D11 | Where does token/cost accounting come from? | `ResultMessage.model_usage`, summed across models; cost from `total_cost_usd`. Per-task totals only, accumulated per agent. | `ResultMessage.usage` is the last API call's usage, not the session's, and per-`AssistantMessage` summing double-counts. |
| D12 | What is the isolation model? | One shared working tree. No git worktrees, no branches. | The problem being solved is *coordination on one codebase*. Worktrees would convert a write-time collision into a merge-time collision, which is a strictly harder problem and a different project. A shared tree with a write-time veto is the honest version of this shape: N agents really are editing one checkout, and the protection is checked before the write rather than promised in a document. |
| D13 | What is the concurrency-control story? | One process, one scheduler, SQLite WAL, `BEGIN IMMEDIATE` for the assignment transaction, `busy_timeout` set. Lease exclusion is a PK constraint, not application logic. | Without it, two schedulers could assign the same task twice and safety would rest on the accident that assignment happens once per tick. Making exclusion a primary key means the database enforces it whatever the application believes. |
| D14 | Do heartbeats emit events? | No. Only state transitions do. | Heartbeats are liveness, not history. Emitting them would flood the table the dashboard, the SSE stream, and the recording all read from. |
| D15 | Does a failed task strand its dependents? | No. They are explicitly marked `blocked_upstream` with the failing ancestor named. | A cascade that fires only on success leaves dependents of a failed task `pending` forever: never runnable, never reported, and indistinguishable from work that has not started yet. Naming the ancestor turns a silent stall into a diagnosable state that `retry` can undo. |
| D16 | What happens when the coordination server is unreachable from a hook? | Deny. Fail closed. Task fails `infra`, retryable. | Failing open reintroduces the exact collision the system exists to prevent. |
| D17 | Is there a planner that decomposes a request into tasks? | Not in v1. Tasks come from `POST /tasks` or a YAML file. | It is a separable concern, and it is the part of a system like this that most easily absorbs the entire complexity budget. The interesting claim here is the coordination, and a planner would sit cleanly on top of the same API later. |
| D18 | Is the CLI an HTTP client or does it touch the database? | HTTP client, exclusively. | A second writer is a second set of invariants. A CLI that writes to the store directly can leave state the server's own rules would never have produced, and the two disagree about what the fleet is doing. One writer, one set of invariants. |
| D19 | Can revocation recall a write the hook has already authorized? | No, and the design says so rather than implying otherwise. | Fencing bumps an epoch; the runner learns about it on its next call. A `PreToolUse` hook that already returned *allow* has authorized a filesystem operation nothing can now intercept, so on the cancel, stale and deadline paths a write can land after its lease was released to another task. The fence is as fast as it can be and is still not synchronous with the tool call. Making it synchronous means a write path the server can invalidate mid-flight — per-attempt isolation, or a broker that revalidates the lease at write time — which is a different and larger system (see D12). Documented as a bounded hole in the recovery paths, not papered over. |
| D20 | Does a requeue undo what the failed attempt already wrote? | No. Its leases are released and its edits stay. | Restoring the pre-attempt tree means knowing what the attempt changed and being sure nobody else has since touched it — in one shared tree with N live agents, neither holds. The honest consequence is that failed and cancelled attempts still influence the final checkout, and it is stated in the README rather than described as merely wasted work. This is the strongest argument for per-attempt isolation, and the reason D12 is a trade rather than a free choice. |
| D21 | Is the lease an authorization boundary? | No. It is mutual exclusion only. | Acquisition asks whether another task holds the path, never whether this task should be touching it — any uncontended path in the tree is granted. Making declared scope authoritative would break the premise the design rests on: scope is a guess written before the agent read the code, and the demo turns on exactly that guess being wrong. So blast radius is bounded by the working tree, not by the task, and `file_scope` is documented as advisory everywhere it appears. An opt-in strict mode is the obvious next feature; shipping it off-by-default and unexercised would be worse than saying plainly that it does not exist yet. |
| D22 | What does `succeeded` mean? | The session ended cleanly and reported success. Not that the code works. | Nothing inside the coordination layer can distinguish an agent that solved the task from one that stopped early and said it had. That distinction has to come from outside, so `codefleet run --verify <command>` runs the caller's own checks over the tree once the fleet drains and its exit code decides the run's. Fleet-level rather than per-task on purpose: agents share one tree, so a suite run mid-flight fails on another agent's half-finished edit and blames the wrong task. Per-task verification needs per-task isolation. |

---

## 8. Non-goals

Explicitly out of scope for this repository. Each is a defensible feature; none is needed to
demonstrate the thing this is about, and several would obscure it.

- **Git automation.** No worktrees, no branch-per-task, no commits, no PRs. The fleet edits the
  working tree; the human decides what to do with the diff. (See D12.)
- **Merge-conflict resolution.** Prevention at write time is the claim. Reconciling divergent
  branches is a different, larger problem.
- **LLM task decomposition / planning.** (D17.)
- **Capability-based routing.** Any idle agent can take any task. Adding capability matching means
  matching against something on the task, and neither side of that pairing exists yet.
- **Multi-host fleets.** One coordination server, runners on the same machine, SQLite. Distributing
  it would mean swapping the store and adding leader election, and it would not make the veto more
  interesting.
- **Authentication, authorization, multi-tenancy.** The server binds to `127.0.0.1` by default and
  assumes a trusted single-operator context. This is stated in the README rather than half-built.
- **A web UI.** The dashboard is a terminal client over SSE. The SSE stream is public API, so a web
  UI is a downstream project, not a missing feature.
- **Semantic search over tasks / duplicate detection.** No embeddings, no similarity search over
  descriptions. Nothing in the coordination path reads them (§3.10).
- **Session resumption after a crash.** Sessions are per-task and per-attempt; a failed attempt
  starts fresh. Resuming would require persisting the session id at the `init` message and a defined
  resume path, and a fresh attempt against a partially-modified tree is the more predictable
  behaviour.
- **Cost governance beyond per-task caps.** There is a per-task `max_budget_usd` and a turn cap.
  There is no fleet-wide budget, no spend alerting, and no rate-limit backoff policy beyond
  respecting the SDK's `RateLimitEvent`.
- **Windows support.** POSIX paths, POSIX signals, tested on macOS and Linux.
- **A container image.** There is no external service to stand up: the store is an in-process SQLite
  file and the only network dependency is the Anthropic API. `claude-agent-sdk` ships the Claude Code
  CLI as a bundled ~245 MB binary and prefers it over anything on `PATH`, so `uv sync` alone produces
  a working runner with no Node, no npm, and no separate CLI install. A Dockerfile would add a large
  image, a mounted working tree and a forwarded API key in exchange for nothing. The quickstart is
  `uv sync && uv run codefleet demo`.

---

## 9. Testing contract

Stated here because it constrains the design, not because it is an implementation detail.

| Tier | Runs | Needs |
| --- | --- | --- |
| Scheduler unit tests | always | nothing. No DB, no event loop, no fixtures. `FleetState` is built literally. |
| Store + API tests | always | a temp-file SQLite DB and `httpx.ASGITransport`. No network. |
| Coordination tests | always | the real `Runner` with a `ScriptedExecutor`, against the real server. No API key, no `claude_agent_sdk` import. |
| Live end-to-end | `-m live`, deselected by default | `ANTHROPIC_API_KEY`. Runs the demo graph against `examples/demo-repo` and asserts: the cascade fired, exactly one veto occurred, the vetoed task succeeded on retry, and `demo-repo`'s own test suite still passes. |

The three always-tiers are unconditional. They import nothing optional, so there is no guard that
could turn the suite green by skipping itself, and a test that asserts a coordination rule asserts
the real thing rather than the behaviour of a mock standing in for it.

Specific tests the design owes:

- Every `EventType` member is emitted at least once during the scripted demo run.
- N scripted runners racing for one path: exactly one `allow`, N−1 `deny`, one lease row.
- A stale agent's leases are released in the same transaction as its task requeue.
- A vetoed task's `file_scope` contains the denied path afterwards, and the scheduler no longer
  co-schedules it with the holder.
- `attempts` exhaustion moves a task to `failed` and its dependents to `blocked_upstream`.
- No module under `codefleet/` except `codefleet.session` imports `claude_agent_sdk`.
- No module under `codefleet.runner` imports `codefleet.store`.
- The SSE stream replayed from `since=0` reproduces the same terminal frames as the live run.
