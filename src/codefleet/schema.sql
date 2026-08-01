-- CodeFleet schema. Applied by Store.open() on every connect; every statement is
-- idempotent, so opening an existing database is a no-op.
--
-- Timestamps are ISO-8601 UTC text with a `Z` suffix (2026-07-31T18:04:22.117Z).
-- That form sorts lexicographically, so ORDER BY on a timestamp column is a plain
-- text comparison, and it is readable straight out of the sqlite3 CLI.
--
-- Connection pragmas (WAL, busy_timeout, foreign_keys) are set in Python: they are
-- per-connection state, not schema, and journal_mode is persistent but the others
-- are not.

CREATE TABLE IF NOT EXISTS agents (
    id                TEXT PRIMARY KEY,
    name              TEXT NOT NULL UNIQUE,
    status            TEXT NOT NULL,
    epoch             INTEGER NOT NULL,
    current_task_id   TEXT,
    workdir           TEXT NOT NULL,
    pid               INTEGER,
    last_heartbeat_at TEXT NOT NULL,
    last_assigned_at  TEXT,
    tasks_succeeded   INTEGER NOT NULL DEFAULT 0,
    tasks_failed      INTEGER NOT NULL DEFAULT 0,
    input_tokens      INTEGER NOT NULL DEFAULT 0,
    output_tokens     INTEGER NOT NULL DEFAULT 0,
    cost_usd          REAL    NOT NULL DEFAULT 0.0,
    registered_at     TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id                TEXT PRIMARY KEY,
    title             TEXT NOT NULL,
    description       TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',
    priority          INTEGER NOT NULL DEFAULT 3,
    -- JSON array of relative POSIX paths. A scheduling hint, not a lock; the
    -- server widens it after a veto so the retry is not co-scheduled again.
    file_scope        TEXT NOT NULL DEFAULT '[]',
    assigned_agent_id TEXT REFERENCES agents(id),
    attempts          INTEGER NOT NULL DEFAULT 0,
    max_attempts      INTEGER NOT NULL DEFAULT 3,
    backoff_until     TEXT,
    result_summary    TEXT,
    error             TEXT,
    error_kind        TEXT,
    blocked_on_path   TEXT,
    input_tokens      INTEGER NOT NULL DEFAULT 0,
    output_tokens     INTEGER NOT NULL DEFAULT 0,
    cost_usd          REAL    NOT NULL DEFAULT 0.0,
    duration_ms       INTEGER,
    session_id        TEXT,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    assigned_at       TEXT,
    started_at        TEXT,
    completed_at      TEXT
);

-- The scheduler's hot query: pending tasks in (priority DESC, created_at) order.
CREATE INDEX IF NOT EXISTS idx_tasks_queue ON tasks (status, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_agent ON tasks (assigned_agent_id);

-- The only stored representation of the graph. There is no blocked_by column;
-- runnability is a join (SPEC 4.1). Cycles are rejected before insert.
CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id       TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on_id)
);

-- The PK above indexes the forward edge; the cascade walk needs the reverse one.
CREATE INDEX IF NOT EXISTS idx_deps_depends_on ON task_dependencies (depends_on_id);

-- The PRIMARY KEY on `path` *is* the mutual exclusion. Acquisition is
-- INSERT ... ON CONFLICT(path) DO NOTHING and the decision is the rowcount, so two
-- racing acquisitions cannot both succeed. There is deliberately no expires_at:
-- leases are released when the holding task goes terminal or is requeued, and when
-- the holding agent is marked stale, both of which already happen in a transaction
-- that touches the task anyway.
CREATE TABLE IF NOT EXISTS file_leases (
    path        TEXT PRIMARY KEY,
    agent_id    TEXT NOT NULL,
    task_id     TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leases_task ON file_leases (task_id);
CREATE INDEX IF NOT EXISTS idx_leases_agent ON file_leases (agent_id);

-- Observational ledger written from PostToolUse. Never read by the scheduler.
CREATE TABLE IF NOT EXISTS file_changes (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id  TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    path     TEXT NOT NULL,
    tool     TEXT NOT NULL,
    at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_changes_task ON file_changes (task_id);

-- Append-only. `id` is the SSE cursor, which is why it is AUTOINCREMENT: rowids of
-- deleted rows must never be handed out again, or a reconnecting client would skip
-- events. Nothing updates or deletes a row here except Store.reset().
CREATE TABLE IF NOT EXISTS events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    at       TEXT NOT NULL,
    type     TEXT NOT NULL,
    task_id  TEXT,
    agent_id TEXT,
    payload  TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_events_type ON events (type, id);
