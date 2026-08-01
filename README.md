# CodeFleet

Run several Claude coding agents in parallel against one working tree, and stop the second one
before it overwrites the first one's file.

<!-- TODO: CI badge — needs the public repo URL. Points at .github/workflows/ci.yml, job `ci`. -->

![Three agents working one repository: T4 is denied a write to api.py because runner-2 holds it for T3, backs off, and succeeds on retry once the file is released.](docs/demo.gif)

That recording is a real run — three runners, five tasks, live Claude Agent SDK sessions. The red
`VETO` line is a write that did not happen.

It is also a real race, which is worth being precise about. Two of the five tasks declare different
files but both end up needing `api.py`, and whichever agent asks second is denied. Across live runs
that fires most of the time but not every time — sometimes one agent finishes and releases the file
before the other gets there, and the run reports that no write was vetoed. Which agent loses varies
too. `--dry-run` scripts the timing, so there the veto is deterministic; that is the version CI
asserts on.

## The problem

Starting several coding agents on one repository is easy. Finishing is not, because two things go
wrong and only one of them is obvious.

**Ordering.** Task B needs task A's output. Without a dependency graph you either run B too early
against a half-finished tree, or you serialize everything and lose the parallelism you wanted.

**Collision.** Two agents open the same file. Both read a stale version. Both write. The last writer
silently wins — nothing errors, nothing warns, and you find out later from a failing test.

Most multi-agent setups handle ordering and ignore collision, or handle collision by detecting
overlapping edits after the fact. Detection after the write has landed is not much use; the damage is
already in the tree.

CodeFleet puts both in one place. A coordination server owns the task graph and decides who runs
what. And before any agent writes a file, a `PreToolUse` hook asks that server for a lease on it. If
another agent holds it, the hook returns `permissionDecision: "deny"` and **the write never happens**.
The denied agent is told who holds the file, stops cleanly, and its task is requeued — with the
contended path folded into its file scope, so the scheduler runs it after the holder finishes rather
than rolling the dice again.

That is conflict prevention rather than conflict detection, and it is the reason this repository
exists.

## Quickstart

Needs Python 3.12+, [uv](https://docs.astral.sh/uv/), and an `ANTHROPIC_API_KEY`.

```bash
git clone <repo-url> && cd codefleet   # TODO: repo URL
uv sync
export ANTHROPIC_API_KEY=sk-ant-...
uv run codefleet demo
```

`uv sync` is the whole install — no Node, no npm, no separate Claude Code install. `claude-agent-sdk`
ships the CLI as a bundled binary and prefers it over anything on your `PATH`. It is large (~245 MB),
which is most of the install time.

`codefleet demo` copies `examples/demo-repo` to a scratch workspace, starts the server, loads a
five-task graph, runs three agents against it, renders the dashboard, and finishes by running the
target repository's own test suite. It leaves your checkout untouched. On the default model a run
costs about $0.20 and finishes in under a minute.

No API key, or want to see the machinery without spending anything:

```bash
uv run codefleet demo --dry-run
```

That swaps real agent sessions for scripted ones and exercises the identical coordination path —
same server, same scheduler, same leases, same veto. It is what CI runs.

If something looks wrong before you spend money:

```bash
uv run codefleet doctor
```

### Driving it yourself

```bash
uv run codefleet serve                      # coordination server
uv run codefleet load examples/demo-tasks.yaml
uv run codefleet run --runners 3            # start the fleet, wait for it to drain
uv run codefleet watch                      # the dashboard, in another terminal
uv run codefleet tasks --json               # machine-readable state
```

Point it at your own code with `CODEFLEET_WORKDIR`, and write your own graph in the shape of
`examples/demo-tasks.yaml`. Every setting is an environment variable with a `CODEFLEET_` prefix, and
every one except `CODEFLEET_RUN_DIR` is also a flag on the command it applies to — `--help` on that
command lists them. `.env.example` lists all of them, with defaults.

## How it works

```
                    ┌──────────────────────────────────────────┐
   codefleet run ──▶│  coordination server (FastAPI, SQLite)   │
   codefleet watch  │                                          │
        ▲           │   schedule(state, now) -> [Decision]     │
        │ SSE       │   pure · no I/O · no clock of its own    │
        └───────────│                                          │
                    └──────┬──────────────┬──────────────┬─────┘
                           │              │              │
                      ┌────▼───┐     ┌────▼───┐     ┌────▼───┐
                      │runner-1│     │runner-2│     │runner-3│
                      └────┬───┘     └────┬───┘     └────┬───┘
                           │  one Claude Agent SDK session per task
                           │
                           │  PreToolUse  ─▶ POST /leases/acquire ─▶ allow → write proceeds
                           │                                      ─▶ deny  → write vetoed
                           ▼
                    one shared working tree
```

Runners are deliberately stupid. Register, heartbeat, poll for an assignment, run one session, report
the result. They hold no queue, evaluate no dependencies, and make no decisions about who should do
what. All of that is server-side, in one place, where you can read it.

The scheduler is a pure function: `schedule(state, now) -> list[Decision]`. It takes a frozen snapshot
and returns what should happen. It cannot read a clock, open a socket, or touch the database — the
server applies the decisions it returns. That is why `tests/unit/test_scheduler.py` builds its state
literally and needs no fixtures, no event loop, and no database, and why each test reads as a
statement of one rule.

Start with `src/codefleet/scheduler.py`. It is the core of the whole thing and it is 337 lines.

### The interesting decision

The veto is a `PreToolUse` hook. Two more obvious options were rejected first, each for a reason you
can re-check against the SDK:

- **`can_use_tool`** looks like the right API — a permission callback the host controls. It requires
  streaming-input mode, and it is *shadowed* the moment you set `permission_mode="bypassPermissions"`.
  The SDK emits a `CanUseToolShadowedWarning` that points you at `PreToolUse` itself.
- **`permission_mode="acceptEdits"`** auto-accepts file edits and leaves every other tool on the
  standard permission path, which prompts — and an unattended runner has nobody to answer a prompt.
  It also buys nothing back: the deny still has to arrive as a `PreToolUse` hook either way, since
  that is the only place the server's answer can reach a tool call before it runs.

So: `bypassPermissions` plus a hook matched on `Write|Edit|MultiEdit|NotebookEdit`, which asks the
server and returns `deny` when the file is spoken for.

The cost of that choice, stated plainly: **the coordination server becomes the only thing standing
between an agent and the filesystem.** `bypassPermissions` takes the CLI's own permission rules out of
play. That is why the hook fails *closed* when the server is unreachable, why any path resolving
outside the working tree is denied locally, why the hook body does nothing but one loopback call with
a hard timeout, and why the session's tool set is an explicit allowlist with no shell on it — a write
the matcher never sees is a write the server never gets to veto.

Leases are per file and acquired lazily, at the first write — not up front from the declared
`file_scope`. Declared scope is a guess someone wrote before the agent read the code, and agents
follow the code. Locking a guess up front either strands the agent on a file it did not declare, or
forces it to widen a lock mid-task, which is hold-and-wait and therefore a real deadlock condition. A
lazy lease has neither: a denial is an immediate veto, never a wait, so no agent ever holds one lease
while waiting for another.

The demo graph exercises exactly that. T3 (`api.py`) and T4 (`middleware.py`) declare disjoint scopes,
so the scheduler runs them together — correctly, by its own rules. But T4 cannot finish without
registering its middleware in `api.py`, which T3 holds. The declaration was a good-faith guess and it
was wrong. The lease is what made that safe.

`SPEC.md` has the rest: the data model, every coordination rule, the full HTTP API, and the
alternatives that were considered and dropped.

## Tests

```bash
uv run pytest              # everything except the live tier: no network, no API key
uv run pytest -m live      # the live tier, which spends money
```

The whole coordination suite runs against a `ScriptedExecutor` — a runner whose brain is a script
instead of an SDK session. Assignment, cascade, stale-agent requeue, lease exclusion under
concurrency, veto-and-retry, attempt exhaustion: all of it, offline.

That is a claim, so something enforces it. `tests/unit/test_boundaries.py` walks the AST and asserts
that no module except `session.py` imports `claude_agent_sdk`, and that `runner.py` imports neither
the store nor the scheduler. If coordination logic ever leaks into a runner, a coordination test would
have to import the SDK to reach it, and that test breaks.

## Limitations

Real ones, not modesty.

- **One shared working tree. No git automation** — no worktrees, no branch per task, no commits, no
  PRs. The fleet edits files; you decide what to do with the diff. Worktrees would turn a write-time
  collision into a merge-time collision, which is a harder problem and a different project.
- **A failed attempt leaves its edits behind.** An agent can edit correctly for a while and then be
  denied on a later file, or time out. What it already wrote stays in the tree, and once its task is
  requeued those files are unlocked — so a half-applied change is visible to every other agent and to
  the retry. The veto lands at the *first* write to the contended file and attempts are capped, so it
  is bounded, but "the fleet finished" does not mean "only completed work is in the tree". Undoing
  this properly means running each attempt somewhere disposable, which is the isolation trade below.
- **Revocation cannot recall a write already in flight.** Cancelling a task, or reaping a runner that
  stopped heartbeating, bumps that runner's epoch — but the runner only learns it was fenced on its
  next call to the server. If its `PreToolUse` hook had already returned *allow*, that write is
  authorized and can still land after the lease was released and handed to someone else. The window
  is small and confined to the recovery paths, but the "one writer per file" guarantee is not
  absolute across them. Closing it needs a write path the server can invalidate mid-flight — per
  attempt isolation, or a broker that revalidates the lease at write time — not a faster fence.
- **The lease is exclusivity, not authorization.** It answers "is anyone else holding this file",
  never "is this task allowed to touch this file". Any uncontended path in the tree is granted, so a
  confused agent can rewrite something unrelated and the veto will not object — it was never asked to.
  `file_scope` steers the scheduler and the prompt; it does not bound the blast radius.
- **Leases are per path, not per region.** Two agents editing unrelated functions in one large file
  serialize. Finer granularity means understanding the edit, which is a different system.
- **Single machine.** One server, SQLite, runners as local processes. No leader election, no remote
  runners.
- **No authentication.** Binds to `127.0.0.1` and assumes one trusted operator. Do not expose it.
- **No task planning.** Tasks come from the API or a YAML file. Nothing decomposes a request into a
  graph for you.
- **A green run is not a verified run.** A task becomes `succeeded` when its session ends cleanly and
  reports success, which means the model stopped — not that what it wrote compiles. The coordination
  layer has no way to tell those apart from the inside, so pass `codefleet run --verify "pytest -q"`
  and let the exit code decide; `codefleet demo` does this with the target repository's own suite.
  Verification is fleet-level by necessity: agents share one tree, so running a suite mid-flight
  would fail on somebody else's half-written file and blame the wrong task.
- **Agents cannot run commands.** The session is given exactly
  `Read`/`Write`/`Edit`/`MultiEdit`/`NotebookEdit`/`Glob`/`Grep`; `Bash` is deliberately not among
  them. Bash is a write path the veto cannot see: `sed -i`, a formatter, `cat > file` — none of it
  reaches a hook matched on tool names, so it would take no lease and appear in no ledger row, and
  the veto would hold for structured edits while silently not holding for shell ones. Gating it
  instead means deciding what an arbitrary shell command writes. The price is paid by the tasks: one
  cannot run the tests it just wrote, or a linter, or `git`. Verification happens to the tree after
  the fleet drains — which is what `codefleet demo` does with the target repository's own suite.
- **POSIX only.** Tested on macOS and Linux.

## License

TODO — licensing is being settled separately. An MIT `LICENSE` is prepared but not committed.
