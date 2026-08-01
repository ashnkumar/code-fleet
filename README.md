# CodeFleet

Run several Claude coding agents in parallel against one working tree, and stop the second one
before it overwrites the first one's file.

<!-- TODO: CI badge — needs the public repo URL. Points at .github/workflows/ci.yml, job `ci`. -->

![Three agents working one repository: T4 is denied a write to api.py because runner-2 holds it for T3, backs off, and succeeds on retry once the file is released.](docs/demo.gif)

That recording is a real run — three runners, five tasks, live Claude Agent SDK sessions. The red
`VETO` line is a write that did not happen.

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
target repository's own test suite. It leaves your checkout untouched. A run costs roughly $0.25 and
takes about a minute on the default model.

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
`examples/demo-tasks.yaml`. Every setting is an environment variable with a `CODEFLEET_` prefix and
also a flag; `.env.example` lists all of them.

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

The veto is a `PreToolUse` hook. Two more obvious options were tried first and rejected on evidence:

- **`can_use_tool`** looks like the right API — a permission callback the host controls. It requires
  streaming-input mode, and it is *shadowed* the moment you set `permission_mode="bypassPermissions"`.
  The SDK raises `CanUseToolShadowedWarning` and points you at `PreToolUse` itself.
- **`permission_mode="acceptEdits"`** did not write in a headless run. Tested: sessions completed,
  reported success, and had changed nothing on disk. `bypassPermissions` wrote correctly.

So: `bypassPermissions` plus a hook matched on `Write|Edit|MultiEdit|NotebookEdit`, which asks the
server and returns `deny` when the file is spoken for.

The cost of that choice, stated plainly: **the coordination server becomes the only thing standing
between an agent and the filesystem.** `bypassPermissions` takes the CLI's own permission rules out of
play. That is why the hook fails *closed* when the server is unreachable, why any path resolving
outside the working tree is denied locally, and why the hook body does nothing but one loopback call
with a hard timeout.

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
uv run pytest              # 183 tests, no network, no API key
uv run pytest -m live      # the two that spend money
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
- **A veto costs work.** An agent can edit correctly for a while and then be denied on a later file.
  What it already wrote stays in the tree while its task goes back to the queue. The veto lands at the
  *first* write to the contended file, and attempts are capped, so it is bounded — but it is not free.
- **Leases are per path, not per region.** Two agents editing unrelated functions in one large file
  serialize. Finer granularity means understanding the edit, which is a different system.
- **Single machine.** One server, SQLite, runners as local processes. No leader election, no remote
  runners.
- **No authentication.** Binds to `127.0.0.1` and assumes one trusted operator. Do not expose it.
- **No task planning.** Tasks come from the API or a YAML file. Nothing decomposes a request into a
  graph for you.
- **Agents are told to stay in scope, not forced to.** `file_scope` shapes scheduling and appears in
  the prompt. The lease is the enforcement; everything before it is advice.
- **POSIX only.** Tested on macOS and Linux.

## License

TODO — licensing is being settled separately. An MIT `LICENSE` is prepared but not committed.
