# CodeFleet

The file lock for parallel Claude coding agents.

[![ci](https://github.com/ashnkumar/code-fleet/actions/workflows/ci.yml/badge.svg)](https://github.com/ashnkumar/code-fleet/actions/workflows/ci.yml)
[![python](https://img.shields.io/badge/python-3.12+-blue)](https://www.python.org/downloads/)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

![Three agents working one repository: T4 is denied a write to api.py because runner-2 holds it for T3, backs off, and succeeds on retry once the file is released.](docs/demo.gif)

A real run: five tasks, three agents, one tree. The red `VETO` is the moment two agents reached for
the same file — the second one was refused instead of overwriting it, and finished on retry. That is
a genuine race, so it does not fire on every live run; `--dry-run` makes the timing deterministic,
which is what CI asserts on.

*See the **[technical post](https://example.com/codefleet-technical-post)** for more details.*

## Quickstart

```bash
git clone https://github.com/ashnkumar/code-fleet && cd code-fleet
uv sync
export ANTHROPIC_API_KEY=sk-ant-...
uv run codefleet demo
```

Needs Python 3.12+ and [uv](https://docs.astral.sh/uv/). `uv sync` is the whole install — no Node, no
npm, no separate Claude Code install, because `claude-agent-sdk` ships the CLI as a bundled binary.

`codefleet demo` copies `examples/demo-repo` to a scratch workspace, runs three agents over a
five-task graph, and finishes by running that repo's own test suite. Your checkout is untouched. A
run costs about $0.12 and takes under a minute.

**No API key, or don't want to spend anything:**

```bash
uv run codefleet demo --dry-run
```

Same server, same scheduler, same leases, same veto — scripted agent sessions instead of real ones.
Free, deterministic, and what CI runs. If something looks wrong, try `uv run codefleet doctor`.

## The problem

You have multiple changes to make to one codebase and want a fleet of Claude Code agents to make them
all at once.

Then two agents reach for the same file. Neither is doing anything wrong — one is adding routes to
`api.py`, the other was told to write a middleware module and register it, and registering it means
`api.py`. Both read the file, both edit it against different versions of it, and one agent's work
disappears. Both sessions report success, and you find out from the diff.

The usual answer is a worktree per agent, a branch each, and a merge at the end — the right call when
you want independent branches and a human reviewing PRs. It does not remove the collision, though. It
moves it to merge time, after both agents have finished and one has spent its whole run against a
copy that went stale halfway through.

**CodeFleet takes the other trade — one shared working tree, no branches, and the second write is
refused at the moment it is attempted.** A small coordination server tracks which agent is working in
which file. Every agent asks it before writing; if someone else is in that file, the write is refused
and never reaches the disk. The refused agent's task goes back in the queue and runs again once the
file is free — against real code rather than conflict markers.

You lose parallelism on any file two agents both want, and a task blocked partway through has already
paid for however much of its run it had done. In exchange you never reconcile two versions of one
file.

## How it works

![Three panels. One: a five-task graph is declared, with T3 and T4 scoped to different files. Two: three runners execute it in parallel while the server cascades dependencies. Three: T4 reaches for a file another runner holds and the write is vetoed, requeued, and retried.](docs/how-it-works.png)

- **Runners are deliberately thin.** They hold no queue, evaluate no dependencies, and decide nothing
  about who does what — that is all server-side, in one place, where you can read it. A runner that
  stops heartbeating is fenced and its task requeued.
- **The scheduler returns an ordered list, and the order is the contract.** The server applies six
  kinds of decision top to bottom in one transaction. Placing new work is only correct against the
  state the earlier steps leave behind: a lease held by a runner that just went offline has to be
  released first, or assignment reads that path as busy. Apply the list out of order and you
  co-schedule two tasks onto the same file.
- **Leases are per file and lazy**, acquired at the first write rather than up front from a declared
  scope. A denial is an immediate veto, never a wait, so no agent ever holds one lease while waiting
  on another.

### Architecture

![Four stacked layers: the CLI, the coordination server holding the FastAPI surface, the pure scheduler and a SQLite store, the local runners each wrapping a Claude Agent SDK session behind a PreToolUse hook, and one shared working tree underneath.](docs/architecture.png)

| # | Component | Module | What it does |
|---|---|---|---|
| **1** | Command line | `cli.py` | The only interface at the moment |
| **2** | HTTP surface | `server.py` | Every coordination decision, plus the SSE stream the dashboard reads |
| **3** | Scheduler & engine | `scheduler.py`, `engine.py` | `schedule(state, now) -> [Decision]`, pure; the engine's tick loop applies what it returns |
| **4** | State | `store.py`, `schema.sql` | SQLite in WAL mode. Lease exclusion is a primary-key constraint, not application logic |
| **5** | Runners | `runner.py` | Thin: register, heartbeat, poll, run one session, report |
| **6** | Agent session | `session.py` | The only module that imports `claude_agent_sdk`, and where the hook is installed |
| **7** | Working tree | your code | One shared checkout. Agents edit it in place |

## The veto

Before any agent writes, a `PreToolUse` hook matched on `Write|Edit|MultiEdit|NotebookEdit` asks the
server for a lease on the path. If another agent holds it, the hook returns
`permissionDecision: "deny"` and **that write never happens**. The denied agent is told who holds the
file, and its task is requeued with the contended path folded into its file scope — so the scheduler
runs it after the holder releases.

Only half of that is enforced. A denied tool call does not execute, and that part is the CLI rather
than the model's cooperation. What the agent does *next* is its own business: the deny carries no
stop signal, so a session can keep going and reach for a different file — and gets the same answer on
anything else that is held. The guarantee is per write, not per session.

The session runs `permission_mode="dontAsk"` with `allowed_tools` naming seven tools. Nothing
prompts, and anything that is *not* on the list — a tool from the target repo's own `.mcp.json`, a
plugin, a settings file — is refused rather than run. Two more obvious APIs were rejected first:

| Rejected | Why |
|---|---|
| `can_use_tool` | The permission callback the host controls, which looks exactly right. It requires streaming-input mode, and it is *shadowed* by `permission_mode="bypassPermissions"` and by every whole-tool entry in `allowed_tools`. The SDK emits a `CanUseToolShadowedWarning` that points you at `PreToolUse` itself. |
| `permission_mode="acceptEdits"` | It auto-accepts file edits, which is the part we want — but every *other* tool stays on the standard permission path, which prompts, and an unattended runner has nobody to answer a prompt. |

The two modes that do stop the prompting were rejected on principle. `bypassPermissions` approves
everything that reaches the permission step — the SDK docs say to use it *"with extreme caution"* —
where `dontAsk` denies what it wasn't told about instead. `auto` hands the decision to *"a model
classifier"*, and a lock whose boundary is a model's opinion is the thing this project exists not to
build.

**The cost of that choice.** The mode decides whether a *tool* may run; only the hook knows whether
this agent holds this file. So the hook fails *closed* when the server is unreachable, any path
resolving outside the working tree is denied locally, and the session's tool set is an explicit
allowlist with no shell on it. A write the matcher never sees is a write the server never gets to
veto.

The demo graph stages exactly this, on purpose. T3 (`api.py`) and T4 (`middleware.py`) declare
disjoint scopes, so the scheduler runs them together — correctly, by its own rules. But T4's prompt
tells the agent to register its middleware in `api.py` *before* writing the file its scope actually
names. That is the shape of the mistake someone makes writing a file list before reading the code,
and in a demo you have to schedule it: staged, the veto fires in four of five recorded live runs;
left merely likely, it fired in three of six. The agent is free to reorder, which is the whole reason
a declared scope cannot be the lock.

`SPEC.md` has the rest: the data model, every coordination rule, the full HTTP API, and the
alternatives that were considered and dropped.

## Commands

| Command | What it does |
|---|---|
| `codefleet demo` | The whole thing: fresh workspace, server, fleet, dashboard, verdict |
| `codefleet doctor` | Preflight the things that make a run fail five minutes in |
| `codefleet serve` | The coordination server |
| `codefleet load <graph.yaml>` | Post a task graph; the batch lands whole or not at all |
| `codefleet run --runners 3` | Start the fleet against a running server and wait for it to drain |
| `codefleet watch` | Live dashboard, in another terminal; read-only |
| `codefleet tasks --json` | Machine-readable state |
| `codefleet reset` | Truncate every table; guarded by `CODEFLEET_ALLOW_RESET=1` |

Point it at your own code with `CODEFLEET_WORKDIR`, and write your graph in the shape of
`examples/demo-tasks.yaml`. Every setting is a `CODEFLEET_`-prefixed environment variable, and every
one except `CODEFLEET_RUN_DIR` is also a flag on the command it applies to. `.env.example` lists them
all.

## Tests

```bash
uv run pytest              # everything except the live tier: no network, no API key
uv run pytest -m live      # the live tier, which spends money
```

The whole coordination suite runs against a `ScriptedExecutor` — a runner whose brain is a script
instead of an SDK session. Assignment, cascade, stale-agent requeue, lease exclusion under
concurrency, veto-and-retry, attempt exhaustion: all of it, offline.

That separation is enforced by a test: `tests/unit/test_boundaries.py` walks the AST and asserts
that no module except `session.py` imports `claude_agent_sdk`, and that `runner.py` imports neither
the store nor the scheduler.

## Limitations

- **One shared working tree, and no git automation** — no worktrees, no branches, no commits, no
  PRs. Agents edit your checkout in place, and what you are left with is an uncommitted diff to read
  and commit yourself.
- **Agents cannot run commands.** The session gets exactly
  `Read`/`Write`/`Edit`/`MultiEdit`/`NotebookEdit`/`Glob`/`Grep`. `Bash` is excluded because a shell
  write takes no lease and lands in no ledger row, and no tool-name matcher can see it coming. Claude
  Code's sandboxed `Bash` bounds the write blast radius, but a sandbox policy is fixed when the session
  starts and a lease is not, so it cannot say *this file, right now, belongs to another runner*. The
  price is that a task cannot run the tests it just wrote, or a linter, or `git`.
- **A green run is not a verified run.** A task succeeds when its session ends cleanly, which means
  the model stopped — not that what it wrote compiles. Pass `codefleet run --verify "pytest -q"` and
  let the exit code decide; `codefleet demo` does this with the target repository's own suite. That
  runs over a tree the agents just wrote, so it executes agent-authored code — point the fleet at a
  checkout you would be willing to `git checkout -- .`.

`SPEC.md` has the full list: §7 covers what a failed attempt leaves behind and the one window where
revocation cannot recall a write already authorized; §8 covers what is deliberately out of scope —
including authentication, which the server has none of. It binds `127.0.0.1` and should stay there.

## License

MIT — see [LICENSE](LICENSE).
