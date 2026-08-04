"""The one module that talks to the Claude Agent SDK.

Everything that knows what an SDK message looks like lives here, so the rest of
CodeFleet — server, scheduler, runner — can be tested with no API key and no
`claude_agent_sdk` import. A test enforces that boundary.

The module owns three things that are easy to get wrong and were settled
empirically rather than by reading docs:

* the write veto. `permission_mode="bypassPermissions"` plus a `PreToolUse` hook
  that returns `permissionDecision: "deny"`. That hook is the only thing standing
  between an agent and the filesystem, so it fails *closed*: a path it cannot
  place inside the working tree is refused locally, a write tool whose input
  names no path it can read is refused too, and a coordination callback that
  raises denies rather than allows. `SESSION_TOOLS` is the other half of it —
  the session is given only tools whose writes the hook can see.
* hermeticity. `setting_sources=[]` keeps the host user's `~/.claude` agents,
  skills and CLAUDE.md out of the session, and the captured `init` frame is the
  record proving it.
* accounting. Tokens come from `ResultMessage.model_usage`, which is the
  cumulative per-model record; `ResultMessage.usage` is the last API call only,
  and per-`AssistantMessage` usage repeats itself once per content block.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from contextlib import ExitStack, aclosing, suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

import claude_agent_sdk as sdk
from claude_agent_sdk import (
    ClaudeAgentOptions,
    HookContext,
    HookJSONOutput,
    HookMatcher,
    ResultMessage,
    SystemMessage,
    Transport,
)

from codefleet.config import Settings
from codefleet.models import WRITE_TOOL_MATCHER, ErrorKind, Task

# A single tool result carrying a large file overflows one NDJSON line and kills
# the session with an unrecoverable CLIJSONDecodeError. The SDK default is 1 MiB.
MAX_BUFFER_SIZE = 32 * 1024 * 1024

# The only tools a session may call. Every write path in this list is one the
# PreToolUse veto can actually see: Write, Edit, MultiEdit and NotebookEdit each
# name their target in their tool input, so the hook can resolve a path and ask
# the coordination server for a lease before the write lands.
#
# Bash is absent on purpose, and so is Task. A shell command writes files
# without ever handing a path to a hook — `sed -i`, a redirect, a formatter, a
# codegen script, `python -c` — so gating it would mean parsing arbitrary shell,
# which is not a boundary anyone should trust; Task is excluded because a
# subagent would get its own tool set and take Bash with it. The cost is real:
# an agent cannot run the test suite or any other command from inside a task.
# That is the price of the lease being the only way a file changes.
SESSION_TOOLS: tuple[str, ...] = (
    "Read",
    "Write",
    "Edit",
    "MultiEdit",
    "NotebookEdit",
    "Glob",
    "Grep",
)

OUTSIDE_WORKDIR_REASON = "outside_workdir"

_DEFAULT_DENY_REASON = (
    "{path} is held by another agent. Do not retry this file and do not edit around it. "
    "Stop now and report that you are blocked on {path}."
)

# Result subtypes the CLI reports when it stopped the session itself rather than
# because the model finished or errored.
_BUDGET_STOP_SUBTYPES = frozenset({"error_max_turns", "error_max_budget_usd"})


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PreWriteDecision:
    """What the coordination layer says about a pending write.

    `message` is handed to the model verbatim as the denial reason, so it should
    read as an instruction to stop rather than an error string. `path` names the
    file the decision was actually about: one tool call can ask for several
    paths and be refused over any one of them, and the task's `blocked_on_path`
    has to be the file someone else holds, not whichever path happened to be
    listed first.
    """

    allow: bool
    message: str | None = None
    path: str | None = None


@dataclass(frozen=True, slots=True)
class SessionOutcome:
    """Everything one SDK session produced. The runner turns this into a `TaskResult`."""

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
    init_frame: dict[str, Any] | None = None


PreWriteCallback = Callable[[list[str], str], Awaitable[PreWriteDecision]]
PostWriteCallback = Callable[[str, str], Awaitable[None]]


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------


def build_prompt(task: Task) -> str:
    """The whole of what the agent is told.

    Two things beyond the task itself are load-bearing: that the tree is shared,
    and that a denied write is final. Without the second, a denied agent tries
    the same file again, or worse, achieves the same edit from a file it is
    allowed to touch.
    """
    if task.file_scope:
        scope = "\n".join(f"  - {path}" for path in task.file_scope)
    else:
        scope = "  (none declared)"

    return f"""You are one agent in a fleet of coding agents working in parallel on this
same working tree. Other agents are editing other files in it while you work.

# Task: {task.title}

{task.description}

# Files this task is expected to touch

{scope}

That list was written before anyone read the code. It is a hint, not a boundary —
follow the code where it actually leads.

# Fleet rules

- Stay inside this working tree. Writes outside it are refused.
- A write may be denied because another agent currently holds that file. If that
  happens the file is not yours to change: do not retry it, do not achieve the
  same change by editing a different file, and do not ask again. Stop immediately
  and say which file blocked you.
- Do not commit, branch, stash, or otherwise rewrite shared git state. The
  operator owns the diff.
- Finish with one or two sentences describing what you changed.
"""


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


def extract_paths(tool_input: Any) -> list[str]:
    """Pull the target paths out of a write tool's input, in the order given.

    `Write` and `Edit` carry a single `file_path`, `NotebookEdit` spells it
    `notebook_path`, some tools spell it `path`, and a `MultiEdit`-shaped input
    may carry an `edits` list whose entries name their own file. Duplicates are
    collapsed so a multi-edit of one file asks for one lease.

    Every key a gated tool can use has to be here: a write tool whose target
    this function cannot find is refused outright rather than waved through, so
    a miss costs a task rather than the lease.

    The argument is typed `Any` rather than `Mapping` on purpose. The hook is
    handed whatever the CLI put in `tool_input`, which is model-shaped JSON, not
    a validated schema. A list, a string or a number there must come back as "no
    paths" — which the caller turns into a denial — rather than as an
    `AttributeError` thrown out of the hook, because an exception leaving the
    hook is decided by the CLI rather than by us.
    """
    if not isinstance(tool_input, Mapping):
        return []

    found: list[str] = []

    def add(value: Any) -> None:
        if isinstance(value, str) and value and value not in found:
            found.append(value)

    add(tool_input.get("file_path"))
    add(tool_input.get("path"))
    add(tool_input.get("notebook_path"))

    edits = tool_input.get("edits")
    if isinstance(edits, list):
        for edit in edits:
            if isinstance(edit, Mapping):
                add(edit.get("file_path"))
                add(edit.get("path"))
                add(edit.get("notebook_path"))

    return found


def normalize_path(raw: str, workdir: Path) -> str | None:
    """Resolve `raw` inside `workdir`, or return None if it lands outside.

    Symlinks are resolved on both sides, so a link inside the tree pointing out
    of it does not smuggle a write past the lease table. The result is the
    relative POSIX form the lease table is keyed by.

    A string the filesystem refuses to even look at — an embedded NUL, a name
    longer than the OS allows — is `None` too, not an exception: the caller
    turns `None` into a denial, and a raise here would leave the decision to the
    CLI instead.
    """
    try:
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = workdir / candidate

        resolved = candidate.resolve()
        root = workdir.resolve()
    except (ValueError, OSError):
        return None
    if not resolved.is_relative_to(root):
        return None
    return resolved.relative_to(root).as_posix()


# ---------------------------------------------------------------------------
# Hook responses
# ---------------------------------------------------------------------------


def allow_response() -> HookJSONOutput:
    """An empty object is how a PreToolUse hook says "no opinion, proceed"."""
    return {}


def deny_response(reason: str) -> HookJSONOutput:
    """The exact shape the CLI accepts as a veto. Verified against a live run."""
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------


@dataclass
class SessionRecorder:
    """What the message stream and the hooks accumulated over one session.

    Kept separate from `run_session` so the outcome rules — which failure wins
    when several happened — can be exercised without an SDK stream.
    """

    denied_paths: list[str] = field(default_factory=list)
    out_of_bounds_paths: list[str] = field(default_factory=list)
    unreadable_writes: list[str] = field(default_factory=list)
    files_written: list[str] = field(default_factory=list)
    hook_error: str | None = None
    init_frame: dict[str, Any] | None = None
    session_id: str | None = None
    result: ResultMessage | None = None

    def record_denial(self, path: str) -> None:
        self.denied_paths.append(path)

    def record_out_of_bounds(self, path: str) -> None:
        self.out_of_bounds_paths.append(path)

    def record_unreadable_write(self, tool: str) -> None:
        """A gated write tool whose input named no path the veto could read."""
        self.unreadable_writes.append(tool)

    def record_hook_error(self, exc: BaseException) -> None:
        # First failure wins; a coordination server that is down will fail every
        # subsequent hook the same way and the first message is the useful one.
        if self.hook_error is None:
            self.hook_error = f"{type(exc).__name__}: {exc}"

    def record_write(self, path: str) -> None:
        if path not in self.files_written:
            self.files_written.append(path)

    def observe(self, message: Any) -> None:
        """Take from one streamed message the little we keep."""
        if isinstance(message, SystemMessage):
            # thinking_tokens frames are the bulk of the stream (21 of 32
            # messages in a trivial run) and carry nothing we record.
            if message.subtype == "init":
                self.init_frame = dict(message.data)
                session_id = message.data.get("session_id")
                if isinstance(session_id, str):
                    self.session_id = session_id
            return
        if isinstance(message, ResultMessage):
            self.result = message
            self.session_id = message.session_id

    def outcome(self, *, duration_ms: int) -> SessionOutcome:
        """Collapse everything that happened into one verdict.

        Precedence is deliberate. An infra failure outranks a veto because a
        denial caused by an unreachable server is not evidence that anyone holds
        the file — recording it as a veto would widen the task's `file_scope`
        with a path nobody ever contended.
        """
        result = self.result
        input_tokens, output_tokens, model_cost = sum_model_usage(
            result.model_usage if result else None
        )
        cost_usd = result.total_cost_usd if result and result.total_cost_usd else model_cost

        common: dict[str, Any] = {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost_usd": cost_usd,
            "duration_ms": duration_ms,
            "session_id": self.session_id,
            "files_written": tuple(self.files_written),
            "init_frame": self.init_frame,
        }
        summary = result.result if result else None

        if self.hook_error is not None:
            return SessionOutcome(
                ok=False,
                error=f"write coordination failed: {self.hook_error}",
                error_kind=ErrorKind.INFRA,
                **common,
            )

        if self.unreadable_writes:
            # Infra, not agent error: the agent asked for a legal write and the
            # extractor could not name its target, which means this module has
            # fallen behind a tool schema. Loud is the point — the alternative
            # is a write the lease table never saw.
            tool = self.unreadable_writes[0]
            return SessionOutcome(
                ok=False,
                error=(
                    f"refused a {tool} write: its input named no path the veto could read, "
                    "so no lease could be taken out on it"
                ),
                error_kind=ErrorKind.INFRA,
                **common,
            )

        if self.denied_paths:
            blocked = self.denied_paths[0]
            return SessionOutcome(
                ok=False,
                error=f"blocked: {blocked} is held by another agent",
                error_kind=ErrorKind.VETO,
                blocked_on_path=blocked,
                **common,
            )

        if self.out_of_bounds_paths:
            escaped = self.out_of_bounds_paths[0]
            return SessionOutcome(
                ok=False,
                error=f"refused a write outside the working tree: {escaped}",
                error_kind=ErrorKind.AGENT_ERROR,
                **common,
            )

        if result is None:
            return SessionOutcome(
                ok=False,
                error="session ended without a result message",
                error_kind=ErrorKind.INFRA,
                **common,
            )

        if result.is_error or result.subtype != "success":
            kind = (
                ErrorKind.BUDGET
                if result.subtype in _BUDGET_STOP_SUBTYPES or result.terminal_reason == "max_turns"
                else ErrorKind.AGENT_ERROR
            )
            return SessionOutcome(
                ok=False,
                error=summary or f"session ended with subtype {result.subtype!r}",
                error_kind=kind,
                **common,
            )

        return SessionOutcome(ok=True, summary=summary, **common)


def sum_model_usage(model_usage: Mapping[str, Mapping[str, Any]] | None) -> tuple[int, int, float]:
    """Sum `(input_tokens, output_tokens, cost_usd)` across every model in a session.

    A session that falls back, or delegates to a cheaper model, has one entry per
    model and the totals are the sum. Older CLIs omit the field entirely, and a
    malformed entry counts as zero rather than raising — a missing or broken
    accounting record must not fail a task that already ran and was already paid
    for.
    """
    if not model_usage:
        return (0, 0, 0.0)

    def total(*keys: str) -> float:
        return sum(
            _numeric(usage.get(key))
            for usage in model_usage.values()
            if isinstance(usage, Mapping)
            for key in keys
        )

    return (int(total(*_INPUT_TOKEN_KEYS)), int(total("outputTokens")), total("costUSD"))


# Cached prompt tokens are input tokens that were read and billed; they just
# arrive in their own fields. Leaving them out puts `input_tokens` and
# `cost_usd` — which prices them — on different denominators, and the two are
# persisted side by side.
_INPUT_TOKEN_KEYS = ("inputTokens", "cacheReadInputTokens", "cacheCreationInputTokens")


def _numeric(value: Any) -> float:
    """Read one usage field, treating anything that is not a number as zero."""
    return float(value) if isinstance(value, int | float) else 0.0


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------


def make_pre_write_hook(
    *,
    workdir: Path,
    on_pre_write: PreWriteCallback,
    recorder: SessionRecorder,
) -> Callable[[dict[str, Any], str | None, HookContext], Awaitable[HookJSONOutput]]:
    """Build the veto hook.

    Registered under a `Write|Edit|MultiEdit|NotebookEdit` matcher, so it is
    never called for a read.
    """

    async def hook(
        data: dict[str, Any],
        _tool_use_id: str | None,
        _context: HookContext,
    ) -> HookJSONOutput:
        tool: str = data.get("tool_name", "")
        tool_input = data.get("tool_input") or {}

        paths: list[str] = []
        for raw in extract_paths(tool_input):
            relative = normalize_path(raw, workdir)
            if relative is None:
                # Decided here, not by the coordination server: a path outside
                # the tree is not a lease question, and asking would put a path
                # in the lease table that the workdir cannot key.
                recorder.record_out_of_bounds(raw)
                return deny_response(
                    f"{raw} is outside this working tree ({OUTSIDE_WORKDIR_REASON}). "
                    "Only files under the working directory may be modified. "
                    "Do not retry this path."
                )
            paths.append(relative)

        if not paths:
            # The matcher only fires for a write tool, so this is a write whose
            # input shape this module does not recognize. Allowing it would put
            # a file change past the lease table unseen, which is the one thing
            # this hook exists to prevent.
            recorder.record_unreadable_write(tool)
            return deny_response(
                f"This {tool} call names no file path that write coordination can read, "
                "so it cannot be allowed. Stop now and report that the write could not "
                "be coordinated."
            )

        try:
            decision = await on_pre_write(paths, tool)
        except Exception as exc:
            # Fail closed. Allowing the write because coordination is unavailable
            # reintroduces exactly the collision this system exists to prevent.
            # The failure is not swallowed: it decides the outcome, as infra.
            recorder.record_hook_error(exc)
            return deny_response(
                "Write coordination is unavailable, so this write cannot be allowed. "
                "Stop now and report that the coordination server could not be reached."
            )

        if decision.allow:
            return allow_response()

        # The coordination layer names the path it refused when it knows it; a
        # tool call asking for several paths can be denied over any one of them,
        # and that path is what the task is blocked on.
        named = decision.path
        blocked = named if named is not None and named in paths else paths[0]
        recorder.record_denial(blocked)
        return deny_response(decision.message or _DEFAULT_DENY_REASON.format(path=blocked))

    return hook


def make_post_write_hook(
    *,
    workdir: Path,
    on_post_write: PostWriteCallback,
    recorder: SessionRecorder,
) -> Callable[[dict[str, Any], str | None, HookContext], Awaitable[HookJSONOutput]]:
    """Build the ledger hook. It observes; it never vetoes."""

    async def hook(
        data: dict[str, Any],
        _tool_use_id: str | None,
        _context: HookContext,
    ) -> HookJSONOutput:
        tool: str = data.get("tool_name", "")
        tool_input = data.get("tool_input") or {}
        for raw in extract_paths(tool_input):
            relative = normalize_path(raw, workdir)
            if relative is None:
                continue
            recorder.record_write(relative)
            await on_post_write(relative, tool)
        return allow_response()

    return hook


# ---------------------------------------------------------------------------
# Running a session
# ---------------------------------------------------------------------------


def build_options(
    *,
    workdir: Path,
    settings: Settings,
    hooks: dict[str, list[HookMatcher]],
    stderr: Callable[[str], None] | None,
) -> ClaudeAgentOptions:
    """Assemble the options for one task session.

    `setting_sources=[]` is what makes the run hermetic: the SDK default loads
    the host user's `~/.claude` settings, agents, skills and CLAUDE.md, so
    without it the same demo behaves differently on every machine. Note that
    setting `skills=` silently flips this back on — if skills are ever enabled
    here, `setting_sources` has to be passed again in the same call.

    `tools=` is the base set the session gets, not a permission list: under
    `bypassPermissions` an `allowed_tools` entry would only pre-approve, while
    this removes everything not named from the session entirely. See
    `SESSION_TOOLS` for why the list is what it is.

    `strict_mcp_config=True` closes the gap `setting_sources=[]` leaves open.
    That one gates settings *files*; MCP configuration loads on its own path, so
    without this a target repository carrying a `.mcp.json` would hand the
    session tools nobody here chose. Those tools are outside `SESSION_TOOLS`,
    they do not match `WRITE_TOOL_MATCHER`, and under `bypassPermissions` they
    are approved without being asked about — a write path with no lease behind
    it, which is the same hole `Bash` is excluded to avoid.
    """
    return ClaudeAgentOptions(
        model=settings.model,
        cwd=str(workdir),
        permission_mode="bypassPermissions",
        setting_sources=[],
        strict_mcp_config=True,
        tools=list(SESSION_TOOLS),
        max_turns=settings.max_turns,
        max_budget_usd=settings.task_budget_usd,
        max_buffer_size=MAX_BUFFER_SIZE,
        stderr=stderr,
        hooks=hooks,
    )


async def run_session(
    *,
    task: Task,
    workdir: Path,
    settings: Settings,
    on_pre_write: PreWriteCallback,
    on_post_write: PostWriteCallback,
    stderr_path: Path | None = None,
) -> SessionOutcome:
    """Run one task as one Claude Agent SDK session and report what happened.

    `on_pre_write(paths, tool)` is consulted before every write and decides it;
    where that decision comes from is the caller's business. `on_post_write(path,
    tool)` records a write that already happened and must not raise — it is on
    the agent's critical path, and a failing ledger should not fail a task.

    Exceptions from the SDK itself are not caught here. A session that cannot
    start is a real failure and the runner reports it as such.
    """
    recorder = SessionRecorder()
    started = time.monotonic()

    with ExitStack() as stack:
        stderr_callback: Callable[[str], None] | None = None
        if stderr_path is not None:
            stderr_path.parent.mkdir(parents=True, exist_ok=True)
            # Line buffered, because the interesting case is reading this file
            # while the session it belongs to is still wedged.
            handle = stack.enter_context(
                stderr_path.open("a", encoding="utf-8", buffering=1, errors="replace")
            )
            stderr_callback = _line_writer(handle)

        options = build_options(
            workdir=workdir,
            settings=settings,
            hooks={
                # Matcher-based filtering rather than a Python-side tool-name
                # check: the CLI does exact set membership on the `|`-split list,
                # and the intent is reviewable in one line.
                "PreToolUse": [
                    HookMatcher(
                        matcher=WRITE_TOOL_MATCHER,
                        hooks=[
                            make_pre_write_hook(
                                workdir=workdir, on_pre_write=on_pre_write, recorder=recorder
                            )
                        ],
                    )
                ],
                "PostToolUse": [
                    HookMatcher(
                        matcher=WRITE_TOOL_MATCHER,
                        hooks=[
                            make_post_write_hook(
                                workdir=workdir, on_post_write=on_post_write, recorder=recorder
                            )
                        ],
                    )
                ],
            },
            stderr=stderr_callback,
        )

        prompt = build_prompt(task)
        transport = _new_transport(prompt=prompt, options=options)
        try:
            await asyncio.wait_for(
                _drain(prompt=prompt, options=options, recorder=recorder, transport=transport),
                timeout=settings.task_timeout,
            )
        except TimeoutError:
            duration_ms = int((time.monotonic() - started) * 1000)
            reaped = _reap_transport_child(transport)
            return SessionOutcome(
                ok=False,
                error=(
                    f"session exceeded {settings.task_timeout:.0f}s wall clock"
                    + (f"; killed CLI pid {reaped}" if reaped is not None else "")
                ),
                error_kind=ErrorKind.TIMEOUT,
                duration_ms=duration_ms,
                session_id=recorder.session_id,
                files_written=tuple(recorder.files_written),
                init_frame=recorder.init_frame,
            )

    return recorder.outcome(duration_ms=int((time.monotonic() - started) * 1000))


def _line_writer(handle: TextIO) -> Callable[[str], None]:
    """The SDK hands stderr to the callback a line at a time, newline stripped."""

    def write(line: str) -> None:
        # The SDK's stderr reader is a detached task and can outlive the session
        # it belongs to — on the timeout path this file is closed while the CLI
        # is still being killed. A late line has nowhere to go; raising here
        # would surface inside the SDK's own task as a spurious failure.
        if handle.closed:
            return
        handle.write(line + "\n")

    return write


async def _drain(
    *,
    prompt: str,
    options: ClaudeAgentOptions,
    recorder: SessionRecorder,
    transport: Transport | None = None,
) -> None:
    """Consume the whole message stream, keeping only what the recorder wants.

    Draining promptly matters: the SDK buffers 100 messages internally, so a slow
    consumer throttles the agent it is watching.
    """
    async with aclosing(sdk.query(prompt=prompt, options=options, transport=transport)) as stream:
        async for message in stream:
            recorder.observe(message)


def _new_transport(*, prompt: str, options: ClaudeAgentOptions) -> Transport:
    """Build the CLI transport ourselves so this session owns a handle on its child.

    `query()` otherwise spawns the subprocess internally and hands the caller
    nothing to kill, which matters on the `asyncio.wait_for` path: the
    transport's terminate/kill escalation is skipped there, because its `close()`
    shields against anyio cancellation but not against a raw asyncio one. The
    only thing the SDK leaves behind is an entry in a module-global registry
    shared by every runner in this process, so reaping by "what appeared since I
    started" attributes a sibling runner's healthy CLI to whoever timed out.
    Passing our own transport is a documented `query()` parameter and removes
    the guesswork: this session kills this session's child and nothing else.
    """
    from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport

    return SubprocessCLITransport(prompt=prompt, options=options)


def _reap_transport_child(transport: Transport) -> int | None:
    """Kill this session's own CLI subprocess if the SDK left it running, and say which.

    SIGKILL rather than SIGTERM: this runs only after the wall clock expired on a
    session that is by definition not responding, and asyncio's child watcher
    reaps the exit status without us waiting on it.
    """
    from claude_agent_sdk._internal.transport import subprocess_cli

    # A private attribute, because the transport exposes no public handle on its
    # child. A unit test asserts it is still there, so a rename in the SDK is
    # caught offline rather than by a leaked CLI process on the next timeout.
    child = getattr(transport, "_process", None)
    if child is None or child.returncode is not None:
        return None
    with suppress(ProcessLookupError):
        child.kill()
    # Drop it from the SDK's atexit sweep; this one is already dealt with.
    subprocess_cli._ACTIVE_CHILDREN.discard(child)
    pid: int = child.pid
    return pid
