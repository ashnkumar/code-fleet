"""The one module that talks to the Claude Agent SDK.

Everything that knows what an SDK message looks like lives here, so the rest of
CodeFleet — server, scheduler, runner — can be tested with no API key and no
`claude_agent_sdk` import. A test enforces that boundary.

The module owns three things that are easy to get wrong and were settled
empirically rather than by reading docs:

* the write veto. `permission_mode="bypassPermissions"` plus a `PreToolUse` hook
  that returns `permissionDecision: "deny"`. That hook is the only thing standing
  between an agent and the filesystem, so it fails *closed*: a path it cannot
  place inside the working tree is refused locally, and a coordination callback
  that raises denies rather than allows.
* hermeticity. `setting_sources=[]` keeps the host user's `~/.claude` agents,
  skills and CLAUDE.md out of the session, and the captured `init` frame is the
  record proving it.
* accounting. Tokens come from `ResultMessage.model_usage`, which is the
  cumulative per-model record; `ResultMessage.usage` is the last API call only,
  and per-`AssistantMessage` usage repeats itself once per content block.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from contextlib import ExitStack, aclosing, suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

from codefleet.config import Settings
from codefleet.models import WRITE_TOOL_MATCHER, ErrorKind, Task

# The Claude Code CLI stamps CLAUDECODE and CLAUDE_CODE_ENTRYPOINT into the
# environment of everything it spawns. A session launched from inside a Claude
# Code session inherits them, the child CLI reads them as "I am nested", and
# behaves differently from the same session launched from a bare shell. Clearing
# them here — before the SDK is imported and captures the environment — makes a
# run identical wherever it was started from.
os.environ.pop("CLAUDECODE", None)
os.environ.pop("CLAUDE_CODE_ENTRYPOINT", None)

import claude_agent_sdk as sdk
from claude_agent_sdk import (
    ClaudeAgentOptions,
    HookContext,
    HookJSONOutput,
    HookMatcher,
    ResultMessage,
    SystemMessage,
)

# A single tool result carrying a large file overflows one NDJSON line and kills
# the session with an unrecoverable CLIJSONDecodeError. The SDK default is 1 MiB.
MAX_BUFFER_SIZE = 32 * 1024 * 1024

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
    read as an instruction to stop rather than an error string.
    """

    allow: bool
    message: str | None = None


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


def extract_paths(tool_input: Mapping[str, Any]) -> list[str]:
    """Pull the target paths out of a write tool's input, in the order given.

    `Write` and `Edit` carry a single `file_path`. Some tools spell it `path`,
    and a `MultiEdit`-shaped input may carry an `edits` list whose entries name
    their own file. Duplicates are collapsed so a multi-edit of one file asks for
    one lease.
    """
    found: list[str] = []

    def add(value: Any) -> None:
        if isinstance(value, str) and value and value not in found:
            found.append(value)

    add(tool_input.get("file_path"))
    add(tool_input.get("path"))

    edits = tool_input.get("edits")
    if isinstance(edits, list):
        for edit in edits:
            if isinstance(edit, Mapping):
                add(edit.get("file_path"))
                add(edit.get("path"))

    return found


def normalize_path(raw: str, workdir: Path) -> str | None:
    """Resolve `raw` inside `workdir`, or return None if it lands outside.

    Symlinks are resolved on both sides, so a link inside the tree pointing out
    of it does not smuggle a write past the lease table. The result is the
    relative POSIX form the lease table is keyed by.
    """
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = workdir / candidate

    resolved = candidate.resolve()
    root = workdir.resolve()
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
    files_written: list[str] = field(default_factory=list)
    hook_error: str | None = None
    init_frame: dict[str, Any] | None = None
    session_id: str | None = None
    result: ResultMessage | None = None

    def record_denial(self, path: str) -> None:
        self.denied_paths.append(path)

    def record_out_of_bounds(self, path: str) -> None:
        self.out_of_bounds_paths.append(path)

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
    model and the totals are the sum. Older CLIs omit the field entirely, which
    reads as zero rather than as an error — a missing accounting record should
    not fail a task that otherwise succeeded.
    """
    if not model_usage:
        return (0, 0, 0.0)

    def total(key: str) -> float:
        return sum(float(usage.get(key) or 0) for usage in model_usage.values())

    return (int(total("inputTokens")), int(total("outputTokens")), total("costUSD"))


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
            return allow_response()

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

        recorder.record_denial(paths[0])
        return deny_response(decision.message or _DEFAULT_DENY_REASON.format(path=paths[0]))

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
    """
    return ClaudeAgentOptions(
        model=settings.model,
        cwd=str(workdir),
        permission_mode="bypassPermissions",
        setting_sources=[],
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

        preexisting_children = _live_cli_children()
        try:
            await asyncio.wait_for(
                _drain(prompt=build_prompt(task), options=options, recorder=recorder),
                timeout=settings.task_timeout,
            )
        except TimeoutError:
            duration_ms = int((time.monotonic() - started) * 1000)
            reaped = _reap_cli_children(preexisting_children)
            return SessionOutcome(
                ok=False,
                error=(
                    f"session exceeded {settings.task_timeout:.0f}s wall clock"
                    + (f"; killed CLI pid {reaped}" if reaped else "")
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
        handle.write(line + "\n")

    return write


async def _drain(*, prompt: str, options: ClaudeAgentOptions, recorder: SessionRecorder) -> None:
    """Consume the whole message stream, keeping only what the recorder wants.

    Draining promptly matters: the SDK buffers 100 messages internally, so a slow
    consumer throttles the agent it is watching.
    """
    async with aclosing(sdk.query(prompt=prompt, options=options)) as stream:
        async for message in stream:
            recorder.observe(message)


def _live_cli_children() -> frozenset[Any]:
    """Snapshot the CLI subprocesses the SDK currently has open.

    This reaches into the SDK's private registry on purpose. `query()` gives the
    caller no handle on the child it spawns, and on the `asyncio.wait_for` path
    the transport's own terminate/kill escalation is skipped — its `close()`
    shields against anyio cancellation but not against a raw asyncio one, as its
    docstring says. What survives is the registry entry, which is the only thing
    left to reap by.
    """
    from claude_agent_sdk._internal.transport.subprocess_cli import _ACTIVE_CHILDREN

    return frozenset(_ACTIVE_CHILDREN)


def _reap_cli_children(preexisting: Iterable[Any]) -> list[int]:
    """Kill CLI subprocesses this session started and the SDK did not clean up.

    Anything already running when the session began belongs to someone else and
    is left alone. SIGKILL rather than SIGTERM: this path runs only after the
    wall clock expired on a session that is by definition not responding, and
    asyncio's child watcher reaps the exit status without us waiting on it.
    """
    from claude_agent_sdk._internal.transport import subprocess_cli

    known = set(preexisting)
    reaped: list[int] = []
    for child in list(subprocess_cli._ACTIVE_CHILDREN):
        if child in known or child.returncode is not None:
            continue
        with suppress(ProcessLookupError):
            child.kill()
        reaped.append(child.pid)
        subprocess_cli._ACTIVE_CHILDREN.discard(child)
    return reaped
