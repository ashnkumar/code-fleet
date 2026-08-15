"""Tests for the SDK session module.

Nothing here talks to the API. The pieces that matter — what the agent is told,
which paths a write tool is asking for, whether a path escapes the working tree,
the literal dicts the CLI accepts as allow and deny, how usage is summed, and
which failure wins when several happened — are all pure and are tested as such.
The one test that needs the network is marked `live` and deselected by default.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import sys
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest
from claude_agent_sdk import ClaudeAgentOptions, HookContext, ResultMessage, SystemMessage

from codefleet.config import Settings
from codefleet.models import WRITE_TOOL_MATCHER, ErrorKind, Task
from codefleet.session import (
    SESSION_TOOLS,
    PreWriteDecision,
    SessionRecorder,
    _new_transport,
    _reap_transport_child,
    allow_response,
    build_options,
    build_prompt,
    deny_response,
    extract_paths,
    make_post_write_hook,
    make_pre_write_hook,
    normalize_path,
    run_session,
    sum_model_usage,
)

CONTEXT: HookContext = {"signal": None}


def make_task(**overrides: Any) -> Task:
    fields: dict[str, Any] = {
        "id": "T4",
        "title": "Add request logging middleware",
        "description": "Write a middleware that logs method, path and status.",
        "file_scope": ("linkstash/middleware.py",),
    }
    fields.update(overrides)
    return Task(**fields)


def make_result(**overrides: Any) -> ResultMessage:
    fields: dict[str, Any] = {
        "subtype": "success",
        "duration_ms": 12000,
        "duration_api_ms": 9000,
        "is_error": False,
        "num_turns": 4,
        "session_id": "sess_abc",
        "total_cost_usd": 0.0094,
        "result": "Added the middleware and wired it into the app.",
        "model_usage": {
            "claude-haiku-4-5-20251001": {
                "inputTokens": 8421,
                "outputTokens": 613,
                "costUSD": 0.0094,
            }
        },
    }
    fields.update(overrides)
    return ResultMessage(**fields)


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    tree = tmp_path / "demo-repo"
    (tree / "linkstash").mkdir(parents=True)
    (tree / "linkstash" / "api.py").write_text("# api\n")
    return tree


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------


def test_prompt_carries_the_task_and_the_fleet_rules() -> None:
    prompt = build_prompt(make_task())

    assert "Add request logging middleware" in prompt
    assert "logs method, path and status" in prompt
    assert "linkstash/middleware.py" in prompt
    # The two facts the agent cannot infer from the task text.
    assert "parallel" in prompt
    assert "do not retry it" in prompt


def test_prompt_without_a_declared_scope_says_so() -> None:
    prompt = build_prompt(make_task(file_scope=()))

    assert "(none declared)" in prompt


# ---------------------------------------------------------------------------
# Path extraction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("tool_input", "expected"),
    [
        ({"file_path": "a.py", "content": "x"}, ["a.py"]),
        ({"path": "a.py"}, ["a.py"]),
        ({"file_path": "a.py", "path": "b.py"}, ["a.py", "b.py"]),
        ({"file_path": "a.py", "edits": [{"old_string": "x", "new_string": "y"}]}, ["a.py"]),
        ({"edits": [{"file_path": "a.py"}, {"file_path": "b.py"}]}, ["a.py", "b.py"]),
        # One lease request per file, however many edits target it.
        ({"file_path": "a.py", "edits": [{"file_path": "a.py"}]}, ["a.py"]),
        # NotebookEdit is in the veto matcher and spells its target differently.
        ({"notebook_path": "nb.ipynb", "new_source": "x"}, ["nb.ipynb"]),
        ({"edits": [{"notebook_path": "nb.ipynb"}]}, ["nb.ipynb"]),
        ({}, []),
        ({"file_path": None}, []),
        ({"edits": "not-a-list"}, []),
        ({"edits": ["not-a-mapping"]}, []),
    ],
)
def test_extract_paths(tool_input: dict[str, Any], expected: list[str]) -> None:
    assert extract_paths(tool_input) == expected


# ---------------------------------------------------------------------------
# Path normalization
# ---------------------------------------------------------------------------


def test_relative_path_resolves_against_the_workdir(workdir: Path) -> None:
    assert normalize_path("linkstash/api.py", workdir) == "linkstash/api.py"


def test_absolute_path_inside_the_workdir_becomes_relative(workdir: Path) -> None:
    absolute = str(workdir / "linkstash" / "api.py")

    assert normalize_path(absolute, workdir) == "linkstash/api.py"


def test_redundant_segments_are_collapsed(workdir: Path) -> None:
    assert normalize_path("./linkstash/../linkstash/api.py", workdir) == "linkstash/api.py"


@pytest.mark.parametrize("raw", ["../outside.py", "/etc/passwd", "linkstash/../../outside.py"])
def test_paths_outside_the_workdir_are_rejected(raw: str, workdir: Path) -> None:
    assert normalize_path(raw, workdir) is None


def test_a_symlink_pointing_out_of_the_tree_does_not_smuggle_a_write(
    workdir: Path, tmp_path: Path
) -> None:
    outside = tmp_path / "secrets.txt"
    outside.write_text("shh\n")
    (workdir / "link.txt").symlink_to(outside)

    assert normalize_path("link.txt", workdir) is None


# ---------------------------------------------------------------------------
# Hook response shapes
# ---------------------------------------------------------------------------


def test_allow_is_an_empty_object() -> None:
    assert allow_response() == {}


def test_deny_is_the_exact_shape_the_cli_accepts() -> None:
    assert deny_response("held by runner-2") == {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": "held by runner-2",
        }
    }


# ---------------------------------------------------------------------------
# PreToolUse hook
# ---------------------------------------------------------------------------


async def test_pre_write_hook_allows_and_reports_normalized_paths(workdir: Path) -> None:
    seen: list[tuple[list[str], str]] = []

    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        seen.append((paths, tool))
        return PreWriteDecision(allow=True)

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    response = await hook(
        {"tool_name": "Edit", "tool_input": {"file_path": str(workdir / "linkstash/api.py")}},
        None,
        CONTEXT,
    )

    assert response == {}
    assert seen == [(["linkstash/api.py"], "Edit")]
    assert recorder.denied_paths == []


async def test_pre_write_hook_denies_with_the_callbacks_message(workdir: Path) -> None:
    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        return PreWriteDecision(allow=False, message="linkstash/api.py is held by runner-2.")

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    response = await hook(
        {"tool_name": "Write", "tool_input": {"file_path": "linkstash/api.py"}}, None, CONTEXT
    )

    assert response["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert (
        response["hookSpecificOutput"]["permissionDecisionReason"]
        == "linkstash/api.py is held by runner-2."
    )
    assert recorder.denied_paths == ["linkstash/api.py"]


async def test_pre_write_hook_denies_with_a_usable_default_message(workdir: Path) -> None:
    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        return PreWriteDecision(allow=False)

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    response = await hook(
        {"tool_name": "Write", "tool_input": {"file_path": "linkstash/api.py"}}, None, CONTEXT
    )

    reason = response["hookSpecificOutput"]["permissionDecisionReason"]
    assert "linkstash/api.py" in reason
    assert "do not retry" in reason.lower()


async def test_a_path_outside_the_workdir_is_denied_without_asking(workdir: Path) -> None:
    calls: list[list[str]] = []

    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        calls.append(paths)
        return PreWriteDecision(allow=True)

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    response = await hook(
        {"tool_name": "Write", "tool_input": {"file_path": "/etc/passwd"}}, None, CONTEXT
    )

    assert response["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert "outside_workdir" in response["hookSpecificOutput"]["permissionDecisionReason"]
    assert calls == []
    assert recorder.out_of_bounds_paths == ["/etc/passwd"]
    # Not a lease conflict, so nothing to widen the task's file_scope with.
    assert recorder.denied_paths == []


async def test_pre_write_hook_fails_closed_when_coordination_raises(workdir: Path) -> None:
    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        raise ConnectionError("server unreachable")

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    response = await hook(
        {"tool_name": "Write", "tool_input": {"file_path": "linkstash/api.py"}}, None, CONTEXT
    )

    assert response["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert recorder.hook_error == "ConnectionError: server unreachable"
    assert recorder.outcome(duration_ms=1).error_kind is ErrorKind.INFRA


async def test_a_notebook_edit_asks_for_a_lease_like_any_other_write(workdir: Path) -> None:
    """NotebookEdit is in the matcher, so its target has to reach the server."""
    seen: list[tuple[list[str], str]] = []

    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        seen.append((paths, tool))
        return PreWriteDecision(allow=False, message="held by runner-2")

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    response = await hook(
        {
            "tool_name": "NotebookEdit",
            "tool_input": {"notebook_path": "analysis.ipynb", "new_source": "1 + 1"},
        },
        None,
        CONTEXT,
    )

    assert seen == [(["analysis.ipynb"], "NotebookEdit")]
    assert response["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert recorder.denied_paths == ["analysis.ipynb"]


async def test_a_notebook_edit_outside_the_workdir_is_refused(workdir: Path) -> None:
    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=noop_pre_write, recorder=recorder)

    response = await hook(
        {"tool_name": "NotebookEdit", "tool_input": {"notebook_path": "/tmp/loot.ipynb"}},
        None,
        CONTEXT,
    )

    assert response["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert recorder.out_of_bounds_paths == ["/tmp/loot.ipynb"]


async def test_a_write_tool_with_no_readable_path_is_refused(workdir: Path) -> None:
    """A gated write whose shape we do not understand is an ungated write.

    The matcher only fires for write tools, so an input this module cannot read
    a path out of means the extractor has fallen behind a tool schema. Allowing
    it would put a file change past the lease table unseen — the exact hole a
    `notebook_path` the extractor did not know about used to open.
    """
    calls: list[list[str]] = []

    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        calls.append(paths)
        return PreWriteDecision(allow=True)

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    response = await hook(
        {"tool_name": "SomeFutureWriteTool", "tool_input": {"target": "a.py"}}, None, CONTEXT
    )

    assert response["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert calls == []
    assert recorder.unreadable_writes == ["SomeFutureWriteTool"]
    # Loud, and not a lease conflict: nothing to widen the task's file_scope with.
    outcome = recorder.outcome(duration_ms=1)
    assert outcome.error_kind is ErrorKind.INFRA
    assert outcome.blocked_on_path is None


@pytest.mark.parametrize(
    "tool_input",
    [
        pytest.param(["linkstash/api.py"], id="list"),
        pytest.param("linkstash/api.py", id="string"),
        pytest.param(42, id="number"),
        pytest.param(None, id="null"),
        pytest.param({"file_path": {"path": "linkstash/api.py"}}, id="nested-object"),
        pytest.param({"file_path": ["linkstash/api.py"]}, id="list-valued-path"),
    ],
)
async def test_a_write_whose_input_is_not_shaped_like_one_is_denied_not_raised(
    workdir: Path, tool_input: object
) -> None:
    """`tool_input` is model-shaped JSON, not a validated schema.

    An exception thrown out of the hook is a decision handed to the CLI rather
    than made here, and there is no shape of input for which that is the answer
    we want. Anything the extractor cannot read a path out of — including
    something that is not a mapping at all — comes back as a deny.
    """
    calls: list[list[str]] = []

    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        calls.append(paths)
        return PreWriteDecision(allow=True)

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    response = await hook({"tool_name": "Write", "tool_input": tool_input}, None, CONTEXT)

    assert response["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert calls == []
    assert recorder.unreadable_writes == ["Write"]


async def test_a_path_the_filesystem_refuses_to_resolve_is_denied_not_raised(
    workdir: Path,
) -> None:
    """`Path.resolve()` raises on an embedded NUL. A raise is not a veto.

    It would leave the tool call's fate to the CLI rather than to this hook,
    which is the one thing the veto may never do.
    """
    raw = "linkstash/\x00api.py"
    calls: list[list[str]] = []

    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        calls.append(paths)
        return PreWriteDecision(allow=True)

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    response = await hook({"tool_name": "Write", "tool_input": {"file_path": raw}}, None, CONTEXT)

    assert response["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert calls == []
    assert recorder.out_of_bounds_paths == [raw]


async def test_the_post_write_ledger_survives_an_input_it_cannot_read(workdir: Path) -> None:
    """The ledger hook sees the same hostile shapes and must not raise either.

    It runs after the write, so raising here cannot protect anything — it only
    turns a recorded change into a failed turn.
    """
    logged: list[tuple[str, str]] = []

    async def on_post_write(path: str, tool: str) -> None:
        logged.append((path, tool))

    recorder = SessionRecorder()
    hook = make_post_write_hook(workdir=workdir, on_post_write=on_post_write, recorder=recorder)

    assert await hook({"tool_name": "Write", "tool_input": ["a.py"]}, None, CONTEXT) == {}
    assert (
        await hook({"tool_name": "Write", "tool_input": {"file_path": "a\x00b"}}, None, CONTEXT)
        == {}
    )
    assert logged == []
    assert recorder.files_written == []


async def test_the_denied_path_is_the_one_coordination_named(workdir: Path) -> None:
    """One tool call, several paths, and only the second one is held."""

    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        return PreWriteDecision(allow=False, message="held", path="linkstash/store.py")

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    await hook(
        {
            "tool_name": "MultiEdit",
            "tool_input": {
                "edits": [{"file_path": "linkstash/api.py"}, {"file_path": "linkstash/store.py"}]
            },
        },
        None,
        CONTEXT,
    )

    assert recorder.denied_paths == ["linkstash/store.py"]
    assert recorder.outcome(duration_ms=1).blocked_on_path == "linkstash/store.py"


async def test_an_unnamed_denial_still_blocks_on_a_real_path(workdir: Path) -> None:
    """A coordination layer that names no path leaves the first one as the answer."""

    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        return PreWriteDecision(allow=False, message="held")

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    await hook(
        {"tool_name": "Edit", "tool_input": {"file_path": "linkstash/api.py"}}, None, CONTEXT
    )

    assert recorder.denied_paths == ["linkstash/api.py"]


# ---------------------------------------------------------------------------
# PostToolUse hook
# ---------------------------------------------------------------------------


async def test_post_write_hook_records_the_ledger_entry(workdir: Path) -> None:
    logged: list[tuple[str, str]] = []

    async def on_post_write(path: str, tool: str) -> None:
        logged.append((path, tool))

    recorder = SessionRecorder()
    hook = make_post_write_hook(workdir=workdir, on_post_write=on_post_write, recorder=recorder)

    response = await hook(
        {"tool_name": "Write", "tool_input": {"file_path": str(workdir / "linkstash/api.py")}},
        None,
        CONTEXT,
    )

    assert response == {}
    assert logged == [("linkstash/api.py", "Write")]
    assert recorder.files_written == ["linkstash/api.py"]


async def test_post_write_hook_lists_each_file_once(workdir: Path) -> None:
    async def on_post_write(path: str, tool: str) -> None:
        return None

    recorder = SessionRecorder()
    hook = make_post_write_hook(workdir=workdir, on_post_write=on_post_write, recorder=recorder)

    for _ in range(3):
        await hook(
            {"tool_name": "Edit", "tool_input": {"file_path": "linkstash/api.py"}}, None, CONTEXT
        )

    assert recorder.files_written == ["linkstash/api.py"]


# ---------------------------------------------------------------------------
# Usage accounting
# ---------------------------------------------------------------------------


def test_usage_of_a_single_model() -> None:
    usage = {"claude-haiku-4-5": {"inputTokens": 657, "outputTokens": 94, "costUSD": 0.0012}}

    assert sum_model_usage(usage) == (657, 94, 0.0012)


def test_usage_is_summed_across_models() -> None:
    usage = {
        "claude-haiku-4-5": {"inputTokens": 600, "outputTokens": 90, "costUSD": 0.001},
        "claude-sonnet-4-5": {"inputTokens": 400, "outputTokens": 10, "costUSD": 0.004},
    }

    assert sum_model_usage(usage) == (1000, 100, pytest.approx(0.005))


@pytest.mark.parametrize("usage", [None, {}])
def test_absent_usage_reads_as_zero(usage: dict[str, Any] | None) -> None:
    assert sum_model_usage(usage) == (0, 0, 0.0)


def test_partial_usage_entries_do_not_break_accounting() -> None:
    usage = {"some-model": {"inputTokens": 12}}

    assert sum_model_usage(usage) == (12, 0, 0.0)


def test_cached_prompt_tokens_count_as_input_tokens() -> None:
    """They were read and they were billed; cost_usd already prices them."""
    usage = {
        "claude-haiku-4-5": {
            "inputTokens": 421,
            "cacheReadInputTokens": 38_112,
            "cacheCreationInputTokens": 9_004,
            "outputTokens": 613,
            "costUSD": 0.0094,
        }
    }

    assert sum_model_usage(usage) == (47_537, 613, pytest.approx(0.0094))


def test_a_malformed_usage_record_does_not_fail_a_session_that_ran() -> None:
    """The task already ran and was already paid for; accounting is not a verdict."""
    usage = {
        "claude-haiku-4-5": {"inputTokens": "n/a", "outputTokens": 10, "costUSD": None},
        "claude-sonnet-4-5": "not-a-mapping",
    }

    assert sum_model_usage(usage) == (0, 10, 0.0)


# ---------------------------------------------------------------------------
# Message observation
# ---------------------------------------------------------------------------


def test_the_init_frame_is_captured_whole() -> None:
    recorder = SessionRecorder()
    data = {
        "session_id": "sess_abc",
        "cwd": "/tmp/demo-repo",
        "model": "claude-haiku-4-5-20251001",
        "permissionMode": "dontAsk",
        "tools": ["Read", "Write", "Edit"],
    }

    recorder.observe(SystemMessage(subtype="init", data=data))

    assert recorder.init_frame == data
    assert recorder.session_id == "sess_abc"


def test_thinking_token_frames_are_dropped() -> None:
    recorder = SessionRecorder()

    recorder.observe(SystemMessage(subtype="thinking_tokens", data={"tokens": 128}))

    assert recorder.init_frame is None
    assert recorder.session_id is None


# ---------------------------------------------------------------------------
# Outcome construction
# ---------------------------------------------------------------------------


def test_a_clean_session_is_a_success_with_usage() -> None:
    recorder = SessionRecorder()
    recorder.record_write("linkstash/middleware.py")
    recorder.observe(make_result())

    outcome = recorder.outcome(duration_ms=15612)

    assert outcome.ok is True
    assert outcome.error_kind is None
    assert outcome.summary == "Added the middleware and wired it into the app."
    assert (outcome.input_tokens, outcome.output_tokens) == (8421, 613)
    assert outcome.cost_usd == pytest.approx(0.0094)
    assert outcome.duration_ms == 15612
    assert outcome.session_id == "sess_abc"
    assert outcome.files_written == ("linkstash/middleware.py",)


def test_cost_falls_back_to_the_model_usage_sum() -> None:
    recorder = SessionRecorder()
    recorder.observe(make_result(total_cost_usd=None))

    assert recorder.outcome(duration_ms=1).cost_usd == pytest.approx(0.0094)


def test_a_reported_error_is_an_agent_error() -> None:
    recorder = SessionRecorder()
    recorder.observe(make_result(is_error=True, result="tests failed"))

    outcome = recorder.outcome(duration_ms=1)

    assert outcome.ok is False
    assert outcome.error_kind is ErrorKind.AGENT_ERROR
    assert outcome.error == "tests failed"


@pytest.mark.parametrize(
    ("subtype", "terminal_reason"),
    [("error_max_turns", None), ("error_max_budget_usd", None), ("success", "max_turns")],
)
def test_an_sdk_imposed_stop_is_a_budget_error(subtype: str, terminal_reason: str | None) -> None:
    recorder = SessionRecorder()
    recorder.observe(make_result(subtype=subtype, terminal_reason=terminal_reason, is_error=True))

    assert recorder.outcome(duration_ms=1).error_kind is ErrorKind.BUDGET


def test_a_veto_beats_whatever_the_session_reported() -> None:
    recorder = SessionRecorder()
    recorder.record_denial("linkstash/api.py")
    recorder.record_denial("linkstash/store.py")
    recorder.observe(make_result(is_error=True, result="I could not edit api.py"))

    outcome = recorder.outcome(duration_ms=1)

    assert outcome.error_kind is ErrorKind.VETO
    assert outcome.blocked_on_path == "linkstash/api.py"


def test_an_unreachable_server_beats_the_veto_it_caused() -> None:
    recorder = SessionRecorder()
    recorder.record_hook_error(ConnectionError("boom"))
    recorder.record_denial("linkstash/api.py")

    outcome = recorder.outcome(duration_ms=1)

    # Recording this as a veto would widen file_scope with a path nobody held.
    assert outcome.error_kind is ErrorKind.INFRA
    assert outcome.blocked_on_path is None


def test_only_the_first_hook_failure_is_kept() -> None:
    recorder = SessionRecorder()
    recorder.record_hook_error(ConnectionError("first"))
    recorder.record_hook_error(ConnectionError("second"))

    assert recorder.hook_error == "ConnectionError: first"


def test_an_escape_attempt_fails_the_task() -> None:
    recorder = SessionRecorder()
    recorder.record_out_of_bounds("/etc/passwd")
    recorder.observe(make_result())

    outcome = recorder.outcome(duration_ms=1)

    assert outcome.ok is False
    assert outcome.error_kind is ErrorKind.AGENT_ERROR
    assert "/etc/passwd" in (outcome.error or "")


def test_a_stream_that_never_produced_a_result_is_infra() -> None:
    outcome = SessionRecorder().outcome(duration_ms=1)

    assert outcome.ok is False
    assert outcome.error_kind is ErrorKind.INFRA


# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------


def test_options_pin_the_posture_the_veto_depends_on(workdir: Path) -> None:
    settings = Settings(max_turns=40, task_budget_usd=0.5, model="claude-haiku-4-5-20251001")

    options = build_options(workdir=workdir, settings=settings, hooks={}, stderr=None)

    # Deny-by-default, not approve-by-default. `bypassPermissions` would also
    # stop an unattended runner hanging on a prompt, but it approves everything
    # that reaches the permission step — the opposite failure direction from the
    # one a fleet pointed at someone else's checkout can afford.
    assert options.permission_mode == "dontAsk"
    # The two lists do different jobs: `tools` is what the session has at all,
    # `allowed_tools` is what `dontAsk` will approve without prompting.
    assert options.tools == list(SESSION_TOOLS)
    assert options.allowed_tools == list(SESSION_TOOLS)
    # Hermetic: no host ~/.claude agents, skills or CLAUDE.md.
    assert options.setting_sources == []
    assert options.cwd == str(workdir)
    assert options.max_turns == 40
    assert options.max_budget_usd == 0.5
    assert options.max_buffer_size is not None
    assert options.max_buffer_size > 1024 * 1024


def test_a_target_repository_cannot_hand_the_session_its_own_mcp_tools(workdir: Path) -> None:
    """The fleet runs against someone else's checkout, so that checkout is untrusted input.

    `setting_sources=[]` gates settings *files* and stops there; MCP servers load
    on a separate path. A repository that ships a `.mcp.json` — plenty do — would
    otherwise give the session tools that are not in `SESSION_TOOLS` and do not
    match `WRITE_TOOL_MATCHER`. `dontAsk` denies them for being unlisted, which is
    the whole reason for preferring it, but a tool that never enters the session
    cannot be reached by a bug in the layer that does the denying.

    Asserting the SDK's default as well is deliberate: the flag only means
    something because the default is permissive, and a default that flipped would
    make this test pass for the wrong reason.
    """
    (workdir / ".mcp.json").write_text('{"mcpServers": {"fs": {"command": "writes-files"}}}\n')

    options = build_options(workdir=workdir, settings=Settings(), hooks={}, stderr=None)

    assert options.strict_mcp_config is True
    assert ClaudeAgentOptions().strict_mcp_config is False
    assert not options.mcp_servers


def test_the_session_gets_no_shell() -> None:
    """Bash is left out of the tool set on purpose, and this is where that is stated.

    The veto is a PreToolUse hook on Write/Edit/MultiEdit/NotebookEdit: it sees a
    write because the tool input names a file. A shell command names nothing —
    `sed -i`, a redirect, a formatter, a codegen script — so a file written
    through Bash takes no lease and never reaches the change ledger, which makes
    the lease stop being the only way a file changes. Gating it instead would
    mean parsing arbitrary shell, which is not a boundary worth trusting. If you
    are here because an agent needs to run the test suite: it cannot, and that
    is the trade. Change the design, not this list.
    """
    assert "Bash" not in SESSION_TOOLS
    # A subagent would come with its own tool set, Bash included.
    assert "Task" not in SESSION_TOOLS


def test_every_gated_write_tool_is_a_tool_the_session_actually_has() -> None:
    """The veto matcher and the tool set have to describe the same session."""
    assert set(WRITE_TOOL_MATCHER.split("|")) <= set(SESSION_TOOLS)


# The tools in `SESSION_TOOLS` that cannot mutate the working tree, and so need
# no lease. Spelled out rather than derived, because the whole point is that
# adding a tool to the session has to be an explicit claim about which side of
# the veto it falls on.
READ_ONLY_SESSION_TOOLS = frozenset({"Read", "Glob", "Grep"})


def test_no_tool_in_the_session_writes_without_passing_the_veto() -> None:
    """The other direction of the test above, and the stronger one.

    The matcher being a subset of the tool set says every gated tool exists. It
    does not say every tool that can write is gated — and that is the claim the
    whole design rests on. Adding a tool to `SESSION_TOOLS` without either
    gating it or declaring it read-only fails here, which is the only moment
    anyone will think about it.
    """
    ungated = set(SESSION_TOOLS) - set(WRITE_TOOL_MATCHER.split("|")) - READ_ONLY_SESSION_TOOLS

    assert not ungated, (
        f"{', '.join(sorted(ungated))} is in the session's tool set but neither gated by "
        f"{WRITE_TOOL_MATCHER!r} nor declared read-only. If it can touch the tree it has to "
        "go in `WriteTool` so the PreToolUse hook sees it; if it cannot, say so by adding it "
        "to READ_ONLY_SESSION_TOOLS here. A write tool nobody gated takes no lease and lands "
        "in no ledger, which is the one failure this system exists to prevent."
    )


# ---------------------------------------------------------------------------
# run_session, with the SDK stream stubbed out
# ---------------------------------------------------------------------------


async def noop_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
    return PreWriteDecision(allow=True)


async def noop_post_write(path: str, tool: str) -> None:
    return None


async def test_run_session_reports_what_the_stream_produced(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_drain(
        *, prompt: str, options: Any, recorder: SessionRecorder, transport: Any = None
    ) -> None:
        assert "Add request logging middleware" in prompt
        recorder.observe(SystemMessage(subtype="init", data={"session_id": "sess_abc"}))
        recorder.record_write("linkstash/middleware.py")
        recorder.observe(make_result())

    monkeypatch.setattr("codefleet.session._drain", fake_drain)

    outcome = await run_session(
        task=make_task(),
        workdir=workdir,
        settings=Settings(),
        on_pre_write=noop_pre_write,
        on_post_write=noop_post_write,
    )

    assert outcome.ok is True
    assert outcome.session_id == "sess_abc"
    assert outcome.files_written == ("linkstash/middleware.py",)
    assert outcome.init_frame == {"session_id": "sess_abc"}


async def test_run_session_registers_both_hooks_under_the_write_matcher(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}

    async def fake_drain(
        *, prompt: str, options: Any, recorder: SessionRecorder, transport: Any = None
    ) -> None:
        captured["options"] = options
        recorder.observe(make_result())

    monkeypatch.setattr("codefleet.session._drain", fake_drain)

    await run_session(
        task=make_task(),
        workdir=workdir,
        settings=Settings(),
        on_pre_write=noop_pre_write,
        on_post_write=noop_post_write,
    )

    hooks = captured["options"].hooks
    assert set(hooks) == {"PreToolUse", "PostToolUse"}
    for matchers in hooks.values():
        assert len(matchers) == 1
        assert matchers[0].matcher == WRITE_TOOL_MATCHER
        assert len(matchers[0].hooks) == 1


async def hang(
    *, prompt: str, options: Any, recorder: SessionRecorder, transport: Any = None
) -> None:
    recorder.observe(SystemMessage(subtype="init", data={"session_id": "sess_hung"}))
    await asyncio.sleep(30)


async def a_sleeping_process() -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(30)")


async def test_run_session_times_out_and_reaps(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    child = await a_sleeping_process()

    class OwnTransport:
        _process = child

    monkeypatch.setattr("codefleet.session._drain", hang)
    monkeypatch.setattr("codefleet.session._new_transport", lambda **_: OwnTransport())

    outcome = await run_session(
        task=make_task(),
        workdir=workdir,
        settings=Settings(task_timeout=0.05),
        on_pre_write=noop_pre_write,
        on_post_write=noop_post_write,
    )

    assert outcome.ok is False
    assert outcome.error_kind is ErrorKind.TIMEOUT
    assert outcome.session_id == "sess_hung"
    assert str(child.pid) in (outcome.error or "")
    assert await child.wait() != 0


async def test_a_timeout_leaves_another_runners_cli_child_alone(
    workdir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every runner in this process shares the SDK's one child registry.

    A sibling whose session started after this one is not this one's to kill —
    reaping by "what appeared since I started" takes down a healthy agent
    mid-write and reports it as an unexplained infra failure.
    """
    from claude_agent_sdk._internal.transport import subprocess_cli

    sibling = await a_sleeping_process()

    async def hang_while_a_sibling_starts(
        *, prompt: str, options: Any, recorder: SessionRecorder, transport: Any = None
    ) -> None:
        subprocess_cli._ACTIVE_CHILDREN.add(sibling)
        await asyncio.sleep(30)

    monkeypatch.setattr("codefleet.session._drain", hang_while_a_sibling_starts)

    try:
        outcome = await run_session(
            task=make_task(),
            workdir=workdir,
            settings=Settings(task_timeout=0.05),
            on_pre_write=noop_pre_write,
            on_post_write=noop_post_write,
        )

        assert outcome.error_kind is ErrorKind.TIMEOUT
        await asyncio.sleep(0.1)
        assert sibling.returncode is None, "the sibling runner's CLI was killed"
    finally:
        subprocess_cli._ACTIVE_CHILDREN.discard(sibling)
        # Tolerant so a reaper that already killed it fails on the assertion above.
        with suppress(ProcessLookupError):
            sibling.kill()
        await sibling.wait()


def test_the_transport_still_hands_us_the_child_the_reaper_kills(workdir: Path) -> None:
    """The reaper reads an SDK private, so assert it is still there.

    An unconnected transport has no child and there is nothing to kill; the
    point is that a rename in the SDK fails here rather than silently leaking a
    CLI process on every timeout.
    """
    from claude_agent_sdk._internal.transport import subprocess_cli

    options = build_options(workdir=workdir, settings=Settings(), hooks={}, stderr=None)
    transport = _new_transport(prompt="hello", options=options)

    assert transport._process is None  # type: ignore[attr-defined]
    assert _reap_transport_child(transport) is None
    assert isinstance(subprocess_cli._ACTIVE_CHILDREN, set)


async def test_run_session_writes_stderr_to_its_own_file(
    workdir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_drain(
        *, prompt: str, options: Any, recorder: SessionRecorder, transport: Any = None
    ) -> None:
        options.stderr("cli said something")
        recorder.observe(make_result())

    monkeypatch.setattr("codefleet.session._drain", fake_drain)
    stderr_path = tmp_path / "runs" / "runner-1.stderr.log"

    await run_session(
        task=make_task(),
        workdir=workdir,
        settings=Settings(),
        on_pre_write=noop_pre_write,
        on_post_write=noop_post_write,
        stderr_path=stderr_path,
    )

    assert stderr_path.read_text() == "cli said something\n"


async def test_a_stderr_line_arriving_after_the_session_ends_is_dropped(
    workdir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The SDK's stderr reader is detached and outlives a session we timed out.

    It still holds the callback, and the log file it writes to is closed by
    then; raising there surfaces inside the SDK's own task.
    """
    captured: dict[str, Any] = {}

    async def hang_holding_stderr(
        *, prompt: str, options: Any, recorder: SessionRecorder, transport: Any = None
    ) -> None:
        captured["stderr"] = options.stderr
        await asyncio.sleep(30)

    monkeypatch.setattr("codefleet.session._drain", hang_holding_stderr)
    stderr_path = tmp_path / "runs" / "runner-1.stderr.log"

    outcome = await run_session(
        task=make_task(),
        workdir=workdir,
        settings=Settings(task_timeout=0.05),
        on_pre_write=noop_pre_write,
        on_post_write=noop_post_write,
        stderr_path=stderr_path,
    )

    assert outcome.error_kind is ErrorKind.TIMEOUT
    captured["stderr"]("a line the dying CLI wrote on its way out")


# ---------------------------------------------------------------------------
# Import-time behavior
# ---------------------------------------------------------------------------


def test_importing_the_module_does_not_rewrite_the_hosts_environment() -> None:
    """The SDK filters CLAUDECODE and forces CLAUDE_CODE_ENTRYPOINT at spawn time.

    Doing it here as well changed nothing about the child and everything about
    the parent — `codefleet` shells out to git and pytest after this import.
    """
    env = {**os.environ, "CLAUDECODE": "1", "CLAUDE_CODE_ENTRYPOINT": "cli"}
    probe = (
        "import os, codefleet.session; "
        "print(os.environ.get('CLAUDECODE'), os.environ.get('CLAUDE_CODE_ENTRYPOINT'))"
    )

    result = subprocess.run(
        [sys.executable, "-c", probe], env=env, capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == "1 cli"


# ---------------------------------------------------------------------------
# Live
# ---------------------------------------------------------------------------


@pytest.mark.live
async def test_a_denied_write_never_lands_and_the_runner_reports_it_blocked(tmp_path: Path) -> None:
    """The mechanism, end to end, against the real API.

    Everything above proves the pieces are shaped right; only this proves the CLI
    actually honors the veto.

    Named for what it asserts and no more. The deny carries no `continue_: false`,
    so the model is free to keep going and ask for something else — it would be
    refused on anything else held. What is enforced is that the denied call does
    not execute and the outcome comes back `VETO`; a session that also chose to
    stop is the model cooperating, not the guarantee.
    """
    tree = tmp_path / "tree"
    tree.mkdir()
    target = tree / "settings.py"
    original = 'THEME = "light"\nRETRIES = 3\n'
    target.write_text(original)

    async def deny_everything(paths: list[str], tool: str) -> PreWriteDecision:
        return PreWriteDecision(
            allow=False,
            message=(
                f"{paths[0]} is held by another agent. Do not retry it and do not edit "
                f"around it. Stop now and report that you are blocked on {paths[0]}."
            ),
        )

    async def ignore(path: str, tool: str) -> None:
        return None

    task = make_task(
        title="Bump the retry count",
        description="In settings.py change RETRIES from 3 to 5. Nothing else.",
        file_scope=("settings.py",),
    )

    outcome = await run_session(
        task=task,
        workdir=tree,
        settings=Settings(max_turns=6, task_timeout=180),
        on_pre_write=deny_everything,
        on_post_write=ignore,
    )

    assert outcome.error_kind is ErrorKind.VETO
    assert outcome.blocked_on_path == "settings.py"
    assert target.read_text() == original
    assert outcome.init_frame is not None
    assert outcome.init_frame["permissionMode"] == "dontAsk"
    assert outcome.input_tokens > 0
    shutil.rmtree(tree)
