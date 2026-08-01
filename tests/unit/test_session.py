"""Tests for the SDK session module.

Nothing here talks to the API. The pieces that matter — what the agent is told,
which paths a write tool is asking for, whether a path escapes the working tree,
the literal dicts the CLI accepts as allow and deny, how usage is summed, and
which failure wins when several happened — are all pure and are tested as such.
The one test that needs the network is marked `live` and deselected by default.
"""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from typing import Any

import pytest
from claude_agent_sdk import HookContext, ResultMessage, SystemMessage

from codefleet.config import Settings
from codefleet.models import WRITE_TOOL_MATCHER, ErrorKind, Task
from codefleet.session import (
    PreWriteDecision,
    SessionRecorder,
    _live_cli_children,
    _reap_cli_children,
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


async def test_a_write_tool_with_no_extractable_path_is_not_a_lease_question(
    workdir: Path,
) -> None:
    calls: list[list[str]] = []

    async def on_pre_write(paths: list[str], tool: str) -> PreWriteDecision:
        calls.append(paths)
        return PreWriteDecision(allow=True)

    recorder = SessionRecorder()
    hook = make_pre_write_hook(workdir=workdir, on_pre_write=on_pre_write, recorder=recorder)

    response = await hook({"tool_name": "Write", "tool_input": {}}, None, CONTEXT)

    assert response == {}
    assert calls == []


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


# ---------------------------------------------------------------------------
# Message observation
# ---------------------------------------------------------------------------


def test_the_init_frame_is_captured_whole() -> None:
    recorder = SessionRecorder()
    data = {
        "session_id": "sess_abc",
        "cwd": "/tmp/demo-repo",
        "model": "claude-haiku-4-5-20251001",
        "permissionMode": "bypassPermissions",
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

    assert options.permission_mode == "bypassPermissions"
    # Hermetic: no host ~/.claude agents, skills or CLAUDE.md.
    assert options.setting_sources == []
    assert options.cwd == str(workdir)
    assert options.max_turns == 40
    assert options.max_budget_usd == 0.5
    assert options.max_buffer_size is not None
    assert options.max_buffer_size > 1024 * 1024


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
    async def fake_drain(*, prompt: str, options: Any, recorder: SessionRecorder) -> None:
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

    async def fake_drain(*, prompt: str, options: Any, recorder: SessionRecorder) -> None:
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


async def test_run_session_times_out_and_reaps(
    workdir: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    async def hang(*, prompt: str, options: Any, recorder: SessionRecorder) -> None:
        recorder.observe(SystemMessage(subtype="init", data={"session_id": "sess_hung"}))
        await asyncio.sleep(30)

    reaped: list[frozenset[Any]] = []

    def fake_reap(preexisting: frozenset[Any]) -> list[int]:
        reaped.append(preexisting)
        return [4242]

    monkeypatch.setattr("codefleet.session._drain", hang)
    monkeypatch.setattr("codefleet.session._reap_cli_children", fake_reap)

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
    assert len(reaped) == 1
    assert "4242" in (outcome.error or "")


def test_the_reaper_finds_the_sdks_child_registry() -> None:
    """The reaper reads an SDK private, so assert it is still there.

    With no session running the registry is empty and there is nothing to kill;
    the point of the test is that a rename in the SDK fails here rather than
    silently leaking a CLI process on every timeout.
    """
    assert _reap_cli_children(_live_cli_children()) == []


async def test_run_session_writes_stderr_to_its_own_file(
    workdir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_drain(*, prompt: str, options: Any, recorder: SessionRecorder) -> None:
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


# ---------------------------------------------------------------------------
# Live
# ---------------------------------------------------------------------------


@pytest.mark.live
async def test_a_denied_write_stops_the_agent_and_leaves_the_file_alone(tmp_path: Path) -> None:
    """The mechanism, end to end, against the real API.

    Everything above proves the pieces are shaped right; only this proves the CLI
    actually honours the veto.
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
    assert outcome.init_frame["permissionMode"] == "bypassPermissions"
    assert outcome.input_tokens > 0
    shutil.rmtree(tree)
