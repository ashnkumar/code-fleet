"""README.md and SPEC.md are checked against the code, not proofread.

Both documents make claims that are cheap to verify mechanically — the command
list, the settings table, a line count, a permission-mode fact — and expensive to
notice going stale. Each assertion below is one such claim. A failure here means
the document and the code have moved apart, and the pair needs reconciling; it
does not mean the assertion should be relaxed.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from codefleet import session
from codefleet.cli import app
from codefleet.config import Settings

_ROOT = Path(__file__).resolve().parents[2]
README = (_ROOT / "README.md").read_text(encoding="utf-8")
SPEC = (_ROOT / "SPEC.md").read_text(encoding="utf-8")
ENV_EXAMPLE = (_ROOT / ".env.example").read_text(encoding="utf-8")

COMMANDS = sorted(info.name or info.callback.__name__ for info in app.registered_commands)
SETTINGS_FIELDS = frozenset(Settings.model_fields)
MODULES = sorted(
    path.stem
    for path in (_ROOT / "src" / "codefleet").glob("*.py")
    if path.stem != "__init__" and path.stat().st_size > 0
)

# Bash is absent from SESSION_TOOLS on purpose: it is a write path no tool-name
# matcher can gate, so a shell write would take no lease and land in no ledger
# row. Both documents have to state the exclusion and what it costs.
SESSION_TOOLS = tuple(session.SESSION_TOOLS)


def _flagged_settings() -> frozenset[str]:
    """Setting names that some command exposes as a CLI option."""
    return (
        frozenset(
            name
            for info in app.registered_commands
            for name in inspect.signature(info.callback).parameters
        )
        & SETTINGS_FIELDS
    )


@pytest.mark.parametrize("command", COMMANDS)
def test_spec_names_every_shipped_command(command: str) -> None:
    """SPEC's component table is the only enumeration of the CLI surface."""
    assert f"`codefleet {command}`" in SPEC


@pytest.mark.parametrize("module", MODULES)
def test_spec_names_every_shipped_module(module: str) -> None:
    """A component table that lists most of the package is read as if it listed all of it."""
    assert f"`codefleet.{module}`" in SPEC


@pytest.mark.parametrize("field", sorted(SETTINGS_FIELDS))
def test_every_setting_is_documented_in_both_places(field: str) -> None:
    variable = f"CODEFLEET_{field.upper()}"
    assert variable in ENV_EXAMPLE, f"{variable} missing from .env.example"
    assert variable in SPEC, f"{variable} missing from the SPEC configuration table"


def test_run_dir_is_still_the_only_setting_without_a_flag() -> None:
    """Both documents claim this exception by name, so adding a flag has to update them."""
    assert sorted(SETTINGS_FIELDS - _flagged_settings()) == ["run_dir"]
    assert "CODEFLEET_RUN_DIR" in README


def test_readme_scheduler_line_count_is_current() -> None:
    source = (_ROOT / "src" / "codefleet" / "scheduler.py").read_text(encoding="utf-8")
    assert f"it is {len(source.splitlines())} lines" in README


def test_spec_does_not_describe_itself_as_unimplemented() -> None:
    assert "implementation pending" not in SPEC.lower()


def test_spec_defines_a_thin_runner_by_the_boundary_that_is_enforced() -> None:
    """A line budget nothing checks drifts; the import boundary has a test behind it."""
    assert "tests/unit/test_boundaries.py" in SPEC


def test_both_documents_quote_the_write_matcher_the_code_installs() -> None:
    """A truncated matcher in prose reads as a shorter gated tool set than the one that ships."""
    from codefleet.models import WRITE_TOOL_MATCHER

    truncated = WRITE_TOOL_MATCHER.rsplit("|", 1)[0]
    for document in (README, SPEC):
        assert WRITE_TOOL_MATCHER in document
        assert document.count(truncated) == document.count(WRITE_TOOL_MATCHER)


def test_neither_document_says_the_sdk_raises_the_shadow_warning() -> None:
    """It warns. The SDK's own `_warn_if_can_use_tool_shadowed` docstring says "no raise"."""
    from claude_agent_sdk.types import CanUseToolShadowedWarning

    assert issubclass(CanUseToolShadowedWarning, Warning)
    for document in (README, SPEC):
        assert not re.search(r"raises?\s+`?CanUseToolShadowedWarning", document)


def test_the_acceptedits_rejection_rests_on_what_the_sdk_documents() -> None:
    """The older claim — that acceptEdits sessions wrote nothing — does not reproduce (SPEC 6.1)."""
    from claude_agent_sdk import types as sdk_types

    sdk_source = Path(inspect.getfile(sdk_types)).read_text(encoding="utf-8")
    assert "Auto-accept file edit operations." in sdk_source
    for document in (README, SPEC):
        assert "auto-accepts file edits" in document
    assert "withdrawn" in SPEC


def test_neither_document_says_a_denied_agent_stops_cleanly() -> None:
    """The deny stops the tool call. Stopping the session is the model cooperating.

    `deny_response` returns no `continue_: false`, so nothing in the mechanism ends
    the turn — a document that says otherwise describes a guarantee we do not have.
    """
    from codefleet.session import deny_response

    assert "continue" not in str(deny_response("blocked")).replace("permissionDecision", "")
    for document in (README, SPEC):
        assert "stops cleanly" not in document


@pytest.mark.parametrize("tool", SESSION_TOOLS)
def test_both_documents_name_the_tools_the_session_may_use(tool: str) -> None:
    assert tool in README
    assert tool in SPEC


def test_both_documents_state_that_bash_is_excluded_and_what_it_costs() -> None:
    assert "Bash" not in SESSION_TOOLS
    for document in (README, SPEC):
        assert "Bash" in document
        assert "cannot run" in document
