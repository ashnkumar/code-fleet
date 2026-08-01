"""The architecture, enforced mechanically instead of promised in prose.

Spec 6.3 makes three claims: exactly one module may talk to the Claude Agent SDK,
the runner holds no coordination logic, and the scheduler is a pure function.
Claims like that rot — not because anyone disagrees with them, but because the
shortest path to a feature is usually one import away from breaking one of them,
and nothing complains.

These tests read the source with `ast` rather than importing it. That matters
three times over: an import inside a function body is caught, an import guarded
by `if TYPE_CHECKING:` is caught, and a module that cannot be imported at all in
this environment is still checked. Importing would also make the test itself the
thing that pulls the SDK in, which is the opposite of the point.

Every failure message below explains why the boundary exists, because at the
moment someone trips one of these, that message is the only documentation they
will read.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

PACKAGE = "codefleet"

# The one module allowed to speak to the SDK, and the two the runner may not know
# about. Spelled out as data so a violation reads as a diff on this list.
SDK = "claude_agent_sdk"
SESSION = "codefleet.session"
RUNNER = "codefleet.runner"
SCHEDULER = "codefleet.scheduler"
STORE = "codefleet.store"

# Anything that reaches a database, a socket, an event loop or a web framework.
# The scheduler takes a snapshot and returns decisions; if it needs any of these
# it has stopped being a pure function of its arguments.
IMPURE = ("codefleet.store", "httpx", "asyncio", "fastapi", "aiosqlite", "sqlite3")


@pytest.fixture(scope="module")
def modules(repo_root: Path) -> dict[str, Path]:
    """Every module in the package, keyed by its dotted name."""
    source_root = repo_root / "src" / PACKAGE
    found = {
        f"{PACKAGE}.{path.stem}": path
        for path in sorted(source_root.glob("*.py"))
        if path.stem != "__init__"
    }
    assert found, f"no modules found under {source_root}"
    return found


def imports_of(path: Path) -> set[str]:
    """Every module name imported anywhere in a file, however it is spelled.

    `ast.walk` rather than a scan of the top level, so an import buried in a
    function or behind `TYPE_CHECKING` counts the same as one at the top. A
    `from x import y` records both `x` and `x.y`, because that is how a submodule
    import looks and there is no way to tell it from a name import without
    resolving the package.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            found.add(node.module)
            found.update(f"{node.module}.{alias.name}" for alias in node.names)
    return found


def imports(path: Path, module: str) -> bool:
    """True if the file imports `module` or anything inside it."""
    return any(name == module or name.startswith(f"{module}.") for name in imports_of(path))


def test_only_the_session_module_talks_to_the_sdk(modules: dict[str, Path]) -> None:
    offenders = sorted(
        name for name, path in modules.items() if name != SESSION and imports(path, SDK)
    )
    assert not offenders, (
        f"{', '.join(offenders)} imports {SDK}, which must live only in {SESSION}.\n"
        "Confining it there is what lets every coordination test in this suite run a real "
        "server and real runners with no API key, no model call and no spend. The moment a "
        "second module needs the SDK, a test that exercises it needs the SDK too, and the "
        "claim that the coordination is independent of the agent becomes unfalsifiable."
    )


def test_the_session_module_does_talk_to_the_sdk(modules: dict[str, Path]) -> None:
    """Guards the test above against passing for the wrong reason.

    If `session.py` were renamed or emptied, every other assertion here would go
    green while the boundary it describes had ceased to exist.
    """
    assert imports(modules[SESSION], SDK), (
        f"{SESSION} no longer imports {SDK}. Either the SDK moved — in which case the "
        "boundary above is now guarding nothing — or the module was renamed and this file "
        "needs to follow it."
    )


@pytest.mark.parametrize("forbidden", [STORE, SCHEDULER])
def test_the_runner_holds_no_coordination_logic(modules: dict[str, Path], forbidden: str) -> None:
    assert not imports(modules[RUNNER], forbidden), (
        f"{RUNNER} imports {forbidden}. A runner registers, heartbeats, reads the assignment "
        "the server already committed, and reports; it does not decide anything. Touching "
        f"{forbidden} means it now evaluates dependencies, compares file scopes or writes "
        "coordination state itself — three copies of the policy that have to agree, and a "
        "fleet whose behaviour depends on which runner build is deployed."
    )


@pytest.mark.parametrize("forbidden", IMPURE)
def test_the_scheduler_is_a_pure_function(modules: dict[str, Path], forbidden: str) -> None:
    assert not imports(modules[SCHEDULER], forbidden), (
        f"{SCHEDULER} imports {forbidden}. `schedule(state, now)` is handed a frozen snapshot "
        "and returns a list of decisions: it cannot read a clock, open a socket or touch "
        "SQLite. That is what makes its tests three lines of literal `FleetState` with no "
        "fixture, no event loop and no mock database — and what makes the assignment policy "
        "reviewable in one file rather than inferred from the order of a transaction."
    )
