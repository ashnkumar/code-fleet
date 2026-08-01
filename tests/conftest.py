"""Fixtures that more than one test tier needs.

Deliberately short. Anything only one module uses is defined in that module,
because a fixture two files away costs more to read than the three lines it
saved. What genuinely is shared is where the checkout lives and how to get a
disposable copy of the demo target codebase — the committed tree under
`examples/demo-repo` is read-only as far as the suite is concerned, since a test
that edits it in place would leave the repository dirty and the next run
starting from a different codebase than the last.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Build artefacts of the target repo's own test runs; never worth copying.
_JUNK = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache")


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def demo_repo() -> Path:
    source = REPO_ROOT / "examples" / "demo-repo"
    if not source.is_dir():
        pytest.skip(f"{source} is missing; run from a source checkout")
    return source


@pytest.fixture(scope="session")
def demo_tasks() -> Path:
    graph = REPO_ROOT / "examples" / "demo-tasks.yaml"
    if not graph.is_file():
        pytest.skip(f"{graph} is missing; run from a source checkout")
    return graph


@pytest.fixture
def workspace(tmp_path: Path, demo_repo: Path) -> Path:
    """A throwaway copy of the demo repository for a fleet to edit."""
    destination = tmp_path / "workspace"
    shutil.copytree(demo_repo, destination, ignore=_JUNK)
    return destination
