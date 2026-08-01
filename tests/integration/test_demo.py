"""The quickstart, executed.

`codefleet demo --dry-run` is the first command anyone runs, and the thing it
exists to show is the write veto: two tasks whose declared scopes do not overlap
are co-scheduled — correctly, by the scheduler's own rules — one of them reaches
for a file it never declared, and the write is denied before it lands. So this
runs the shipped demo, against the shipped graph, with the shipped scripted plan,
and asks the resulting event log whether that actually happened.

`test_coordination` already proves the mechanism, but it proves it with a graph
written for it. A graph written for a test cannot notice the demo's own graph and
the demo's own script drifting out of alignment, and a demo that quietly stops
demonstrating anything still exits 0.

The veto here is not a lucky race. Both contending tasks keep the contested file
for longer than one poll interval after taking it, so whichever runner picks its
work up first, the other is denied. That is the property this test guards: an
earlier plan held the file for less time than two runners can drift apart, and
whether the demo showed a veto depended on which runner happened to poll first.
"""

from __future__ import annotations

import asyncio
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from codefleet import cli
from codefleet.config import Settings
from codefleet.models import EventType, Task, TaskStatus
from codefleet.store import Store

# Long enough to absorb a loaded CI box, short enough to fail rather than hang.
DEMO_TIMEOUT_S = 180.0


@dataclass(frozen=True, slots=True)
class DemoRecord:
    """What one `codefleet demo --dry-run` left behind: its verdict and its log."""

    exit_code: int
    events: list[dict[str, Any]]
    tasks: dict[str, Task]

    def of_type(self, kind: EventType) -> list[dict[str, Any]]:
        return [event for event in self.events if event["type"] == kind]


@pytest.fixture(scope="module")
def demo(tmp_path_factory: pytest.TempPathFactory, demo_repo: Path, demo_tasks: Path) -> DemoRecord:
    root = tmp_path_factory.mktemp("demo")
    return asyncio.run(_run_demo(root, demo_repo, demo_tasks))


async def _run_demo(root: Path, source: Path, graph: Path) -> DemoRecord:
    workspace = root / "workspace"
    shutil.copytree(source, workspace, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))

    # Everything not named here stays at its shipped default — in particular the
    # poll interval, which is the drift the veto has to survive.
    settings = Settings(
        db=root / "codefleet.db",
        workdir=workspace,
        run_dir=root / "runs",
        port=cli._free_port("127.0.0.1"),
    )
    exit_code = await cli._demo(settings, graph, dry_run=True, timeout=DEMO_TIMEOUT_S)

    # Read after the fact from the file the run left, because by now the server
    # that served `GET /events` has been shut down by the demo itself.
    store = await Store.open(settings.db)
    try:
        replay = await store.events_since(0, limit=5000)
        events = [event.model_dump(mode="json") for event in replay]
        tasks = await store.list_tasks()
    finally:
        await store.close()
    return DemoRecord(exit_code, events, {task.id: task for task in tasks})


def test_the_demo_finishes_green(demo: DemoRecord) -> None:
    """Every task succeeded and the edited repository's own suite still passes."""
    assert demo.exit_code == 0
    assert {task_id: task.status for task_id, task in demo.tasks.items()} == {
        "T1": TaskStatus.SUCCEEDED,
        "T2": TaskStatus.SUCCEEDED,
        "T3": TaskStatus.SUCCEEDED,
        "T4": TaskStatus.SUCCEEDED,
        "T5": TaskStatus.SUCCEEDED,
    }


def test_the_demo_cascades(demo: DemoRecord) -> None:
    """A dependent is scheduled by the server, after its dependency and unasked."""
    unblocked = demo.of_type(EventType.TASK_UNBLOCKED)
    assert {event["task_id"] for event in unblocked} == {"T2", "T5"}

    for event in unblocked:
        succeeded = next(
            other
            for other in demo.of_type(EventType.TASK_SUCCEEDED)
            if other["task_id"] == event["payload"]["unblocked_by"]
        )
        assigned = next(
            other
            for other in demo.of_type(EventType.TASK_ASSIGNED)
            if other["task_id"] == event["task_id"]
        )
        assert succeeded["id"] < event["id"] < assigned["id"]


def test_the_demo_vetoes_a_write_and_the_loser_recovers(demo: DemoRecord) -> None:
    """The headline, asserted rather than hoped for."""
    denials = demo.of_type(EventType.LEASE_DENIED)
    assert len(denials) == 1, (
        "the demo did not demonstrate the one thing it exists to demonstrate; "
        f"denials seen: {denials}"
    )
    denial = denials[0]
    assert denial["payload"]["path"] == cli.DEMO_CONTESTED_PATH

    # The write never landed, so the denied task recorded no change against it.
    assert not [
        event
        for event in demo.of_type(EventType.FILE_CHANGED)
        if event["task_id"] == denial["task_id"]
        and event["payload"]["path"] == cli.DEMO_CONTESTED_PATH
        and event["id"] < denial["id"]
    ], "a vetoed write reached the ledger, so it reached the file"

    loser = demo.tasks[denial["task_id"]]
    assert loser.attempts >= 2
    assert loser.status is TaskStatus.SUCCEEDED
    assert cli.DEMO_CONTESTED_PATH in loser.file_scope, (
        "the denied path was not folded into the task's scope, so the retry could "
        "be co-scheduled with the holder all over again (spec 4.5 step 8)"
    )

    released = next(
        event
        for event in demo.of_type(EventType.LEASE_RELEASED)
        if event["task_id"] == denial["payload"]["holder_task_id"]
        and event["payload"]["path"] == cli.DEMO_CONTESTED_PATH
    )
    retry = [
        event
        for event in demo.of_type(EventType.TASK_ASSIGNED)
        if event["task_id"] == denial["task_id"]
    ][1]
    assert denial["id"] < released["id"] < retry["id"], (
        "the vetoed task was retried before the holder let go of the file"
    )
