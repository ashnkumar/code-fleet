"""The demo, for real: three Claude sessions editing one working tree.

Everything else in this suite proves the coordination is correct against a
scripted agent. This proves the coordination survives an agent that was not
told what it would do — that the veto fires on a write nobody scripted, that the
denied agent stops instead of editing around the block, and that the tree the
fleet leaves behind still passes the target repository's own tests.

Deselected by default (`-m 'not live'` lives in `pyproject.toml`) because it
costs money and needs `ANTHROPIC_API_KEY`. Kept cheap: five small tasks on the
default model, which is Haiku, against a ~200-line codebase.

Run it with `uv run pytest -m live`.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import aclosing, asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
import yaml

from codefleet.config import Settings
from codefleet.dashboard import stream_events
from codefleet.models import EventType, TaskStatus
from codefleet.runner import Runner
from codefleet.server import create_app

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        not os.environ.get("ANTHROPIC_API_KEY"),
        reason="ANTHROPIC_API_KEY is not set; the live tier talks to the real API",
    ),
]

RUN_TIMEOUT = 900.0
RUNNERS = 3


async def test_the_demo_run_coordinates_real_agents(
    tmp_path: Path, workspace: Path, demo_tasks: Path
) -> None:
    graph = yaml.safe_load(demo_tasks.read_text(encoding="utf-8"))["tasks"]
    record = await _run_fleet(tmp_path, workspace, graph)

    _assert_every_task_succeeded(record.tasks)
    _assert_the_cascade_fired(record.events, graph)
    _assert_a_write_was_vetoed(record.events, record.tasks)
    _assert_the_target_suite_still_passes(workspace)


# -- assertions --------------------------------------------------------------


def _assert_every_task_succeeded(tasks: dict[str, dict[str, Any]]) -> None:
    unfinished = {
        task_id: (task["status"], task["error"])
        for task_id, task in tasks.items()
        if task["status"] != TaskStatus.SUCCEEDED
    }
    assert not unfinished, f"the fleet did not drain cleanly: {unfinished}"


def _assert_the_cascade_fired(events: list[dict[str, Any]], graph: list[dict[str, Any]]) -> None:
    """No dependent may be assigned before every dependency has succeeded."""
    edges = [
        (task["id"], dependency) for task in graph for dependency in task.get("depends_on") or ()
    ]
    assert edges, f"{len(graph)} tasks and no edges — this graph proves nothing"

    for task_id, dependency_id in edges:
        succeeded = _one(events, EventType.TASK_SUCCEEDED, dependency_id)
        assignments = _select(events, EventType.TASK_ASSIGNED, task_id)
        assert assignments, f"{task_id} was never assigned"
        assert assignments[0]["id"] > succeeded["id"], (
            f"{task_id} was assigned before {dependency_id} succeeded"
        )
        unblocked = _select(events, EventType.TASK_UNBLOCKED, task_id)
        assert unblocked, f"{task_id} never became runnable through the cascade"


def _assert_a_write_was_vetoed(
    events: list[dict[str, Any]], tasks: dict[str, dict[str, Any]]
) -> None:
    """The headline: a write that a declared scope did not predict, stopped in flight."""
    denials = _select(events, EventType.LEASE_DENIED)
    assert denials, (
        "no write was vetoed. Either both agents kept to their declared scopes — in which "
        "case the demo graph no longer forces a collision — or the PreToolUse hook did not "
        "fire, which would mean nothing at all is standing between an agent and a file."
    )

    denied = denials[0]
    path = denied["payload"]["path"]
    task_id = denied["task_id"]
    holder_id = denied["payload"]["holder_task_id"]
    assert holder_id != task_id

    task = tasks[task_id]
    assert path in task["file_scope"], (
        f"{path} was not folded into {task_id}'s scope, so its retry was a coin flip"
    )
    assert task["attempts"] >= 2, "the vetoed task did not need a second attempt to succeed"
    assert task["status"] == TaskStatus.SUCCEEDED

    released = next(
        event
        for event in _select(events, EventType.LEASE_RELEASED, holder_id)
        if event["payload"]["path"] == path
    )
    retry = _select(events, EventType.TASK_ASSIGNED, task_id)[1]
    assert released["id"] < retry["id"], "the vetoed task was retried before the holder let go"


def _assert_the_target_suite_still_passes(workspace: Path) -> None:
    """The assertion the fleet cannot fake: the edited codebase still works."""
    completed = subprocess.run(  # fixed argv, no shell
        [sys.executable, "-m", "pytest", "-q", "--color=no"],
        cwd=workspace,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, (
        f"the fleet left {workspace} broken:\n{completed.stdout}\n{completed.stderr}"
    )


# -- the run -----------------------------------------------------------------


class _Record:
    def __init__(self, events: list[dict[str, Any]], tasks: list[dict[str, Any]]) -> None:
        self.events = events
        self.tasks = {task["id"]: task for task in tasks}


async def _run_fleet(tmp_path: Path, workspace: Path, graph: list[dict[str, Any]]) -> _Record:
    settings = Settings(
        db=tmp_path / "codefleet.db",
        workdir=workspace,
        run_dir=tmp_path / "runs",
        host="127.0.0.1",
        port=_free_port(),
        runners=RUNNERS,
    )
    async with (
        _serving(settings) as base_url,
        httpx.AsyncClient(base_url=base_url, timeout=30.0) as client,
    ):
        created = await client.post("/tasks", json={"tasks": graph})
        assert created.status_code == 201, created.text

        fleet = [
            Runner(
                name=f"runner-{index + 1}",
                base_url=base_url,
                workdir=workspace,
                settings=settings,
            )
            for index in range(RUNNERS)
        ]
        loops = [asyncio.create_task(r.run_forever(), name=r.name) for r in fleet]
        try:
            await asyncio.wait_for(_until_finished(base_url), timeout=RUN_TIMEOUT)
        finally:
            for runner in fleet:
                await runner.shutdown()
            await asyncio.gather(*loops, return_exceptions=True)

        events = (await client.get("/events", params={"since": 0, "limit": 5000})).json()
        tasks = (await client.get("/tasks", params={"limit": 500})).json()
    return _Record(events["events"], tasks["tasks"])


async def _until_finished(base_url: str) -> None:
    async with (
        httpx.AsyncClient(base_url=base_url, timeout=None) as client,
        aclosing(stream_events(client, since=0)) as stream,
    ):
        async for event in stream:
            if event["type"] == EventType.RUN_FINISHED:
                return


@asynccontextmanager
async def _serving(settings: Settings) -> AsyncIterator[str]:
    config = uvicorn.Config(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_level="error",
        access_log=False,
    )
    server = uvicorn.Server(config)
    serving = asyncio.create_task(server.serve(), name="codefleet-live-server")
    while not server.started:
        if serving.done():
            await serving
        await asyncio.sleep(0.01)
    try:
        yield settings.base_url
    finally:
        server.should_exit = True
        await serving


def _free_port(host: str = "127.0.0.1") -> int:
    with socket.socket() as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def _select(
    events: list[dict[str, Any]], type: EventType, task_id: str | None = None
) -> list[dict[str, Any]]:
    return [
        event
        for event in events
        if event["type"] == type and (task_id is None or event["task_id"] == task_id)
    ]


def _one(events: list[dict[str, Any]], type: EventType, task_id: str) -> dict[str, Any]:
    matches = _select(events, type, task_id)
    assert len(matches) == 1, f"expected one {type} for {task_id}, got {len(matches)}"
    return matches[0]
