"""`codefleet run`, invoked the way an operator invokes it.

Everything else in the suite drives the fleet from inside one event loop.
`codefleet run` cannot be tested that way: it is a synchronous command that
talks to a server in *another process*, and it reads its own summary back over a
blocking client. So the server here runs in a thread and the command runs
through Typer's runner, which is as close to `$ codefleet run` as a test gets.

Both cases below are ones a stranger hits before anything interesting happens:
running the fleet before posting a graph, and running it after posting one.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
from typer.testing import CliRunner

from codefleet import cli
from codefleet.config import Settings
from codefleet.server import create_app

runner = CliRunner()

# Generous: this bounds a hang, it does not measure speed. The graph below is
# two scripted writes, so a working command finishes in about a second.
COMMAND_TIMEOUT_S = 60.0

GRAPH: list[dict[str, Any]] = [
    {
        "id": "R1",
        "title": "Write the config",
        "description": "One scripted write.",
        "file_scope": ["service/config.py"],
    },
    {
        "id": "R2",
        "title": "Use the config",
        "description": "Waits for R1, then writes.",
        "file_scope": ["service/app.py"],
        "depends_on": ["R1"],
    },
]


@contextmanager
def _serving(settings: Settings) -> Iterator[None]:
    """A real server in a background thread, with its own event loop."""
    config = uvicorn.Config(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_level="error",
        access_log=False,
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="codefleet-test-server", daemon=True)
    thread.start()
    deadline = time.monotonic() + 30.0
    while not server.started:
        assert thread.is_alive() and time.monotonic() < deadline, "the server never bound"
        time.sleep(0.01)
    try:
        yield
    finally:
        server.should_exit = True
        thread.join(timeout=30.0)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    return Settings(
        db=tmp_path / "codefleet.db",
        workdir=workspace,
        run_dir=tmp_path / "runs",
        port=cli._free_port("127.0.0.1"),
        runners=2,
        tick_interval=0.05,
        poll_interval=0.05,
        heartbeat_interval=0.25,
    )


def _invoke_run(settings: Settings) -> Any:
    """Run the command on a worker thread so a regression fails instead of hanging.

    `codefleet run` waits for `run_finished` with no deadline of its own, which is
    right for a real run of unknown length and wrong for a test: without this the
    bug being guarded against here would wedge the suite rather than report.
    """
    result: list[Any] = []

    def invoke() -> None:
        result.append(
            runner.invoke(
                cli.app,
                [
                    "run",
                    "--dry-run",
                    "--port",
                    str(settings.port),
                    "--workdir",
                    str(settings.workdir),
                ],
                catch_exceptions=False,
            )
        )

    thread = threading.Thread(target=invoke, name="codefleet-run", daemon=True)
    thread.start()
    thread.join(timeout=COMMAND_TIMEOUT_S)
    assert result, f"`codefleet run` did not return within {COMMAND_TIMEOUT_S:.0f}s"
    return result[0]


def test_run_against_an_empty_queue_says_so_rather_than_waiting_forever(
    settings: Settings,
) -> None:
    """An empty board is not a finished run, so `run_finished` never arrives.

    The server is right to treat "no tasks" as "nothing to do yet" — work can
    still be posted — but that means the event the command waits for is one the
    server will never emit, and waiting for it is an indefinite silent hang.
    """
    with _serving(settings):
        result = _invoke_run(settings)

    assert result.exit_code == 0
    assert "nothing to run" in result.output
    assert "codefleet load" in result.output


def test_run_drains_a_posted_graph_and_reports_it(settings: Settings) -> None:
    """The happy path of the command itself: post a graph, run it, exit 0."""
    with _serving(settings):
        created = httpx.post(f"{settings.base_url}/tasks", json={"tasks": GRAPH}, timeout=10.0)
        assert created.status_code == 201, created.text

        result = _invoke_run(settings)

        statuses = {
            task["id"]: task["status"]
            for task in httpx.get(f"{settings.base_url}/tasks", timeout=10.0).json()["tasks"]
        }

    assert result.exit_code == 0, result.output
    assert statuses == {"R1": "succeeded", "R2": "succeeded"}
    assert "no write was vetoed in this run" in result.output
