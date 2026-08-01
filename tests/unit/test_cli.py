"""CLI tests: the failure a stranger hits first.

Every command except `demo` talks to a server somebody else started, so the most
likely thing to go wrong on a first run is that nobody started one. That path
used to end in a rich-rendered httpx traceback six frames deep, which reads as a
bug in CodeFleet rather than a missing process, so it is asserted here.

The port these tests point at is one the kernel just handed out and nobody bound,
so a connection to it is refused immediately. Deliberately not a port held open
but unlistened: that produces a connect *timeout* instead, which is a different
failure and eight seconds slower.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from codefleet import cli

runner = CliRunner()


@pytest.fixture
def closed_port() -> int:
    """A loopback port with nothing behind it."""
    return cli._free_port("127.0.0.1")


@pytest.mark.parametrize(
    "command",
    [
        ["tasks"],
        ["reset"],
        ["watch"],
        ["run", "--dry-run", "--runners", "1"],
        ["load", "{graph}"],
    ],
    ids=lambda argv: argv[0],
)
def test_a_command_with_no_server_says_so_and_stops(
    command: list[str], closed_port: int, demo_tasks: Path
) -> None:
    argv = [part.format(graph=demo_tasks) for part in command]
    result = runner.invoke(cli.app, [*argv, "--port", str(closed_port)], catch_exceptions=False)
    assert result.exit_code == cli.UNREACHABLE_EXIT
    assert "no coordination server" in result.output
    assert "codefleet serve" in result.output
    assert "Traceback" not in result.output


def test_an_empty_graph_file_is_rejected_before_anything_is_posted(tmp_path: Path) -> None:
    """A YAML file that parses but describes nothing is a mistake, not an empty batch."""
    empty = tmp_path / "tasks.yaml"
    empty.write_text("tasks: []\n", encoding="utf-8")
    result = runner.invoke(cli.app, ["load", str(empty)], catch_exceptions=False)
    assert result.exit_code == 1
    assert "contains no tasks" in result.output
