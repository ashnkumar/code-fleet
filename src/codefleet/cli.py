"""The operator's side of CodeFleet.

Every command here is an HTTP client and nothing else — no command opens the
database. That is what keeps `reset`, the scheduler and the API from disagreeing
about state: there is one writer, and it is the server.

`codefleet demo` is the whole quickstart in one process. It copies the demo
repository into a fresh run directory so the committed tree stays pristine and
`git status` stays clean, starts a server on a free port with its own database,
loads the demo graph, runs the fleet, shows the dashboard, and finishes by
running the target repository's own test suite. `--dry-run` swaps the Claude
sessions for `ScriptedExecutor`, so the entire flow — including the veto — is
demonstrable with no API key and no spend. That is the variant CI runs.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Coroutine, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Annotated, Any

import httpx
import typer
import yaml
from rich.console import Console
from rich.table import Table

from codefleet.config import Settings, get_settings
from codefleet.dashboard import STATUS_STYLE, stream_events, watch
from codefleet.models import EventType, Task, TaskStatus
from codefleet.runner import Executor, Runner, ScriptedExecutor, ScriptedWrite

app = typer.Typer(
    name="codefleet",
    help="Coordinate a fleet of parallel Claude coding agents on one working tree.",
    no_args_is_help=True,
    add_completion=False,
)

# Highlighting off: every colour in this CLI is chosen to mean something, and
# rich's automatic number/path highlighting competes with that.
console = Console(highlight=False)

MIN_PYTHON = (3, 12)

# Statuses that mean the fleet still has work in front of it (spec 4.8).
_UNFINISHED = frozenset(
    {TaskStatus.PENDING.value, TaskStatus.ASSIGNED.value, TaskStatus.RUNNING.value}
)


def _denials_after(event_id: int) -> dict[str, Any]:
    """Query parameters for the vetoes in one run, from the one table that
    records them (spec 3.6)."""
    return {"since": event_id, "type": EventType.LEASE_DENIED.value}


# ---------------------------------------------------------------------------
# Shared plumbing
# ---------------------------------------------------------------------------


def _resolve_settings(**overrides: Any) -> Settings:
    """Environment first, then whichever flags were actually passed."""
    supplied = {key: value for key, value in overrides.items() if value is not None}
    return get_settings().model_copy(update=supplied)


def _repo_root() -> Path:
    """The checkout this package was installed from, which is where examples/ lives."""
    return Path(__file__).resolve().parents[2]


def _error_line(response: httpx.Response) -> str:
    """One line describing a failed response, using the API's error envelope."""
    if response.headers.get("content-type", "").startswith("application/json"):
        error = response.json().get("error", {})
        return f"{error.get('code', 'error')}: {error.get('message', '')}"
    return response.text[:300]


def _expect_ok(response: httpx.Response) -> httpx.Response:
    """Turn a server error into a readable line and a non-zero exit, not a traceback."""
    if response.is_success:
        return response
    console.print(f"[bold red]HTTP {response.status_code}[/] {_error_line(response)}")
    raise typer.Exit(1)


def _client(settings: Settings) -> httpx.Client:
    return httpx.Client(base_url=settings.base_url, timeout=30.0)


# Distinct from 1, which every command uses to mean "the run itself was not
# clean". Nothing was reached, so there is no run to have an opinion about.
UNREACHABLE_EXIT = 2


@contextmanager
def _reaching(settings: Settings) -> Iterator[None]:
    """Turn "nothing is listening" into one line instead of a stack trace.

    Every command here is an HTTP client, so the first thing anyone hits after
    forgetting `codefleet serve` is a connection error raised six frames deep
    inside httpx. Errors the server itself returns are left alone — those arrive
    as JSON with a code, and `_expect_ok` already renders them.
    """
    try:
        yield
    except (httpx.ConnectError, httpx.ConnectTimeout):
        console.print(
            f"[bold red]no coordination server at {settings.base_url}[/]\n"
            "start one with [bold]codefleet serve[/], or run [bold]codefleet demo[/], "
            "which brings up its own."
        )
        raise typer.Exit(UNREACHABLE_EXIT) from None


def _load_graph(path: Path) -> list[dict[str, Any]]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    tasks = document.get("tasks") if isinstance(document, dict) else document
    if not isinstance(tasks, list) or not tasks:
        console.print(f"[bold red]{path} contains no tasks[/]")
        raise typer.Exit(1)
    return tasks


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------


@app.command()
def serve(
    host: Annotated[str | None, typer.Option(help="Interface to bind.")] = None,
    port: Annotated[int | None, typer.Option(help="Port to bind.")] = None,
    db: Annotated[Path | None, typer.Option(help="SQLite database file.")] = None,
    workdir: Annotated[Path | None, typer.Option(help="The shared working tree.")] = None,
    tick_interval: Annotated[float | None, typer.Option(help="Reconciliation sweep, s.")] = None,
    heartbeat_interval: Annotated[float | None, typer.Option(help="Heartbeat, s.")] = None,
    stale_after: Annotated[float | None, typer.Option(help="Agent stale threshold, s.")] = None,
    max_attempts: Annotated[int | None, typer.Option(help="Default attempts per task.")] = None,
    allow_reset: Annotated[bool | None, typer.Option(help="Permit POST /reset.")] = None,
) -> None:
    """Run the coordination server."""
    settings = _resolve_settings(
        host=host,
        port=port,
        db=db,
        workdir=workdir,
        tick_interval=tick_interval,
        heartbeat_interval=heartbeat_interval,
        stale_after=stale_after,
        max_attempts=max_attempts,
        allow_reset=allow_reset,
    )
    import uvicorn

    from codefleet.server import create_app

    console.print(f"[bold]codefleet[/] serving on {settings.base_url}  db={settings.db}")
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_level="warning",
        access_log=False,
    )


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


@app.command()
def run(
    runners: Annotated[int | None, typer.Option(help="How many agent slots to start.")] = None,
    host: Annotated[str | None, typer.Option(help="Server host.")] = None,
    port: Annotated[int | None, typer.Option(help="Server port.")] = None,
    workdir: Annotated[Path | None, typer.Option(help="The shared working tree.")] = None,
    model: Annotated[str | None, typer.Option(help="Model for agent sessions.")] = None,
    task_timeout: Annotated[float | None, typer.Option(help="Per-task wall clock, s.")] = None,
    task_budget_usd: Annotated[float | None, typer.Option(help="Per-task budget, USD.")] = None,
    max_turns: Annotated[int | None, typer.Option(help="Per-session turn cap.")] = None,
    poll_interval: Annotated[float | None, typer.Option(help="Assignment poll, s.")] = None,
    heartbeat_interval: Annotated[float | None, typer.Option(help="Heartbeat, s.")] = None,
    dry_run: Annotated[bool, typer.Option(help="Scripted executors; no API calls.")] = False,
    verify: Annotated[
        str | None,
        typer.Option(
            help=(
                "Command to run in the working tree once the fleet drains, e.g. "
                "'pytest -q'. Its exit code decides this command's exit code."
            )
        ),
    ] = None,
) -> None:
    """Start runners against a server that is already up, and wait for the run to finish.

    `--verify` is worth using on anything real. A task reaching `succeeded` means
    its session ended cleanly, not that the code it wrote works; the fleet has no
    way to tell those apart from the inside. Handing it the command you would
    have run yourself is what makes a green run mean something.
    """
    settings = _resolve_settings(
        runners=runners,
        host=host,
        port=port,
        workdir=workdir,
        model=model,
        task_timeout=task_timeout,
        task_budget_usd=task_budget_usd,
        max_turns=max_turns,
        poll_interval=poll_interval,
        heartbeat_interval=heartbeat_interval,
    )
    factory = _scripted_factory(settings) if dry_run else None
    # This command prints nothing until the run drains, which for real sessions is
    # minutes. Say what is happening and where to watch it, or an idle terminal
    # reads as a hang.
    console.print(
        f"[bold]codefleet[/] {settings.runners} runner(s) on {settings.workdir}"
        f"{' [dim](scripted)[/]' if dry_run else ''}\n"
        "[dim]waiting for the run to finish · `codefleet watch` for the live view[/]"
    )
    with _reaching(settings):
        code = asyncio.run(_run_fleet(settings, executor_factory=factory))
    # Only when the fleet itself drained cleanly: running checks over a tree whose
    # tasks failed would report the fleet's failure as a test failure.
    if (
        verify
        and code == 0
        and not _verify(settings.workdir, shlex.split(verify), f"verifying: {verify}")
    ):
        console.print("\n[bold red]verify failed[/] the fleet finished but the tree does not pass")
        code = 1
    raise typer.Exit(code)


async def _run_fleet(
    settings: Settings,
    *,
    executor_factory: Callable[[], Executor] | None = None,
    timeout: float | None = None,
) -> int:
    """Start the fleet, wait for `run_finished`, print a summary, return an exit code."""
    # Anchor on the event log before any runner exists. The server keeps its whole
    # history, so a second `codefleet run` against a long-lived server would
    # otherwise replay the *previous* run's `run_finished` and return before this
    # fleet had done anything — and then report that run's vetoes as its own.
    with _client(settings) as client:
        state = _expect_ok(client.get("/state")).json()
    since = int(state["last_event_id"])

    if not any(task["status"] in _UNFINISHED for task in state["tasks"]):
        # The server treats an empty queue as "nothing to do yet" rather than as a
        # finished run, and it is right to: work can still be posted. But that
        # means `run_finished` will never arrive, so waiting for it here would
        # hang with no output at all.
        console.print(
            "[bold yellow]nothing to run[/] — no task is pending, assigned or running.\n"
            "[dim]post a graph first: [bold]codefleet load <file.yaml>[/][/]"
        )
        if not state["tasks"]:
            return 0
        _print_summary(state["tasks"], 0.0)
        return 0 if all(task["status"] == TaskStatus.SUCCEEDED for task in state["tasks"]) else 1

    fleet = _spawn_runners(settings, executor_factory)
    started = time.monotonic()
    try:
        await _await_run_finished(settings.base_url, since, timeout, fleet)
    finally:
        await _stop_runners(fleet)

    with _client(settings) as client:
        tasks = _expect_ok(client.get("/tasks", params={"limit": 500})).json()["tasks"]
        denials = _expect_ok(client.get("/events", params=_denials_after(since))).json()["events"]
    _print_summary(tasks, time.monotonic() - started)
    _print_vetoes(denials)
    return 0 if all(task["status"] == TaskStatus.SUCCEEDED for task in tasks) else 1


def _spawn_runners(
    settings: Settings, executor_factory: Callable[[], Executor] | None
) -> list[tuple[Runner, asyncio.Task[None]]]:
    fleet: list[tuple[Runner, asyncio.Task[None]]] = []
    for index in range(settings.runners):
        runner = Runner(
            name=f"runner-{index + 1}",
            base_url=settings.base_url,
            workdir=settings.workdir,
            settings=settings,
            executor=executor_factory() if executor_factory else None,
        )
        fleet.append(
            (runner, asyncio.create_task(runner.run_forever(), name=f"{runner.name}-main"))
        )
    return fleet


async def _stop_runners(fleet: Sequence[tuple[Runner, asyncio.Task[None]]]) -> None:
    """Shut runners down before the server: deregistration needs somewhere to go.

    A runner that died on its own is reported here rather than discarded. This is
    the last place its exception exists — after this the task is gone — and a
    fleet that quietly shrank is the difference between a run that is slow and a
    run that is never going to finish.
    """
    for runner, _ in fleet:
        await runner.shutdown()
    outcomes = await asyncio.gather(*(task for _, task in fleet), return_exceptions=True)
    for (runner, _), outcome in zip(fleet, outcomes, strict=True):
        if isinstance(outcome, BaseException) and not isinstance(outcome, asyncio.CancelledError):
            console.print(f"[bold red]{runner.name} died[/] {type(outcome).__name__}: {outcome}")


async def _wait_while_the_fleet_lives(
    watcher: Coroutine[Any, Any, None],
    fleet: Sequence[tuple[Runner, asyncio.Task[None]]],
    *,
    timeout: float | None,
) -> None:
    """Wait for `watcher`, unless the fleet dies out or `timeout` elapses first.

    Waiting on `run_finished` is only sound while somebody is still running: the
    server emits it when the queue drains, and a queue with no runner left never
    drains. Racing the wait against the fleet's own liveness is what turns "every
    runner is gone" from a silent, permanent hang into a line and an exit code.
    """
    waiting = [asyncio.ensure_future(watcher)]
    if fleet:
        waiting.append(asyncio.create_task(_outlive(fleet), name="fleet-liveness"))
    try:
        done, _ = await asyncio.wait(waiting, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in waiting:
            task.cancel()
        await asyncio.gather(*waiting, return_exceptions=True)

    if not done:
        console.print(f"[bold yellow]run did not finish within {timeout:.0f}s[/]")
        return
    if waiting[0] not in done:
        console.print("[bold red]every runner has stopped[/] — nothing is left to finish this run")
        return
    waiting[0].result()  # whatever ended the wait, including a broken event stream


async def _outlive(fleet: Sequence[tuple[Runner, asyncio.Task[None]]]) -> None:
    """Return once no runner is left standing."""
    await asyncio.gather(*(task for _, task in fleet), return_exceptions=True)


async def _await_run_finished(
    base_url: str,
    since: int,
    timeout: float | None,
    fleet: Sequence[tuple[Runner, asyncio.Task[None]]] = (),
) -> None:
    async def wait() -> None:
        async with httpx.AsyncClient(base_url=base_url, timeout=None) as client:
            async for event in stream_events(client, since=since):
                if event.get("type") == EventType.RUN_FINISHED:
                    return

    await _wait_while_the_fleet_lives(wait(), fleet, timeout=timeout)


DEMO_CONTESTED_PATH = "linkstash/api.py"
DEMO_UNDECLARED_SCOPE = "linkstash/middleware.py"

# How long a scripted agent "thinks" before each write. Small, but not zero: two
# instantaneous writes never overlap, and an overlap is the thing being shown.
_DEMO_THINK_S = 0.4


def _scripted_factory(settings: Settings) -> Callable[[], Executor]:
    return lambda: ScriptedExecutor(partial(_dry_run_plan, hold_s=_demo_hold_s(settings)))


def _demo_hold_s(settings: Settings) -> float:
    """How long a demo task keeps the contested file once it has it.

    The collision has to happen even when the two contending runners pick their
    work up out of step, and a poll interval is the widest they can drift: the
    server assigns both in one tick, but each runner learns about it on its own
    poll. Holding the file for longer than that drift makes the loser of the race
    hit the veto whichever of the two it turns out to be, so the demo does not
    depend on which runner polled first.
    """
    return settings.poll_interval + 3 * _DEMO_THINK_S


def _dry_run_plan(task: Task, *, hold_s: float) -> list[ScriptedWrite]:
    """What a scripted agent 'does' for a demo task.

    The extra writes are what make a dry run a real rehearsal rather than a green
    screenshot. A task that declares `middleware.py` still has to register its
    middleware in `api.py` — the write its declared scope did not predict — and
    both tasks keep working on `api.py` after they reach it, so the file is held
    rather than touched and released in the same instant.
    """
    writes = [ScriptedWrite(path, pause_before=_DEMO_THINK_S) for path in task.file_scope]
    if DEMO_UNDECLARED_SCOPE in task.file_scope:
        writes.append(ScriptedWrite(DEMO_CONTESTED_PATH, tool="Edit", pause_before=_DEMO_THINK_S))
    if any(write.path == DEMO_CONTESTED_PATH for write in writes):
        writes.append(ScriptedWrite(DEMO_CONTESTED_PATH, tool="Edit", pause_before=hold_s))
    return writes


# ---------------------------------------------------------------------------
# watch / tasks / load / reset
# ---------------------------------------------------------------------------


@app.command(name="watch")
def watch_command(
    host: Annotated[str | None, typer.Option(help="Server host.")] = None,
    port: Annotated[int | None, typer.Option(help="Server port.")] = None,
    since: Annotated[
        int | None, typer.Option(help="Start the log at this event id instead of replaying.")
    ] = None,
) -> None:
    """Watch a run in the terminal. Read-only; closing it changes nothing."""
    settings = _resolve_settings(host=host, port=port)
    with _reaching(settings):
        try:
            asyncio.run(watch(settings.base_url, since=since, console=console))
        except KeyboardInterrupt:
            raise typer.Exit(0) from None


@app.command(name="tasks")
def tasks_command(
    status: Annotated[TaskStatus | None, typer.Option(help="Filter by status.")] = None,
    limit: Annotated[int, typer.Option(help="Page size.")] = 100,
    offset: Annotated[int, typer.Option(help="Page offset.")] = 0,
    host: Annotated[str | None, typer.Option(help="Server host.")] = None,
    port: Annotated[int | None, typer.Option(help="Server port.")] = None,
    as_json: Annotated[bool, typer.Option("--json", help="Print the raw response.")] = False,
) -> None:
    """List tasks."""
    settings = _resolve_settings(host=host, port=port)
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if status is not None:
        params["status"] = status.value
    with _reaching(settings), _client(settings) as client:
        payload = _expect_ok(client.get("/tasks", params=params)).json()

    if as_json:
        console.print_json(json.dumps(payload))
        return
    console.print(_task_summary_table(payload["tasks"]))


@app.command(name="load")
def load_command(
    file: Annotated[Path, typer.Argument(help="YAML file describing the task graph.")],
    host: Annotated[str | None, typer.Option(help="Server host.")] = None,
    port: Annotated[int | None, typer.Option(help="Server port.")] = None,
) -> None:
    """Post a YAML task graph. The batch lands whole or not at all."""
    settings = _resolve_settings(host=host, port=port)
    tasks = _load_graph(file)
    with _reaching(settings), _client(settings) as client:
        created = _expect_ok(client.post("/tasks", json={"tasks": tasks})).json()["created"]
    console.print(f"created {len(created)} task(s): [bold]{', '.join(created)}[/]")


@app.command(name="reset")
def reset_command(
    host: Annotated[str | None, typer.Option(help="Server host.")] = None,
    port: Annotated[int | None, typer.Option(help="Server port.")] = None,
) -> None:
    """Truncate every table. Requires CODEFLEET_ALLOW_RESET=1 on the server."""
    settings = _resolve_settings(host=host, port=port)
    with _reaching(settings), _client(settings) as client:
        _expect_ok(client.post("/reset"))
    console.print("[bold]reset[/] — every table is empty")


# ---------------------------------------------------------------------------
# demo
# ---------------------------------------------------------------------------


@app.command()
def demo(
    runners: Annotated[int | None, typer.Option(help="How many agent slots to start.")] = None,
    model: Annotated[str | None, typer.Option(help="Model for agent sessions.")] = None,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Scripted agents: no API key, no spend.")
    ] = False,
    timeout: Annotated[float, typer.Option(help="Give up on the run after this many s.")] = 900.0,
    demo_repo: Annotated[Path | None, typer.Option(help="Target repo to copy.")] = None,
    tasks_file: Annotated[Path | None, typer.Option("--tasks", help="Task graph.")] = None,
) -> None:
    """Run the whole demo: fresh workspace, server, fleet, dashboard, verdict."""
    source = demo_repo or _repo_root() / "examples" / "demo-repo"
    graph = tasks_file or _repo_root() / "examples" / "demo-tasks.yaml"
    for path in (source, graph):
        if not path.exists():
            console.print(f"[bold red]missing {path}[/] — run the demo from a source checkout")
            raise typer.Exit(1)

    base = _resolve_settings(runners=runners, model=model)
    run_dir = base.run_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    workspace = run_dir / "workspace"
    # The committed demo repo stays pristine: the fleet edits a copy, so a demo
    # run never shows up in `git status`.
    shutil.copytree(
        source, workspace, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache")
    )

    settings = base.model_copy(
        update={
            "db": run_dir / "codefleet.db",
            "workdir": workspace,
            "port": _free_port(base.host),
            "run_dir": run_dir,
        }
    )
    console.print(f"[bold]demo[/] workspace={workspace}  server={settings.base_url}")
    raise typer.Exit(asyncio.run(_demo(settings, graph, dry_run=dry_run, timeout=timeout)))


async def _demo(settings: Settings, graph: Path, *, dry_run: bool, timeout: float) -> int:
    import uvicorn

    from codefleet.server import create_app

    config = uvicorn.Config(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_level="error",
        access_log=False,
    )
    server = uvicorn.Server(config)
    serving = asyncio.create_task(server.serve(), name="demo-server")
    await _wait_until_serving(server, serving)

    started = time.monotonic()
    # The demo owns this server, so it is the demo's job to take it down again —
    # on the happy path, on a rejected graph, and on anything that raises.
    try:
        async with httpx.AsyncClient(base_url=settings.base_url, timeout=30.0) as client:
            response = await client.post("/tasks", json={"tasks": _load_graph(graph)})
            if not response.is_success:
                console.print(f"[bold red]{graph} was rejected[/] {_error_line(response)}")
                return 1

            fleet = _spawn_runners(settings, _scripted_factory(settings) if dry_run else None)
            try:
                await _wait_while_the_fleet_lives(
                    watch(settings.base_url, stop_on_run_finished=True, console=console),
                    fleet,
                    timeout=timeout,
                )
            finally:
                await _stop_runners(fleet)

            tasks = (await client.get("/tasks", params={"limit": 500})).json()["tasks"]
            denials = (await client.get("/events", params=_denials_after(0))).json()["events"]
    finally:
        server.should_exit = True
        await serving

    elapsed = time.monotonic() - started
    _print_summary(tasks, elapsed)
    _print_vetoes(denials)

    tests_passed = _verify(
        settings.workdir, DEMO_VERIFY, "running the target repo's own test suite"
    )
    all_succeeded = all(task["status"] == TaskStatus.SUCCEEDED for task in tasks)
    verdict = "green" if all_succeeded and tests_passed else "red"
    console.print(
        f"\n[bold {verdict}]verdict[/] "
        f"tasks {'all succeeded' if all_succeeded else 'incomplete'} · "
        f"target tests {'pass' if tests_passed else 'fail'}"
    )
    return 0 if all_succeeded and tests_passed else 1


async def _wait_until_serving(server: Any, serving: asyncio.Task[None]) -> None:
    while not server.started:
        if serving.done():
            await serving  # surfaces whatever stopped it from binding
            return
        await asyncio.sleep(0.02)


def _free_port(host: str) -> int:
    """Ask the kernel for an unused port. Racy in principle, fine for one demo."""
    with socket.socket() as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


DEMO_VERIFY: tuple[str, ...] = (sys.executable, "-m", "pytest", "-q", "--color=no")


def _verify(workspace: Path, command: Sequence[str], label: str) -> bool:
    """Run the target's own checks over the tree the fleet left behind.

    This is the only assertion in the system a fleet cannot satisfy by agreeing
    with itself. A task is marked `succeeded` when its session ended cleanly and
    reported success — which says the model stopped, not that what it wrote
    compiles or works. Nothing inside the coordination layer can tell those
    apart, so something outside it has to.

    Deliberately fleet-level rather than per-task: agents share one working tree,
    so running a suite while other agents are still editing it would fail on
    their half-finished work and blame the wrong task. That is a real limit of
    the shared-tree model, not an oversight — per-task verification needs
    per-task isolation.
    """
    console.print(f"\n[bold]{label}[/] [dim]{workspace}[/]")
    completed = subprocess.run(  # caller-supplied argv, never a shell string
        list(command),
        cwd=workspace,
        capture_output=True,
        text=True,
    )
    tail = (completed.stdout or completed.stderr).strip().splitlines()[-6:]
    for line in tail:
        console.print(f"  {line}", style="dim", markup=False)
    return completed.returncode == 0


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------


@app.command()
def doctor(
    host: Annotated[str | None, typer.Option(help="Server host.")] = None,
    port: Annotated[int | None, typer.Option(help="Server port.")] = None,
    workdir: Annotated[Path | None, typer.Option(help="The shared working tree.")] = None,
) -> None:
    """Preflight the things that make a run fail five minutes in."""
    settings = _resolve_settings(host=host, port=port, workdir=workdir)
    checks = [
        _check_python(),
        _check_api_key(),
        _check_sdk(),
        _check_port(settings),
        _check_workdir(settings.workdir),
        _check_workdir_clean(settings.workdir),
    ]

    table = Table(box=None, pad_edge=False, header_style="bold dim")
    table.add_column("check", no_wrap=True)
    table.add_column("")
    table.add_column("detail")
    for name, ok, detail in checks:
        mark = "[bold green]ok[/]" if ok else "[bold red]xx[/]"
        table.add_row(name, mark, detail)
    console.print(table)
    raise typer.Exit(0 if all(ok for _, ok, _ in checks) else 1)


def _check_python() -> tuple[str, bool, str]:
    version = ".".join(str(part) for part in sys.version_info[:3])
    return ("python", sys.version_info >= MIN_PYTHON, f"{version} (need >= 3.12)")


def _check_api_key() -> tuple[str, bool, str]:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return ("api key", False, "ANTHROPIC_API_KEY unset — only `demo --dry-run` will work")
    return ("api key", True, f"ANTHROPIC_API_KEY set ({len(key)} chars)")


def _check_sdk() -> tuple[str, bool, str]:
    found = importlib.util.find_spec("claude_agent_sdk") is not None
    return ("agent sdk", found, "claude_agent_sdk importable" if found else "not installed")


def _check_port(settings: Settings) -> tuple[str, bool, str]:
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((settings.host, settings.port))
        except OSError as exc:
            return ("port", False, f"{settings.host}:{settings.port} unavailable ({exc.strerror})")
    return ("port", True, f"{settings.host}:{settings.port} free")


def _check_workdir(workdir: Path) -> tuple[str, bool, str]:
    exists = workdir.is_dir()
    return ("workdir", exists, f"{workdir}" if exists else f"{workdir} does not exist")


def _check_workdir_clean(workdir: Path) -> tuple[str, bool, str]:
    """A dirty target tree means a previous run's edits are still in it."""
    if not workdir.is_dir():
        return ("workdir clean", False, "no workdir to inspect")
    completed = subprocess.run(  # fixed argv, no shell
        ["git", "status", "--porcelain", "--", "."],
        cwd=workdir,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return ("workdir clean", True, "not a git repository — nothing to compare against")
    dirty = [line for line in completed.stdout.splitlines() if line.strip()]
    if dirty:
        return ("workdir clean", False, f"{len(dirty)} modified path(s); commit or restore first")
    return ("workdir clean", True, "git reports no changes")


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _task_summary_table(tasks: Sequence[dict[str, Any]]) -> Table:
    table = Table(box=None, pad_edge=False, header_style="bold dim")
    # The two prose columns wrap rather than truncate: on an 80-column terminal
    # a no-wrap column is squeezed to nothing, and an empty id column is worse
    # than a two-line row.
    table.add_column("id", no_wrap=True)
    table.add_column("title", max_width=44)
    table.add_column("status", no_wrap=True)
    table.add_column("try", justify="right", no_wrap=True)
    table.add_column("cost", justify="right", no_wrap=True)
    table.add_column("note", max_width=44)

    for task in tasks:
        status = str(task.get("status", ""))
        note = task.get("error") or task.get("result_summary") or ""
        table.add_row(
            str(task.get("id", "")),
            str(task.get("title", "")),
            f"[{_status_colour(status)}]{status}[/]",
            f"{task.get('attempts', 0)}/{task.get('max_attempts', 0)}",
            f"${float(task.get('cost_usd') or 0.0):.4f}",
            str(note),
        )
    return table


def _status_colour(status: str) -> str:
    """One mapping, shared with the live dashboard.

    A summary table that colours a status differently from the screen the run was
    just watched on reads as a disagreement about what happened. `cyan` covers a
    status this build has not heard of.
    """
    return STATUS_STYLE.get(status, "cyan")


def _print_summary(tasks: Sequence[dict[str, Any]], elapsed_s: float) -> None:
    cost = sum(float(task.get("cost_usd") or 0.0) for task in tasks)
    # `/tasks` answers in queue order, which is the right order while work is
    # being handed out and the wrong one for a report: a reader comparing this
    # table against the graph they wrote wants it in the order they wrote it.
    in_graph_order = sorted(
        tasks, key=lambda task: (task.get("created_at", ""), task.get("id", ""))
    )
    console.print()
    console.print(_task_summary_table(in_graph_order))
    console.print(
        f"\n[dim]total[/] ${cost:.4f} · {elapsed_s:.1f}s wall clock · {len(tasks)} task(s)"
    )


def _print_vetoes(denials: Sequence[dict[str, Any]]) -> None:
    """The veto is the headline, so it gets its own paragraph rather than a row."""
    if not denials:
        # The contended file is a genuine race between two live agents, so it is
        # not guaranteed to fire — one can finish and release before the other
        # asks. Say so, rather than leaving a reader wondering what they missed.
        console.print(
            "[dim]no write was vetoed in this run — the two agents that contend for"
            " one file happened not to overlap. Try again, or use --dry-run, where"
            " the timing is scripted and the veto always fires.[/]"
        )
        return
    console.print()
    for event in denials:
        payload = event.get("payload", {})
        console.print(
            f"[bold white on red] VETO [/] [bold]{payload.get('path')}[/] — "
            f"{event.get('task_id')} was denied; "
            f"{payload.get('holder_agent_name')} held it for {payload.get('holder_task_id')}"
        )
