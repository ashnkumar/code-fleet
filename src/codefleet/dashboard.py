"""The terminal view of a run, built entirely from what the server already knows.

The dashboard is a pure consumer: it fetches `GET /state` for the tables and
tails `GET /events/stream` for the log. It stores nothing the server does not
have, and closing it changes nothing about the run.

Two deliberate choices. First, the tables are re-read from `/state` rather than
reconstructed by replaying events into a local reducer — a second implementation
of the state machine is a second thing that can be wrong, and the snapshot is one
cheap loopback call. Events drive the log and the pacing. Second, `lease_denied`
is rendered unlike anything else on the screen. The veto is the moment the whole
system exists for; if it scrolls past looking like a log line, the demo has
failed to show its own point.

Every renderable is built by a small named function taking plain data, so each
panel can be asserted on without a terminal or a server.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from contextlib import suppress
from typing import Any

import httpx
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from codefleet.models import EventType, TaskStatus

__all__ = [
    "agent_table",
    "event_line",
    "event_log",
    "fleet_footer",
    "lease_panel",
    "render",
    "stream_events",
    "task_table",
    "watch",
]

MAX_EVENTS = 500
EVENT_PANEL_HEIGHT = 12
TITLE_WIDTH = 44

STATUS_STYLE: dict[str, str] = {
    TaskStatus.PENDING: "dim",
    TaskStatus.ASSIGNED: "cyan",
    TaskStatus.RUNNING: "bold yellow",
    TaskStatus.SUCCEEDED: "bold green",
    TaskStatus.FAILED: "bold red",
    TaskStatus.BLOCKED_UPSTREAM: "magenta",
    TaskStatus.CANCELLED: "dim strike",
}

AGENT_STYLE: dict[str, str] = {
    "idle": "cyan",
    "busy": "bold yellow",
    "offline": "dim red",
}

EVENT_STYLE: dict[str, str] = {
    EventType.TASK_SUCCEEDED: "green",
    EventType.TASK_FAILED: "bold red",
    EventType.TASK_BLOCKED_UPSTREAM: "magenta",
    EventType.TASK_REQUEUED: "yellow",
    EventType.TASK_UNBLOCKED: "bold cyan",
    EventType.LEASE_ACQUIRED: "blue",
    EventType.LEASE_RELEASED: "dim blue",
    EventType.AGENT_OFFLINE: "red",
    EventType.FLEET_STARTED: "bold",
    EventType.FLEET_IDLE: "bold",
    EventType.RUN_FINISHED: "bold green",
}


# ---------------------------------------------------------------------------
# Panels
# ---------------------------------------------------------------------------


def task_table(tasks: Sequence[Mapping[str, Any]], agent_names: Mapping[str, str]) -> Table:
    table = Table(
        expand=True,
        box=None,
        pad_edge=False,
        header_style="bold dim",
        row_styles=["", "on grey11"],
    )
    # Only the title flexes; everything else sizes to its content, so a narrow
    # terminal loses words from the title rather than digits from the numbers.
    table.add_column("id", no_wrap=True)
    table.add_column("title", ratio=1, max_width=TITLE_WIDTH, overflow="ellipsis", no_wrap=True)
    table.add_column("status", no_wrap=True)
    table.add_column("agent", no_wrap=True)
    table.add_column("try", justify="right")
    table.add_column("cost", justify="right")

    for task in tasks:
        status = str(task.get("status", ""))
        agent = agent_names.get(str(task.get("assigned_agent_id") or ""), "")
        blocked = task.get("blocked_on_path")
        label = Text(status, style=STATUS_STYLE.get(status, ""))
        if blocked and status != TaskStatus.SUCCEEDED:
            # The path a task is stuck behind is the single most useful thing to
            # see next to its status, so it rides along in the same cell.
            label.append(f" ⟂{_basename(str(blocked))}", style="red")
        table.add_row(
            str(task.get("id", "")),
            str(task.get("title", "")),
            label,
            agent,
            f"{task.get('attempts', 0)}/{task.get('max_attempts', 0)}",
            _money(task.get("cost_usd", 0.0)),
        )
    return table


def agent_table(agents: Sequence[Mapping[str, Any]]) -> Table:
    table = Table(expand=True, box=None, pad_edge=False, header_style="bold dim")
    table.add_column("agent", no_wrap=True)
    table.add_column("status", no_wrap=True)
    table.add_column("task", ratio=1, no_wrap=True, overflow="ellipsis")

    for agent in agents:
        status = str(agent.get("status", ""))
        stale = " (stale)" if agent.get("stale") else ""
        table.add_row(
            str(agent.get("name", "")),
            Text(status + stale, style=AGENT_STYLE.get(status, "")),
            str(agent.get("current_task_id") or "—"),
        )
    return table


def lease_panel(leases: Sequence[Mapping[str, Any]], agent_names: Mapping[str, str]) -> Table:
    table = Table(expand=True, box=None, pad_edge=False, header_style="bold dim")
    table.add_column("held file", ratio=1, overflow="ellipsis", no_wrap=True)
    table.add_column("holder", no_wrap=True)

    if not leases:
        table.add_row(Text("no files held", style="dim"), "")
        return table

    for lease in leases:
        holder = agent_names.get(str(lease.get("agent_id", "")), str(lease.get("agent_id", "")))
        table.add_row(str(lease.get("path", "")), Text(holder, style="bold"))
    return table


def event_line(event: Mapping[str, Any]) -> Text:
    """One line of the log. Vetoes do not look like the others."""
    kind = str(event.get("type", ""))
    payload = event.get("payload") or {}
    task = str(event.get("task_id") or "")

    if kind == EventType.LEASE_DENIED:
        line = Text("  VETO  ", style="bold white on red")
        line.append(f" {payload.get('path', '?')}", style="bold red")
        line.append(" denied to ", style="red")
        line.append(task or "?", style="bold red")
        line.append(
            f" — held by {payload.get('holder_agent_name', '?')}"
            f" for {payload.get('holder_task_id', '?')}",
            style="red",
        )
        return line

    line = Text(f"{kind:<22}", style=EVENT_STYLE.get(kind, "dim"))
    if task:
        line.append(f"{task:<5} ", style="bold")
    line.append(_event_detail(kind, payload), style="dim")
    return line


def _event_detail(kind: str, payload: Mapping[str, Any]) -> str:
    if kind in (EventType.LEASE_ACQUIRED, EventType.LEASE_RELEASED, EventType.FILE_CHANGED):
        detail = str(payload.get("path", ""))
        reason = payload.get("reason") or payload.get("tool")
        return f"{detail} ({reason})" if reason else detail
    if kind == EventType.TASK_BLOCKED_UPSTREAM:
        return f"ancestor {payload.get('failed_ancestor_id', '?')} failed"
    for key in ("error", "reason", "summary", "title", "name"):
        if payload.get(key):
            return str(payload[key])
    return ""


def event_log(events: Iterable[Mapping[str, Any]], *, height: int = EVENT_PANEL_HEIGHT) -> Panel:
    """Newest last, so the eye follows the run downwards like a terminal."""
    tail = list(events)[-(height - 2) :]
    body = Group(*(event_line(event) for event in tail)) if tail else Text("waiting…", style="dim")
    return Panel(body, title="events", title_align="left", border_style="grey30")


def fleet_footer(
    tasks: Sequence[Mapping[str, Any]],
    events: Iterable[Mapping[str, Any]],
    elapsed_s: float,
) -> Panel:
    statuses = [str(task.get("status", "")) for task in tasks]
    done = sum(1 for status in statuses if status == TaskStatus.SUCCEEDED)
    failed = sum(
        1
        for status in statuses
        if status in (TaskStatus.FAILED, TaskStatus.BLOCKED_UPSTREAM, TaskStatus.CANCELLED)
    )
    conflicts = sum(1 for event in events if event.get("type") == EventType.LEASE_DENIED)
    cost = sum(float(task.get("cost_usd", 0.0) or 0.0) for task in tasks)

    line = Text()
    line.append("tasks ", style="dim")
    line.append(f"{done}/{len(tasks)}", style="bold green" if done == len(tasks) else "bold")
    if failed:
        line.append(f"  failed {failed}", style="bold red")
    line.append("   conflicts ", style="dim")
    line.append(str(conflicts), style="bold red" if conflicts else "bold")
    line.append("   cost ", style="dim")
    line.append(_money(cost), style="bold")
    line.append("   elapsed ", style="dim")
    line.append(_clock(elapsed_s), style="bold")
    return Panel(line, border_style="grey30")


def render(
    state: Mapping[str, Any], events: Sequence[Mapping[str, Any]], elapsed_s: float
) -> Layout:
    """Assemble one frame from a `/state` snapshot and the events seen so far."""
    tasks = list(state.get("tasks", ()))
    agents = list(state.get("agents", ()))
    leases = list(state.get("leases", ()))
    agent_names = {str(agent.get("id", "")): str(agent.get("name", "")) for agent in agents}

    layout = Layout()
    layout.split_column(
        Layout(_header(), name="header", size=3),
        Layout(name="body", ratio=1),
        Layout(event_log(events), name="events", size=EVENT_PANEL_HEIGHT),
        Layout(fleet_footer(tasks, events, elapsed_s), name="footer", size=3),
    )
    layout["body"].split_row(
        Layout(
            Panel(
                task_table(tasks, agent_names),
                title="tasks",
                title_align="left",
                border_style="grey30",
            ),
            ratio=2,
        ),
        Layout(name="side", ratio=1),
    )
    layout["side"].split_column(
        Layout(
            Panel(agent_table(agents), title="agents", title_align="left", border_style="grey30")
        ),
        Layout(
            Panel(
                lease_panel(leases, agent_names),
                title="leases",
                title_align="left",
                border_style="grey30",
            )
        ),
    )
    return layout


def _header() -> Panel:
    title = Text("codefleet", style="bold")
    title.append("  ·  parallel agents, one working tree, one lease per file", style="dim")
    return Panel(title, border_style="grey30")


# ---------------------------------------------------------------------------
# Wire
# ---------------------------------------------------------------------------


async def stream_events(
    client: httpx.AsyncClient, *, since: int = 0
) -> AsyncIterator[dict[str, Any]]:
    """Yield events from the SSE endpoint, replaying everything after `since` first.

    Replay and live tail are the same code path on the server, so a client that
    remembers the last id it saw can reconnect by passing it back here.
    """
    async with client.stream(
        "GET", "/events/stream", params={"since": since}, timeout=None
    ) as response:
        response.raise_for_status()
        data: list[str] = []
        async for line in response.aiter_lines():
            if line.startswith(":"):  # a keepalive comment
                continue
            if line == "":
                if data:
                    yield json.loads("\n".join(data))
                    data = []
                continue
            field, _, value = line.partition(":")
            if field == "data":
                data.append(value.lstrip())


async def watch(
    base_url: str,
    *,
    since: int | None = None,
    stop_on_run_finished: bool = False,
    refresh_per_second: float = 4.0,
    console: Console | None = None,
) -> None:
    """Render the fleet until interrupted, or until `run_finished` if asked.

    `since=None` replays the whole event log, so attaching late still shows the
    vetoes that already happened; pass the `last_event_id` from `/state` to see
    only what follows.
    """
    events: deque[dict[str, Any]] = deque(maxlen=MAX_EVENTS)
    finished = asyncio.Event()
    started = time.monotonic()

    async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=15.0) as client:
        state = await _get_state(client)
        cursor = since if since is not None else 0
        pump = asyncio.create_task(
            _pump(client, cursor, events, finished, stop_on_run_finished), name="dashboard-pump"
        )
        try:
            with Live(
                render(state, events, 0.0),
                console=console,
                refresh_per_second=refresh_per_second,
                screen=False,
            ) as live:
                while not finished.is_set():
                    state = await _get_state(client)
                    live.update(render(state, events, time.monotonic() - started))
                    with suppress(TimeoutError):
                        await asyncio.wait_for(finished.wait(), timeout=1 / refresh_per_second)
                live.update(render(await _get_state(client), events, time.monotonic() - started))
        finally:
            pump.cancel()
            with suppress(asyncio.CancelledError):
                await pump
        # A pump that died of anything other than end-of-stream is a real error
        # and must not look like a run that simply ended.
        if not pump.cancelled() and pump.exception() is not None:
            raise pump.exception()  # type: ignore[misc]


async def _pump(
    client: httpx.AsyncClient,
    since: int,
    events: deque[dict[str, Any]],
    finished: asyncio.Event,
    stop_on_run_finished: bool,
) -> None:
    try:
        async for event in stream_events(client, since=since):
            events.append(event)
            if stop_on_run_finished and event.get("type") == EventType.RUN_FINISHED:
                return
    finally:
        finished.set()


async def _get_state(client: httpx.AsyncClient) -> dict[str, Any]:
    response = await client.get("/state")
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def _money(value: Any) -> str:
    return f"${float(value or 0.0):.4f}"


def _clock(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    return f"{minutes:02d}:{secs:02d}"


def _basename(path: str) -> str:
    return path.rsplit("/", 1)[-1]
