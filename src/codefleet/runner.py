"""The agent side of the protocol: register, heartbeat, poll, execute, report.

This is deliberately the least interesting module in CodeFleet. It holds no
queue, evaluates no dependency, compares no file scope, and never decides what to
work on next — it reads decisions the server already made and durably recorded.
That is why it imports neither `codefleet.store` nor `codefleet.scheduler`, and
why swapping its executor for `ScriptedExecutor` leaves every coordination test
in the suite passing with no API key and no SDK session.

Three rules are worth stating out loud, because getting any of them wrong fails
silently rather than loudly:

* Every call after registration carries `X-Agent-Epoch`. A `409 stale_epoch`
  means this process is a zombie — whatever session it is running belongs to
  nobody now, so the session is abandoned and its result discarded rather than
  reported over the top of whoever owns the task.
* The pre-write callback fails **closed**. If the coordination server cannot be
  reached, the write is denied, the task fails as `infra`, and it is retried.
  Failing open would reintroduce exactly the collision the fleet exists to
  prevent.
* The post-write callback never raises. The ledger is observational; a failed
  ledger write must not fail a task that otherwise succeeded.
* Every other coordination failure is weather. A runner is a long-lived process
  whose entire job is to stay up, so a 5xx, a reset connection or a momentary
  timeout is retried with a bounded backoff and logged — never allowed to end a
  loop silently, which shrinks the fleet with nothing on screen to say so. Only
  a fault that outlasts the whole retry budget ends the runner, and it ends it
  loudly.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import httpx

from codefleet.config import Settings
from codefleet.models import ErrorKind, Task, TaskResult

if TYPE_CHECKING:
    from codefleet.session import (
        PostWriteCallback,
        PreWriteCallback,
        PreWriteDecision,
        SessionOutcome,
    )

__all__ = [
    "AssignmentLost",
    "Executor",
    "Runner",
    "ScriptedExecutor",
    "ScriptedWrite",
    "StaleEpoch",
]

# Coordination calls are loopback and must never outlive the write they gate: a
# PreToolUse hook that misses its deadline does not fall through to the tool, the
# CLI stops the turn, and it looks like a mysterious agent failure.
REQUEST_TIMEOUT_S = 10.0

# The ledger is fire-and-forget, so it gets a much shorter leash than a decision.
LEDGER_TIMEOUT_S = 2.0

# How many positional slots a runner will walk past before giving up. Generous:
# the only way to exhaust it is to have that many live runners already.
MAX_SLOT_PROBES = 64

# How long a loop waits after each consecutive transport failure, and — by its
# length — how many it rides out before giving up. Bounded on purpose: the
# server's stale sweep requeues the work of a runner that stops answering, so
# retrying costs the fleet nothing, but a runner that can never reach the server
# again is a fault the operator has to see rather than a slot spinning forever.
RETRY_BACKOFF_S: tuple[float, ...] = (0.5, 1.0, 2.0, 4.0, 8.0)

logger = logging.getLogger("codefleet.runner")

_DENY_INSTRUCTION = (
    "{path} is held by {holder} for task {task}. Do not retry this file or edit "
    "around it. Stop now and report that you are blocked on {path}."
)


class StaleEpoch(RuntimeError):
    """The server fenced this runner: something took its work away."""

    def __init__(self, epoch: int) -> None:
        super().__init__(f"epoch {epoch} is stale")
        self.epoch = epoch


class AssignmentLost(RuntimeError):
    """A 409 that is not about the epoch — this task is no longer ours."""


class Executor(Protocol):
    """What actually performs a task. The real one runs a Claude Agent SDK session."""

    async def __call__(
        self,
        *,
        task: Task,
        workdir: Path,
        on_pre_write: PreWriteCallback,
        on_post_write: PostWriteCallback,
    ) -> SessionOutcome: ...


def _next_slot(name: str, offset: int) -> str:
    """`runner-2` + 3 -> `runner-5`; a name with no trailing number gets a suffix."""
    match = re.fullmatch(r"(.*?)(\d+)", name)
    if match is None:
        return f"{name}-{offset + 1}"
    stem, number = match.groups()
    return f"{stem}{int(number) + offset}"


class Runner:
    """One agent slot: an identity on the server and a loop that feeds it work."""

    def __init__(
        self,
        name: str,
        base_url: str,
        workdir: Path | str,
        settings: Settings,
        executor: Executor | None = None,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.workdir = Path(workdir).resolve()
        self.settings = settings
        self.agent_id: str | None = None
        self.epoch = 0

        self._executor: Executor = executor or self._sdk_session
        # An injected client is the seam the unit tests use to stand up a stub
        # server with no socket; we only close what we opened ourselves.
        self._client = client or httpx.AsyncClient(
            base_url=self.base_url, timeout=REQUEST_TIMEOUT_S
        )
        self._owns_client = client is None

        self._stop = asyncio.Event()
        self._registering = asyncio.Lock()
        self._session: asyncio.Task[SessionOutcome] | None = None

    # -- lifecycle ---------------------------------------------------------

    async def run_forever(self) -> None:
        """Register, then heartbeat and poll until `shutdown()` is called."""
        await self._register()
        loops = [
            asyncio.create_task(self._heartbeat_loop(), name=f"{self.name}-heartbeat"),
            asyncio.create_task(self._poll_loop(), name=f"{self.name}-poll"),
        ]
        try:
            await asyncio.gather(*loops)
        except Exception as died:
            # The last place this exception can still be explained: the caller
            # gathers runners with `return_exceptions=True`, so an unlogged
            # death here is a fleet that quietly shrank.
            logger.error("%s: stopping — %s", self.name, _describe(died))
            raise
        finally:
            for loop in loops:
                loop.cancel()
            # Awaited, not just cancelled: one loop failing brings the other down
            # with it, and an unretrieved second failure is a warning at exit
            # instead of a diagnosis.
            await asyncio.gather(*loops, return_exceptions=True)
            await self._abandon_session()
            await self._deregister()
            if self._owns_client:
                await self._client.aclose()

    async def shutdown(self) -> None:
        """Ask both loops to finish. `run_forever` deregisters on its way out."""
        self._stop.set()

    # -- loops -------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                await self._heartbeat_once()
            except httpx.HTTPError as blip:
                failures = await self._ride_out("heartbeat", blip, failures)
                continue
            failures = 0
            await self._sleep(self.settings.heartbeat_interval)

    async def _heartbeat_once(self) -> None:
        try:
            await self._request("POST", f"/agents/{self.agent_id}/heartbeat", json={})
        except StaleEpoch as fenced:
            # This is the one signal that a task was taken away mid-flight.
            await self._abandon_session()
            await self._reregister(fenced.epoch)

    async def _poll_loop(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                await self._poll_once()
            except httpx.HTTPError as blip:
                # Anything in flight is abandoned rather than reported late over
                # the top of whoever the server hands the task to next; the
                # deadline sweep is what puts the task back on the queue.
                await self._abandon_session()
                failures = await self._ride_out("poll", blip, failures)
                continue
            failures = 0

    async def _poll_once(self) -> None:
        """One pass of the poll loop, fenced but not retried.

        Split out from the loop so that re-registration — which is itself an
        HTTP call, and the thing most likely to fail while a server is coming
        back up — sits inside the loop's retry arm rather than beside it.
        """
        try:
            task = await self._fetch_assignment()
            if task is None:
                await self._sleep(self.settings.poll_interval)
                return
            await self._execute(task)
        except StaleEpoch as fenced:
            await self._abandon_session()
            await self._reregister(fenced.epoch)
        except AssignmentLost:
            # The server moved the task on between the poll and the start call.
            # There is nothing to report and nothing to clean up.
            await self._sleep(self.settings.poll_interval)

    async def _ride_out(self, where: str, failure: httpx.HTTPError, failures: int) -> int:
        """Back off after a transient coordination failure, or re-raise having tried.

        Retrying is strictly safer than exiting: the server already requeues the
        work of a runner that stops answering, so a loop that waits out a blip
        costs the fleet a few seconds, while a loop that returns costs it a
        runner for the rest of the run. The budget is what keeps that from
        becoming a slot that spins forever against a server it will never reach.
        """
        if failures >= len(RETRY_BACKOFF_S):
            logger.error(
                "%s: %s failed %d times in a row (%s); giving up",
                self.name,
                where,
                failures + 1,
                _describe(failure),
            )
            raise failure
        delay = RETRY_BACKOFF_S[failures]
        logger.warning(
            "%s: %s failed (%s); retrying in %.1fs",
            self.name,
            where,
            _describe(failure),
            delay,
        )
        await self._sleep(delay)
        return failures + 1

    async def _fetch_assignment(self) -> Task | None:
        response = await self._request("GET", f"/agents/{self.agent_id}/assignment")
        if response.status_code == 204:
            return None
        # The assignment body is a projection of the row, not the whole row;
        # unknown keys (`deadline`) are dropped and absent ones take defaults.
        return Task.model_validate(response.json()["task"])

    async def _execute(self, task: Task) -> None:
        epoch = self.epoch
        await self._request("POST", f"/tasks/{task.id}/start", json={"agent_id": self.agent_id})

        session = asyncio.create_task(
            self._executor(
                task=task,
                workdir=self.workdir,
                on_pre_write=partial(self._on_pre_write, task.id),
                on_post_write=partial(self._on_post_write, task.id),
            ),
            name=f"{self.name}-session",
        )
        self._session = session
        await asyncio.wait({session})
        if self._session is session:
            self._session = None

        # A session that was cancelled, or that ran across a re-registration,
        # belongs to a task this process no longer owns. Reporting it would write
        # over whoever owns it now, which is the whole point of the fencing token.
        if session.cancelled() or self.epoch != epoch:
            return

        failure = session.exception()
        if failure is not None:
            result = self._infra_result(task, failure)
        else:
            outcome = session.result()
            self._log_session_posture(task, outcome)
            result = self._result_of(task, outcome)
        await self._request(
            "POST", f"/tasks/{task.id}/complete", json=result.model_dump(mode="json")
        )

    # -- write coordination ------------------------------------------------

    async def _on_pre_write(self, task_id: str, paths: list[str], tool: str) -> PreWriteDecision:
        """Ask the server whether this write may land. Raising here denies it."""
        response = await self._request(
            "POST",
            "/leases/acquire",
            json={
                "agent_id": self.agent_id,
                "task_id": task_id,
                "paths": paths,
                "tool": tool,
            },
        )
        body = response.json()
        if body.get("decision") == "allow":
            return _decision(allow=True)
        # The denied path travels with the decision, not just inside the prose.
        # One tool call can ask for several paths and be refused over any one of
        # them; the session records `decision.path` as what the task is blocked
        # on, and the server widens the task's `file_scope` with it on retry.
        # Falling back to `paths[0]` there would put a file nobody contended
        # into the scope of the next attempt.
        return _decision(
            allow=False,
            message=_denial_message(body, paths),
            path=_denied_path(body),
        )

    async def _on_post_write(self, task_id: str, path: str, tool: str) -> None:
        """Record a write that already happened. Must not raise; see module docstring."""
        with suppress(httpx.HTTPError, StaleEpoch, AssignmentLost):
            await self._request(
                "POST",
                "/changes",
                json={
                    "agent_id": self.agent_id,
                    "task_id": task_id,
                    "path": path,
                    "tool": tool,
                },
                timeout=LEDGER_TIMEOUT_S,
            )

    # -- registration ------------------------------------------------------

    async def _register(self) -> None:
        """Claim a slot, walking past any name a live runner already holds.

        Slot names are positional (`runner-1`, `runner-2`), so a second fleet
        started against the same server asks for names the first fleet is still
        using. The server refuses those with `name_in_use` rather than reclaiming
        them — reclaiming would fence a working runner and drop its leases — so
        adding capacity mid-run means taking the next free number instead.
        """
        name = self.name
        for offset in range(MAX_SLOT_PROBES):
            response = await self._client.post(
                "/agents/register",
                json={"name": name, "workdir": str(self.workdir), "pid": os.getpid()},
            )
            if response.status_code == 409:
                error = response.json().get("error", {})
                if error.get("code") == "name_in_use":
                    name = _next_slot(self.name, offset + 1)
                    continue
            response.raise_for_status()
            body = response.json()
            self.name = name
            self.agent_id = body["agent_id"]
            self.epoch = int(body["epoch"])
            return
        raise RuntimeError(
            f"no free runner slot after {MAX_SLOT_PROBES} attempts starting at {self.name}"
        )

    async def _reregister(self, stale_epoch: int) -> None:
        async with self._registering:
            # Both loops can see the same fencing; only the first re-registers.
            if self.epoch != stale_epoch:
                return
            await self._register()

    async def _deregister(self) -> None:
        if self.agent_id is None:
            return
        with suppress(httpx.HTTPError, StaleEpoch, AssignmentLost):
            await self._request("DELETE", f"/agents/{self.agent_id}")

    # -- plumbing ----------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        timeout: Any = httpx.USE_CLIENT_DEFAULT,
    ) -> httpx.Response:
        response = await self._client.request(
            method,
            path,
            json=json,
            headers={"X-Agent-Epoch": str(self.epoch)},
            timeout=timeout,
        )
        if response.status_code == 409:
            code = response.json().get("error", {}).get("code")
            if code == "stale_epoch":
                raise StaleEpoch(self.epoch)
            raise AssignmentLost(str(code))
        response.raise_for_status()
        return response

    async def _sleep(self, seconds: float) -> None:
        """Sleep, but wake immediately on shutdown."""
        with suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    async def _abandon_session(self) -> None:
        session, self._session = self._session, None
        if session is None or session.done():
            return
        session.cancel()
        with suppress(asyncio.CancelledError):
            await session

    async def _sdk_session(
        self,
        *,
        task: Task,
        workdir: Path,
        on_pre_write: PreWriteCallback,
        on_post_write: PostWriteCallback,
    ) -> SessionOutcome:
        """The default executor. Imported lazily so a Runner needs no SDK to exist."""
        from codefleet.session import run_session

        return await run_session(
            task=task,
            workdir=workdir,
            settings=self.settings,
            on_pre_write=on_pre_write,
            on_post_write=on_post_write,
            stderr_path=self.settings.run_dir / f"{self.name}.stderr.log",
        )

    def _log_session_posture(self, task: Task, outcome: SessionOutcome) -> None:
        """Report what the session resolved to, and check the one claim we can check.

        The SDK's init frame is the only record of the settings a session really
        ran under, as opposed to the ones it was asked for — the model an alias
        resolved to, the permission mode, the tool set, and the directory the
        agent believes it is working in. That last one is the runner's business:
        a session whose cwd is not the shared tree writes outside everything the
        leases coordinate, so a mismatch is a warning rather than a statistic.
        Without this the frame is captured, carried through `SessionOutcome` and
        discarded, which is worse than not capturing it.
        """
        frame = outcome.init_frame
        if frame is None:
            return
        logger.info(
            "%s: %s ran model=%s permission_mode=%s tools=%d cwd=%s session=%s",
            self.name,
            task.id,
            frame.get("model"),
            frame.get("permissionMode"),
            len(frame.get("tools") or ()),
            frame.get("cwd"),
            outcome.session_id,
        )
        cwd = frame.get("cwd")
        if isinstance(cwd, str) and Path(cwd).resolve() != self.workdir:
            logger.warning(
                "%s: %s ran in %s, not the shared tree %s — its writes were not coordinated",
                self.name,
                task.id,
                cwd,
                self.workdir,
            )

    def _result_of(self, task: Task, outcome: SessionOutcome) -> TaskResult:
        """`attempt` comes from the assignment, not from a re-read of the row.

        The number this report is about was fixed when the task was handed over.
        Asking the server for it now would read whatever attempt is current, which
        is the stale-report bug rather than the fix for it.
        """
        assert self.agent_id is not None
        return TaskResult(
            agent_id=self.agent_id,
            attempt=task.attempts,
            ok=outcome.ok,
            summary=outcome.summary,
            error=outcome.error,
            error_kind=outcome.error_kind,
            blocked_on_path=outcome.blocked_on_path,
            input_tokens=outcome.input_tokens,
            output_tokens=outcome.output_tokens,
            cost_usd=outcome.cost_usd,
            duration_ms=outcome.duration_ms,
            session_id=outcome.session_id,
            files_written=outcome.files_written,
        )

    def _infra_result(self, task: Task, failure: BaseException) -> TaskResult:
        """An executor that blew up is a reportable outcome, not a lost task.

        The exception text travels to the server so it lands in `tasks.error`
        rather than only in this process's traceback.
        """
        assert self.agent_id is not None
        return TaskResult(
            agent_id=self.agent_id,
            attempt=task.attempts,
            ok=False,
            error=f"{type(failure).__name__}: {failure}",
            error_kind=ErrorKind.INFRA,
        )


def _describe(failure: BaseException) -> str:
    """One line an operator can act on. A status code says more than a repr."""
    if isinstance(failure, httpx.HTTPStatusError):
        return f"HTTP {failure.response.status_code} from {failure.request.url.path}"
    return f"{type(failure).__name__}: {failure}"


def _denial_message(body: dict[str, Any], paths: Sequence[str]) -> str:
    """Turn a denial into an instruction the model will actually obey.

    The server's `message` is a statement of fact; what stops an agent from
    editing around the veto is the instruction appended to it.
    """
    denied = _denied_entries(body)
    if not denied:
        return str(body.get("message") or f"{paths[0]} is held by another agent. Stop now.")
    first = denied[0]
    return _DENY_INSTRUCTION.format(
        path=first.get("path", paths[0]),
        holder=first.get("holder_agent_name", "another agent"),
        task=first.get("holder_task_id", "another task"),
    )


def _denied_entries(body: dict[str, Any]) -> list[dict[str, Any]]:
    """The `denied` list, defended against a body that is not shaped like one."""
    denied = body.get("denied")
    if not isinstance(denied, list):
        return []
    return [entry for entry in denied if isinstance(entry, dict)]


def _denied_path(body: dict[str, Any]) -> str | None:
    """Which file the server actually refused, when it says so."""
    for entry in _denied_entries(body):
        path = entry.get("path")
        if isinstance(path, str) and path:
            return path
    return None


# `PreWriteDecision` and `SessionOutcome` live in the one module allowed to
# import the SDK, so they are constructed through these two helpers rather than
# imported at module scope — that is what keeps `Runner` constructible, and the
# whole coordination surface testable, without the SDK.
def _decision(
    *, allow: bool, message: str | None = None, path: str | None = None
) -> PreWriteDecision:
    from codefleet.session import PreWriteDecision

    return PreWriteDecision(allow=allow, message=message, path=path)


def _outcome(**fields: Any) -> SessionOutcome:
    from codefleet.session import SessionOutcome

    return SessionOutcome(**fields)


# ---------------------------------------------------------------------------
# The fake brain
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ScriptedWrite:
    """One write a scripted agent attempts.

    `pause_before` is what makes a scripted run resemble a real one: it is how
    long the "agent" thinks before touching the file, and therefore how long it
    holds the files it already has.
    """

    path: str
    tool: str = "Write"
    content: str | None = None
    pause_before: float = 0.0


class ScriptedExecutor:
    """An executor that follows a script instead of running an SDK session.

    It speaks the same protocol the real session does — ask before every write,
    stop dead when denied, report the ledger afterward — so a fleet driven by it
    exercises the entire coordination surface: assignment, dependency cascade,
    lease exclusion, veto, backoff and retry. Nothing here imports the SDK, so
    this is what runs in CI and behind `codefleet demo --dry-run`.
    """

    def __init__(
        self,
        plan: Callable[[Task], Sequence[ScriptedWrite]] | None = None,
        *,
        think_time: float = 0.0,
    ) -> None:
        self._plan = plan if plan is not None else partial(_declared_scope_plan, pause=think_time)
        self.executed: list[str] = []

    async def __call__(
        self,
        *,
        task: Task,
        workdir: Path,
        on_pre_write: PreWriteCallback,
        on_post_write: PostWriteCallback,
    ) -> SessionOutcome:
        self.executed.append(task.id)
        written: list[str] = []

        for write in self._plan(task):
            if write.pause_before:
                await asyncio.sleep(write.pause_before)
            try:
                decision = await on_pre_write([write.path], write.tool)
            except Exception as exc:
                # Mirrors the real hook: coordination that cannot be reached
                # denies the write, and the failure decides the outcome.
                return _outcome(
                    ok=False,
                    error=f"write coordination failed: {type(exc).__name__}: {exc}",
                    error_kind=ErrorKind.INFRA,
                    files_written=tuple(written),
                )
            if not decision.allow:
                return _outcome(
                    ok=False,
                    error=f"blocked: {write.path} is held by another agent",
                    error_kind=ErrorKind.VETO,
                    blocked_on_path=write.path,
                    files_written=tuple(written),
                )

            _apply(workdir, write, task)
            if write.path not in written:
                written.append(write.path)
            await on_post_write(write.path, write.tool)

        return _outcome(
            ok=True,
            summary=f"scripted run of {task.id} touched {len(written)} file(s)",
            files_written=tuple(written),
        )


def _declared_scope_plan(task: Task, *, pause: float) -> list[ScriptedWrite]:
    """The default script: write once to every path the task declared."""
    return [ScriptedWrite(path, pause_before=pause) for path in task.file_scope]


def _apply(workdir: Path, write: ScriptedWrite, task: Task) -> None:
    """Perform the scripted write.

    The default content is an appended comment rather than a replacement, so a
    scripted run over a real repository leaves it syntactically intact and its
    test suite still passing — which is the thing the demo checks at the end.
    """
    target = workdir / write.path
    target.parent.mkdir(parents=True, exist_ok=True)
    content = write.content if write.content is not None else f"# {task.id}: {task.title}\n"
    with target.open("a", encoding="utf-8") as handle:
        handle.write(content)
