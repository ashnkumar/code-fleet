"""Configuration.

Every knob is an environment variable with a `CODEFLEET_` prefix and a default
that works out of the box. `.env.example` lists all of them; there is no config
file the code reads but does not document.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CODEFLEET_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Server
    host: str = "127.0.0.1"
    port: int = 8099
    db: Path = Path("./codefleet.db")

    # Fleet
    workdir: Path = Path("./examples/demo-repo")
    runners: int = 3

    # Agent sessions
    model: str = "claude-haiku-4-5-20251001"
    task_timeout: float = 600.0
    task_budget_usd: float = 0.50
    max_turns: int = 40

    # Liveness
    heartbeat_interval: float = 5.0
    stale_after: float = 20.0
    poll_interval: float = 1.0
    tick_interval: float = 0.5

    # Retries. The backoff curve itself is not a knob — it lives in the
    # scheduler, which is pure and must stay independent of Settings.
    max_attempts: int = 3

    # Escape hatches
    allow_reset: bool = False
    run_dir: Path = Field(default=Path("./runs"), description="Per-run logs and workspaces.")

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def lease_grace_s(self) -> float:
        """How long past a task's own deadline the server waits before taking it back.

        The runner enforces `task_timeout` on its own session. This is the
        server's backstop for the case that clock fails — a runner still
        heartbeating but no longer making progress. Heartbeat staleness cannot
        catch that one, because the heartbeat is fine.
        """
        return 60.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
