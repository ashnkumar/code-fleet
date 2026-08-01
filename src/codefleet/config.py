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

    # Retries
    max_attempts: int = 3
    backoff_step_s: float = 2.0
    backoff_max_s: float = 30.0

    # Escape hatches
    allow_reset: bool = False
    run_dir: Path = Field(default=Path("./runs"), description="Per-run logs and workspaces.")

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def lease_grace_s(self) -> float:
        """Extra time the server waits past a task's deadline before requeueing."""
        return 60.0

    def backoff_for(self, attempts: int) -> float:
        return min(self.backoff_step_s * max(attempts, 1), self.backoff_max_s)


@lru_cache
def get_settings() -> Settings:
    return Settings()
