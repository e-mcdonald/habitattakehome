"""Runtime configuration.

Loaded from environment (12-factor). Source-level config lives in the
per-source YAML files under ``sources/`` and is not represented here.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings resolved from environment variables and ``.env``."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Postgres connection string, e.g. postgresql://user:pass@host:5432/db.
    database_url: str = Field(default="postgresql://habitat:habitat@localhost:5432/habitat")

    # "prod" -> JSON structlog; anything else -> colored console.
    pipeline_env: str = Field(default="dev")

    # Where landing/validated JSONL files live.
    pipeline_data_dir: Path = Field(default=Path("data"))

    # Where per-source YAML configs live.
    pipeline_sources_dir: Path = Field(default=Path("sources"))

    # Where sql/schema.sql and sql/transforms/*.sql live.
    pipeline_sql_dir: Path = Field(default=Path("sql"))


def get_settings() -> Settings:
    """Return a fresh Settings() — cheap; no need to cache at this scope."""
    return Settings()
