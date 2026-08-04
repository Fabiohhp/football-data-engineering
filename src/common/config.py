"""Central configuration, loaded from environment variables / ``.env``.

Nothing in the pipeline should read ``os.environ`` directly — import the typed
config objects from here instead. This keeps credentials out of code and gives
us one place to document every knob. See ``.env.example`` for the full list.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _find_dotenv(start: Path | None = None) -> Path | None:
    """Walk upwards from ``start`` (or cwd) looking for a ``.env`` file."""
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        env_file = candidate / ".env"
        if env_file.exists():
            return env_file
    return None


def _load_dotenv_if_present() -> None:
    """Populate ``os.environ`` from ``.env`` without overriding real env vars.

    We intentionally do not overwrite variables that are already set, so values
    injected by Docker / Airflow Connections always win over the local file.
    """
    env_file = _find_dotenv()
    if env_file is None:
        return
    # python-dotenv is a declared dependency, but keep the import local so that
    # importing this module never hard-fails if it is somehow missing.
    from dotenv import dotenv_values

    for key, value in dotenv_values(env_file).items():
        if value is not None and key not in os.environ:
            os.environ[key] = value


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name!r} is not set")
    return value


@dataclass(frozen=True)
class PostgresConfig:
    """Connection details for the Gold-layer warehouse."""

    user: str
    password: str
    database: str
    host: str
    port: int

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def jdbc_url(self) -> str:
        """URL for Spark's JDBC writer. Credentials go in separate options, not
        here — a JDBC URL ends up in Spark UI pages and executor logs."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class MinioConfig:
    """Connection details for the S3-compatible object store (Bronze/Silver)."""

    endpoint: str
    access_key: str
    secret_key: str
    bronze_bucket: str
    silver_bucket: str


@dataclass(frozen=True)
class FootballDataConfig:
    """Football-Data.org API settings."""

    api_token: str
    base_url: str = "https://api.football-data.org/v4"


@dataclass(frozen=True)
class UnderstatConfig:
    """Understat scraping settings (JSON endpoints)."""

    base_url: str = "https://understat.com"
    # Politeness delay (seconds) between requests. Understat is scraping-adjacent
    # and fragile; never remove this.
    request_delay_seconds: float = 3.0


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def postgres_config() -> PostgresConfig:
    _load_dotenv_if_present()
    return PostgresConfig(
        user=_require("POSTGRES_USER"),
        password=_require("POSTGRES_PASSWORD"),
        database=_require("POSTGRES_DB"),
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=_int_env("POSTGRES_PORT", 5432),
    )


def minio_config() -> MinioConfig:
    _load_dotenv_if_present()
    return MinioConfig(
        endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        access_key=_require("MINIO_ROOT_USER"),
        secret_key=_require("MINIO_ROOT_PASSWORD"),
        bronze_bucket=os.environ.get("MINIO_BRONZE_BUCKET", "bronze"),
        silver_bucket=os.environ.get("MINIO_SILVER_BUCKET", "silver"),
    )


def football_data_config() -> FootballDataConfig:
    _load_dotenv_if_present()
    return FootballDataConfig(api_token=_require("FOOTBALL_DATA_API_TOKEN"))


@lru_cache(maxsize=1)
def understat_config() -> UnderstatConfig:
    _load_dotenv_if_present()
    delay = os.environ.get("UNDERSTAT_REQUEST_DELAY_SECONDS")
    return UnderstatConfig(
        request_delay_seconds=float(delay) if delay else 3.0,
    )
