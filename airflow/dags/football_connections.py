"""Adapters from Airflow Connections to the pipeline's own configuration objects.

CLAUDE.md §7 requires the DAG to reach MinIO, PostgreSQL and Football-Data
through Airflow Connections rather than hardcoded credentials, while §8 makes
``src/common/config.py`` the one place the pipeline reads configuration from.
These functions bridge the two: they read a Connection and return the frozen
dataclass the pipeline already expects. No task ever sees a literal credential,
and ``src/`` keeps no Airflow import — the transforms still run under a bare
``spark-submit`` with environment variables, exactly as they did before Airflow.

Two rules hold everywhere below:

* **Look connections up at task runtime, never at parse time.** The DAG file is
  re-parsed on a timer; resolving a Connection at module scope would query the
  backend on every cycle and bake the secret into the serialized DAG.
* **The Spark stages get templates, not values.** Their driver is a separate
  process, so it is configured by environment rather than by a Python object.
  :func:`spark_env_templates` returns Jinja that Airflow renders when the task
  runs, which keeps the same runtime-only rule for a process we do not own.
"""

from __future__ import annotations

from airflow.sdk import BaseHook
from src.common.config import FootballDataConfig, MinioConfig, PostgresConfig

__all__ = [
    "FOOTBALL_DATA_CONN_ID",
    "MINIO_CONN_ID",
    "POSTGRES_CONN_ID",
    "SPARK_CONN_ID",
    "football_data_config_from_connection",
    "minio_config_from_connection",
    "postgres_config_from_connection",
    "spark_env_templates",
]

# Defined in docker-compose.yml as AIRFLOW_CONN_* environment variables, so they
# exist as soon as the stack is up and never reach the metadata database.
SPARK_CONN_ID = "spark_default"
POSTGRES_CONN_ID = "postgres_gold"
MINIO_CONN_ID = "minio_default"
FOOTBALL_DATA_CONN_ID = "football_data_api"


def _require(value: str | None, conn_id: str, field: str) -> str:
    """Fail loudly on a half-configured Connection.

    A missing password arrives as ``None`` and would otherwise travel a long way
    before surfacing — as a MinIO 403 inside a Spark executor, say — so the
    connection id and the missing field are named here instead.
    """
    if not value:
        raise ValueError(f"Airflow connection {conn_id!r} is missing a value for {field!r}")
    return value


def minio_config_from_connection(conn_id: str = MINIO_CONN_ID) -> MinioConfig:
    """Build the Bronze/Silver object-store config from an Airflow Connection."""
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson
    return MinioConfig(
        endpoint=_require(extra.get("endpoint_url"), conn_id, "extra.endpoint_url"),
        access_key=_require(conn.login, conn_id, "login"),
        secret_key=_require(conn.password, conn_id, "password"),
        # Bucket names are not secrets, but keeping them on the connection means
        # one object describes the whole store rather than splitting it across a
        # connection and two loose environment variables.
        bronze_bucket=extra.get("bronze_bucket", "bronze"),
        silver_bucket=extra.get("silver_bucket", "silver"),
    )


def postgres_config_from_connection(conn_id: str = POSTGRES_CONN_ID) -> PostgresConfig:
    """Build the warehouse config from an Airflow Connection.

    Airflow stores the database name in a Connection's ``schema`` field, which
    is a long-standing misnomer: it is the dbname, not a PostgreSQL schema.
    """
    conn = BaseHook.get_connection(conn_id)
    return PostgresConfig(
        user=_require(conn.login, conn_id, "login"),
        password=_require(conn.password, conn_id, "password"),
        database=_require(conn.schema, conn_id, "schema"),
        host=_require(conn.host, conn_id, "host"),
        port=conn.port or 5432,
    )


def football_data_config_from_connection(
    conn_id: str = FOOTBALL_DATA_CONN_ID,
) -> FootballDataConfig:
    """Build the Football-Data.org config from an Airflow Connection.

    The API token lives in the Connection's ``password`` field so that Airflow's
    secret masker redacts it from task logs and the rendered-fields view.
    """
    conn = BaseHook.get_connection(conn_id)
    host = _require(conn.host, conn_id, "host")
    scheme = conn.schema or "https"
    return FootballDataConfig(
        api_token=_require(conn.password, conn_id, "password"),
        base_url=f"{scheme}://{host}/v4",
    )


def spark_env_templates(
    minio_conn_id: str = MINIO_CONN_ID,
    postgres_conn_id: str = POSTGRES_CONN_ID,
) -> dict[str, str]:
    """Environment for a ``spark-submit`` driver, as Jinja over Connections.

    The Spark stages run as their own process, so they configure themselves
    through ``src.common.config``, which reads the environment. Handing
    SparkSubmitOperator these templates rather than resolved values keeps the
    credential out of the DAG file and out of the serialized DAG: ``env_vars``
    is a template field, so Airflow renders it at task-run time and masks the
    secrets it recognises.

    Pure by design — it builds strings and touches no Connection — so the
    mapping is unit-testable without a running Airflow.
    """
    minio = f"conn.{minio_conn_id}"
    postgres = f"conn.{postgres_conn_id}"
    return {
        "MINIO_ENDPOINT": f"{{{{ {minio}.extra_dejson.endpoint_url }}}}",
        "MINIO_ROOT_USER": f"{{{{ {minio}.login }}}}",
        "MINIO_ROOT_PASSWORD": f"{{{{ {minio}.password }}}}",
        "MINIO_BRONZE_BUCKET": f"{{{{ {minio}.extra_dejson.bronze_bucket }}}}",
        "MINIO_SILVER_BUCKET": f"{{{{ {minio}.extra_dejson.silver_bucket }}}}",
        "POSTGRES_HOST": f"{{{{ {postgres}.host }}}}",
        "POSTGRES_PORT": f"{{{{ {postgres}.port }}}}",
        "POSTGRES_USER": f"{{{{ {postgres}.login }}}}",
        "POSTGRES_PASSWORD": f"{{{{ {postgres}.password }}}}",
        "POSTGRES_DB": f"{{{{ {postgres}.schema }}}}",
    }
