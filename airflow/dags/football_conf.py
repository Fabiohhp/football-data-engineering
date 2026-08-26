"""Settings the three medallion DAGs share: schedule anchor, retries, Spark size.

Split out so the DAG files below stay a description of *what runs in what
order*. Anything here that is duplicated into a DAG is a chance for the layers
to drift apart — three different retry policies, a Spark stage sized differently
from its neighbour — which is exactly the kind of difference nobody notices
until a night when it matters.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from pendulum import datetime

from football_connections import SPARK_CONN_ID, spark_env_templates

__all__ = [
    "DEFAULT_ARGS",
    "PROJECT_ROOT",
    "START_DATE",
    "SparkSubmitOperatorConf",
    "spark_application",
]

# The date the pipeline was first verified end-to-end. Aware, because Airflow
# rejects a naive start_date; identical across the three DAGs so that they
# describe one pipeline rather than three that happen to be connected.
START_DATE = datetime(2025, 11, 1, tz="UTC")

# Where the repository is mounted in the Airflow image (see docker-compose.yml).
# The Spark cluster mounts the same tree at the same path, so one path argument
# means the same file on both sides of the submit.
PROJECT_ROOT = "/opt/football"

DEFAULT_ARGS: dict[str, Any] = {
    # Both sources are third-party and occasionally flaky; the extractors
    # already retry individual HTTP calls with backoff via tenacity, so these
    # retries are for the failure modes that outlive a single request — a source
    # briefly down, MinIO not yet accepting connections, a Spark worker
    # restarting. Every task is idempotent on its match date, so a retry is
    # always safe.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# Everything a SparkSubmitOperator in this pipeline needs beyond its application
# and arguments. Spread into each operator (`**SparkSubmitOperatorConf`) so both
# Spark stages are submitted to the same cluster, with the same credentials, at
# the same size.
SparkSubmitOperatorConf: dict[str, Any] = {
    # No master URL in the DAG: the connection owns it (CLAUDE.md §7).
    "conn_id": SPARK_CONN_ID,
    # Jinja over Connections, rendered when the task runs — the driver is a
    # separate process configured by environment, and a resolved value here
    # would be a secret baked into the serialized DAG.
    "env_vars": spark_env_templates(),
    # The whole day is ~1300 rows; the defaults would reserve far more than this
    # stack has to spare next to Airflow's own processes.
    "driver_memory": "1g",
    "executor_memory": "1g",
    "total_executor_cores": 2,
}


def spark_application(relative_path: str) -> str:
    """Absolute path to a Spark job, as both the driver and the cluster see it."""
    return f"{PROJECT_ROOT}/{relative_path}"
