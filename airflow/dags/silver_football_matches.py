"""Silver: the Bronze JSON of one match date as typed, deduplicated Parquet.

Triggered by assets, not by a clock: this DAG is queued the moment *both* Bronze
extracts have published (``schedule=`` lists both assets, which Airflow reads as
"all of them"). A day when only one source landed leaves Silver waiting rather
than half-loading, and the wait costs nothing — no sensor is holding a slot.

==================================== ============================================
Task                                 Does
==================================== ============================================
``resolve_match_date``               reads the date off the triggering events
``process_bronze_to_silver_spark``   PySpark: clean, cast, dedupe -> Parquet
``publish_silver_tables``            publishes the five Silver assets
==================================== ============================================

Design notes:

* **The date comes from the event, not the clock.** An asset-triggered run has
  no logical date of its own, and 06:00-scheduled Bronze processes the *previous*
  evening's matches, so "today" would be wrong by a day even when it looked
  plausible. See :mod:`football_run_date`.
* **The Spark stage is submitted, not embedded.** SparkSubmitOperator runs
  ``spark-submit`` against the standalone cluster over ``spark_default``; the
  driver is a separate process configured by environment, which is why its
  credentials arrive as templates from :mod:`football_connections`.
* **Publishing is its own task.** SparkSubmitOperator cannot stamp the match
  date onto an asset event — only a task callable gets that accessor — so the
  submit stays a plain submit and a one-line task marks the layer as published.
  It also makes "Silver is complete" a state the graph shows rather than one
  inferred from a green Spark task.
* **Idempotent by partition.** The job writes with dynamic partition overwrite
  keyed on ``match_date``, so re-running a date replaces exactly that date.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag, task

from assets.football import bronze_football_assets, silver_football_assets
from football_conf import (
    DEFAULT_ARGS,
    START_DATE,
    SparkSubmitOperatorConf,
    spark_application,
)
from football_run_date import (
    MATCH_DATE_PARAMS,
    RESOLVED_MATCH_DATE,
    publish_match_date,
    resolve_match_date,
)


@dag(
    dag_id="silver_football_matches",
    description="Silver: Bronze JSON for one match date into typed, deduplicated Parquet.",
    start_date=START_DATE,
    # Both Bronze assets: a list is an AND, so this run starts once each source
    # has landed the same night's data.
    schedule=list(bronze_football_assets.values()),
    catchup=False,
    # One Spark driver at a time in the scheduler container.
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    params=MATCH_DATE_PARAMS,
    doc_md=__doc__,
    tags=["football", "medallion", "silver"],
)
def silver_football_matches():
    match_date = resolve_match_date()

    process_bronze_to_silver_spark = SparkSubmitOperator(
        task_id="process_bronze_to_silver_spark",
        application=spark_application("src/transform/bronze_to_silver.py"),
        application_args=["--date", RESOLVED_MATCH_DATE],
        # Names the application in the Spark master UI, where a failed night is
        # otherwise one of several identically named apps.
        name=f"bronze_to_silver_{RESOLVED_MATCH_DATE}",
        **SparkSubmitOperatorConf,
    )

    @task(outlets=list(silver_football_assets.values()))
    def publish_silver_tables(target_date: str, *, outlet_events: Any) -> str:
        """Mark the five Silver tables as updated for ``target_date``.

        Runs only if the submit succeeded, so an asset event means the Parquet
        is actually on disk. The date travels on the event, which is how Gold
        learns which day to load.
        """
        publish_match_date(outlet_events, silver_football_assets.values(), target_date)
        return target_date

    match_date >> process_bronze_to_silver_spark >> publish_silver_tables(target_date=match_date)


silver_football_matches_dag = silver_football_matches()
