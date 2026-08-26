"""Gold: one match date of Silver merged into the star schema, then validated.

Triggered by the five Silver assets. This is the last DAG in the chain and the
one that publishes the contract the analytics and ML subprojects consume
(CLAUDE.md §1), so it is also where the run is allowed to fail loudly.

==================================== ============================================
Task                                 Does
==================================== ============================================
``resolve_match_date``               reads the date off the triggering events
``load_silver_to_gold_postgres``     PySpark: stage + merge into facts and dims
``data_quality_check``               21 Gold invariants; publishes on success
==================================== ============================================

Design notes:

* **The Gold assets are published by the quality gate, not by the load.** An
  update therefore means "loaded *and* validated", and a downstream consumer
  subscribing to ``gold_fact_matches`` never wakes up on a night whose data did
  not pass. It is the one place in the pipeline where the asset boundary is
  deliberately drawn after a check rather than after a write.
* **The quality gate does not retry.** Every other task retries twice with
  exponential backoff, because their failures are transient. Corrupt rows do not
  improve on a second read; retrying only buries the report under two attempts
  and delays the alert by fifteen minutes.
* **The load is idempotent.** ``silver_to_gold.py`` stages the date and merges
  with upserts, so re-running a date corrects it instead of duplicating it —
  which is what makes a repair run safe.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag, task
from src.quality import run_checks

from assets.football import gold_football_assets, silver_football_assets
from football_conf import (
    DEFAULT_ARGS,
    START_DATE,
    SparkSubmitOperatorConf,
    spark_application,
)
from football_connections import postgres_config_from_connection
from football_run_date import (
    MATCH_DATE_PARAMS,
    RESOLVED_MATCH_DATE,
    publish_match_date,
    resolve_match_date,
)


@dag(
    dag_id="gold_football_star_schema",
    description="Gold: merge one match date of Silver into the star schema and validate it.",
    start_date=START_DATE,
    # All five Silver tables: the star schema is built from every one of them,
    # and they are published together by a single upstream task.
    schedule=list(silver_football_assets.values()),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    params=MATCH_DATE_PARAMS,
    doc_md=__doc__,
    tags=["football", "medallion", "gold"],
)
def gold_football_star_schema():
    match_date = resolve_match_date()

    load_silver_to_gold_postgres = SparkSubmitOperator(
        task_id="load_silver_to_gold_postgres",
        application=spark_application("src/transform/silver_to_gold.py"),
        application_args=["--date", RESOLVED_MATCH_DATE],
        name=f"silver_to_gold_{RESOLVED_MATCH_DATE}",
        **SparkSubmitOperatorConf,
    )

    @task(
        outlets=list(gold_football_assets.values()),
        # The gate on publishing a night's load; see the module docstring.
        retries=0,
    )
    def data_quality_check(target_date: str, *, outlet_events: Any) -> None:
        """Validate Gold, then publish it.

        ``run_checks.run`` raises ``DataQualityError`` on an ERROR-severity
        failure, which fails this task — and because the outlets are attached
        here, that failure also withholds the asset events. The checks are
        schema-wide rather than date-scoped: a merge can only corrupt the tables
        it touched, but the invariants worth asserting (orphan keys, duplicate
        grains) are properties of the whole star schema.
        """
        run_checks.run(config=postgres_config_from_connection())
        publish_match_date(outlet_events, gold_football_assets.values(), target_date)

    match_date >> load_silver_to_gold_postgres >> data_quality_check(target_date=match_date)


gold_football_star_schema_dag = gold_football_star_schema()
