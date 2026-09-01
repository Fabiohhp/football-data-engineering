"""Integrity tests for the three medallion DAGs: their shape and their handoff.

A broken DAG file does not fail loudly — Airflow catches the exception during
parsing, records an import error and simply carries on scheduling nothing. The
first symptom is usually a day with no data. Splitting the pipeline across three
asset-linked DAGs adds a second silent failure of the same family: a producer
and a consumer that name different assets both parse perfectly and the
downstream DAG just never runs. These tests turn both into a red build.

They need Airflow, which ``requirements.txt`` deliberately omits (see
``requirements-airflow.txt``), so they skip on a plain local install and run in
CI where Airflow is installed. The rest of the suite stays JVM- and
Airflow-free.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# `airflow.sdk`, not `airflow`: this repository has its own `airflow/` directory
# at the root, which Python happily imports as a namespace package when the real
# one is absent. Skipping on the bare name would therefore never skip — it would
# succeed and then fail on the first real import.
pytest.importorskip(
    "airflow.sdk",
    reason="Airflow is installed from requirements-airflow.txt (CI and the Airflow image only)",
)

from airflow.sdk import AssetAll  # noqa: E402

from assets.football import (  # noqa: E402
    bronze_football_assets,
    gold_football_assets,
    silver_football_assets,
)
from football_conf import FOOTBALL_DATA_POOL  # noqa: E402
from football_run_date import RESOLVED_MATCH_DATE, SCHEDULED_MATCH_DATE  # noqa: E402

DAGS_FOLDER = Path(__file__).resolve().parents[1] / "airflow" / "dags"

BRONZE_DAG_ID = "bronze_football_matches"
SILVER_DAG_ID = "silver_football_matches"
GOLD_DAG_ID = "gold_football_star_schema"
DAG_IDS = (BRONZE_DAG_ID, SILVER_DAG_ID, GOLD_DAG_ID)

# The pipeline stages from CLAUDE.md §7, as task ids. Spelled out rather than
# derived from the DAGs so that renaming or dropping a stage has to be a
# deliberate edit here too.
EXPECTED_TASK_IDS = {
    BRONZE_DAG_ID: {
        "check_if_api_is_available",
        "extract_football_data",
        "extract_football_data_teams",
        "extract_understat",
    },
    SILVER_DAG_ID: {
        "resolve_match_date",
        "process_bronze_to_silver_spark",
        "publish_silver_tables",
    },
    GOLD_DAG_ID: {
        "resolve_match_date",
        "load_silver_to_gold_postgres",
        "data_quality_check",
    },
}

# Every task that is expected to publish assets, and what it must publish.
EXPECTED_OUTLETS = {
    (BRONZE_DAG_ID, "extract_football_data"): {bronze_football_assets["football_data"].name},
    (BRONZE_DAG_ID, "extract_football_data_teams"): {
        bronze_football_assets["football_data_teams"].name
    },
    (BRONZE_DAG_ID, "extract_understat"): {bronze_football_assets["understat"].name},
    (SILVER_DAG_ID, "publish_silver_tables"): {a.name for a in silver_football_assets.values()},
    (GOLD_DAG_ID, "data_quality_check"): {a.name for a in gold_football_assets.values()},
}

SPARK_TASK_IDS = {
    SILVER_DAG_ID: "process_bronze_to_silver_spark",
    GOLD_DAG_ID: "load_silver_to_gold_postgres",
}


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


@pytest.fixture(scope="module")
def dags(dagbag):
    return dagbag.dags


def _schedule_assets(dag) -> set[str]:
    """The names of the assets whose events trigger ``dag``."""
    return {asset.name for _, asset in dag.timetable.asset_condition.iter_assets()}


def _outlets(dag, task_id: str) -> set[str]:
    return {asset.name for asset in dag.get_task(task_id).outlets}


def test_dag_files_import_cleanly(dagbag):
    """No syntax error, no bad import, no exception at module scope."""
    assert dagbag.import_errors == {}
    # Equality, not containment: a leftover DAG file from an earlier shape of
    # the pipeline would otherwise keep scheduling alongside its replacement.
    assert set(dagbag.dags) == set(DAG_IDS)


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_every_pipeline_stage_is_present(dags, dag_id):
    assert set(dags[dag_id].task_ids) == EXPECTED_TASK_IDS[dag_id]


def test_bronze_extracts_only_after_the_source_answers(dags):
    downstream = {
        task_id: dags[BRONZE_DAG_ID].get_task(task_id).downstream_task_ids
        for task_id in dags[BRONZE_DAG_ID].task_ids
    }
    assert downstream["check_if_api_is_available"] == {
        "extract_football_data",
        "extract_football_data_teams",
        "extract_understat",
    }
    assert downstream["extract_football_data"] == set()
    assert downstream["extract_football_data_teams"] == set()
    assert downstream["extract_understat"] == set()


def test_the_extractors_do_not_depend_on_each_other(dags):
    """Neither host should be able to block another, and the two Football-Data
    endpoints are separate tasks so a roster outage cannot fail the results."""
    extractors = [
        dags[BRONZE_DAG_ID].get_task(task_id)
        for task_id in ("extract_football_data", "extract_football_data_teams", "extract_understat")
    ]
    for task in extractors:
        others = {other.task_id for other in extractors} - {task.task_id}
        assert not (others & task.downstream_task_ids)


@pytest.mark.parametrize(
    ("dag_id", "spark_task_id", "publish_task_id"),
    [
        (SILVER_DAG_ID, "process_bronze_to_silver_spark", "publish_silver_tables"),
        (GOLD_DAG_ID, "load_silver_to_gold_postgres", "data_quality_check"),
    ],
)
def test_the_date_is_resolved_before_the_stage_that_needs_it(
    dags, dag_id, spark_task_id, publish_task_id
):
    """Resolve, then transform, then publish.

    Asserted as edges rather than as a sorted list: a topological order would
    also accept a DAG whose stages merely happen to sort correctly while being
    entirely unconnected, which is exactly the bug this guards against.
    """
    dag = dags[dag_id]
    resolve = dag.get_task("resolve_match_date")

    # Both consumers of the date depend on it; the publish step also waits for
    # the stage itself, so the asset is never published on a failed transform.
    assert resolve.downstream_task_ids == {spark_task_id, publish_task_id}
    assert dag.get_task(spark_task_id).downstream_task_ids == {publish_task_id}
    assert dag.get_task(publish_task_id).downstream_task_ids == set()


def test_the_layers_are_wired_to_each_other_by_assets(dags):
    """The handoff itself: what Bronze publishes is what Silver waits for.

    Without this, a renamed asset on one side leaves both DAGs parsing happily
    while the downstream one simply never runs again.
    """
    bronze_published = set().union(
        *(
            _outlets(dags[BRONZE_DAG_ID], task_id)
            for task_id in (
                "extract_football_data",
                "extract_football_data_teams",
                "extract_understat",
            )
        )
    )
    assert _schedule_assets(dags[SILVER_DAG_ID]) == bronze_published
    assert _schedule_assets(dags[GOLD_DAG_ID]) == _outlets(
        dags[SILVER_DAG_ID], "publish_silver_tables"
    )


@pytest.mark.parametrize("dag_id", [SILVER_DAG_ID, GOLD_DAG_ID])
def test_a_layer_waits_for_all_of_its_inputs(dags, dag_id):
    """A list of assets is an AND, and it has to stay one.

    Any-of semantics would start Silver on whichever source landed first and
    load half a matchday, which is precisely the failure the split into layers
    is supposed to make impossible.
    """
    assert isinstance(dags[dag_id].timetable.asset_condition, AssetAll)


@pytest.mark.parametrize(("dag_id", "task_id"), sorted(EXPECTED_OUTLETS))
def test_each_layer_publishes_exactly_its_own_assets(dags, dag_id, task_id):
    assert _outlets(dags[dag_id], task_id) == EXPECTED_OUTLETS[(dag_id, task_id)]


def test_gold_is_published_by_the_quality_gate_not_by_the_load(dags):
    """The Gold assets are the contract with the analytics/ML subprojects.

    Publishing them from the load task would mean a consumer wakes up on a
    night that failed validation; attaching them to the check makes an update
    mean "loaded *and* validated".
    """
    assert _outlets(dags[GOLD_DAG_ID], "load_silver_to_gold_postgres") == set()
    assert _outlets(dags[GOLD_DAG_ID], "data_quality_check")


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_backfills_cannot_stampede_the_rate_limited_sources(dags, dag_id):
    """CLAUDE.md §4 forbids rebuilding history; §10 forbids bypassing limits.

    ``catchup`` on plus unbounded active runs is how a DAG that respects rate
    limits per-request still manages to hammer a source: a stack switched off
    for a week wakes up and fires every missed day at once. Downstream the same
    setting keeps two Spark drivers out of the scheduler container at once.
    """
    assert dags[dag_id].catchup is False
    assert dags[dag_id].max_active_runs == 1


# Everything that talks to api.football-data.org, and therefore everything that
# spends from the one shared 10-per-minute budget. Understat is deliberately
# absent: different host, different limit, and only one task touches it.
FOOTBALL_DATA_TASK_IDS = (
    "check_if_api_is_available",
    "extract_football_data",
    "extract_football_data_teams",
)


@pytest.mark.parametrize("task_id", FOOTBALL_DATA_TASK_IDS)
def test_football_data_tasks_share_one_rate_limit_slot(dags, task_id):
    """Per-request politeness is not enough when requests run in parallel.

    The extractors honour the throttle headers, but each builds its own client
    with its own view of the budget. Two of them running at once race for the
    same server-side 10 calls/minute and neither can see the other's spend — the
    exact bypass CLAUDE.md §10 forbids. The pool is what serialises them.
    """
    assert dags[BRONZE_DAG_ID].get_task(task_id).pool == FOOTBALL_DATA_POOL


def test_understat_is_not_held_behind_the_football_data_limit(dags):
    """A different host under a different limit should not queue behind this one.

    Understat's politeness delay lives in its own extractor; putting it in the
    Football-Data pool would serialise two independent sources and roughly double
    the wall clock of a Bronze run for nothing.
    """
    assert dags[BRONZE_DAG_ID].get_task("extract_understat").pool == "default_pool"


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_every_layer_can_be_replayed_for_one_date(dags, dag_id):
    """An asset-triggered DAG has no logical date to re-run against.

    The ``match_date`` param is the only way to repair a single day of Silver or
    Gold without replaying the layer above it, so every DAG has to offer it.
    """
    assert "match_date" in dags[dag_id].params


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_tasks_retry_but_the_quality_gate_does_not(dags, dag_id):
    """Transient failures retry; corrupt data does not get a second opinion."""
    for task_id in EXPECTED_TASK_IDS[dag_id] - {"data_quality_check"}:
        assert dags[dag_id].get_task(task_id).retries > 0, f"{task_id} has no retries"

    if dag_id == GOLD_DAG_ID:
        # Re-running the checks would re-read the same rows and fail
        # identically, just 15 minutes later and with the report buried under
        # two retries.
        assert dags[dag_id].get_task("data_quality_check").retries == 0


@pytest.mark.parametrize("dag_id", sorted(SPARK_TASK_IDS))
def test_spark_stages_process_the_resolved_date_not_today(dags, dag_id):
    """The date must come from the run, or a retry silently changes its meaning.

    A task that computes ``date.today()`` looks correct until it is retried
    after midnight, replayed, or re-run to repair a bad day — at which point it
    processes the wrong date and overwrites a good partition with an empty one.
    Downstream of Bronze the run has no date of its own at all, so it can only
    come from the triggering asset event, via ``resolve_match_date``.
    """
    task = dags[dag_id].get_task(SPARK_TASK_IDS[dag_id])
    assert task.application_args == ["--date", RESOLVED_MATCH_DATE]


@pytest.mark.parametrize("dag_id", sorted(SPARK_TASK_IDS))
def test_spark_stages_go_through_the_airflow_connection(dags, dag_id):
    """No hardcoded master URL and no hardcoded credentials (CLAUDE.md §7)."""
    task = dags[dag_id].get_task(SPARK_TASK_IDS[dag_id])
    assert task._conn_id == "spark_default"

    # Credentials reach the driver as Jinja over Connections, resolved when the
    # task runs. A literal here would mean a secret in the DAG file.
    for value in task.env_vars.values():
        assert value.strip().startswith("{{ conn."), f"{dag_id} passes a literal: {value!r}"


@pytest.mark.parametrize(
    "task_id",
    ["extract_football_data", "extract_football_data_teams", "extract_understat"],
)
def test_extractors_process_the_run_date_not_today(dags, task_id):
    task = dags[BRONZE_DAG_ID].get_task(task_id)
    assert task.op_kwargs == {"target_date": SCHEDULED_MATCH_DATE}


def _render_scheduled_date(dag, *, ds, override=None):
    """Render SCHEDULED_MATCH_DATE the way a task run would."""
    from airflow.sdk.execution_time import macros

    template = dag.get_template_env().from_string(SCHEDULED_MATCH_DATE)
    return template.render(params={"match_date": override}, ds=ds, macros=macros)


def test_bronze_processes_the_day_before_it_runs(dags):
    """The 06:00 run of day D must ask the sources for D-1, not for D.

    Airflow 3's cron timetable gives a run a zero-width data interval, so ``ds``
    is the day the run fires rather than the day it covers. Left uncorrected,
    breakfast on the 29th asks both sources about fixtures that kick off that
    evening: Football-Data answers ``TIMED`` and Understat, which publishes a
    match only once it has been played, answers with nothing. Neither is an
    error, so the day loads with every expected-goals measure null.

    Rendered rather than string-compared: the offset is the behaviour worth
    pinning, and a future rewrite of the template should be free to look
    different as long as the 06:00 run of the 29th still processes the 28th.
    """
    assert _render_scheduled_date(dags[BRONZE_DAG_ID], ds="2026-08-29") == "2026-08-28"


def test_an_explicit_match_date_is_used_exactly_as_given(dags):
    """A repair run aims at one day; the offset must not shift it too.

    ``--conf '{"match_date": …}'`` names the match date directly, so subtracting
    a day from it as well would silently repair the wrong day.
    """
    rendered = _render_scheduled_date(dags[BRONZE_DAG_ID], ds="2026-08-29", override="2025-11-01")
    assert rendered == "2025-11-01"


def test_availability_check_frees_its_slot_between_pokes(dags):
    """A poke-mode sensor holds an executor slot for its whole timeout.

    Under LocalExecutor that slot is one of the few the Spark driver will need
    later, so a sensor that waits half an hour in `poke` mode can deadlock the
    very chain it is gating.
    """
    sensor = dags[BRONZE_DAG_ID].get_task("check_if_api_is_available")
    assert sensor.mode == "reschedule"
