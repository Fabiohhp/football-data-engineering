"""Bronze: one match date of raw JSON from both sources into MinIO.

The first of the three medallion DAGs (CLAUDE.md §3). It is the only one on a
clock — Silver and Gold are woken by the assets this one publishes — so it also
decides which date the whole chain processes:

==================================== ============================================
Task                                 Does
==================================== ============================================
``check_if_api_is_available``        polls Football-Data before we start
``extract_football_data``            results & standings -> Bronze
``extract_understat``                xG, rosters and shots -> Bronze
==================================== ============================================

Design notes:

* **The date comes from the run, never from ``date.today()``.** It is derived
  from the run's logical date, so a retry tomorrow, a manual re-run next week and
  a repair of an old day all process the day they were meant to. The extractors
  write to fixed, date-derived Bronze keys, so re-running a date is a correction
  rather than a duplication. A ``match_date`` param overrides it for exactly that
  repair case.
* **The run processes yesterday, not today.** Airflow 3's cron timetable makes
  ``ds`` the day the run fires, so the 06:00 run of day D would otherwise ask
  both sources for fixtures that kick off that evening — Football-Data returns
  them as ``TIMED`` and Understat, which publishes only played matches, returns
  nothing at all. :data:`SCHEDULED_MATCH_DATE` subtracts the day; see
  :mod:`football_run_date` for why the offset is not a fudge factor.
* **Scheduled at 06:00 UTC, not midnight.** Matches finish around 22:00 UTC and
  Understat needs a couple of hours to publish shot data, so 06:00 the following
  morning is comfortably clear of both — where a midnight cut would still be
  racing the sources for the evening that just ended.
* **Backfills are throttled by construction.** ``catchup=False`` plus
  ``max_active_runs=1`` means a stack that was switched off for a week does not
  wake up and fire seven concurrent runs at two rate-limited sources
  (CLAUDE.md §4). Missed days are re-run deliberately, one at a time.
* **Nothing is transformed here** (CLAUDE.md §10). The extractors persist each
  response exactly as received, which is what makes re-processing a changed
  Silver rule possible without going back to the sources.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import requests
from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task
from src.common.minio_client import BronzeWriter
from src.extract.extract_footballdata import (
    FootballDataClient,
    extract_competition_teams,
    extract_daily_matches,
)
from src.extract.extract_understat import extract_daily_understat

from assets.football import bronze_football_assets
from football_conf import DEFAULT_ARGS, FOOTBALL_DATA_POOL, START_DATE
from football_connections import (
    football_data_config_from_connection,
    minio_config_from_connection,
)
from football_run_date import (
    MATCH_DATE_PARAMS,
    SCHEDULED_MATCH_DATE,
    as_match_date,
    publish_match_date,
)


@dag(
    dag_id="bronze_football_matches",
    description="Bronze: raw Football-Data and Understat JSON for one match date into MinIO.",
    start_date=START_DATE,
    # An @daily cut at 06:00 UTC rather than midnight, so a run does not race
    # the sources for the previous evening's fixtures.
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    # Independent of AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: a reviewer's
    # first `docker compose up` should start scheduling, whatever the env says.
    is_paused_upon_creation=False,
    default_args=DEFAULT_ARGS,
    params=MATCH_DATE_PARAMS,
    doc_md=__doc__,
    tags=["football", "medallion", "bronze"],
)
def bronze_football_matches():
    @task.sensor(
        # `reschedule` frees the worker slot between pokes instead of holding it
        # for the whole timeout — with LocalExecutor those slots are the same
        # ones the Spark driver will need later in the chain. It frees the pool
        # slot too, so a poke waiting out an outage does not block the extracts.
        mode="reschedule",
        pool=FOOTBALL_DATA_POOL,
        poke_interval=timedelta(minutes=5),
        timeout=timedelta(minutes=30),
    )
    def check_if_api_is_available() -> bool:
        """Poke Football-Data with one cheap authenticated request.

        Returning ``False`` reschedules the poke; raising fails the run outright.
        The distinction is deliberate: a 5xx or a connection error is the API
        having a bad minute and is worth waiting out, while a 401/403 is a bad or
        expired token that no amount of waiting will fix — that should page a
        human on the first poke rather than after half an hour of silence.

        Costs one call from the 10-per-minute budget, and the client honours the
        throttle headers on the way through (CLAUDE.md §4).
        """
        client = FootballDataClient(config=football_data_config_from_connection())
        today = date.today()
        try:
            client.fetch_matches("PL", today, today)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (401, 403):
                raise AirflowFailException(
                    f"Football-Data rejected our token ({status}). "
                    "Check FOOTBALL_DATA_API_TOKEN in .env — retrying will not help."
                ) from exc
            return False
        except (requests.ConnectionError, requests.Timeout):
            return False
        return True

    @task(outlets=[bronze_football_assets["football_data"]], pool=FOOTBALL_DATA_POOL)
    def extract_football_data(target_date: str, *, outlet_events: Any) -> list[str]:
        """Football-Data results for one date into Bronze."""
        written = extract_daily_matches(
            target_date=as_match_date(target_date),
            client=FootballDataClient(config=football_data_config_from_connection()),
            writer=BronzeWriter(config=minio_config_from_connection()),
        )
        publish_match_date(
            outlet_events,
            [bronze_football_assets["football_data"]],
            target_date,
            objects=len(written),
        )
        return written

    @task(
        outlets=[bronze_football_assets["football_data_teams"]],
        pool=FOOTBALL_DATA_POOL,
    )
    def extract_football_data_teams(target_date: str, *, outlet_events: Any) -> list[str]:
        """Competition rosters — and their venues — for one date into Bronze.

        Five calls against the same rate-limited host as ``extract_football_data``
        but a separate task, so a roster outage cannot fail the night's results
        and a retry re-runs only the endpoint that broke.
        """
        written = extract_competition_teams(
            target_date=as_match_date(target_date),
            client=FootballDataClient(config=football_data_config_from_connection()),
            writer=BronzeWriter(config=minio_config_from_connection()),
        )
        publish_match_date(
            outlet_events,
            [bronze_football_assets["football_data_teams"]],
            target_date,
            objects=len(written),
        )
        return written

    @task(
        outlets=[bronze_football_assets["understat"]],
        # ~3s of mandated politeness delay per request, and a matchday can be 10
        # fixtures across five leagues.
        execution_timeout=timedelta(minutes=30),
    )
    def extract_understat(target_date: str, *, outlet_events: Any) -> list[str]:
        """Understat xG, rosters and shots for one date into Bronze.

        No client is injected: Understat needs no credentials, and its politeness
        delay belongs to the extractor's own config rather than to a Connection.
        """
        written = extract_daily_understat(
            target_date=as_match_date(target_date),
            writer=BronzeWriter(config=minio_config_from_connection()),
        )
        publish_match_date(
            outlet_events,
            [bronze_football_assets["understat"]],
            target_date,
            objects=len(written),
        )
        return written

    # Parallel rather than merged into one task: either can fail alone, and a
    # retry should re-run only the source that actually failed. Silver waits for
    # every asset, so a half-landed Bronze does not wake it.
    #
    # "Parallel" is now bounded, though: the two Football-Data tasks share the
    # single-slot FOOTBALL_DATA_POOL and so take turns, while Understat is a
    # different host under a different limit and runs alongside them.
    check_if_api_is_available() >> [
        extract_football_data(target_date=SCHEDULED_MATCH_DATE),
        extract_football_data_teams(target_date=SCHEDULED_MATCH_DATE),
        extract_understat(target_date=SCHEDULED_MATCH_DATE),
    ]


bronze_football_matches_dag = bronze_football_matches()
