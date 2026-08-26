"""Which match date a run processes, and how that date travels between DAGs.

The pipeline's one rule about dates (CLAUDE.md §7) is that a task is *given* the
date it works on and never computes it, so that a retry after midnight, a manual
re-run and a repair of an old day all process the day they were meant to. Under
a single cron DAG that date was simply ``{{ ds }}``. Split across three
asset-linked DAGs it needs one more step, because **an asset-triggered run has
no logical date of its own** — Airflow 3 leaves ``logical_date`` unset when a DAG
is woken by an event, and the wall clock at that moment is not the match date
anyway (Bronze runs at 06:00 for the previous evening's fixtures).

So the date rides along with the handoff:

1. Bronze takes ``{{ ds }}`` — or the ``match_date`` param when a run is
   repairing a specific day — and stamps it on every asset event it emits
   (:func:`publish_match_date`).
2. Silver and Gold start with :func:`resolve_match_date`, which reads that stamp
   back off the events that triggered the run and pushes it to XCom.
3. Their Spark stages template it in as :data:`RESOLVED_MATCH_DATE`.

Each layer re-stamps the date on its own events, so the value is handed down the
chain rather than re-derived at each hop.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, task

from assets.football import MATCH_DATE_KEY

__all__ = [
    "MATCH_DATE_PARAMS",
    "RESOLVED_MATCH_DATE",
    "RESOLVE_TASK_ID",
    "SCHEDULED_MATCH_DATE",
    "as_match_date",
    "match_dates_from_events",
    "publish_match_date",
    "resolve_match_date",
    "resolve_match_date_value",
]

RESOLVE_TASK_ID = "resolve_match_date"

# What the asset-triggered DAGs hand to `spark-submit`. Jinja rather than the
# XComArg itself, because it also has to render inside strings (the Spark
# application name) and because a literal template is what the DAG tests can
# assert on.
RESOLVED_MATCH_DATE = f"{{{{ ti.xcom_pull(task_ids='{RESOLVE_TASK_ID}') }}}}"

# What the cron-scheduled DAG hands to its tasks: the run's own logical date,
# unless the run was triggered with an explicit date to repair.
SCHEDULED_MATCH_DATE = "{{ params.match_date or ds }}"

# Offered by all three DAGs so that any layer can be re-run for one specific day
# from the UI or the CLI: `--conf '{"match_date": "2025-11-01"}'`. Without it,
# an asset-triggered DAG could only ever be replayed by replaying the layer
# above it.
MATCH_DATE_PARAMS = {
    "match_date": Param(
        None,
        type=["null", "string"],
        title="Match date",
        description=(
            "ISO date (YYYY-MM-DD) to process instead of the date this run would "
            "otherwise take. Leave empty for the normal schedule."
        ),
    )
}


def as_match_date(value: str) -> date:
    """Parse a rendered match date, rejecting the two ways it arrives wrong.

    An override arrives as free text from a run's ``conf``, and ``{{ ds }}``
    renders empty on a run that has no logical date — which is every manually
    triggered run in Airflow 3 unless one is given. Both are caught here, in the
    first second of the task and with the offending value in the message. Passed
    through, they become a Spark job that dies on argument parsing several
    minutes later, or — worse — a day of empty partitions.
    """
    if not value or not value.strip():
        raise AirflowFailException(
            "This run has no match date: it was triggered without a logical date and "
            "without one in its conf. Re-trigger it with "
            '--conf \'{"match_date": "YYYY-MM-DD"}\'.'
        )
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise AirflowFailException(
            f"match_date must be an ISO date (YYYY-MM-DD), got {value!r}"
        ) from exc


def publish_match_date(
    outlet_events: Any,
    assets: Iterable[Any],
    target_date: str,
    **extra: Any,
) -> None:
    """Stamp ``target_date`` on the events this task is about to emit.

    ``outlet_events`` is the accessor Airflow puts in the task context; writing
    to it attaches metadata to the asset event that the task's success produces.
    Anything passed as ``extra`` rides along with it and shows up in the Assets
    view, which is the cheapest way to see what a night's run actually did.
    """
    for asset in assets:
        outlet_events[asset].extra = {MATCH_DATE_KEY: target_date, **extra}


def match_dates_from_events(triggering_asset_events: Mapping[Any, Iterable[Any]]) -> list[str]:
    """The distinct match dates carried by the events that triggered a run.

    Iterated rather than indexed by asset: a run is triggered by whichever
    events happened to be waiting, and asking for one by name would raise on the
    manual runs that have no events at all.
    """
    dates = {
        event.extra[MATCH_DATE_KEY]
        for asset in triggering_asset_events
        for event in triggering_asset_events[asset]
        if event.extra and MATCH_DATE_KEY in event.extra
    }
    return sorted(dates)


def resolve_match_date_value(
    override: str | None,
    triggering_asset_events: Mapping[Any, Iterable[Any]],
) -> str:
    """Decide the one date this run processes, or fail saying why it cannot.

    Both failure modes are deliberate dead ends rather than a guess:

    * **No date at all** — someone triggered the DAG by hand without one. The
      alternative would be to invent one from the clock, which is exactly the
      bug CLAUDE.md §7 exists to prevent.
    * **More than one date** — two upstream runs finished before the scheduler
      created this one, so their events were batched into a single run. Each
      stage loads exactly one date, so silently taking the newest would drop the
      other day on the floor. Re-running once per date recovers both, and the
      message says so.
    """
    if override:
        return as_match_date(override).isoformat()

    dates = match_dates_from_events(triggering_asset_events)
    if not dates:
        raise AirflowFailException(
            "This run carries no match date: it was not triggered by an upstream asset "
            'event. Trigger it with an explicit date — --conf \'{"match_date": "YYYY-MM-DD"}\'.'
        )
    if len(dates) > 1:
        raise AirflowFailException(
            f"Triggered by events for {len(dates)} different match dates ({', '.join(dates)}). "
            "One run loads one date; re-run this DAG once per date with "
            '--conf \'{"match_date": "YYYY-MM-DD"}\'.'
        )
    return as_match_date(dates[0]).isoformat()


@task
def resolve_match_date(*, params: Any, triggering_asset_events: Any) -> str:
    """Publish the run's match date to XCom, for the rest of the DAG to template in.

    Defined once and called inside both asset-triggered DAGs: the task id is the
    function name in each, which is what :data:`RESOLVED_MATCH_DATE` pulls from.
    """
    return resolve_match_date_value(params.get("match_date"), triggering_asset_events)
