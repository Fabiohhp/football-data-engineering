"""The match date's round trip between DAGs: stamped on an event, read back off it.

This is the one piece of state the three DAGs share. If it breaks, nothing fails
at parse time and nothing looks wrong in the UI — Silver and Gold simply process
some other day, or the day they were meant to process is quietly skipped. So the
publish and resolve halves are tested against each other here, without Airflow
running: both sides only touch the accessors' mapping behaviour, which a plain
dict reproduces exactly.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

pytest.importorskip(
    "airflow.sdk",
    reason="Airflow is installed from requirements-airflow.txt (CI and the Airflow image only)",
)

from airflow.exceptions import AirflowFailException  # noqa: E402

from assets.football import MATCH_DATE_KEY, bronze_football_assets  # noqa: E402
from football_run_date import (  # noqa: E402
    as_match_date,
    match_dates_from_events,
    publish_match_date,
    resolve_match_date_value,
)

FOOTBALL_DATA = bronze_football_assets["football_data"]
UNDERSTAT = bronze_football_assets["understat"]


class FakeOutletEvents:
    """Stand-in for Airflow's outlet event accessor: one mutable entry per asset.

    Keyed by name rather than by the ``Asset`` itself, as the real accessor is:
    an ``Asset`` carries a mutable ``extra`` dict and is therefore not hashable.
    """

    def __init__(self) -> None:
        self.entries: dict = {}

    def __getitem__(self, asset):
        return self.entries.setdefault(asset.name, SimpleNamespace(extra=None))


def triggering(**events: dict | None) -> dict:
    """Build a triggering-events mapping: one asset per keyword, one event each."""
    return {asset: [SimpleNamespace(extra=extra)] for asset, extra in events.items()}


def test_a_published_date_is_the_date_that_resolves():
    """The round trip, which is the only property that actually matters."""
    outlet_events = FakeOutletEvents()
    publish_match_date(outlet_events, [FOOTBALL_DATA, UNDERSTAT], "2025-11-01")

    events = {asset: [entry] for asset, entry in outlet_events.entries.items()}
    assert resolve_match_date_value(None, events) == "2025-11-01"


def test_publish_carries_extra_metadata_alongside_the_date():
    outlet_events = FakeOutletEvents()
    publish_match_date(outlet_events, [FOOTBALL_DATA], "2025-11-01", objects=12)

    assert outlet_events[FOOTBALL_DATA].extra == {MATCH_DATE_KEY: "2025-11-01", "objects": 12}


def test_both_sources_agreeing_is_one_date_not_two():
    events = triggering(
        a={MATCH_DATE_KEY: "2025-11-01"},
        b={MATCH_DATE_KEY: "2025-11-01"},
    )
    assert match_dates_from_events(events) == ["2025-11-01"]


def test_events_without_a_date_are_ignored():
    """An event can be stamped by hand from the UI or the API, with no extra."""
    events = triggering(a=None, b={}, c={MATCH_DATE_KEY: "2025-11-01"})
    assert match_dates_from_events(events) == ["2025-11-01"]


def test_an_explicit_override_wins_over_the_events():
    """Replaying one day of Silver must not be overruled by whatever triggered it."""
    events = triggering(a={MATCH_DATE_KEY: "2025-11-01"})
    assert resolve_match_date_value(" 2025-10-05 ", events) == "2025-10-05"


def test_a_run_with_no_date_at_all_fails_loudly():
    """Manually triggered, no events, no conf: inventing a date is the bug."""
    with pytest.raises(AirflowFailException, match="match_date"):
        resolve_match_date_value(None, {})


def test_events_for_two_different_dates_fail_loudly():
    """Two upstream runs batched into one; taking the newest would drop a day."""
    events = triggering(
        a={MATCH_DATE_KEY: "2025-11-01"},
        b={MATCH_DATE_KEY: "2025-11-02"},
    )
    with pytest.raises(AirflowFailException) as excinfo:
        resolve_match_date_value(None, events)

    # The message has to name both dates: recovering means re-running each.
    assert "2025-11-01" in str(excinfo.value)
    assert "2025-11-02" in str(excinfo.value)


def test_bronze_fails_clearly_when_ds_rendered_empty():
    """`{{ params.match_date or ds }}` renders empty on a run with no logical date.

    Every manually triggered Airflow 3 run is one unless a date is given, and
    `date.fromisoformat("")` would fail with a message that says nothing about
    how to fix it.
    """
    with pytest.raises(AirflowFailException, match="match_date"):
        as_match_date("")


def test_a_rendered_date_becomes_a_date():
    assert as_match_date(" 2025-11-01 ") == date(2025, 11, 1)


@pytest.mark.parametrize("override", ["yesterday", "01/11/2025", "2025-13-01", ""])
def test_a_malformed_override_is_rejected_before_it_reaches_spark(override):
    events = triggering(a={MATCH_DATE_KEY: "2025-11-01"})
    if override == "":
        # Empty conf means "no override", not a broken one — the events decide.
        assert resolve_match_date_value(override, events) == "2025-11-01"
        return

    with pytest.raises(AirflowFailException, match="ISO date"):
        resolve_match_date_value(override, events)
