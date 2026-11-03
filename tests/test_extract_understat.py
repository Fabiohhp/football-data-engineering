"""Tests for Understat season logic, incremental match selection, and flow."""

from __future__ import annotations

from datetime import date

import pytest
import requests
from src.common.config import UnderstatConfig
from src.extract.extract_understat import (
    UnderstatClient,
    UnderstatUnavailable,
    extract_daily_understat,
    match_ids_for_date,
    season_for_date,
)


def test_season_for_date_second_half_of_year_is_current_year() -> None:
    assert season_for_date(date(2026, 8, 15)) == "2026"
    assert season_for_date(date(2026, 12, 31)) == "2026"


def test_season_for_date_first_half_of_year_is_previous_year() -> None:
    assert season_for_date(date(2026, 5, 10)) == "2025"
    assert season_for_date(date(2026, 1, 1)) == "2025"


_LEAGUE_DATA = {
    "dates": [
        {"id": 100, "isResult": True, "datetime": "2026-06-18 19:00:00"},
        {"id": 101, "isResult": True, "datetime": "2026-06-18 21:00:00"},
        {"id": 102, "isResult": True, "datetime": "2026-06-17 19:00:00"},
        {"id": 103, "isResult": False, "datetime": "2026-06-18 19:00:00"},  # not played
        {"id": None, "isResult": True, "datetime": "2026-06-18 19:00:00"},  # no id
        {"id": 104, "isResult": True, "datetime": "garbage"},  # unparseable
    ]
}


def test_match_ids_for_date_selects_only_played_matches_on_that_day() -> None:
    ids = match_ids_for_date(_LEAGUE_DATA, date(2026, 6, 18))
    assert ids == ["100", "101"]  # ids are stringified; other rows filtered out


def test_match_ids_for_date_empty_when_no_matches() -> None:
    assert match_ids_for_date(_LEAGUE_DATA, date(2026, 6, 1)) == []


def test_match_ids_for_date_handles_missing_dates_key() -> None:
    assert match_ids_for_date({}, date(2026, 6, 18)) == []


class _StubClient:
    def __init__(self, league_data: dict, match_data: dict) -> None:
        self._league_data = league_data
        self._match_data = match_data
        self.league_calls: list[tuple[str, str]] = []
        self.match_calls: list[str] = []

    def fetch_league_data(self, league: str, season: str) -> dict:
        self.league_calls.append((league, season))
        return self._league_data

    def fetch_match_data(self, match_id: str) -> dict:
        self.match_calls.append(match_id)
        return self._match_data


class _RecordingWriter:
    def __init__(self) -> None:
        self.written: list[tuple[str, dict]] = []

    def ensure_bucket(self) -> None:
        pass

    def write_json(self, key: str, payload: dict) -> str:
        self.written.append((key, payload))
        return f"s3://bronze/{key}"


def test_extract_daily_understat_writes_league_and_each_match() -> None:
    target = date(2026, 6, 18)
    client = _StubClient(_LEAGUE_DATA, {"shots": {"h": [], "a": []}})
    writer = _RecordingWriter()

    uris = extract_daily_understat(
        target_date=target, leagues=("EPL",), client=client, writer=writer
    )

    assert client.league_calls == [("EPL", "2025")]  # June -> 2025 season
    assert client.match_calls == ["100", "101"]  # only that day's played matches
    keys = [k for k, _ in writer.written]
    assert keys == [
        "understat/league_data/year=2026/month=06/day=18/EPL_2025.json",
        "understat/match_data/year=2026/month=06/day=18/100.json",
        "understat/match_data/year=2026/month=06/day=18/101.json",
    ]
    assert len(uris) == 3


class _MaintenanceSession(requests.Session):
    """Answers every request the way understat.com did in November 2026."""

    def get(self, url: str, **kwargs) -> requests.Response:
        response = requests.Response()
        response.status_code = 200
        response.url = url
        response._content = b"<html><body>We will come back soon</body></html>"
        return response


def test_extract_daily_understat_fails_on_a_maintenance_page() -> None:
    client = UnderstatClient(
        config=UnderstatConfig(),
        session=_MaintenanceSession(),
        sleep=lambda _: None,
    )
    writer = _RecordingWriter()

    with pytest.raises(UnderstatUnavailable, match="2025-11-01"):
        extract_daily_understat(target_date=date(2025, 11, 1), client=client, writer=writer)

    assert writer.written == []


def test_extract_daily_understat_accepts_a_day_with_no_matches() -> None:
    client = _StubClient({"dates": []}, {})
    writer = _RecordingWriter()

    uris = extract_daily_understat(
        target_date=date(2026, 6, 18), leagues=("EPL",), client=client, writer=writer
    )

    assert len(uris) == 1
    assert client.match_calls == []


def test_extract_daily_understat_skips_one_failing_league() -> None:
    class _OneLeagueDown(_StubClient):
        def fetch_league_data(self, league: str, season: str) -> dict:
            if league == "EPL":
                raise ValueError("not JSON")
            return super().fetch_league_data(league, season)

    writer = _RecordingWriter()

    uris = extract_daily_understat(
        target_date=date(2026, 6, 18),
        leagues=("EPL", "Serie_A"),
        client=_OneLeagueDown({"dates": []}, {}),
        writer=writer,
    )

    assert [k for k, _ in writer.written] == [
        "understat/league_data/year=2026/month=06/day=18/Serie_A_2025.json"
    ]
    assert len(uris) == 1
