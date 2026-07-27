"""Tests for Football-Data throttling and the daily extraction flow."""

from __future__ import annotations

from datetime import date

from src.common.config import FootballDataConfig
from src.extract.extract_footballdata import (
    FootballDataClient,
    extract_daily_matches,
    parse_throttle_headers,
)


def test_parse_throttle_headers_reads_budget() -> None:
    state = parse_throttle_headers(
        {"x-requests-available-minute": "8", "X-RequestCounter-Reset": "59"}
    )
    assert state.available == 8
    assert state.reset_seconds == 59


def test_parse_throttle_headers_defaults_when_missing() -> None:
    state = parse_throttle_headers({})
    assert state.available is None
    assert state.reset_seconds == 60


def test_parse_throttle_headers_tolerates_garbage() -> None:
    state = parse_throttle_headers(
        {"x-requests-available-minute": "nope", "X-RequestCounter-Reset": "x"}
    )
    assert state.available is None
    assert state.reset_seconds == 60


class _FakeResponse:
    def __init__(self, status_code: int, headers: dict, payload: dict) -> None:
        self.status_code = status_code
        self.headers = headers
        self._payload = payload

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"status {self.status_code}")


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = responses
        self.headers: dict = {}
        self.calls: list[dict] = []

    def get(self, url: str, params=None, timeout=None, headers=None) -> _FakeResponse:
        self.calls.append({"url": url, "params": params})
        return self._responses.pop(0)


def _client(responses: list[_FakeResponse]) -> tuple[FootballDataClient, list[float]]:
    slept: list[float] = []
    session = _FakeSession(responses)
    client = FootballDataClient(
        config=FootballDataConfig(api_token="t"),
        session=session,
        sleep=slept.append,
    )
    return client, slept


def test_fetch_matches_returns_payload_without_sleeping_when_budget_healthy() -> None:
    resp = _FakeResponse(
        200, {"x-requests-available-minute": "9", "X-RequestCounter-Reset": "60"}, {"ok": 1}
    )
    client, slept = _client([resp])
    assert client.fetch_matches("PL", date(2026, 6, 18), date(2026, 6, 18)) == {"ok": 1}
    assert slept == []  # budget healthy -> no proactive wait


def test_get_backs_off_on_429_then_retries() -> None:
    responses = [
        _FakeResponse(429, {"X-RequestCounter-Reset": "5"}, {}),
        _FakeResponse(200, {"x-requests-available-minute": "9"}, {"ok": 1}),
    ]
    client, slept = _client(responses)
    assert client.fetch_matches("PL", date(2026, 6, 18), date(2026, 6, 18)) == {"ok": 1}
    assert slept == [6]  # reset + 1


def test_get_waits_when_budget_exhausted() -> None:
    resp = _FakeResponse(
        200, {"x-requests-available-minute": "0", "X-RequestCounter-Reset": "30"}, {"ok": 1}
    )
    client, slept = _client([resp])
    client.fetch_matches("PL", date(2026, 6, 18), date(2026, 6, 18))
    assert slept == [31]  # drained budget -> proactive sleep


def test_default_competitions_pair_with_understat_leagues() -> None:
    # Both extractors must cover exactly the same five leagues: a match ingested
    # from one source with no counterpart in the other lands in Gold with no xG
    # (or no result). Guards against the two default lists drifting apart.
    from src.extract.extract_footballdata import DEFAULT_COMPETITIONS
    from src.extract.extract_understat import DEFAULT_LEAGUES

    pairing = {
        "PL": "EPL",
        "PD": "La_liga",
        "BL1": "Bundesliga",
        "SA": "Serie_A",
        "FL1": "Ligue_1",
    }
    assert set(DEFAULT_COMPETITIONS) == set(pairing)
    assert set(DEFAULT_LEAGUES) == set(pairing.values())


class _StubClient:
    """Returns canned payloads keyed by competition; records calls."""

    def __init__(self, payloads: dict[str, dict]) -> None:
        self._payloads = payloads
        self.fetched: list[str] = []

    def fetch_matches(self, competition: str, date_from, date_to) -> dict:
        self.fetched.append(competition)
        return self._payloads[competition]


class _RecordingWriter:
    def __init__(self) -> None:
        self.written: list[tuple[str, dict]] = []

    def ensure_bucket(self) -> None:  # noqa: D401 - no-op for tests
        pass

    def write_json(self, key: str, payload: dict) -> str:
        self.written.append((key, payload))
        return f"s3://bronze/{key}"


def test_extract_daily_matches_writes_only_competitions_with_matches() -> None:
    target = date(2026, 6, 18)
    payloads = {
        "PL": {"resultSet": {"count": 2}, "matches": [{"id": 1}, {"id": 2}]},
        "SA": {"resultSet": {"count": 0}, "matches": []},
    }
    client = _StubClient(payloads)
    writer = _RecordingWriter()

    uris = extract_daily_matches(
        target_date=target, competitions=("PL", "SA"), client=client, writer=writer
    )

    assert client.fetched == ["PL", "SA"]  # both queried (incremental window)
    assert len(writer.written) == 1  # only PL had matches
    key, payload = writer.written[0]
    assert key == "football_data/matches/year=2026/month=06/day=18/PL.json"
    assert payload == payloads["PL"]
    assert uris == ["s3://bronze/football_data/matches/year=2026/month=06/day=18/PL.json"]
