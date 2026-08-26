"""Extract match results from Football-Data.org into the Bronze layer.

Football-Data is our reliable results & fixtures backbone (no player stats/xG).
Design notes:

* **Incremental** — we request only a date window (default: yesterday) per
  competition, never the full season.
* **Header-aware throttling** — Football-Data asks clients to read
  ``x-requests-available-minute`` / ``X-RequestCounter-Reset`` rather than
  blind-sleeping. We wait exactly the reset window when the budget hits 0, and
  treat a 429 the same way. See :func:`parse_throttle_headers`.
* **Resilience** — transient network errors are retried with backoff via
  ``tenacity``.
* **Idempotency** — output is written to a fixed Bronze key per (competition,
  date), so re-running a day overwrites rather than duplicates.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.common.config import FootballDataConfig, football_data_config
from src.common.logging import get_logger
from src.common.minio_client import BronzeWriter, bronze_key
from src.common.sources import FOOTBALL_DATA as SOURCE
from src.common.sources import season_for_date

logger = get_logger(__name__)

# The big-5 domestic leagues, and only those. The free tier also grants the
# Championship, Eredivisie, Primeira Liga, Brasileirão and the cup competitions,
# but Understat covers the big-5 exclusively — matches from any other
# competition would land in Gold with no xG and no joinable player events. We
# ingest exactly the leagues both sources agree on. Pairs 1:1 with
# ``extract_understat.DEFAULT_LEAGUES``. Overridable per call.
DEFAULT_COMPETITIONS: tuple[str, ...] = (
    "PL",  # Premier League      <-> Understat EPL
    "PD",  # Primera Division    <-> Understat La_liga
    "BL1",  # Bundesliga          <-> Understat Bundesliga
    "SA",  # Serie A             <-> Understat Serie_A
    "FL1",  # Ligue 1             <-> Understat Ligue_1
)


@dataclass(frozen=True)
class ThrottleState:
    """Parsed rate-limit budget from a Football-Data response."""

    available: int | None
    reset_seconds: int


def parse_throttle_headers(headers: Mapping[str, str]) -> ThrottleState:
    """Extract the remaining request budget from response headers.

    ``x-requests-available-minute`` is requests left in the current window;
    ``X-RequestCounter-Reset`` is seconds until it resets. Missing/garbage
    values degrade to ``available=None`` and a conservative 60s reset.
    """
    raw_available = headers.get("x-requests-available-minute")
    try:
        available = int(raw_available) if raw_available is not None else None
    except (TypeError, ValueError):
        available = None

    raw_reset = headers.get("X-RequestCounter-Reset")
    try:
        reset_seconds = int(raw_reset) if raw_reset is not None else 60
    except (TypeError, ValueError):
        reset_seconds = 60
    return ThrottleState(available=available, reset_seconds=reset_seconds)


class FootballDataClient:
    """Throttling-aware client for the Football-Data.org REST API."""

    def __init__(
        self,
        config: FootballDataConfig | None = None,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._config = config or football_data_config()
        self._session = session or requests.Session()
        self._session.headers.update({"X-Auth-Token": self._config.api_token})
        self._sleep = sleep

    @retry(
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _get(self, path: str, params: Mapping[str, str] | None = None) -> dict[str, Any]:
        """GET ``path`` honoring the throttle budget; retries transient errors."""
        while True:
            response = self._session.get(
                f"{self._config.base_url}{path}", params=params, timeout=30
            )
            throttle = parse_throttle_headers(response.headers)

            if response.status_code == 429:
                wait = throttle.reset_seconds + 1
                logger.warning("429 from Football-Data — backing off %ss", wait)
                self._sleep(wait)
                continue

            response.raise_for_status()

            # Proactively wait if we've drained the budget, so the next call
            # can't trip the limiter.
            if throttle.available is not None and throttle.available <= 0:
                wait = throttle.reset_seconds + 1
                logger.info("Request budget exhausted — sleeping %ss", wait)
                self._sleep(wait)
            else:
                logger.debug("%s requests left this minute", throttle.available)
            return response.json()

    def fetch_matches(self, competition: str, date_from: date, date_to: date) -> dict[str, Any]:
        """Fetch matches for one competition within an inclusive date window."""
        return self._get(
            f"/competitions/{competition}/matches",
            params={"dateFrom": date_from.isoformat(), "dateTo": date_to.isoformat()},
        )

    def fetch_teams(self, competition: str, season: str | None = None) -> dict[str, Any]:
        """Fetch a competition's roster, and with it every club's home ground.

        This is the pipeline's only source of venue data (``team.venue``), which
        is what populates ``gold.dim_stadiums``.

        ``season`` is the starting year ("2025" means 2025/26). Passing it
        matters for anything but today: the endpoint defaults to the *current*
        season, so backfilling an old date without it would resolve venues
        against a roster that has since been promoted and relegated, and quietly
        attach the wrong ground — or none — to a historical fixture.
        """
        params = {"season": season} if season else None
        return self._get(f"/competitions/{competition}/teams", params=params)


def extract_daily_matches(
    target_date: date | None = None,
    competitions: tuple[str, ...] = DEFAULT_COMPETITIONS,
    client: FootballDataClient | None = None,
    writer: BronzeWriter | None = None,
) -> list[str]:
    """Extract one day's matches per competition into Bronze.

    ``target_date`` defaults to yesterday (matches appear a few hours after
    kickoff, so "today" is usually incomplete). Returns the list of Bronze URIs
    written. Competitions with no matches that day are skipped (nothing to store)
    but do not fail the run.
    """
    target = target_date or (date.today() - timedelta(days=1))
    client = client or FootballDataClient()
    writer = writer or BronzeWriter()
    writer.ensure_bucket()

    written: list[str] = []
    for competition in competitions:
        try:
            payload = client.fetch_matches(competition, target, target)
        except requests.HTTPError as exc:
            # A single competition failing (e.g. off-season 4xx) shouldn't abort
            # the others; log and continue.
            logger.warning("Skipping %s for %s: %s", competition, target, exc)
            continue

        count = payload.get("resultSet", {}).get("count", 0)
        if not count:
            logger.info("No matches for %s on %s — skipping write", competition, target)
            continue

        key = bronze_key(SOURCE, "matches", target, f"{competition}.json")
        written.append(writer.write_json(key, payload))

    logger.info("Football-Data extraction wrote %d object(s) for %s", len(written), target)
    return written


def extract_competition_teams(
    target_date: date | None = None,
    competitions: tuple[str, ...] = DEFAULT_COMPETITIONS,
    client: FootballDataClient | None = None,
    writer: BronzeWriter | None = None,
) -> list[str]:
    """Extract each competition's team roster (and their venues) into Bronze.

    The roster is fetched for the season ``target_date`` falls in, not the
    current one, so a backfill resolves venues against the clubs that actually
    played that season.

    Rosters are reference data, not events: they change at most a few times a
    season. We still re-read them every run rather than once, for two reasons —
    it costs five of the 600 calls a day the free tier allows, and it keeps the
    layer honest, because a promoted club or a renamed ground shows up in Gold
    the next morning instead of whenever someone remembers to refresh it. The
    write is idempotent (fixed key per competition and date), so a re-run
    overwrites.

    Unlike :func:`extract_daily_matches`, an empty response is still written: a
    competition always has a roster, so "no teams" means the source changed and
    the raw payload is exactly what we want to keep for diagnosis.
    """
    target = target_date or (date.today() - timedelta(days=1))
    season = season_for_date(target)
    client = client or FootballDataClient()
    writer = writer or BronzeWriter()
    writer.ensure_bucket()

    written: list[str] = []
    for competition in competitions:
        try:
            payload = client.fetch_teams(competition, season=season)
        except requests.HTTPError as exc:
            # Same rule as the match extract: one competition must not abort the
            # others. A missing roster only costs us that league's venues.
            logger.warning("Skipping teams for %s: %s", competition, exc)
            continue

        key = bronze_key(SOURCE, "teams", target, f"{competition}.json")
        written.append(writer.write_json(key, payload))

    logger.info("Football-Data teams extraction wrote %d object(s) for %s", len(written), target)
    return written


if __name__ == "__main__":  # pragma: no cover - manual/CLI entry point
    uris = extract_daily_matches() + extract_competition_teams()
    for uri in uris:
        logger.info("wrote %s", uri)
