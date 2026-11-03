"""Extract xG / shot / player data from Understat into the Bronze layer.

Understat is our primary player-level & xG source (FBref is Cloudflare-blocked).
Since 2026 the data is served from clean JSON endpoints rather than embedded in
the page HTML:

* ``GET /getLeagueData/{league}/{season}`` -> ``teams`` (per-match xG history),
  ``players`` (season aggregates), ``dates`` (all fixtures + match ids).
* ``GET /getMatchData/{match_id}`` -> ``rosters`` (per-player-per-match) and
  ``shots`` (individual shot events).

Design notes:

* **Politeness** — a mandatory ``sleep`` between every request (scraping-adjacent,
  fragile). Never remove it.
* **Incremental** — we pull the league fixture list, pick only the match ids
  played on the target date, and fetch just those matches.
* **Resilience** — transient network errors retried with backoff via ``tenacity``.
* **Defensive parsing** — Understat's layout can change; the pure helpers tolerate
  missing keys and skip rather than crash.
* **Bronze is raw** — payloads are stored exactly as received; flattening/typing
  happens later in Silver.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import date, datetime
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.common.config import UnderstatConfig, understat_config
from src.common.logging import get_logger
from src.common.minio_client import BronzeWriter, bronze_key
from src.common.sources import UNDERSTAT as SOURCE
from src.common.sources import season_for_date

logger = get_logger(__name__)

__all__ = [
    "DEFAULT_LEAGUES",
    "SOURCE",
    "UnderstatClient",
    "UnderstatUnavailable",
    "extract_daily_understat",
    "match_ids_for_date",
    "season_for_date",
]

# Understat league slugs for the big-5 domestic leagues.
DEFAULT_LEAGUES: tuple[str, ...] = (
    "EPL",
    "La_liga",
    "Bundesliga",
    "Serie_A",
    "Ligue_1",
)

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
}


class UnderstatUnavailable(RuntimeError):
    """Understat answered, but not one league came back as data."""


def match_ids_for_date(league_data: dict[str, Any], target_date: date) -> list[str]:
    """Return played-match ids kicked off on ``target_date``.

    Reads the ``dates`` fixture list defensively: entries without an ``id``, an
    unparseable ``datetime``, or ``isResult`` false are skipped.
    """
    ids: list[str] = []
    for fixture in league_data.get("dates", []):
        if not fixture.get("isResult"):
            continue
        raw_dt = fixture.get("datetime")
        match_id = fixture.get("id")
        if not raw_dt or match_id is None:
            continue
        try:
            kicked_off = datetime.strptime(raw_dt, "%Y-%m-%d %H:%M:%S").date()
        except (ValueError, TypeError):
            logger.warning("Unparseable fixture datetime %r — skipping", raw_dt)
            continue
        if kicked_off == target_date:
            ids.append(str(match_id))
    return ids


class UnderstatClient:
    """Polite JSON client for Understat's data endpoints."""

    def __init__(
        self,
        config: UnderstatConfig | None = None,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._config = config or understat_config()
        self._session = session or requests.Session()
        self._session.headers.update(_BROWSER_HEADERS)
        self._sleep = sleep

    @retry(
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _get_json(self, path: str, referer: str | None = None) -> dict[str, Any]:
        # Politeness delay before every request.
        self._sleep(self._config.request_delay_seconds)
        headers = {"Referer": referer} if referer else {}
        response = self._session.get(f"{self._config.base_url}{path}", headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch_league_data(self, league: str, season: str) -> dict[str, Any]:
        return self._get_json(
            f"/getLeagueData/{league}/{season}",
            referer=f"{self._config.base_url}/league/{league}/{season}",
        )

    def fetch_match_data(self, match_id: str) -> dict[str, Any]:
        return self._get_json(
            f"/getMatchData/{match_id}",
            referer=f"{self._config.base_url}/match/{match_id}",
        )


def extract_daily_understat(
    target_date: date,
    leagues: tuple[str, ...] = DEFAULT_LEAGUES,
    client: UnderstatClient | None = None,
    writer: BronzeWriter | None = None,
) -> list[str]:
    """Extract league data + that day's match details into Bronze.

    For each league we store the full league-data payload once (it carries the
    fixture list and season aggregates), then fetch and store only the matches
    played on ``target_date``. Returns the Bronze URIs written. A single
    failing league/match is logged and skipped rather than aborting the run.

    Raises ``UnderstatUnavailable`` when no league at all could be read. A day with
    no fixtures still stores every league's payload, so nothing written means the
    source is down. Understat's maintenance page answers 200 with HTML, which no
    network error reports; returning normally would publish the Bronze asset and
    send Silver and Gold a night with no xG, players or shots.
    """
    client = client or UnderstatClient()
    writer = writer or BronzeWriter()
    writer.ensure_bucket()
    season = season_for_date(target_date)

    written: list[str] = []
    for league in leagues:
        try:
            league_data = client.fetch_league_data(league, season)
        except (requests.HTTPError, ValueError) as exc:
            logger.warning("Skipping league %s (%s): %s", league, season, exc)
            continue

        league_key = bronze_key(SOURCE, "league_data", target_date, f"{league}_{season}.json")
        written.append(writer.write_json(league_key, league_data))

        match_ids = match_ids_for_date(league_data, target_date)
        logger.info("%s: %d match(es) on %s", league, len(match_ids), target_date)
        for match_id in match_ids:
            try:
                match_data = client.fetch_match_data(match_id)
            except (requests.HTTPError, ValueError) as exc:
                logger.warning("Skipping match %s: %s", match_id, exc)
                continue
            match_key = bronze_key(SOURCE, "match_data", target_date, f"{match_id}.json")
            written.append(writer.write_json(match_key, match_data))

    if not written:
        raise UnderstatUnavailable(f"No league could be read from Understat for {target_date}")

    logger.info("Understat extraction wrote %d object(s) for %s", len(written), target_date)
    return written


if __name__ == "__main__":  # pragma: no cover - manual/CLI entry point
    from datetime import timedelta

    uris = extract_daily_understat(date.today() - timedelta(days=1))
    for uri in uris:
        logger.info("wrote %s", uri)
