"""Pure Bronze->Silver flatteners.

Bronze holds raw source payloads; Silver needs flat, typed rows. This module
does that conversion and **nothing else** — no Spark, no S3, no network — so the
logic that actually shapes the warehouse is unit-testable in CI, which installs
neither PySpark nor a JVM. ``bronze_to_silver`` supplies the I/O and the Spark
schemas around these functions.

Two source-shape facts drive everything here:

* **Understat's ``getMatchData`` is stringly-typed and dict-keyed.** ``rosters``
  and ``shots`` are ``{"h": ..., "a": ...}`` rather than arrays, ``rosters[side]``
  is further keyed by a dynamic roster id, and every scalar arrives as a string
  (``"goals": "0"``, ``"xG": "0.0289…"``). Schema inference is useless against
  that; we cast explicitly.
* **Understat's ``getLeagueData`` is not.** Its ``teams[].history`` entries are
  real JSON numbers. Same source, different endpoint, different types — hence
  the tolerant coercion helpers rather than assuming either.

Every parser is defensive: a record missing an id or carrying an unparseable
value is skipped with a warning rather than crashing a nightly run, because a
single malformed row must not cost us the whole day's load.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from src.common.logging import get_logger
from src.common.team_mapping import normalize_team_name

logger = get_logger(__name__)

_UNDERSTAT_DT_FORMAT = "%Y-%m-%d %H:%M:%S"

# Football-Data's score.winner -> the single-character result code used in Gold.
_WINNER_TO_RESULT = {"HOME_TEAM": "H", "AWAY_TEAM": "A", "DRAW": "D"}


def _to_int(value: Any) -> int | None:
    """Coerce to ``int``, returning ``None`` for absent/unparseable values."""
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    """Coerce to ``float``, returning ``None`` for absent/unparseable values."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_str(value: Any) -> str | None:
    """Trim to a non-empty string, or ``None``."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_understat_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value), _UNDERSTAT_DT_FORMAT)
    except (TypeError, ValueError):
        return None


def _parse_iso_utc(value: Any) -> datetime | None:
    """Parse Football-Data's ``2025-11-01T15:00:00Z`` timestamps (naive UTC)."""
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError):
        return None


def _pick_referee(referees: Any) -> dict[str, Any]:
    """Return the main referee from Football-Data's officials list.

    The list carries assistants and the VAR official too; we only model the
    match referee. Falls back to the first entry when no ``REFEREE`` type is
    present, and to empty values when the list is absent — free-tier payloads
    sometimes omit officials entirely.
    """
    if not isinstance(referees, list) or not referees:
        return {"referee_source_id": None, "referee_name": None, "referee_nationality": None}
    chosen = next((r for r in referees if r.get("type") == "REFEREE"), referees[0])
    return {
        "referee_source_id": _to_int(chosen.get("id")),
        "referee_name": _to_str(chosen.get("name")),
        "referee_nationality": _to_str(chosen.get("nationality")),
    }


def parse_football_data_matches(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten a Football-Data ``/matches`` payload into Silver match rows.

    One row per match. Scores stay ``None`` for matches that are not finished —
    that is a legitimate source state, not a data-quality failure, and Gold
    records it via ``status``.
    """
    rows: list[dict[str, Any]] = []
    for match in payload.get("matches", []):
        match_id = _to_int(match.get("id"))
        home = match.get("homeTeam") or {}
        away = match.get("awayTeam") or {}
        home_name = _to_str(home.get("name"))
        away_name = _to_str(away.get("name"))
        if match_id is None or not home_name or not away_name:
            logger.warning("Skipping Football-Data match without id/teams: %r", match.get("id"))
            continue

        kickoff = _parse_iso_utc(match.get("utcDate"))
        if kickoff is None:
            logger.warning(
                "Skipping match %s: unparseable utcDate %r", match_id, match.get("utcDate")
            )
            continue

        score = match.get("score") or {}
        full_time = score.get("fullTime") or {}
        half_time = score.get("halfTime") or {}
        competition = match.get("competition") or {}
        season = match.get("season") or {}

        rows.append(
            {
                "source_match_id": match_id,
                "competition_code": _to_str(competition.get("code")),
                "competition_name": _to_str(competition.get("name")),
                "season_id": _to_int(season.get("id")),
                "utc_kickoff": kickoff,
                "match_date": kickoff.date(),
                "matchday": _to_int(match.get("matchday")),
                "stage": _to_str(match.get("stage")),
                "status": _to_str(match.get("status")),
                "home_team": normalize_team_name(home_name),
                "home_team_raw": home_name,
                "home_team_source_id": _to_int(home.get("id")),
                "away_team": normalize_team_name(away_name),
                "away_team_raw": away_name,
                "away_team_source_id": _to_int(away.get("id")),
                "home_goals_ft": _to_int(full_time.get("home")),
                "away_goals_ft": _to_int(full_time.get("away")),
                "home_goals_ht": _to_int(half_time.get("home")),
                "away_goals_ht": _to_int(half_time.get("away")),
                "result": _WINNER_TO_RESULT.get(str(score.get("winner"))),
                **_pick_referee(match.get("referees")),
            }
        )
    return rows


def parse_understat_fixtures(
    league_data: dict[str, Any],
    league: str,
    season: str,
    target_date: date | None = None,
) -> list[dict[str, Any]]:
    """Flatten ``getLeagueData``'s ``dates`` into Silver fixture rows.

    ``dates`` is the best match-level xG source: unlike ``teams[].history`` it
    carries the Understat match id alongside both sides' goals and xG, so no
    (team, date) reconstruction is needed. Only played fixtures are returned;
    pass ``target_date`` to keep the daily load incremental.
    """
    rows: list[dict[str, Any]] = []
    for fixture in league_data.get("dates", []):
        if not fixture.get("isResult"):
            continue
        match_id = _to_int(fixture.get("id"))
        kickoff = _parse_understat_dt(fixture.get("datetime"))
        home = fixture.get("h") or {}
        away = fixture.get("a") or {}
        home_name = _to_str(home.get("title"))
        away_name = _to_str(away.get("title"))
        if match_id is None or kickoff is None or not home_name or not away_name:
            logger.warning(
                "Skipping Understat fixture %r: missing id/date/teams", fixture.get("id")
            )
            continue
        if target_date is not None and kickoff.date() != target_date:
            continue

        goals = fixture.get("goals") or {}
        xg = fixture.get("xG") or {}
        rows.append(
            {
                "understat_match_id": match_id,
                "league": league,
                "season": season,
                "utc_kickoff": kickoff,
                "match_date": kickoff.date(),
                "home_team": normalize_team_name(home_name),
                "home_team_raw": home_name,
                "home_team_understat_id": _to_int(home.get("id")),
                "away_team": normalize_team_name(away_name),
                "away_team_raw": away_name,
                "away_team_understat_id": _to_int(away.get("id")),
                "home_goals": _to_int(goals.get("h")),
                "away_goals": _to_int(goals.get("a")),
                "home_xg": _to_float(xg.get("h")),
                "away_xg": _to_float(xg.get("a")),
            }
        )
    return rows


def parse_understat_team_history(
    league_data: dict[str, Any],
    league: str,
    season: str,
    target_date: date | None = None,
) -> list[dict[str, Any]]:
    """Flatten ``teams[].history`` into per-team, per-match metric rows.

    Supplies the advanced measures ``dates`` lacks — npxG, PPDA, deep
    completions, expected points. History entries carry no match id, so the
    grain is (team, match_date, side); ``bronze_to_silver`` joins them onto
    fixtures on exactly that key.

    PPDA arrives as a ``{"att": …, "def": …}`` pair (passes allowed vs defensive
    actions); we keep both components rather than pre-dividing, so the ratio can
    be recomputed in Gold without inheriting a divide-by-zero here.
    """
    rows: list[dict[str, Any]] = []
    for team in (league_data.get("teams") or {}).values():
        team_name = _to_str(team.get("title"))
        team_id = _to_int(team.get("id"))
        if not team_name:
            logger.warning("Skipping Understat team entry without a title (id %r)", team.get("id"))
            continue
        canonical = normalize_team_name(team_name)

        for entry in team.get("history", []):
            kickoff = _parse_understat_dt(entry.get("date"))
            side = _to_str(entry.get("h_a"))
            if kickoff is None or side not in {"h", "a"}:
                logger.warning(
                    "Skipping %s history row: bad date/side %r", team_name, entry.get("date")
                )
                continue
            if target_date is not None and kickoff.date() != target_date:
                continue

            ppda = entry.get("ppda") or {}
            ppda_allowed = entry.get("ppda_allowed") or {}
            rows.append(
                {
                    "league": league,
                    "season": season,
                    "team": canonical,
                    "team_raw": team_name,
                    "team_understat_id": team_id,
                    "match_date": kickoff.date(),
                    "side": side,
                    "xg": _to_float(entry.get("xG")),
                    "xga": _to_float(entry.get("xGA")),
                    "npxg": _to_float(entry.get("npxG")),
                    "npxga": _to_float(entry.get("npxGA")),
                    "npxgd": _to_float(entry.get("npxGD")),
                    "ppda_att": _to_int(ppda.get("att")),
                    "ppda_def": _to_int(ppda.get("def")),
                    "ppda_allowed_att": _to_int(ppda_allowed.get("att")),
                    "ppda_allowed_def": _to_int(ppda_allowed.get("def")),
                    "deep": _to_int(entry.get("deep")),
                    "deep_allowed": _to_int(entry.get("deep_allowed")),
                    "scored": _to_int(entry.get("scored")),
                    "missed": _to_int(entry.get("missed")),
                    "xpts": _to_float(entry.get("xpts")),
                    "result": _to_str(entry.get("result")),
                }
            )
    return rows


def parse_understat_rosters(
    match_data: dict[str, Any], understat_match_id: int
) -> list[dict[str, Any]]:
    """Flatten ``getMatchData``'s ``rosters`` into per-player-per-match rows.

    ``rosters`` is ``{"h"|"a": {roster_id: record}}``; the records carry no match
    id, so the caller passes it (it is in the Bronze object key). This is the
    grain of ``fact_player_events``.
    """
    rows: list[dict[str, Any]] = []
    rosters = match_data.get("rosters") or {}
    for side, players in rosters.items():
        if side not in {"h", "a"} or not isinstance(players, dict):
            logger.warning(
                "Skipping unexpected roster side %r in match %s", side, understat_match_id
            )
            continue
        for roster_id, player in players.items():
            player_id = _to_int(player.get("player_id"))
            player_name = _to_str(player.get("player"))
            if player_id is None or not player_name:
                logger.warning(
                    "Skipping roster row %r in match %s: no player", roster_id, understat_match_id
                )
                continue
            rows.append(
                {
                    "understat_match_id": understat_match_id,
                    "roster_id": _to_int(roster_id),
                    "player_understat_id": player_id,
                    "player_name": player_name,
                    "team_understat_id": _to_int(player.get("team_id")),
                    "side": side,
                    "position": _to_str(player.get("position")),
                    "position_order": _to_int(player.get("positionOrder")),
                    "minutes": _to_int(player.get("time")),
                    "goals": _to_int(player.get("goals")),
                    "own_goals": _to_int(player.get("own_goals")),
                    "shots": _to_int(player.get("shots")),
                    "assists": _to_int(player.get("assists")),
                    "key_passes": _to_int(player.get("key_passes")),
                    "xg": _to_float(player.get("xG")),
                    "xa": _to_float(player.get("xA")),
                    "xg_chain": _to_float(player.get("xGChain")),
                    "xg_buildup": _to_float(player.get("xGBuildup")),
                    "yellow_cards": _to_int(player.get("yellow_card")),
                    "red_cards": _to_int(player.get("red_card")),
                }
            )
    return rows


def parse_understat_shots(match_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten ``getMatchData``'s ``shots`` into one row per shot event.

    Shot records already carry their own ``match_id``. ``X``/``Y`` are
    fractions of the pitch (0-1) from the shooting team's perspective and are
    kept as-is — the primary feature source for the downstream xG model.
    """
    rows: list[dict[str, Any]] = []
    shots = match_data.get("shots") or {}
    for side, events in shots.items():
        if side not in {"h", "a"} or not isinstance(events, list):
            logger.warning("Skipping unexpected shots side %r", side)
            continue
        for shot in events:
            shot_id = _to_int(shot.get("id"))
            match_id = _to_int(shot.get("match_id"))
            if shot_id is None or match_id is None:
                logger.warning("Skipping shot without id/match_id: %r", shot.get("id"))
                continue
            kickoff = _parse_understat_dt(shot.get("date"))
            rows.append(
                {
                    "shot_id": shot_id,
                    "understat_match_id": match_id,
                    "match_date": kickoff.date() if kickoff else None,
                    "season": _to_str(shot.get("season")),
                    "side": side,
                    "minute": _to_int(shot.get("minute")),
                    "player_understat_id": _to_int(shot.get("player_id")),
                    "player_name": _to_str(shot.get("player")),
                    "player_assisted": _to_str(shot.get("player_assisted")),
                    "result": _to_str(shot.get("result")),
                    "situation": _to_str(shot.get("situation")),
                    "shot_type": _to_str(shot.get("shotType")),
                    "last_action": _to_str(shot.get("lastAction")),
                    "x": _to_float(shot.get("X")),
                    "y": _to_float(shot.get("Y")),
                    "xg": _to_float(shot.get("xG")),
                }
            )
    return rows
