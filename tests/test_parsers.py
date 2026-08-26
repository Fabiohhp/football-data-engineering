"""Tests for the pure Bronze->Silver flatteners.

Fixtures are trimmed copies of real Bronze payloads from 2025-11-01 — including
Understat's stringly-typed values and dict-keyed collections — so these tests
fail if a source changes shape rather than passing against an idealized stub.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
from src.common.team_mapping import normalize_team_name
from src.transform.parsers import (
    parse_football_data_matches,
    parse_football_data_teams,
    parse_understat_fixtures,
    parse_understat_rosters,
    parse_understat_shots,
    parse_understat_team_history,
)

FOOTBALL_DATA_PAYLOAD = {
    "resultSet": {"count": 2},
    "matches": [
        {
            "id": 537876,
            "utcDate": "2025-11-01T15:00:00Z",
            "status": "FINISHED",
            "matchday": 10,
            "stage": "REGULAR_SEASON",
            "competition": {"id": 2021, "name": "Premier League", "code": "PL"},
            "season": {"id": 2403, "startDate": "2025-08-15"},
            "homeTeam": {"id": 397, "name": "Brighton & Hove Albion FC"},
            "awayTeam": {"id": 341, "name": "Leeds United FC"},
            "score": {
                "winner": "HOME_TEAM",
                "fullTime": {"home": 3, "away": 0},
                "halfTime": {"home": 1, "away": 0},
            },
            "referees": [
                {"id": 55, "name": "Some Assistant", "type": "ASSISTANT_REFEREE_N1"},
                {
                    "id": 11605,
                    "name": "Michael Oliver",
                    "type": "REFEREE",
                    "nationality": "England",
                },
            ],
        },
        {
            "id": 537877,
            "utcDate": "2025-11-01T17:30:00Z",
            "status": "TIMED",
            "matchday": 10,
            "competition": {"id": 2021, "name": "Premier League", "code": "PL"},
            "season": {"id": 2403},
            "homeTeam": {"id": 65, "name": "Manchester City FC"},
            "awayTeam": {"id": 61, "name": "Chelsea FC"},
            "score": {"winner": None, "fullTime": {"home": None, "away": None}, "halfTime": {}},
            "referees": [],
        },
    ],
}


def test_parse_football_data_matches_flattens_and_canonicalizes() -> None:
    rows = parse_football_data_matches(FOOTBALL_DATA_PAYLOAD)
    assert len(rows) == 2
    first = rows[0]
    assert first["source_match_id"] == 537876
    assert first["home_team"] == "Brighton & Hove Albion"  # canonical, not the raw "… FC"
    assert first["home_team_raw"] == "Brighton & Hove Albion FC"
    assert first["away_team"] == "Leeds United"
    assert first["utc_kickoff"] == datetime(2025, 11, 1, 15, 0, 0)
    assert first["match_date"] == date(2025, 11, 1)
    assert (first["home_goals_ft"], first["away_goals_ft"]) == (3, 0)
    assert (first["home_goals_ht"], first["away_goals_ht"]) == (1, 0)
    assert first["result"] == "H"
    assert first["competition_code"] == "PL"


def test_parse_football_data_picks_the_referee_not_an_assistant() -> None:
    first = parse_football_data_matches(FOOTBALL_DATA_PAYLOAD)[0]
    assert first["referee_name"] == "Michael Oliver"
    assert first["referee_source_id"] == 11605
    assert first["referee_nationality"] == "England"


def test_parse_football_data_unplayed_match_keeps_null_scores() -> None:
    # An unfinished match is a legitimate source state, not a parse failure:
    # keep the row, leave the measures null, let Gold key off `status`.
    second = parse_football_data_matches(FOOTBALL_DATA_PAYLOAD)[1]
    assert second["status"] == "TIMED"
    assert second["home_goals_ft"] is None
    assert second["away_goals_ft"] is None
    assert second["result"] is None
    assert second["referee_name"] is None


@pytest.mark.parametrize(
    ("winner", "expected"),
    [("HOME_TEAM", "H"), ("AWAY_TEAM", "A"), ("DRAW", "D"), (None, None), ("WEIRD", None)],
)
def test_football_data_winner_maps_to_result_code(winner: str | None, expected: str | None) -> None:
    payload = {
        "matches": [
            {
                "id": 1,
                "utcDate": "2025-11-01T15:00:00Z",
                "homeTeam": {"id": 1, "name": "Arsenal FC"},
                "awayTeam": {"id": 2, "name": "Chelsea FC"},
                "score": {"winner": winner, "fullTime": {}, "halfTime": {}},
            }
        ]
    }
    assert parse_football_data_matches(payload)[0]["result"] == expected


def test_parse_football_data_skips_unusable_records() -> None:
    payload = {
        "matches": [
            {"id": None, "utcDate": "2025-11-01T15:00:00Z", "homeTeam": {}, "awayTeam": {}},
            {
                "id": 2,
                "utcDate": "not-a-date",
                "homeTeam": {"name": "A FC"},
                "awayTeam": {"name": "B FC"},
            },
        ]
    }
    assert parse_football_data_matches(payload) == []


LEAGUE_DATA = {
    "dates": [
        {
            "id": "28868",
            "isResult": True,
            "h": {"id": "220", "title": "Brighton"},
            "a": {"id": "245", "title": "Leeds"},
            "goals": {"h": "3", "a": "0"},
            "xG": {"h": "2.64127", "a": "0.35919"},
            "datetime": "2025-11-01 15:00:00",
        },
        {
            "id": "28900",
            "isResult": False,  # not yet played -> excluded
            "h": {"id": "220", "title": "Brighton"},
            "a": {"id": "87", "title": "Liverpool"},
            "goals": {"h": "0", "a": "0"},
            "xG": {"h": "0", "a": "0"},
            "datetime": "2025-11-08 15:00:00",
        },
        {
            "id": "28860",
            "isResult": True,  # different day -> excluded when target_date is set
            "h": {"id": "87", "title": "Liverpool"},
            "a": {"id": "73", "title": "Bournemouth"},
            "goals": {"h": "4", "a": "2"},
            "xG": {"h": "2.33007", "a": "1.57303"},
            "datetime": "2025-10-25 19:00:00",
        },
    ],
    "teams": {
        "220": {
            "id": "220",
            "title": "Brighton",
            "history": [
                {
                    "h_a": "h",
                    "xG": 2.64127,
                    "xGA": 0.35919,
                    "npxG": 2.64127,
                    "npxGA": 0.35919,
                    "npxGD": 2.28208,
                    "ppda": {"att": 227, "def": 12},
                    "ppda_allowed": {"att": 146, "def": 24},
                    "deep": 9,
                    "deep_allowed": 2,
                    "scored": 3,
                    "missed": 0,
                    "xpts": 2.7181,
                    "result": "w",
                    "date": "2025-11-01 15:00:00",
                },
                {
                    "h_a": "a",
                    "xG": 0.9,
                    "ppda": {},
                    "deep": 1,
                    "result": "l",
                    "date": "2025-10-25 15:00:00",
                },
            ],
        }
    },
}


def test_parse_understat_fixtures_keeps_only_played_matches_on_the_target_date() -> None:
    rows = parse_understat_fixtures(LEAGUE_DATA, "EPL", "2025", target_date=date(2025, 11, 1))
    assert len(rows) == 1
    row = rows[0]
    assert row["understat_match_id"] == 28868
    assert row["home_team"] == "Brighton & Hove Albion"  # canonicalized to match Football-Data
    assert row["away_team"] == "Leeds United"
    assert row["home_goals"] == 3 and row["away_goals"] == 0
    assert row["home_xg"] == pytest.approx(2.64127)  # string -> float
    assert row["away_xg"] == pytest.approx(0.35919)
    assert row["league"] == "EPL" and row["season"] == "2025"
    assert row["utc_kickoff"] == datetime(2025, 11, 1, 15, 0, 0)


def test_parse_understat_fixtures_without_target_date_returns_all_played() -> None:
    rows = parse_understat_fixtures(LEAGUE_DATA, "EPL", "2025")
    assert {r["understat_match_id"] for r in rows} == {28868, 28860}


def test_parse_understat_team_history_splits_ppda_components() -> None:
    rows = parse_understat_team_history(LEAGUE_DATA, "EPL", "2025", target_date=date(2025, 11, 1))
    assert len(rows) == 1
    row = rows[0]
    assert row["team"] == "Brighton & Hove Albion"
    assert row["side"] == "h"
    assert row["match_date"] == date(2025, 11, 1)
    # Kept as numerator/denominator so Gold can compute the ratio without
    # inheriting a divide-by-zero from a team that made no defensive actions.
    assert (row["ppda_att"], row["ppda_def"]) == (227, 12)
    assert (row["ppda_allowed_att"], row["ppda_allowed_def"]) == (146, 24)
    assert row["npxg"] == pytest.approx(2.64127)
    assert row["xpts"] == pytest.approx(2.7181)
    assert row["deep"] == 9


def test_parse_understat_team_history_tolerates_missing_ppda() -> None:
    rows = parse_understat_team_history(LEAGUE_DATA, "EPL", "2025", target_date=date(2025, 10, 25))
    assert len(rows) == 1
    assert rows[0]["ppda_att"] is None
    assert rows[0]["ppda_def"] is None
    assert rows[0]["xg"] == pytest.approx(0.9)


MATCH_DATA = {
    "rosters": {
        "h": {
            "748274": {
                "id": "748274",
                "goals": "0",
                "own_goals": "0",
                "shots": "0",
                "xG": "0",
                "time": "90",
                "player_id": "11711",
                "team_id": "220",
                "position": "GK",
                "player": "Bart Verbruggen",
                "h_a": "h",
                "yellow_card": "0",
                "red_card": "0",
                "key_passes": "0",
                "assists": "0",
                "xA": "0",
                "xGChain": "0.028983620926737785",
                "xGBuildup": "0.028983620926737785",
                "positionOrder": "1",
            }
        },
        "a": {
            "748290": {
                "id": "748290",
                "goals": "1",
                "own_goals": "0",
                "shots": "3",
                "xG": "0.44",
                "time": "76",
                "player_id": "9999",
                "team_id": "245",
                "position": "FW",
                "player": "Some Striker",
                "h_a": "a",
                "yellow_card": "1",
                "red_card": "0",
                "key_passes": "2",
                "assists": "1",
                "xA": "0.31",
                "xGChain": "0.8",
                "xGBuildup": "0.2",
                "positionOrder": "11",
            }
        },
    },
    "shots": {
        "h": [
            {
                "id": "650814",
                "minute": "10",
                "result": "Goal",
                "X": "0.9330000305175781",
                "Y": "0.48",
                "xG": "0.5774248242378235",
                "player": "Danny Welbeck",
                "h_a": "h",
                "player_id": "501",
                "situation": "OpenPlay",
                "season": "2025",
                "shotType": "RightFoot",
                "match_id": "28868",
                "date": "2025-11-01 15:00:00",
                "player_assisted": "Mats Wieffer",
                "lastAction": "Pass",
            },
            {
                "id": "650812",
                "minute": "8",
                "result": "SavedShot",
                "X": "0.855",
                "Y": "0.577",
                "xG": "0.075",
                "player": "Yasin Ayari",
                "h_a": "h",
                "player_id": "11385",
                "situation": "OpenPlay",
                "season": "2025",
                "shotType": "RightFoot",
                "match_id": "28868",
                "date": "2025-11-01 15:00:00",
                "player_assisted": None,
                "lastAction": "BallRecovery",
            },
        ],
        "a": [],
    },
}


def test_parse_understat_rosters_flattens_both_sides_and_casts() -> None:
    rows = parse_understat_rosters(MATCH_DATA, understat_match_id=28868)
    assert len(rows) == 2
    by_side = {r["side"]: r for r in rows}

    home = by_side["h"]
    assert home["understat_match_id"] == 28868  # supplied by caller; absent from the payload
    assert home["player_understat_id"] == 11711
    assert home["player_name"] == "Bart Verbruggen"
    assert home["minutes"] == 90  # "90" -> int
    assert home["xg_chain"] == pytest.approx(0.028983620926737785)  # string -> float
    assert home["position"] == "GK"

    away = by_side["a"]
    assert away["goals"] == 1
    assert away["assists"] == 1
    assert away["yellow_cards"] == 1
    assert away["xa"] == pytest.approx(0.31)


def test_parse_understat_rosters_skips_rows_without_a_player() -> None:
    broken = {"rosters": {"h": {"1": {"player_id": None, "player": ""}}}}
    assert parse_understat_rosters(broken, understat_match_id=1) == []


def test_parse_understat_shots_flattens_events() -> None:
    rows = parse_understat_shots(MATCH_DATA)
    assert len(rows) == 2
    goal = next(r for r in rows if r["result"] == "Goal")
    assert goal["shot_id"] == 650814
    assert goal["understat_match_id"] == 28868  # carried on the record itself
    assert goal["match_date"] == date(2025, 11, 1)
    assert goal["x"] == pytest.approx(0.9330000305175781)
    assert goal["y"] == pytest.approx(0.48)
    assert goal["xg"] == pytest.approx(0.5774248242378235)
    assert goal["player_assisted"] == "Mats Wieffer"
    assert goal["shot_type"] == "RightFoot"
    assert goal["side"] == "h"

    unassisted = next(r for r in rows if r["shot_id"] == 650812)
    assert unassisted["player_assisted"] is None  # real null, not the string "None"


def test_parse_understat_shots_handles_empty_and_malformed() -> None:
    assert parse_understat_shots({"shots": {"h": [], "a": []}}) == []
    assert parse_understat_shots({}) == []
    assert parse_understat_shots({"shots": {"h": [{"id": None, "match_id": "1"}]}}) == []


# --- Football-Data teams / venues -------------------------------------------


def _teams_payload(*teams: dict) -> dict:
    return {"competition": {"code": "PL"}, "teams": list(teams)}


def test_parse_teams_extracts_the_venue_and_conforms_the_club() -> None:
    rows = parse_football_data_teams(
        _teams_payload(
            {
                "id": 57,
                "name": "Arsenal FC",
                "shortName": "Arsenal",
                "tla": "ARS",
                "venue": "Emirates Stadium",
                "address": "75 Drayton Park London N5 1BU",
                "area": {"name": "England"},
            }
        )
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["source_team_id"] == 57
    assert row["team_raw"] == "Arsenal FC"
    # The canonical name is what joins this club to Understat's spelling.
    assert row["team"] == normalize_team_name("Arsenal FC")
    assert row["venue"] == "Emirates Stadium"
    assert row["address"] == "75 Drayton Park London N5 1BU"
    assert row["country"] == "England"
    assert row["competition_code"] == "PL"


def test_parse_teams_keeps_a_club_with_no_venue() -> None:
    """The club still belongs in the roster; it just contributes no stadium."""
    rows = parse_football_data_teams(
        _teams_payload({"id": 61, "name": "Chelsea FC", "area": {"name": "England"}})
    )
    assert len(rows) == 1
    assert rows[0]["venue"] is None


def test_parse_teams_skips_records_without_an_id_or_name() -> None:
    rows = parse_football_data_teams(
        _teams_payload(
            {"name": "No Id FC", "venue": "Somewhere"},
            {"id": 99, "venue": "Nameless Park"},
            {"id": 57, "name": "Arsenal FC", "venue": "Emirates Stadium"},
        )
    )
    assert [row["source_team_id"] for row in rows] == [57]


def test_parse_teams_tolerates_an_empty_payload() -> None:
    assert parse_football_data_teams({}) == []
