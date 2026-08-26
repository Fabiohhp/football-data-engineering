"""Spark-backed tests for the Silver -> Gold frame builders.

These need a JVM, so they are gated behind the ``spark`` fixture and run in the
``spark-tests`` CI job rather than the default suite. What they cover is the
logic that the JVM-free tests structurally cannot reach: the joins. Every
interesting failure in this module is a join that silently drops or duplicates
rows — a club that does not conform, an Understat-only match, an assist credited
to a namesake in another fixture — and none of those are visible from the SQL or
the parsers.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

pytest.importorskip(
    "pyspark",
    reason="PySpark is installed from requirements-spark.txt (the spark-tests CI job)",
)

from pyspark.sql.types import (  # noqa: E402
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from src.transform.bronze_to_silver import (  # noqa: E402
    PLAYER_EVENTS_SCHEMA,
    SHOTS_SCHEMA,
    UNDERSTAT_FIXTURES_SCHEMA,
    _rows_to_dataframe,
)
from src.transform.silver_to_gold import (  # noqa: E402
    build_matches,
    build_players,
    build_shots,
    build_stadiums,
    build_teams,
)

MATCH_DAY = date(2025, 11, 1)


def _frame(spark, rows: list[dict], schema: StructType):
    """Build a DataFrame from dicts, ordered strictly by ``schema``."""
    names = [field.name for field in schema.fields]
    return spark.createDataFrame([tuple(row.get(n) for n in names) for row in rows], schema=schema)


# --- build_stadiums ---------------------------------------------------------

FD_TEAMS = StructType(
    [
        StructField("source_team_id", IntegerType()),
        StructField("team", StringType()),
        StructField("team_raw", StringType()),
        StructField("venue", StringType()),
        StructField("address", StringType()),
        StructField("country", StringType()),
    ]
)


def test_build_stadiums_drops_clubs_without_a_venue(spark) -> None:
    """A null venue must not become a null-named stadium in the dimension."""
    teams = _frame(
        spark,
        [
            {"source_team_id": 57, "team": "Arsenal", "venue": "Emirates", "country": "England"},
            {"source_team_id": 61, "team": "Chelsea", "venue": None, "country": "England"},
        ],
        FD_TEAMS,
    )
    rows = build_stadiums(teams).collect()
    assert [r["team_canonical"] for r in rows] == ["Arsenal"]


def test_build_stadiums_keeps_both_clubs_that_share_a_ground(spark) -> None:
    """Inter and Milan both play at San Siro.

    The grain here is the club, because a fixture names clubs — collapsing to
    one row per ground at this stage would leave one of the two unable to
    resolve its stadium_key. sql/load/05_dim_stadiums.sql is what dedups the
    ground itself.
    """
    teams = _frame(
        spark,
        [
            {"source_team_id": 108, "team": "Inter", "venue": "San Siro", "country": "Italy"},
            {"source_team_id": 98, "team": "AC Milan", "venue": "San Siro", "country": "Italy"},
        ],
        FD_TEAMS,
    )
    rows = sorted(build_stadiums(teams).collect(), key=lambda r: r["team_canonical"])
    assert [r["team_canonical"] for r in rows] == ["AC Milan", "Inter"]
    assert {r["stadium_name"] for r in rows} == {"San Siro"}


def test_build_stadiums_is_one_row_per_club(spark) -> None:
    """A club listed twice (re-extract, two competitions) must not double."""
    teams = _frame(
        spark,
        [
            {"source_team_id": 57, "team": "Arsenal", "venue": "Emirates", "country": "England"},
            {"source_team_id": 57, "team": "Arsenal", "venue": "Emirates", "country": "England"},
        ],
        FD_TEAMS,
    )
    assert build_stadiums(teams).count() == 1


# --- build_teams ------------------------------------------------------------

FD_MATCHES = StructType(
    [
        StructField("home_team", StringType()),
        StructField("home_team_raw", StringType()),
        StructField("home_team_source_id", IntegerType()),
        StructField("away_team", StringType()),
        StructField("away_team_raw", StringType()),
        StructField("away_team_source_id", IntegerType()),
    ]
)

US_MATCHES = StructType(
    [
        StructField("home_team", StringType()),
        StructField("home_team_raw", StringType()),
        StructField("home_team_understat_id", IntegerType()),
        StructField("away_team", StringType()),
        StructField("away_team_raw", StringType()),
        StructField("away_team_understat_id", IntegerType()),
    ]
)


def test_build_teams_conforms_one_club_across_both_sources(spark) -> None:
    """The whole point of the dimension: two spellings, one row, both ids.

    If this folds wrong the club splits in two and every downstream fact for it
    is halved — the failure `dim_teams_no_source_id_collision` exists to catch.
    """
    fd = _frame(
        spark,
        [
            {
                "home_team": "Manchester United",
                "home_team_raw": "Manchester United FC",
                "home_team_source_id": 66,
                "away_team": "Arsenal",
                "away_team_raw": "Arsenal FC",
                "away_team_source_id": 57,
            }
        ],
        FD_MATCHES,
    )
    us = _frame(
        spark,
        [
            {
                "home_team": "Manchester United",
                "home_team_raw": "Manchester Utd",
                "home_team_understat_id": 89,
                "away_team": "Arsenal",
                "away_team_raw": "Arsenal",
                "away_team_understat_id": 83,
            }
        ],
        US_MATCHES,
    )

    rows = {r["canonical_name"]: r for r in build_teams(fd, us).collect()}
    assert set(rows) == {"Manchester United", "Arsenal"}
    united = rows["Manchester United"]
    assert united["football_data_name"] == "Manchester United FC"
    assert united["understat_name"] == "Manchester Utd"
    assert united["football_data_id"] == 66
    assert united["understat_id"] == 89


# --- build_matches ----------------------------------------------------------

FD_FULL = StructType(
    [
        StructField("source_match_id", IntegerType()),
        StructField("competition_code", StringType()),
        StructField("competition_name", StringType()),
        StructField("utc_kickoff", TimestampType()),
        StructField("match_date", DateType()),
        StructField("matchday", IntegerType()),
        StructField("status", StringType()),
        StructField("home_team", StringType()),
        StructField("away_team", StringType()),
        StructField("home_goals_ft", IntegerType()),
        StructField("away_goals_ft", IntegerType()),
        StructField("home_goals_ht", IntegerType()),
        StructField("away_goals_ht", IntegerType()),
        StructField("result", StringType()),
        StructField("referee_source_id", IntegerType()),
    ]
)

US_FULL = StructType(
    [
        StructField("understat_match_id", IntegerType()),
        StructField("league", StringType()),
        StructField("utc_kickoff", TimestampType()),
        StructField("match_date", DateType()),
        StructField("home_team", StringType()),
        StructField("away_team", StringType()),
        StructField("home_goals", IntegerType()),
        StructField("away_goals", IntegerType()),
        StructField("home_xg", DoubleType()),
        StructField("away_xg", DoubleType()),
        StructField("home_npxg", DoubleType()),
        StructField("away_npxg", DoubleType()),
        StructField("home_ppda_att", DoubleType()),
        StructField("home_ppda_def", DoubleType()),
        StructField("away_ppda_att", DoubleType()),
        StructField("away_ppda_def", DoubleType()),
        StructField("home_deep", IntegerType()),
        StructField("away_deep", IntegerType()),
        StructField("home_xpts", DoubleType()),
        StructField("away_xpts", DoubleType()),
    ]
)


def _fd_match(**overrides) -> dict:
    row = {
        "source_match_id": 1,
        "competition_code": "PL",
        "competition_name": "Premier League",
        "utc_kickoff": datetime(2025, 11, 1, 15, 0),
        "match_date": MATCH_DAY,
        "matchday": 10,
        "status": "FINISHED",
        "home_team": "Arsenal",
        "away_team": "Chelsea",
        "home_goals_ft": 2,
        "away_goals_ft": 1,
        "result": "H",
        "referee_source_id": 11,
    }
    row.update(overrides)
    return row


def _us_match(**overrides) -> dict:
    row = {
        "understat_match_id": 900,
        "league": "EPL",
        "utc_kickoff": datetime(2025, 11, 1, 15, 0),
        "match_date": MATCH_DAY,
        "home_team": "Arsenal",
        "away_team": "Chelsea",
        "home_goals": 2,
        "away_goals": 1,
        "home_xg": 1.8,
        "away_xg": 0.9,
        "home_ppda_att": 300.0,
        "home_ppda_def": 30.0,
        "away_ppda_att": 200.0,
        "away_ppda_def": 0.0,
    }
    row.update(overrides)
    return row


def test_build_matches_folds_both_sources_onto_one_row(spark) -> None:
    fd = _frame(spark, [_fd_match()], FD_FULL)
    us = _frame(spark, [_us_match()], US_FULL)

    rows = build_matches(fd, us, MATCH_DAY).collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["football_data_match_id"] == 1
    assert row["understat_match_id"] == 900
    assert row["home_goals"] == 2
    assert row["home_xg"] == pytest.approx(1.8)
    assert row["result"] == "HOME_TEAM"
    assert row["season"] == "2025/26"


def test_build_matches_keeps_a_match_only_one_source_reported(spark) -> None:
    """A full outer join, so neither source can drop the other's fixture."""
    fd = _frame(spark, [_fd_match(home_team="Everton", away_team="Fulham")], FD_FULL)
    us = _frame(spark, [_us_match(home_team="Arsenal", away_team="Chelsea")], US_FULL)

    rows = {
        (r["home_team_canonical"], r["away_team_canonical"])
        for r in build_matches(fd, us, MATCH_DAY).collect()
    }
    assert rows == {("Everton", "Fulham"), ("Arsenal", "Chelsea")}


def test_build_matches_derives_the_result_for_an_understat_only_match(spark) -> None:
    """Understat has no `winner` field, but publishes played fixtures only."""
    fd = _frame(spark, [], FD_FULL)
    us = _frame(spark, [_us_match(home_goals=0, away_goals=3)], US_FULL)

    row = build_matches(fd, us, MATCH_DAY).collect()[0]
    assert row["result"] == "AWAY_TEAM"
    assert row["status"] == "FINISHED"


def test_build_matches_guards_the_ppda_division(spark) -> None:
    """PPDA is a ratio Understat ships as two halves; the denominator can be 0."""
    fd = _frame(spark, [], FD_FULL)
    us = _frame(spark, [_us_match()], US_FULL)

    row = build_matches(fd, us, MATCH_DAY).collect()[0]
    assert row["home_ppda"] == pytest.approx(10.0)  # 300 / 30
    assert row["away_ppda"] is None  # denominator 0 -> null, not a crash


# --- build_players / build_shots -------------------------------------------

# The Silver `player_events` table is the roster frame plus the fixture's date
# (bronze_to_silver joins it on before writing), so the builders see both.
PLAYER_EVENTS_WITH_DATE = StructType(
    [*PLAYER_EVENTS_SCHEMA.fields, StructField("match_date", DateType())]
)


def _event(match_id: int, player_id: int, name: str, side: str, **overrides) -> dict:
    row = {
        "understat_match_id": match_id,
        "roster_id": player_id,
        "player_understat_id": player_id,
        "player_name": name,
        "side": side,
        "position": "FW",
        "minutes": 90,
        "goals": 0,
        "own_goals": 0,
        "shots": 1,
        "assists": 0,
        "key_passes": 0,
        "match_date": MATCH_DAY,
    }
    row.update(overrides)
    return row


def _understat_fixture(match_id: int, home: str, away: str) -> dict:
    return {
        "understat_match_id": match_id,
        "league": "EPL",
        "season": "2025",
        "utc_kickoff": datetime(2025, 11, 1, 15, 0),
        "match_date": MATCH_DAY,
        "home_team": home,
        "home_team_raw": home,
        "home_team_understat_id": 83,
        "away_team": away,
        "away_team_raw": away,
        "away_team_understat_id": 80,
    }


def test_build_players_resolves_the_club_from_the_side(spark) -> None:
    """Understat identifies a club only by 'h'/'a' — the fixture supplies it."""
    events = _rows_to_dataframe(
        spark,
        [
            _event(900, 1, "Saka", "h"),
            _event(900, 2, "Palmer", "a", position="MF"),
        ],
        PLAYER_EVENTS_WITH_DATE,
    )
    fixtures = _rows_to_dataframe(
        spark, [_understat_fixture(900, "Arsenal", "Chelsea")], UNDERSTAT_FIXTURES_SCHEMA
    )

    rows = {
        r["understat_player_id"]: r["team_canonical"]
        for r in build_players(events, fixtures).collect()
    }
    assert rows == {1: "Arsenal", 2: "Chelsea"}


def test_build_shots_resolves_the_assister_within_the_same_match_only(spark) -> None:
    """A name is unique inside one roster, but namesakes exist across matches.

    Scoping the lookup to the match is what stops a shot being credited to a
    different player who happens to share the name in another fixture.
    """
    events = _rows_to_dataframe(
        spark,
        [
            _event(900, 10, "Odegaard", "h"),
            _event(900, 11, "Saka", "h"),
            # Same name, different match, different id.
            _event(901, 99, "Odegaard", "h"),
        ],
        PLAYER_EVENTS_WITH_DATE,
    )
    fixtures = _rows_to_dataframe(
        spark,
        [
            _understat_fixture(900, "Arsenal", "Chelsea"),
            _understat_fixture(901, "Everton", "Fulham"),
        ],
        UNDERSTAT_FIXTURES_SCHEMA,
    )
    shots = _rows_to_dataframe(
        spark,
        [
            {
                "shot_id": 5,
                "understat_match_id": 900,
                "match_date": MATCH_DAY,
                "season": "2025",
                "side": "h",
                "minute": 30,
                "player_understat_id": 11,
                "player_name": "Saka",
                "player_assisted": "Odegaard",
                "result": "Goal",
                "situation": "OpenPlay",
                "shot_type": "RightFoot",
                "x": 0.9,
                "y": 0.5,
                "xg": 0.4,
            }
        ],
        SHOTS_SCHEMA,
    )

    rows = build_shots(shots, events, fixtures).collect()
    assert len(rows) == 1
    assert rows[0]["assist_understat_player_id"] == 10  # match 900's Odegaard, not 901's
    assert rows[0]["team_canonical"] == "Arsenal"


def test_build_shots_keeps_an_unassisted_shot(spark) -> None:
    """Most shots have no assist; a left join must not drop them."""
    events = _rows_to_dataframe(spark, [_event(900, 11, "Saka", "h")], PLAYER_EVENTS_WITH_DATE)
    fixtures = _rows_to_dataframe(
        spark, [_understat_fixture(900, "Arsenal", "Chelsea")], UNDERSTAT_FIXTURES_SCHEMA
    )
    shots = _rows_to_dataframe(
        spark,
        [
            {
                "shot_id": 6,
                "understat_match_id": 900,
                "match_date": MATCH_DAY,
                "side": "h",
                "minute": 55,
                "player_understat_id": 11,
                "player_name": "Saka",
                "player_assisted": None,
                "result": "MissedShots",
                "x": 0.8,
                "y": 0.4,
                "xg": 0.1,
            }
        ],
        SHOTS_SCHEMA,
    )

    rows = build_shots(shots, events, fixtures).collect()
    assert len(rows) == 1
    assert rows[0]["assist_understat_player_id"] is None
