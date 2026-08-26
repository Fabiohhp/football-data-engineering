"""Spark-backed tests for the Bronze -> Silver stage.

Gated behind the ``spark`` fixture, like ``test_silver_to_gold_spark``. The
parsers are already covered without a JVM in ``test_parsers``; what needs Spark
is everything those pure functions hand off to — the typed frame construction
that turns a parser's dict into a column, the dedup keys, and the history join
that decides whether a fixture arrives with its advanced metrics or with nulls.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

pytest.importorskip(
    "pyspark",
    reason="PySpark is installed from requirements-spark.txt (the spark-tests CI job)",
)

from src.transform.bronze_to_silver import (  # noqa: E402
    FD_TEAMS_SCHEMA,
    UNDERSTAT_FIXTURES_SCHEMA,
    UNDERSTAT_HISTORY_SCHEMA,
    _rows_to_dataframe,
    enrich_fixtures_with_team_history,
    load_fd_teams,
)

MATCH_DAY = date(2025, 11, 1)


class _FakeReader:
    """Stands in for BronzeReader: canned JSON payloads keyed by object key."""

    def __init__(self, objects: dict[str, dict]) -> None:
        self._objects = objects
        self.prefixes: list[str] = []

    def list_keys(self, prefix: str) -> list[str]:
        self.prefixes.append(prefix)
        return [key for key in self._objects if key.startswith(prefix)]

    def read_json(self, key: str) -> dict:
        return self._objects[key]


def _team(team_id: int, name: str, venue: str | None = "Some Ground") -> dict:
    return {
        "id": team_id,
        "name": name,
        "shortName": name,
        "tla": name[:3].upper(),
        "venue": venue,
        "address": "Somewhere",
        "area": {"name": "England"},
    }


# --- _rows_to_dataframe -----------------------------------------------------


def test_rows_to_dataframe_orders_columns_by_the_schema(spark) -> None:
    """Dicts are unordered; the schema decides which value lands in which column.

    Handing Spark the dicts directly would let a reordered parser silently swap
    two same-typed columns.
    """
    rows = [
        {
            "team": "Arsenal",
            "source_team_id": 57,
            "match_date": MATCH_DAY,
            "venue": "Emirates",
            "country": "England",
        }
    ]
    df = _rows_to_dataframe(spark, rows, FD_TEAMS_SCHEMA)

    assert df.columns == [field.name for field in FD_TEAMS_SCHEMA.fields]
    row = df.collect()[0]
    assert row["source_team_id"] == 57
    assert row["team"] == "Arsenal"
    assert row["venue"] == "Emirates"
    # A key the parser never produced becomes a null column, not a shifted value.
    assert row["tla"] is None


def test_rows_to_dataframe_rejects_a_missing_non_nullable_field(spark) -> None:
    """`source_team_id` is nullable=False, so dropping it must fail loudly here
    rather than land a null id in Silver.

    PySpark raises ``PySparkValueError``, which subclasses ``ValueError`` — the
    broader name is asserted so a PySpark upgrade renaming its own class does
    not fail the suite.
    """
    with pytest.raises(ValueError):
        _rows_to_dataframe(
            spark,
            [{"team": "Arsenal", "match_date": MATCH_DAY}],
            FD_TEAMS_SCHEMA,
        ).collect()


# --- load_fd_teams ----------------------------------------------------------


def test_load_fd_teams_reads_every_competition_and_stamps_the_date(spark) -> None:
    reader = _FakeReader(
        {
            "football_data/teams/year=2025/month=11/day=01/PL.json": {
                "competition": {"code": "PL"},
                "teams": [_team(57, "Arsenal FC", "Emirates Stadium")],
            },
            "football_data/teams/year=2025/month=11/day=01/SA.json": {
                "competition": {"code": "SA"},
                "teams": [_team(108, "FC Internazionale Milano", "San Siro")],
            },
        }
    )

    rows = sorted(
        load_fd_teams(spark, reader, MATCH_DAY).collect(), key=lambda r: r["source_team_id"]
    )

    assert reader.prefixes == ["football_data/teams/year=2025/month=11/day=01/"]
    assert [r["source_team_id"] for r in rows] == [57, 108]
    assert [r["venue"] for r in rows] == ["Emirates Stadium", "San Siro"]
    # The ingestion date, which is what partitions the table.
    assert {r["match_date"] for r in rows} == {MATCH_DAY}


def test_load_fd_teams_dedups_a_club_extracted_twice(spark) -> None:
    """A re-extract into a differently-named key must not double the roster."""
    payload = {"competition": {"code": "PL"}, "teams": [_team(57, "Arsenal FC")]}
    reader = _FakeReader(
        {
            "football_data/teams/year=2025/month=11/day=01/PL.json": payload,
            "football_data/teams/year=2025/month=11/day=01/PL-retry.json": payload,
        }
    )
    assert load_fd_teams(spark, reader, MATCH_DAY).count() == 1


def test_load_fd_teams_on_a_day_with_no_objects(spark) -> None:
    """An empty prefix yields an empty typed frame, not a crash."""
    df = load_fd_teams(spark, _FakeReader({}), MATCH_DAY)
    assert df.count() == 0
    assert df.columns == [field.name for field in FD_TEAMS_SCHEMA.fields]


# --- enrich_fixtures_with_team_history --------------------------------------


def _fixture(**overrides) -> dict:
    row = {
        "understat_match_id": 900,
        "league": "EPL",
        "season": "2025",
        "utc_kickoff": datetime(2025, 11, 1, 15, 0),
        "match_date": MATCH_DAY,
        "home_team": "Arsenal",
        "home_team_raw": "Arsenal",
        "home_team_understat_id": 83,
        "away_team": "Chelsea",
        "away_team_raw": "Chelsea",
        "away_team_understat_id": 80,
        "home_goals": 2,
        "away_goals": 1,
        "home_xg": 1.8,
        "away_xg": 0.9,
    }
    row.update(overrides)
    return row


def _history(team: str, side: str, **overrides) -> dict:
    row = {
        "team": team,
        "team_raw": team,
        "league": "EPL",
        "season": "2025",
        "match_date": MATCH_DAY,
        "side": side,
        "npxg": 1.5,
        "ppda_att": 300,
        "ppda_def": 30,
        "deep": 7,
        "xpts": 2.1,
    }
    row.update(overrides)
    return row


def test_enrich_attaches_each_side_to_the_right_prefix(spark) -> None:
    """The home club's metrics must land on home_*, the away club's on away_*.

    They join on (team, match_date, side) — swap the sides and every advanced
    metric in fact_matches is attributed to the opponent.
    """
    fixtures = _rows_to_dataframe(spark, [_fixture()], UNDERSTAT_FIXTURES_SCHEMA)
    history = _rows_to_dataframe(
        spark,
        [
            _history("Arsenal", "h", npxg=1.5, deep=7),
            _history("Chelsea", "a", npxg=0.6, deep=3),
        ],
        UNDERSTAT_HISTORY_SCHEMA,
    )

    row = enrich_fixtures_with_team_history(fixtures, history).collect()[0]
    assert row["home_npxg"] == pytest.approx(1.5)
    assert row["home_deep"] == 7
    assert row["away_npxg"] == pytest.approx(0.6)
    assert row["away_deep"] == 3


def test_enrich_keeps_a_fixture_whose_history_is_missing(spark) -> None:
    """A left join: no history means null metrics, never a dropped fixture."""
    fixtures = _rows_to_dataframe(spark, [_fixture()], UNDERSTAT_FIXTURES_SCHEMA)
    history = _rows_to_dataframe(spark, [], UNDERSTAT_HISTORY_SCHEMA)

    rows = enrich_fixtures_with_team_history(fixtures, history).collect()
    assert len(rows) == 1
    assert rows[0]["understat_match_id"] == 900
    assert rows[0]["home_npxg"] is None


def test_enrich_does_not_match_the_same_club_on_another_date(spark) -> None:
    """match_date is part of the join key, so yesterday's form cannot leak in."""
    fixtures = _rows_to_dataframe(spark, [_fixture()], UNDERSTAT_FIXTURES_SCHEMA)
    history = _rows_to_dataframe(
        spark,
        [_history("Arsenal", "h", match_date=date(2025, 10, 25), npxg=9.9)],
        UNDERSTAT_HISTORY_SCHEMA,
    )

    row = enrich_fixtures_with_team_history(fixtures, history).collect()[0]
    assert row["home_npxg"] is None
