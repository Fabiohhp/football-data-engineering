"""Tests for the JVM-free pieces of the Silver -> Gold load.

The Spark job itself is verified end-to-end against the running warehouse (see
the README); what is unit-testable is the contract around it — the value
mapping Gold expects, the merge statements' execution order, and the agreement
between the staging DDL and the list of tables the job truncates. Each of these
is a place where a silent mismatch would corrupt a load rather than fail it.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest
from src.common.sources import season_label
from src.transform.gold_sql import (
    LOAD_SQL_DIR,
    RESULT_TO_GOLD,
    STAGING_TABLES,
    load_statements,
)
from src.transform.parsers import WINNER_TO_RESULT

_REPO_ROOT = Path(__file__).resolve().parents[1]
_STAGING_DDL = _REPO_ROOT / "sql" / "ddl" / "30_staging.sql"
_FACT_MATCHES_DDL = _REPO_ROOT / "sql" / "ddl" / "20_fact_matches.sql"


def test_result_mapping_inverts_the_silver_encoding() -> None:
    assert RESULT_TO_GOLD == {"H": "HOME_TEAM", "A": "AWAY_TEAM", "D": "DRAW"}
    for winner, code in WINNER_TO_RESULT.items():
        assert RESULT_TO_GOLD[code] == winner


def test_result_mapping_satisfies_the_fact_matches_check_constraint() -> None:
    """Every value the load can write must be one fact_matches accepts.

    ck_fact_matches_result rejects anything else, so a drift here would abort a
    whole night's load.
    """
    allowed = set(
        re.findall(r"'(\w+)'", _FACT_MATCHES_DDL.read_text().split("ck_fact_matches_result")[1])
    )
    assert set(RESULT_TO_GOLD.values()) <= allowed


def test_season_label_spans_two_calendar_years() -> None:
    assert season_label(date(2025, 11, 1)) == "2025/26"
    assert season_label(date(2026, 5, 10)) == "2025/26"
    assert season_label(date(2026, 8, 1)) == "2026/27"
    assert season_label(date(2029, 12, 31)) == "2029/30"


def test_season_label_fits_the_gold_column() -> None:
    # fact_matches.season is VARCHAR(9).
    assert len(season_label(date(2025, 11, 1))) <= 9


def test_load_statements_are_ordered_by_filename_prefix() -> None:
    names = [name for name, _ in load_statements()]
    assert names == sorted(names)
    # Dimensions must be merged before the facts that reference them, and the
    # player-version lookup before both fact loads that join to it.
    assert names.index("20_dim_players.sql") < names.index("40_player_versions.sql")
    assert names.index("40_player_versions.sql") < names.index("50_fact_player_events.sql")
    assert names.index("40_player_versions.sql") < names.index("60_fact_shots.sql")
    assert names.index("30_fact_matches.sql") < names.index("50_fact_player_events.sql")


def test_load_statements_covers_every_gold_table() -> None:
    sql = "\n".join(body for _, body in load_statements())
    for table in (
        "gold.dim_teams",
        "gold.dim_referees",
        "gold.dim_players",
        "gold.fact_matches",
        "gold.fact_player_events",
        "gold.fact_shots",
    ):
        assert f"INTO {table}" in sql or f"UPDATE {table}" in sql


def test_load_statements_are_non_empty() -> None:
    for name, body in load_statements():
        assert body.strip(), f"{name} is empty"


def test_load_statements_rejects_a_missing_directory(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="not found"):
        load_statements(tmp_path / "nope")


def test_load_statements_rejects_an_empty_directory(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="No .sql files"):
        load_statements(tmp_path)


def test_load_sql_dir_resolves_inside_the_repo() -> None:
    assert LOAD_SQL_DIR == _REPO_ROOT / "sql" / "load"


def test_staging_tables_match_the_staging_ddl() -> None:
    """The job truncates STAGING_TABLES; the DDL declares them. If the two drift,
    a run either fails on a missing table or silently merges stale rows from a
    table nobody emptied."""
    declared = set(
        re.findall(r"CREATE TABLE IF NOT EXISTS staging\.(\w+)", _STAGING_DDL.read_text())
    )
    assert declared == set(STAGING_TABLES)
