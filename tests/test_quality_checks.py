"""Tests for the Gold-layer data quality checks.

The checks' SQL is verified against the running warehouse (see the README);
what CI can test without a database is the catalogue's own integrity and the
runner's decision logic — which failures stop the pipeline and which do not.
That decision is the whole point of the task, so it is worth testing on a fake
cursor rather than only observing it in a live run.
"""

from __future__ import annotations

import sqlite3
import sys
import types
from typing import Any

import pytest
from src.common.config import PostgresConfig
from src.quality.checks import CHECKS, Check, Severity
from src.quality.run_checks import (
    CheckResult,
    DataQualityError,
    _connect,
    evaluate,
    run,
    summarize,
)

GOLD_TABLES = (
    "gold.dim_teams",
    "gold.dim_stadiums",
    "gold.dim_players",
    "gold.fact_matches",
    "gold.fact_player_events",
    "gold.fact_shots",
)


class FakeCursor:
    """Minimal DB-API cursor returning a queued count per execute()."""

    def __init__(self, counts: list[int] | None = None) -> None:
        self.counts = list(counts or [])
        self.executed: list[str] = []
        self._last: tuple[int] | None = None

    def execute(self, sql: str) -> None:
        self.executed.append(sql)
        self._last = (self.counts.pop(0),) if self.counts else (0,)

    def fetchone(self) -> tuple[int] | None:
        return self._last

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: Any) -> None:
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


def _check(name: str = "c", severity: Severity = Severity.ERROR) -> Check:
    return Check(
        name=name,
        table="gold.fact_matches",
        severity=severity,
        description="d",
        sql="SELECT 0",
    )


# --- catalogue integrity ----------------------------------------------------


def test_check_names_are_unique() -> None:
    """The name is how a failure is reported and traced back; duplicates would
    make a red run ambiguous."""
    names = [check.name for check in CHECKS]
    assert len(names) == len(set(names))


def test_every_check_is_a_single_select_statement() -> None:
    """The runner executes each check as one statement and reads one scalar. A
    stray semicolon or a non-SELECT would break that contract at runtime."""
    for check in CHECKS:
        body = check.sql.strip()
        assert body.upper().startswith("SELECT"), check.name
        assert ";" not in body, check.name


def test_every_check_counts_rows() -> None:
    """Zero has to mean 'pass', which only holds if the query counts violations."""
    for check in CHECKS:
        assert "count(*)" in check.sql.lower(), check.name


def test_every_check_queries_its_declared_table() -> None:
    """`table` is what the failure log points at, so it must not drift from the SQL."""
    for check in CHECKS:
        assert check.table in check.sql, check.name


def test_every_gold_table_is_covered() -> None:
    covered = {check.table for check in CHECKS}
    assert set(GOLD_TABLES) <= covered


def test_every_fact_table_guards_the_unknown_member() -> None:
    """The Gold contract forbids silent nulls in facts. Because the FKs default to
    the Unknown member, that rule is only enforced if some ERROR check looks for
    key 0 on each fact table."""
    for table in ("gold.fact_matches", "gold.fact_player_events", "gold.fact_shots"):
        guards = [
            check
            for check in CHECKS
            if check.table == table and check.severity is Severity.ERROR and "_key = 0" in check.sql
        ]
        assert guards, f"{table} has no Unknown-member guard"


def test_every_check_explains_itself() -> None:
    """The description is what an on-call reader sees next to a failure; a
    placeholder there makes a red run unactionable."""
    for check in CHECKS:
        assert len(check.description) > 40, check.name


def test_severities_are_valid() -> None:
    for check in CHECKS:
        assert check.severity in (Severity.ERROR, Severity.WARN)


@pytest.mark.parametrize(
    ("rows", "missing"),
    [
        pytest.param([("FINISHED", 7, 1, 2, 100, 1.2)], 0, id="has xG"),
        pytest.param([("FINISHED", None, 1, 2, 100, None)], 1, id="never delivered"),
        pytest.param([("TIMED", None, 1, 2, 100, None)], 0, id="not played yet"),
        pytest.param(
            [("FINISHED", None, 1, 2, 100, None), ("FINISHED", 7, 1, 2, 101, 1.2)],
            0,
            id="the sources date it a day apart",
        ),
        pytest.param(
            [("FINISHED", None, 1, 2, 100, None), ("FINISHED", 7, 1, 2, 465, 1.2)],
            1,
            id="the same fixture a season later",
        ),
    ],
)
def test_missing_xg_check_counts_only_what_understat_never_delivered(
    rows: list[tuple], missing: int
) -> None:
    """SQLite has no date type, so ``match_date`` is a day number here: subtracting two
    gives days, as subtracting two Postgres dates does."""
    check = next(c for c in CHECKS if c.name == "fact_matches_finished_have_xg")
    con = sqlite3.connect(":memory:")
    con.execute("ATTACH ':memory:' AS gold")
    con.execute(
        "CREATE TABLE gold.fact_matches "
        "(status, understat_match_id, home_team_key, away_team_key, match_date, home_xg)"
    )
    con.executemany("INSERT INTO gold.fact_matches VALUES (?, ?, ?, ?, ?, ?)", rows)

    assert con.execute(check.sql).fetchone()[0] == missing


# --- result semantics -------------------------------------------------------


def test_result_passes_only_on_zero_violations() -> None:
    assert CheckResult(_check(), 0).passed
    assert not CheckResult(_check(), 1).passed


def test_only_error_severity_fails_the_run() -> None:
    assert CheckResult(_check(severity=Severity.ERROR), 1).failed_hard
    assert not CheckResult(_check(severity=Severity.WARN), 1).failed_hard
    assert not CheckResult(_check(severity=Severity.ERROR), 0).failed_hard


# --- runner -----------------------------------------------------------------


def test_evaluate_runs_every_check_in_order() -> None:
    checks = (_check("a"), _check("b"), _check("c"))
    cursor = FakeCursor([0, 3, 0])
    results = evaluate(cursor, checks)

    assert [result.check.name for result in results] == ["a", "b", "c"]
    assert [result.violations for result in results] == [0, 3, 0]
    assert len(cursor.executed) == 3


def test_evaluate_rejects_a_check_returning_no_row() -> None:
    class EmptyCursor(FakeCursor):
        def fetchone(self) -> None:
            return None

    with pytest.raises(DataQualityError, match="single count"):
        evaluate(EmptyCursor(), (_check(),))


def test_run_raises_when_an_error_check_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = FakeCursor([4])
    monkeypatch.setattr("src.quality.run_checks._connect", lambda config: FakeConnection(cursor))

    with pytest.raises(DataQualityError, match="boom"):
        run(config=object(), checks=(_check("boom", Severity.ERROR),))  # type: ignore[arg-type]


def test_run_tolerates_a_failing_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    """A WARN failure is reported but must not fail the DAG — otherwise a source
    quirk we already decided to accept takes the pipeline down nightly."""
    cursor = FakeCursor([7])
    monkeypatch.setattr("src.quality.run_checks._connect", lambda config: FakeConnection(cursor))

    results = run(config=object(), checks=(_check("soft", Severity.WARN),))  # type: ignore[arg-type]
    assert [result.violations for result in results] == [7]


def test_run_evaluates_every_check_before_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failing fast on the first error would hide the rest of the picture."""
    cursor = FakeCursor([1, 1, 1])
    monkeypatch.setattr("src.quality.run_checks._connect", lambda config: FakeConnection(cursor))
    checks = (_check("a"), _check("b"), _check("c"))

    with pytest.raises(DataQualityError):
        run(config=object(), checks=checks)  # type: ignore[arg-type]
    assert len(cursor.executed) == 3


def test_run_closes_the_connection_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = FakeCursor([2])
    connection = FakeConnection(cursor)
    monkeypatch.setattr("src.quality.run_checks._connect", lambda config: connection)

    with pytest.raises(DataQualityError):
        run(config=object(), checks=(_check(),))  # type: ignore[arg-type]
    assert connection.closed


# --- connection -------------------------------------------------------------


def test_connect_holds_one_snapshot_for_the_whole_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """All the checks must read the same snapshot of the warehouse.

    Under psycopg2's default READ COMMITTED every statement takes a fresh
    snapshot, so a Gold load committing mid-run can be half-visible:
    ``fact_shots_goals_reconcile_with_player_events`` compares two fact tables
    that one load rewrites together, and would report the torn read as a
    violation. That is a false alarm that fails the DAG.
    """
    recorded: dict[str, Any] = {}

    class SessionRecordingConnection(FakeConnection):
        def set_session(self, **kwargs: Any) -> None:
            recorded.update(kwargs)

    monkeypatch.setitem(
        sys.modules,
        "psycopg2",
        types.SimpleNamespace(connect=lambda **kwargs: SessionRecordingConnection(FakeCursor())),
    )

    _connect(PostgresConfig(user="u", password="p", database="d", host="h", port=5432))

    assert recorded == {"isolation_level": "REPEATABLE READ", "readonly": True}


# --- report -----------------------------------------------------------------


def test_summarize_reports_counts_and_severities() -> None:
    results = [
        CheckResult(_check("ok"), 0),
        CheckResult(_check("bad", Severity.ERROR), 2),
        CheckResult(_check("soft", Severity.WARN), 5),
    ]
    report = summarize(results)

    assert "ok" in report and "PASS" in report
    assert "ERROR (2 row(s))" in report
    assert "WARN (5 row(s))" in report
    assert "1/3 checks passed; 1 error(s), 1 warning(s)." in report


def test_summarize_handles_an_empty_suite() -> None:
    assert "No data quality checks" in summarize([])
