"""Run the Gold-layer data quality checks and fail the pipeline on corruption.

This is the DAG's final task. It runs every check in ``src.quality.checks``
against the warehouse, logs a report, and raises :class:`DataQualityError` if
any ``ERROR``-severity check found violations — an Airflow task fails on the
exception, so a bad load stops the run instead of quietly reaching consumers.

Two deliberate choices:

* **The whole warehouse, not one date.** Restricting checks to the date just
  loaded would miss corruption an *earlier* run introduced — most obviously an
  SCD version that only overlaps once a later transfer opens the next version.
  Gold is small enough that a full scan is cheap, and the rules are invariants
  about the star schema, not about a partition of it.
* **Every check runs before anything raises.** Stopping at the first failure
  would hide the rest, and the shape of a data problem is usually in the
  *combination* — an unmapped club shows up in the team, match and event checks
  at once. One run should produce the whole picture.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from src.common.config import PostgresConfig, postgres_config
from src.common.logging import get_logger
from src.quality.checks import CHECKS, Check, Severity

logger = get_logger(__name__)


class DataQualityError(RuntimeError):
    """Raised when at least one ERROR-severity check found violating rows."""


@dataclass(frozen=True)
class CheckResult:
    """The outcome of one check."""

    check: Check
    violations: int

    @property
    def passed(self) -> bool:
        return self.violations == 0

    @property
    def failed_hard(self) -> bool:
        """True when this failure must fail the run."""
        return not self.passed and self.check.severity is Severity.ERROR


def evaluate(cursor: Any, checks: Sequence[Check] = CHECKS) -> list[CheckResult]:
    """Execute every check on an open DB-API cursor.

    Takes a cursor rather than a connection so the caller owns the transaction
    and so this stays testable without a database.
    """
    results = []
    for check in checks:
        cursor.execute(check.sql)
        row = cursor.fetchone()
        if row is None:
            raise DataQualityError(
                f"Check {check.name!r} returned no rows; it must return a single count."
            )
        results.append(CheckResult(check=check, violations=int(row[0])))
    return results


def summarize(results: Iterable[CheckResult]) -> str:
    """Render the report. Pure, so its formatting is unit-testable."""
    results = list(results)
    if not results:
        return "No data quality checks were run."

    width = max(len(result.check.name) for result in results)
    lines = []
    for result in results:
        if result.passed:
            status = "PASS"
        else:
            status = f"{result.check.severity.value} ({result.violations} row(s))"
        lines.append(f"  {result.check.name:<{width}}  {status}")

    failed = [result for result in results if not result.passed]
    errors = [result for result in failed if result.failed_hard]
    lines.append(
        f"{len(results) - len(failed)}/{len(results)} checks passed; "
        f"{len(errors)} error(s), {len(failed) - len(errors)} warning(s)."
    )
    return "\n".join(lines)


def _connect(config: PostgresConfig) -> Any:
    """Open a psycopg2 connection to the warehouse.

    Imported lazily for the same reason as in the Silver -> Gold load: the
    driver needs it, and importing this module should never require it.
    """
    import psycopg2

    connection = psycopg2.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=config.database,
    )
    # REPEATABLE READ, not the psycopg2 default of READ COMMITTED: under READ
    # COMMITTED every statement takes a fresh snapshot, so a Gold load
    # committing mid-run would let one check see the state before it and the
    # next check the state after. `fact_shots_goals_reconcile_with_player_events`
    # compares two fact tables that a load rewrites together, and is exactly the
    # check that would report a torn read as a violation. One snapshot for the
    # whole run makes the report internally consistent.
    connection.set_session(isolation_level="REPEATABLE READ", readonly=True)
    return connection


def run(
    config: PostgresConfig | None = None,
    checks: Sequence[Check] = CHECKS,
) -> list[CheckResult]:
    """Run every check against the warehouse.

    Returns the full result list. Raises :class:`DataQualityError` if any
    ERROR-severity check failed — warnings are logged and tolerated.
    """
    config = config or postgres_config()

    connection = _connect(config)
    try:
        # The session is read-only and REPEATABLE READ (see _connect), so every
        # check below reads the same snapshot of the warehouse.
        with connection.cursor() as cursor:
            results = evaluate(cursor, checks)
    finally:
        connection.close()

    logger.info("Gold data quality report:\n%s", summarize(results))

    for result in results:
        if result.passed:
            continue
        logger.log(
            logging.ERROR if result.failed_hard else logging.WARNING,
            "%s on %s: %d row(s). %s",
            result.check.name,
            result.check.table,
            result.violations,
            result.check.description,
        )

    errors = [result for result in results if result.failed_hard]
    if errors:
        raise DataQualityError(
            f"{len(errors)} data quality check(s) failed: "
            + ", ".join(f"{result.check.name} ({result.violations} rows)" for result in errors)
        )
    return results


def main() -> None:
    try:
        run()
    except DataQualityError as exc:
        logger.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
