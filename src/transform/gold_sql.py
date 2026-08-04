"""Pure, JVM-free pieces of the Silver -> Gold load.

Kept apart from ``silver_to_gold`` for the same reason ``parsers`` is kept apart
from ``bronze_to_silver``: nothing in here imports PySpark, so CI unit-tests it
without a JVM.

Holds the merge statements (which live as ``.sql`` files so they can be read and
run by hand against the warehouse) and the value mappings Gold expects.
"""

from __future__ import annotations

from pathlib import Path

from src.transform.parsers import WINNER_TO_RESULT

# Silver's compact result code -> the verbose form fact_matches stores. Derived
# from the Silver encoding so the two can never disagree.
RESULT_TO_GOLD = {code: winner for winner, code in WINNER_TO_RESULT.items()}

# Landing tables emptied at the start of every run. Order is irrelevant (they
# are truncated in one statement) but the list is the single place that knows
# what the load stages.
STAGING_TABLES = (
    "teams",
    "referees",
    "players",
    "matches",
    "player_events",
    "shots",
)

# sql/load/ sits next to src/ in the repo and is mounted read-only into the
# Spark image at the same relative position.
LOAD_SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "load"


def load_statements(directory: Path | None = None) -> list[tuple[str, str]]:
    """Read the merge statements in execution order.

    Returns ``(filename, sql)`` pairs sorted by the numeric filename prefix,
    which encodes the dependency order: dimensions before the facts that
    reference them, and the player-version lookup before both fact loads.

    The whole list is meant to run in **one transaction** — the lookup tables
    are ``ON COMMIT DROP`` temp tables, and a half-applied star schema is not a
    state worth keeping.
    """
    directory = directory or LOAD_SQL_DIR
    if not directory.is_dir():
        raise RuntimeError(
            f"Merge statements not found at {directory}. Inside the Spark container "
            "this means sql/ is not mounted (see docker-compose.yml)."
        )

    statements = [
        (path.name, path.read_text(encoding="utf-8")) for path in sorted(directory.glob("*.sql"))
    ]
    if not statements:
        raise RuntimeError(f"No .sql files in {directory}")
    return statements
