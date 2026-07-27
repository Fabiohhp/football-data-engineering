"""Source identifiers and season conventions shared by extract and transform.

These constants define the Bronze key layout (``s3://bronze/<source>/…``), so
both the writer and the reader must agree on them. They live here rather than in
``src/extract`` so the Spark transforms never import extraction code — that
would drag ``requests``/``tenacity`` into the Spark image for no reason and
couple the two layers together.
"""

from __future__ import annotations

from datetime import date

FOOTBALL_DATA = "football_data"
UNDERSTAT = "understat"


def season_for_date(target_date: date) -> str:
    """Return the Understat season start-year for a date.

    European seasons span two calendar years; Understat labels them by the
    starting year. August or later belongs to that year's season, otherwise the
    previous year's.
    """
    return str(target_date.year if target_date.month >= 8 else target_date.year - 1)
