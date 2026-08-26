"""The datasets the football pipeline publishes, one group per medallion layer.

Airflow 3 can schedule a DAG on *assets*: a task declares an asset in its
``outlets``, and every DAG whose ``schedule`` lists that asset is queued the
moment the event lands. That is the whole handoff between the three DAGs here —
Bronze wakes Silver, Silver wakes Gold — with no cross-DAG trigger operator, no
polling sensor, and an Assets view in the UI that draws the medallion graph from
these definitions rather than from a README diagram that can drift.

Two conventions hold below.

**Assets are identified by name, not by URI.** Where each one physically lives
is configuration: the Bronze and Silver bucket names come from the
``minio_default`` Connection and the warehouse dsn from ``postgres_gold``, so an
``s3://bronze/…`` literal here would either duplicate that configuration or
quietly contradict it wherever the environment sets something else. The name is
the stable identity; the Connection stays the single source of truth for the
location. Airflow treats a name-only asset as a first-class asset.

**Every event carries the match date it was produced for**, under
:data:`MATCH_DATE_KEY`. An asset-triggered run has no logical date of its own,
so ``{{ ds }}`` cannot tell Silver or Gold which day to process — the triggering
event has to. :mod:`football_run_date` reads it back out.
"""

from __future__ import annotations

from airflow.sdk import Asset

__all__ = [
    "MATCH_DATE_KEY",
    "bronze_football_assets",
    "gold_football_assets",
    "silver_football_assets",
]

# The key under which a producing task stamps the match date on its asset event.
MATCH_DATE_KEY = "match_date"

# One asset per extractor rather than one for the whole layer: they hit
# different hosts under different rate limits and either can fail alone, so the
# graph should show which source actually landed.
bronze_football_assets: dict[str, Asset] = {
    "football_data": Asset(name="bronze_football_data_matches"),
    # The competition rosters, which is where venue data comes from. Separate
    # from the matches asset because it is a different endpoint that can fail on
    # its own, and because it is reference data: a consumer that only wants
    # venues should not wake on every night's fixtures.
    "football_data_teams": Asset(name="bronze_football_data_teams"),
    "understat": Asset(name="bronze_understat_match_data"),
}

# The five typed Parquet tables of `src/transform/bronze_to_silver.py`, listed
# individually because they are what a consumer subscribes to — Gold waits on
# all five, but a later DAG that only needs shots should be able to say so.
silver_football_assets: dict[str, Asset] = {
    "fd_matches": Asset(name="silver_fd_matches"),
    "fd_teams": Asset(name="silver_fd_teams"),
    "understat_matches": Asset(name="silver_understat_matches"),
    "player_events": Asset(name="silver_player_events"),
    "shots": Asset(name="silver_shots"),
}

# The star schema in PostgreSQL — the contract this subproject publishes to the
# analytics and ML subprojects (CLAUDE.md §1). These events are emitted by the
# data quality task, not by the load, so an update means "loaded *and* validated"
# and a consumer subscribing to them never wakes up on a bad night's data.
gold_football_assets: dict[str, Asset] = {
    "dim_teams": Asset(name="gold_dim_teams"),
    "dim_players": Asset(name="gold_dim_players"),
    "dim_referees": Asset(name="gold_dim_referees"),
    "dim_stadiums": Asset(name="gold_dim_stadiums"),
    "fact_matches": Asset(name="gold_fact_matches"),
    "fact_player_events": Asset(name="gold_fact_player_events"),
    "fact_shots": Asset(name="gold_fact_shots"),
}
