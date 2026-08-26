"""Bronze -> Silver: raw JSON in MinIO to typed, deduplicated Parquet.

Run for one date; safe to re-run. Produces five Silver tables under
``s3a://silver/``:

===================== ============================================ =============
Table                 Grain                                        Source
===================== ============================================ =============
``fd_matches``        one match                                    Football-Data
``understat_matches`` one match (xG + advanced team metrics)       Understat
``player_events``     one player per match                         Understat
``shots``             one shot                                     Understat
===================== ============================================ =============

Design:

* **Explicit schemas, never inference.** Understat delivers every scalar as a
  string and nests collections in dicts keyed by ``"h"``/``"a"`` and by dynamic
  ids; inferred schemas would be all-string and shaped by whichever file Spark
  sampled first. Types are declared here and enforced on construction.
* **Pure parsing, Spark for the rest.** Flattening lives in ``parsers`` (no JVM,
  unit-tested in CI). Spark owns schema enforcement, deduplication, the
  fixture/team-history join and the partitioned write.
* **Idempotent by partition.** Tables are partitioned by ``match_date`` and
  written with dynamic partition overwrite, so re-running a date replaces
  exactly that date and leaves other days untouched.
"""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.config import MinioConfig, minio_config
from src.common.logging import get_logger
from src.common.minio_client import BronzeReader, _build_s3_client, ensure_bucket
from src.common.sources import FOOTBALL_DATA as FOOTBALL_DATA_SOURCE
from src.common.sources import UNDERSTAT as UNDERSTAT_SOURCE
from src.common.sources import season_for_date
from src.transform.parsers import (
    parse_football_data_matches,
    parse_football_data_teams,
    parse_understat_fixtures,
    parse_understat_rosters,
    parse_understat_shots,
    parse_understat_team_history,
)

logger = get_logger(__name__)

FD_MATCHES_SCHEMA = StructType(
    [
        StructField("source_match_id", IntegerType(), nullable=False),
        StructField("competition_code", StringType()),
        StructField("competition_name", StringType()),
        StructField("season_id", IntegerType()),
        StructField("utc_kickoff", TimestampType(), nullable=False),
        StructField("match_date", DateType(), nullable=False),
        StructField("matchday", IntegerType()),
        StructField("stage", StringType()),
        StructField("status", StringType()),
        StructField("home_team", StringType(), nullable=False),
        StructField("home_team_raw", StringType()),
        StructField("home_team_source_id", IntegerType()),
        StructField("away_team", StringType(), nullable=False),
        StructField("away_team_raw", StringType()),
        StructField("away_team_source_id", IntegerType()),
        # Null until a match is played — see parsers.parse_football_data_matches.
        StructField("home_goals_ft", IntegerType()),
        StructField("away_goals_ft", IntegerType()),
        StructField("home_goals_ht", IntegerType()),
        StructField("away_goals_ht", IntegerType()),
        StructField("result", StringType()),
        StructField("referee_source_id", IntegerType()),
        StructField("referee_name", StringType()),
        StructField("referee_nationality", StringType()),
    ]
)

# Football-Data's competition rosters. `match_date` is the *ingestion* date, not
# a fixture date: this is reference data, and stamping the run's date keeps the
# table partitioned exactly like every other Silver table so `read_silver` needs
# no special case.
FD_TEAMS_SCHEMA = StructType(
    [
        StructField("source_team_id", IntegerType(), nullable=False),
        StructField("team", StringType(), nullable=False),
        StructField("team_raw", StringType()),
        StructField("short_name", StringType()),
        StructField("tla", StringType()),
        # The club's home ground. Null for a club the source has no venue for.
        StructField("venue", StringType()),
        # Free-form and undecomposed — see parsers.parse_football_data_teams.
        StructField("address", StringType()),
        StructField("country", StringType()),
        StructField("competition_code", StringType()),
        StructField("match_date", DateType(), nullable=False),
    ]
)

UNDERSTAT_FIXTURES_SCHEMA = StructType(
    [
        StructField("understat_match_id", IntegerType(), nullable=False),
        StructField("league", StringType(), nullable=False),
        StructField("season", StringType(), nullable=False),
        StructField("utc_kickoff", TimestampType(), nullable=False),
        StructField("match_date", DateType(), nullable=False),
        StructField("home_team", StringType(), nullable=False),
        StructField("home_team_raw", StringType()),
        StructField("home_team_understat_id", IntegerType()),
        StructField("away_team", StringType(), nullable=False),
        StructField("away_team_raw", StringType()),
        StructField("away_team_understat_id", IntegerType()),
        StructField("home_goals", IntegerType()),
        StructField("away_goals", IntegerType()),
        StructField("home_xg", DoubleType()),
        StructField("away_xg", DoubleType()),
    ]
)

UNDERSTAT_HISTORY_SCHEMA = StructType(
    [
        StructField("league", StringType(), nullable=False),
        StructField("season", StringType(), nullable=False),
        StructField("team", StringType(), nullable=False),
        StructField("team_raw", StringType()),
        StructField("team_understat_id", IntegerType()),
        StructField("match_date", DateType(), nullable=False),
        StructField("side", StringType(), nullable=False),
        StructField("xg", DoubleType()),
        StructField("xga", DoubleType()),
        StructField("npxg", DoubleType()),
        StructField("npxga", DoubleType()),
        StructField("npxgd", DoubleType()),
        StructField("ppda_att", IntegerType()),
        StructField("ppda_def", IntegerType()),
        StructField("ppda_allowed_att", IntegerType()),
        StructField("ppda_allowed_def", IntegerType()),
        StructField("deep", IntegerType()),
        StructField("deep_allowed", IntegerType()),
        StructField("scored", IntegerType()),
        StructField("missed", IntegerType()),
        StructField("xpts", DoubleType()),
        StructField("result", StringType()),
    ]
)

PLAYER_EVENTS_SCHEMA = StructType(
    [
        StructField("understat_match_id", IntegerType(), nullable=False),
        StructField("roster_id", IntegerType()),
        StructField("player_understat_id", IntegerType(), nullable=False),
        StructField("player_name", StringType(), nullable=False),
        StructField("team_understat_id", IntegerType()),
        StructField("side", StringType(), nullable=False),
        StructField("position", StringType()),
        StructField("position_order", IntegerType()),
        StructField("minutes", IntegerType()),
        StructField("goals", IntegerType()),
        StructField("own_goals", IntegerType()),
        StructField("shots", IntegerType()),
        StructField("assists", IntegerType()),
        StructField("key_passes", IntegerType()),
        StructField("xg", DoubleType()),
        StructField("xa", DoubleType()),
        StructField("xg_chain", DoubleType()),
        StructField("xg_buildup", DoubleType()),
        StructField("yellow_cards", IntegerType()),
        StructField("red_cards", IntegerType()),
    ]
)

SHOTS_SCHEMA = StructType(
    [
        StructField("shot_id", IntegerType(), nullable=False),
        StructField("understat_match_id", IntegerType(), nullable=False),
        StructField("match_date", DateType()),
        StructField("season", StringType()),
        StructField("side", StringType(), nullable=False),
        StructField("minute", IntegerType()),
        StructField("player_understat_id", IntegerType()),
        StructField("player_name", StringType()),
        StructField("player_assisted", StringType()),
        StructField("result", StringType()),
        StructField("situation", StringType()),
        StructField("shot_type", StringType()),
        StructField("last_action", StringType()),
        StructField("x", DoubleType()),
        StructField("y", DoubleType()),
        StructField("xg", DoubleType()),
    ]
)

# Advanced team metrics lifted from team history onto each side of a fixture.
_HISTORY_METRICS = (
    "npxg",
    "npxga",
    "npxgd",
    "ppda_att",
    "ppda_def",
    "ppda_allowed_att",
    "ppda_allowed_def",
    "deep",
    "deep_allowed",
    "xpts",
)


def build_spark_session(config: MinioConfig, app_name: str = "bronze_to_silver") -> SparkSession:
    """Create a Spark session wired to MinIO over ``s3a``.

    MinIO needs path-style access (no DNS-based bucket subdomains) and plain
    HTTP locally. Credentials come from the shared config, never from a
    hardcoded literal or a baked-in ``core-site.xml``.
    """
    endpoint = config.endpoint
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower()
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Re-running a date must replace only that date's partitions.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
    )
    master = os.environ.get("SPARK_MASTER_URL")
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def _rows_to_dataframe(
    spark: SparkSession, rows: list[dict[str, Any]], schema: StructType
) -> DataFrame:
    """Build a DataFrame from dict rows, ordered strictly by ``schema``.

    Going through explicit tuples rather than handing Spark the dicts means a
    parser that adds, drops or renames a key fails loudly here instead of
    silently producing a null column.
    """
    names = [field.name for field in schema.fields]
    tuples = [tuple(row.get(name) for name in names) for row in rows]
    return spark.createDataFrame(tuples, schema=schema)


def _bronze_date_prefix(source: str, dataset: str, target_date: date) -> str:
    """Bronze key prefix for one source/dataset on one date."""
    return (
        f"{source}/{dataset}"
        f"/year={target_date.year:04d}/month={target_date.month:02d}/day={target_date.day:02d}/"
    )


def load_fd_matches(spark: SparkSession, reader: BronzeReader, target_date: date) -> DataFrame:
    """Read Football-Data Bronze objects for a date into a typed DataFrame."""
    rows: list[dict[str, Any]] = []
    for key in reader.list_keys(_bronze_date_prefix(FOOTBALL_DATA_SOURCE, "matches", target_date)):
        rows.extend(parse_football_data_matches(reader.read_json(key)))
    logger.info("Football-Data: parsed %d match row(s) for %s", len(rows), target_date)
    # One competition per object, so duplicates only appear if a day is
    # re-extracted into differently-named keys; dedup defensively regardless.
    return _rows_to_dataframe(spark, rows, FD_MATCHES_SCHEMA).dropDuplicates(["source_match_id"])


def load_fd_teams(spark: SparkSession, reader: BronzeReader, target_date: date) -> DataFrame:
    """Read Football-Data team rosters for a date into a typed DataFrame.

    One club can appear in only one competition per season, but dedup on the
    source id anyway so a re-extract into differently-named keys cannot double
    the roster.
    """
    rows: list[dict[str, Any]] = []
    for key in reader.list_keys(_bronze_date_prefix(FOOTBALL_DATA_SOURCE, "teams", target_date)):
        for row in parse_football_data_teams(reader.read_json(key)):
            rows.append({**row, "match_date": target_date})
    logger.info("Football-Data: parsed %d team row(s) for %s", len(rows), target_date)
    return _rows_to_dataframe(spark, rows, FD_TEAMS_SCHEMA).dropDuplicates(["source_team_id"])


def load_understat_league_data(
    spark: SparkSession, reader: BronzeReader, target_date: date
) -> tuple[DataFrame, DataFrame]:
    """Read Understat league payloads into (fixtures, team history) DataFrames.

    Both are filtered to ``target_date``: the Bronze object holds the whole
    season, but a daily run must only ever emit that day's rows.
    """
    season = season_for_date(target_date)
    fixtures: list[dict[str, Any]] = []
    history: list[dict[str, Any]] = []
    for key in reader.list_keys(_bronze_date_prefix(UNDERSTAT_SOURCE, "league_data", target_date)):
        payload = reader.read_json(key)
        # Keys look like "<League>_<season>.json"; the league is the identity we
        # carry into Silver.
        league = key.rsplit("/", 1)[-1].removesuffix(".json").rsplit("_", 1)[0]
        fixtures.extend(parse_understat_fixtures(payload, league, season, target_date))
        history.extend(parse_understat_team_history(payload, league, season, target_date))

    logger.info(
        "Understat: parsed %d fixture(s) and %d team-history row(s) for %s",
        len(fixtures),
        len(history),
        target_date,
    )
    fixtures_df = _rows_to_dataframe(spark, fixtures, UNDERSTAT_FIXTURES_SCHEMA).dropDuplicates(
        ["understat_match_id"]
    )
    history_df = _rows_to_dataframe(spark, history, UNDERSTAT_HISTORY_SCHEMA).dropDuplicates(
        ["team", "match_date", "side"]
    )
    return fixtures_df, history_df


def load_understat_match_data(
    spark: SparkSession, reader: BronzeReader, target_date: date
) -> tuple[DataFrame, DataFrame]:
    """Read Understat per-match payloads into (player events, shots)."""
    events: list[dict[str, Any]] = []
    shots: list[dict[str, Any]] = []
    for key in reader.list_keys(_bronze_date_prefix(UNDERSTAT_SOURCE, "match_data", target_date)):
        payload = reader.read_json(key)
        # Roster records carry no match id; it is the Bronze filename.
        match_id = int(key.rsplit("/", 1)[-1].removesuffix(".json"))
        events.extend(parse_understat_rosters(payload, match_id))
        shots.extend(parse_understat_shots(payload))

    logger.info(
        "Understat: parsed %d player event(s) and %d shot(s) for %s",
        len(events),
        len(shots),
        target_date,
    )
    events_df = _rows_to_dataframe(spark, events, PLAYER_EVENTS_SCHEMA).dropDuplicates(
        ["understat_match_id", "roster_id"]
    )
    shots_df = _rows_to_dataframe(spark, shots, SHOTS_SCHEMA).dropDuplicates(["shot_id"])
    return events_df, shots_df


def enrich_fixtures_with_team_history(fixtures: DataFrame, history: DataFrame) -> DataFrame:
    """Attach per-side advanced metrics (npxG, PPDA, deep, xPts) to fixtures.

    History rows carry no match id, so they join on (team, match_date, side) —
    the only key Understat gives us. A club plays at most once per date, which
    makes that combination unique.
    """
    enriched = fixtures
    for side, team_column in (("h", "home_team"), ("a", "away_team")):
        prefix = "home" if side == "h" else "away"
        side_history = history.filter(F.col("side") == side).select(
            F.col("team").alias("_join_team"),
            F.col("match_date").alias("_join_date"),
            *[F.col(metric).alias(f"{prefix}_{metric}") for metric in _HISTORY_METRICS],
        )
        enriched = enriched.join(
            side_history,
            on=(F.col(team_column) == F.col("_join_team"))
            & (enriched["match_date"] == F.col("_join_date")),
            how="left",
        ).drop("_join_team", "_join_date")
    return enriched


def write_silver(df: DataFrame, config: MinioConfig, table: str) -> int:
    """Write one Silver table as date-partitioned Parquet; return the row count."""
    count = df.count()
    path = f"s3a://{config.silver_bucket}/{table}"
    (df.repartition("match_date").write.mode("overwrite").partitionBy("match_date").parquet(path))
    logger.info("Wrote %d row(s) to %s", count, path)
    return count


def run(target_date: date, config: MinioConfig | None = None) -> dict[str, int]:
    """Transform one date of Bronze into Silver. Returns rows written per table."""
    config = config or minio_config()
    ensure_bucket(_build_s3_client(config), config.silver_bucket)

    reader = BronzeReader(config=config)
    spark = build_spark_session(config)
    try:
        fd_matches = load_fd_matches(spark, reader, target_date)
        fd_teams = load_fd_teams(spark, reader, target_date)
        fixtures, history = load_understat_league_data(spark, reader, target_date)
        events, shots = load_understat_match_data(spark, reader, target_date)
        understat_matches = enrich_fixtures_with_team_history(fixtures, history)

        written = {
            "fd_matches": write_silver(fd_matches, config, "fd_matches"),
            "fd_teams": write_silver(fd_teams, config, "fd_teams"),
            "understat_matches": write_silver(understat_matches, config, "understat_matches"),
            "player_events": write_silver(
                events.join(
                    fixtures.select("understat_match_id", "match_date"),
                    on="understat_match_id",
                    how="left",
                ),
                config,
                "player_events",
            ),
            "shots": write_silver(shots, config, "shots"),
        }
    finally:
        spark.stop()

    logger.info("Bronze->Silver complete for %s: %s", target_date, written)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform one date of Bronze into Silver.")
    parser.add_argument(
        "--date",
        dest="target_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today() - timedelta(days=1),
        help="Date to process (YYYY-MM-DD). Defaults to yesterday.",
    )
    args = parser.parse_args()
    run(args.target_date)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
