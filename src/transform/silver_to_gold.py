"""Silver -> Gold: typed Parquet in MinIO to the star schema in PostgreSQL.

Run for one date; safe to re-run. Loads the four Silver tables into the ``gold``
schema: ``dim_teams``, ``dim_players``, ``dim_referees``, ``fact_matches``,
``fact_player_events`` and ``fact_shots``.

Design:

* **Stage, then merge.** Spark's JDBC writer can only append or overwrite, and
  neither is an upsert: appending duplicates a re-run, overwriting destroys
  every earlier date. So Spark lands one date's rows in ``staging.*`` and SQL in
  ``sql/load/`` merges them into Gold. Surrogate keys, SCD Type 2 and
  idempotency are decided there, next to the rows they have to reconcile.
* **One transaction.** Every merge statement runs on a single connection inside
  one transaction, so a failure anywhere leaves Gold exactly as it was. The
  statements assert that no staged row was dropped, which turns an unmapped club
  or a missing player into a failed run instead of a quietly short fact table.
* **Spark does the shaping.** Cross-source joins, the team-name conforming and
  the roster lookups happen here; the SQL only ever sees flat, conformed rows.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from functools import reduce
from typing import Any

from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

from src.common.config import MinioConfig, PostgresConfig, minio_config, postgres_config
from src.common.logging import get_logger
from src.common.sources import season_label
from src.transform.bronze_to_silver import build_spark_session
from src.transform.gold_sql import RESULT_TO_GOLD, STAGING_TABLES, load_statements

logger = get_logger(__name__)

_SIDES = (("home", "h"), ("away", "a"))


def read_silver(
    spark: SparkSession, config: MinioConfig, table: str, target_date: date
) -> DataFrame:
    """Read one Silver table, restricted to a single ``match_date`` partition."""
    path = f"s3a://{config.silver_bucket}/{table}"
    try:
        df = spark.read.parquet(path)
    except AnalysisException as exc:
        raise RuntimeError(
            f"Silver table {table!r} not found at {path}. "
            f"Run bronze_to_silver for {target_date} before loading Gold."
        ) from exc
    return df.filter(F.col("match_date") == F.lit(target_date))


def _result_to_gold(column: Column) -> Column:
    """Map Silver's ``H``/``A``/``D`` onto the verbose form Gold stores."""
    pairs: list[Column] = []
    for code, winner in RESULT_TO_GOLD.items():
        pairs.extend([F.lit(code), F.lit(winner)])
    return F.create_map(*pairs)[column]


def build_teams(fd_matches: DataFrame, understat_matches: DataFrame) -> DataFrame:
    """One row per club, carrying whichever source spellings and ids we saw.

    Both sources contribute two rows per match (home and away). The canonical
    name is already computed in Silver; this only has to fold the four streams
    together and keep the first non-null value of every source-specific column.
    """
    frames = [
        fd_matches.select(
            F.col(f"{side}_team").alias("canonical_name"),
            F.col(f"{side}_team_raw").alias("football_data_name"),
            F.lit(None).cast(StringType()).alias("understat_name"),
            F.col(f"{side}_team_source_id").alias("football_data_id"),
            F.lit(None).cast(IntegerType()).alias("understat_id"),
        )
        for side, _ in _SIDES
    ] + [
        understat_matches.select(
            F.col(f"{side}_team").alias("canonical_name"),
            F.lit(None).cast(StringType()).alias("football_data_name"),
            F.col(f"{side}_team_raw").alias("understat_name"),
            F.lit(None).cast(IntegerType()).alias("football_data_id"),
            F.col(f"{side}_team_understat_id").alias("understat_id"),
        )
        for side, _ in _SIDES
    ]

    union = reduce(DataFrame.unionByName, frames)
    return (
        union.filter(F.col("canonical_name").isNotNull())
        .groupBy("canonical_name")
        .agg(
            *[
                F.first(column, ignorenulls=True).alias(column)
                for column in (
                    "football_data_name",
                    "understat_name",
                    "football_data_id",
                    "understat_id",
                )
            ]
        )
    )


def build_referees(fd_matches: DataFrame) -> DataFrame:
    """One row per match official. Only Football-Data reports them."""
    return (
        fd_matches.select(
            F.col("referee_source_id").alias("football_data_id"),
            F.col("referee_name"),
            F.col("referee_nationality").alias("nationality"),
        )
        .filter(F.col("football_data_id").isNotNull())
        .dropDuplicates(["football_data_id"])
    )


def _with_fixture_teams(df: DataFrame, fixtures: DataFrame) -> DataFrame:
    """Resolve a row's ``side`` (``h``/``a``) to the club's canonical name.

    Understat's per-match payloads identify a club only by side, so the club has
    to come from the fixture the row belongs to.
    """
    fixture_teams = fixtures.select(
        F.col("understat_match_id"),
        F.col("home_team").alias("_home_team"),
        F.col("away_team").alias("_away_team"),
        F.col("match_date").alias("_fixture_date"),
    )
    return df.join(fixture_teams, on="understat_match_id", how="left").withColumn(
        "team_canonical",
        F.when(F.col("side") == F.lit("h"), F.col("_home_team")).otherwise(F.col("_away_team")),
    )


def build_players(player_events: DataFrame, fixtures: DataFrame) -> DataFrame:
    """One row per player seen on this date, with the club he played for.

    ``match_date`` is what the SCD Type 2 merge uses to open and close versions.
    """
    return _with_fixture_teams(player_events, fixtures).select(
        F.col("player_understat_id").alias("understat_player_id"),
        F.col("player_name"),
        F.col("position"),
        F.col("team_canonical"),
        F.coalesce(F.col("match_date"), F.col("_fixture_date")).alias("match_date"),
    )


def build_matches(
    fd_matches: DataFrame, understat_matches: DataFrame, target_date: date
) -> DataFrame:
    """One row per match, with both sources folded onto the same row.

    A full outer join on (date, home, away) — the only key the two sources share
    — so a match reported by just one of them still reaches Gold. That is why
    every shared column is a COALESCE and why the join key itself has to be
    rebuilt from both sides.
    """
    fd = fd_matches.alias("fd")
    us = understat_matches.alias("us")
    joined = fd.join(
        us,
        on=[
            fd["match_date"] == us["match_date"],
            fd["home_team"] == us["home_team"],
            fd["away_team"] == us["away_team"],
        ],
        how="full_outer",
    )

    home_goals = F.coalesce(F.col("fd.home_goals_ft"), F.col("us.home_goals"))
    away_goals = F.coalesce(F.col("fd.away_goals_ft"), F.col("us.away_goals"))
    # Understat-only matches carry no `winner` field, but they are always played
    # fixtures, so the result follows from the score.
    derived_result = (
        F.when(home_goals > away_goals, F.lit("HOME_TEAM"))
        .when(home_goals < away_goals, F.lit("AWAY_TEAM"))
        .when(home_goals == away_goals, F.lit("DRAW"))
    )

    def ppda(prefix: str) -> Column:
        """Passes allowed per defensive action — Understat ships the ratio's two
        halves separately."""
        attacking = F.col(f"us.{prefix}_ppda_att")
        defending = F.col(f"us.{prefix}_ppda_def")
        return F.when(defending > 0, attacking / defending)

    return joined.select(
        F.col("fd.source_match_id").alias("football_data_match_id"),
        F.col("us.understat_match_id").alias("understat_match_id"),
        F.col("fd.competition_code").alias("competition_code"),
        F.coalesce(F.col("fd.competition_name"), F.col("us.league")).alias("competition_name"),
        F.lit(season_label(target_date)).alias("season"),
        F.col("fd.matchday").alias("matchday"),
        F.coalesce(F.col("fd.utc_kickoff"), F.col("us.utc_kickoff")).alias("match_utc"),
        F.coalesce(F.col("fd.match_date"), F.col("us.match_date")).alias("match_date"),
        # Understat publishes played fixtures only (parse_understat_fixtures
        # filters on isResult), so an Understat-only row is finished by definition.
        F.coalesce(F.col("fd.status"), F.lit("FINISHED")).alias("status"),
        F.coalesce(F.col("fd.home_team"), F.col("us.home_team")).alias("home_team_canonical"),
        F.coalesce(F.col("fd.away_team"), F.col("us.away_team")).alias("away_team_canonical"),
        F.col("fd.referee_source_id").alias("referee_football_data_id"),
        home_goals.alias("home_goals"),
        away_goals.alias("away_goals"),
        F.col("fd.home_goals_ht").alias("home_goals_ht"),
        F.col("fd.away_goals_ht").alias("away_goals_ht"),
        F.coalesce(_result_to_gold(F.col("fd.result")), derived_result).alias("result"),
        F.col("us.home_xg").alias("home_xg"),
        F.col("us.away_xg").alias("away_xg"),
        F.col("us.home_npxg").alias("home_npxg"),
        F.col("us.away_npxg").alias("away_npxg"),
        ppda("home").alias("home_ppda"),
        ppda("away").alias("away_ppda"),
        F.col("us.home_deep").alias("home_deep"),
        F.col("us.away_deep").alias("away_deep"),
        F.col("us.home_xpts").alias("home_xpts"),
        F.col("us.away_xpts").alias("away_xpts"),
    )


def build_player_events(player_events: DataFrame, fixtures: DataFrame) -> DataFrame:
    """One row per player per match, keyed on the Understat natural pair."""
    return _with_fixture_teams(player_events, fixtures).select(
        F.col("understat_match_id"),
        F.col("player_understat_id").alias("understat_player_id"),
        F.col("team_canonical"),
        F.coalesce(F.col("match_date"), F.col("_fixture_date")).alias("match_date"),
        F.col("position"),
        F.col("minutes").alias("minutes_played"),
        F.col("goals"),
        F.col("own_goals"),
        F.col("assists"),
        F.col("shots"),
        F.col("key_passes"),
        F.col("xg"),
        F.col("xa"),
        F.col("xg_chain"),
        F.col("xg_buildup"),
        F.col("yellow_cards"),
        F.col("red_cards"),
    )


def build_shots(shots: DataFrame, player_events: DataFrame, fixtures: DataFrame) -> DataFrame:
    """One row per shot, with the assisting player resolved to an id.

    Understat names the assister but never gives his id on the shot record. The
    same match's roster has both, and a name is unique within one roster, so the
    lookup is scoped to the match — never to the whole player universe, where
    namesakes are common.
    """
    roster = player_events.select(
        F.col("understat_match_id").alias("_assist_match_id"),
        F.col("player_name").alias("_assist_name"),
        F.col("player_understat_id").alias("assist_understat_player_id"),
    ).dropDuplicates(["_assist_match_id", "_assist_name"])

    with_teams = _with_fixture_teams(shots, fixtures)
    with_assists = with_teams.join(
        roster,
        on=[
            with_teams["understat_match_id"] == roster["_assist_match_id"],
            with_teams["player_assisted"] == roster["_assist_name"],
        ],
        how="left",
    )

    return with_assists.select(
        F.col("shot_id").alias("understat_shot_id"),
        F.col("understat_match_id"),
        F.col("player_understat_id").alias("understat_player_id"),
        F.col("assist_understat_player_id"),
        F.col("team_canonical"),
        F.coalesce(F.col("match_date"), F.col("_fixture_date")).alias("match_date"),
        F.col("minute"),
        (F.col("side") == F.lit("h")).alias("is_home"),
        F.col("x"),
        F.col("y"),
        F.col("xg"),
        F.col("result"),
        F.col("situation"),
        F.col("shot_type"),
    )


def _connect(config: PostgresConfig) -> Any:
    """Open a psycopg2 connection to the warehouse.

    Imported lazily: only the Spark driver ever talks to Postgres directly, and
    the executors have no reason to carry the dependency.
    """
    import psycopg2

    return psycopg2.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=config.database,
    )


def truncate_staging(config: PostgresConfig) -> None:
    """Empty the landing tables so a run never merges a previous run's rows."""
    tables = ", ".join(f"staging.{table}" for table in STAGING_TABLES)
    with _connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(f"TRUNCATE {tables}")
    connection.close()
    logger.info("Truncated staging tables: %s", tables)


def write_staging(df: DataFrame, config: PostgresConfig, table: str) -> int:
    """Append one staging table over JDBC; return the row count written."""
    count = df.count()
    (
        df.write.format("jdbc")
        .option("url", config.jdbc_url)
        .option("dbtable", f"staging.{table}")
        .option("user", config.user)
        .option("password", config.password)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 1000)
        .mode("append")
        .save()
    )
    logger.info("Staged %d row(s) into staging.%s", count, table)
    return count


def merge_into_gold(config: PostgresConfig) -> None:
    """Run every merge statement in ``sql/load/`` in a single transaction."""
    statements = load_statements()
    connection = _connect(config)
    try:
        with connection.cursor() as cursor:
            for name, sql in statements:
                cursor.execute(sql)
                logger.info("Applied %s", name)
        connection.commit()
    except Exception:
        connection.rollback()
        logger.exception("Merge failed; Gold left unchanged")
        raise
    finally:
        connection.close()


def run(
    target_date: date,
    minio: MinioConfig | None = None,
    postgres: PostgresConfig | None = None,
) -> dict[str, int]:
    """Load one date of Silver into Gold. Returns rows staged per table."""
    minio = minio or minio_config()
    postgres = postgres or postgres_config()

    spark = build_spark_session(minio, app_name="silver_to_gold")
    try:
        fd_matches = read_silver(spark, minio, "fd_matches", target_date)
        understat_matches = read_silver(spark, minio, "understat_matches", target_date)
        player_events = read_silver(spark, minio, "player_events", target_date)
        shots = read_silver(spark, minio, "shots", target_date)

        staged_frames = {
            "teams": build_teams(fd_matches, understat_matches),
            "referees": build_referees(fd_matches),
            "players": build_players(player_events, understat_matches),
            "matches": build_matches(fd_matches, understat_matches, target_date),
            "player_events": build_player_events(player_events, understat_matches),
            "shots": build_shots(shots, player_events, understat_matches),
        }

        truncate_staging(postgres)
        staged = {table: write_staging(df, postgres, table) for table, df in staged_frames.items()}
    finally:
        spark.stop()

    merge_into_gold(postgres)
    logger.info("Silver->Gold complete for %s: %s", target_date, staged)
    return staged


def main() -> None:
    parser = argparse.ArgumentParser(description="Load one date of Silver into Gold.")
    parser.add_argument(
        "--date",
        dest="target_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today() - timedelta(days=1),
        help="Date to load (YYYY-MM-DD). Defaults to yesterday.",
    )
    args = parser.parse_args()
    run(args.target_date)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
