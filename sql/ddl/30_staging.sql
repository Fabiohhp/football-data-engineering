-- =============================================================================
-- Staging layer — landing tables for the Silver -> Gold load.
--
-- Spark cannot upsert over JDBC: its writer only appends or overwrites, which
-- would either duplicate rows or destroy history on a re-run. So the job lands
-- one date's Silver data here (TRUNCATE + append), then the SQL in `sql/load/`
-- merges staging into `gold` — surrogate keys, SCD Type 2 and idempotency are
-- all decided in SQL, where the existing Gold rows are.
--
-- These tables are scratch, not a contract: they hold exactly one run's rows,
-- carry natural keys instead of surrogate keys, and are deliberately loose about
-- constraints. Gold is where integrity is enforced; a staging row that violates
-- it must fail the merge loudly rather than never arrive.
--
-- Teams are referenced by `*_canonical` — the normalized name from
-- src.common.team_mapping — because that is the only key both sources share.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- One row per club seen in this run, with each source's spelling and id.
CREATE TABLE IF NOT EXISTS staging.teams (
    canonical_name      TEXT NOT NULL,
    football_data_name  TEXT,
    understat_name      TEXT,
    football_data_id    INTEGER,
    understat_id        INTEGER
);

-- One row per match official seen in this run (Football-Data only).
CREATE TABLE IF NOT EXISTS staging.referees (
    football_data_id  INTEGER,
    referee_name      TEXT,
    nationality       TEXT
);

-- One row per player seen in this run. `match_date` drives SCD Type 2 validity;
-- `team_canonical` resolves to the version's current_team_key.
CREATE TABLE IF NOT EXISTS staging.players (
    understat_player_id  BIGINT,
    player_name          TEXT,
    position             VARCHAR(10),
    team_canonical       TEXT,
    match_date           DATE
);

-- One row per match, already joined across the two sources in Spark.
CREATE TABLE IF NOT EXISTS staging.matches (
    football_data_match_id    INTEGER,
    understat_match_id        INTEGER,
    competition_code          VARCHAR(10),
    competition_name          TEXT,
    season                    VARCHAR(9),
    matchday                  SMALLINT,
    match_utc                 TIMESTAMPTZ,
    match_date                DATE,
    status                    VARCHAR(20),
    home_team_canonical       TEXT,
    away_team_canonical       TEXT,
    referee_football_data_id  INTEGER,
    home_goals                SMALLINT,
    away_goals                SMALLINT,
    home_goals_ht             SMALLINT,
    away_goals_ht             SMALLINT,
    result                    VARCHAR(10),
    home_xg                   NUMERIC(8,4),
    away_xg                   NUMERIC(8,4),
    home_npxg                 NUMERIC(8,4),
    away_npxg                 NUMERIC(8,4),
    home_ppda                 NUMERIC(8,4),
    away_ppda                 NUMERIC(8,4),
    home_deep                 SMALLINT,
    away_deep                 SMALLINT,
    home_xpts                 NUMERIC(6,4),
    away_xpts                 NUMERIC(6,4)
);

-- One row per player per match.
CREATE TABLE IF NOT EXISTS staging.player_events (
    understat_match_id   INTEGER,
    understat_player_id  BIGINT,
    team_canonical       TEXT,
    match_date           DATE,
    position             VARCHAR(10),
    minutes_played       SMALLINT,
    goals                SMALLINT,
    own_goals            SMALLINT,
    assists              SMALLINT,
    shots                SMALLINT,
    key_passes           SMALLINT,
    xg                   NUMERIC(8,4),
    xa                   NUMERIC(8,4),
    xg_chain             NUMERIC(8,4),
    xg_buildup           NUMERIC(8,4),
    yellow_cards         SMALLINT,
    red_cards            SMALLINT
);

-- One row per shot. The assisting player arrives as a name in the source and is
-- resolved to an Understat id against the same match's roster in Spark.
CREATE TABLE IF NOT EXISTS staging.shots (
    understat_shot_id           BIGINT,
    understat_match_id          INTEGER,
    understat_player_id         BIGINT,
    assist_understat_player_id  BIGINT,
    team_canonical              TEXT,
    match_date                  DATE,
    minute                      SMALLINT,
    is_home                     BOOLEAN,
    x                           NUMERIC(6,4),
    y                           NUMERIC(6,4),
    xg                          NUMERIC(7,4),
    result                      VARCHAR(20),
    situation                   VARCHAR(20),
    shot_type                   VARCHAR(20)
);
