-- staging.players -> gold.dim_players (SCD Type 2).
--
-- Tracked attributes (a change opens a new version): player_name, current team.
-- Type 1 attribute (overwritten in place, no new version): position.
--
-- Position is deliberately NOT versioned. Understat records the position a
-- player occupied *in that match* — a substitute is literally 'Sub' — so
-- versioning on it would mint a new row nearly every matchday and bury the
-- transfers the dimension exists to record. 'Sub' is discarded rather than
-- stored, so a player's position stays the last real one he played.
--
-- Backfills older than a player's current version do not rewrite history: the
-- statements below only act when the staged date is at or after `valid_from`.
-- An older date still loads its facts, pointing at the earliest known version
-- (see 40_player_versions.sql).

-- One row per player for this run: the latest date wins, and a real position
-- beats 'Sub' on the same date.
CREATE TEMP TABLE _src_players ON COMMIT DROP AS
SELECT DISTINCT ON (s.understat_player_id)
       s.understat_player_id,
       s.player_name,
       NULLIF(s.position, 'Sub') AS position,
       t.team_key,
       s.match_date
FROM staging.players s
LEFT JOIN gold.dim_teams t ON t.canonical_name = s.team_canonical
WHERE s.understat_player_id IS NOT NULL
  AND s.player_name IS NOT NULL
  AND s.match_date IS NOT NULL
ORDER BY s.understat_player_id, s.match_date DESC, (s.position = 'Sub'), s.player_name;

-- (a) Close the current version of every player whose tracked attributes moved.
-- A NULL staged team means "this run doesn't know", not "no club", so it never
-- triggers a version.
UPDATE gold.dim_players d
SET valid_to   = s.match_date - 1,
    is_current = false
FROM _src_players s
WHERE d.understat_player_id = s.understat_player_id
  AND d.is_current
  AND d.valid_from < s.match_date
  AND (d.player_name IS DISTINCT FROM s.player_name
       OR (s.team_key IS NOT NULL AND d.current_team_key IS DISTINCT FROM s.team_key));

-- (b) Insert a current version for every player that now lacks one — both
-- first-time players and the ones just closed by (a).
INSERT INTO gold.dim_players (
    understat_player_id, player_name, position, current_team_key, valid_from, is_current
)
SELECT s.understat_player_id, s.player_name, s.position, s.team_key, s.match_date, true
FROM _src_players s
LEFT JOIN gold.dim_players d
       ON d.understat_player_id = s.understat_player_id
      AND d.is_current
WHERE d.player_key IS NULL;

-- (c) Same-day change: correct the version in place instead of opening a second
-- one that would start and end on the same day. This is also what makes a
-- re-run of the same date a no-op.
UPDATE gold.dim_players d
SET player_name      = s.player_name,
    current_team_key = COALESCE(s.team_key, d.current_team_key),
    loaded_at        = now()
FROM _src_players s
WHERE d.understat_player_id = s.understat_player_id
  AND d.is_current
  AND d.valid_from = s.match_date
  AND (d.player_name IS DISTINCT FROM s.player_name
       OR (s.team_key IS NOT NULL AND d.current_team_key IS DISTINCT FROM s.team_key));

-- (d) Type 1 attribute refresh.
UPDATE gold.dim_players d
SET position  = s.position,
    loaded_at = now()
FROM _src_players s
WHERE d.understat_player_id = s.understat_player_id
  AND d.is_current
  AND d.valid_from <= s.match_date
  AND s.position IS NOT NULL
  AND d.position IS DISTINCT FROM s.position;
