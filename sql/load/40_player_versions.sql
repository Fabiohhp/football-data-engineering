-- Resolve every (player, match date) in this run to the dim_players version that
-- was valid on that date. Both fact loads below join against this table, which
-- is what makes fact_player_events.player_key and fact_shots.player_key point at
-- the player *as he was that day* rather than at whoever is current now.
--
-- Fallback: when no version covers the date — a backfill older than the earliest
-- version we ever saw — the earliest version is used. Dropping the fact row
-- instead would lose real data for a bookkeeping reason, and the fact loads
-- assert that nothing gets dropped.

CREATE TEMP TABLE _player_version ON COMMIT DROP AS
SELECT DISTINCT ON (k.understat_player_id, k.match_date)
       k.understat_player_id,
       k.match_date,
       d.player_key
FROM (
    SELECT DISTINCT understat_player_id, match_date
      FROM staging.player_events
     WHERE understat_player_id IS NOT NULL AND match_date IS NOT NULL
    UNION
    SELECT DISTINCT understat_player_id, match_date
      FROM staging.shots
     WHERE understat_player_id IS NOT NULL AND match_date IS NOT NULL
    UNION
    SELECT DISTINCT assist_understat_player_id, match_date
      FROM staging.shots
     WHERE assist_understat_player_id IS NOT NULL AND match_date IS NOT NULL
) k
JOIN gold.dim_players d ON d.understat_player_id = k.understat_player_id
ORDER BY k.understat_player_id,
         k.match_date,
         (k.match_date >= d.valid_from
          AND (d.valid_to IS NULL OR k.match_date <= d.valid_to)) DESC,
         d.valid_from;

CREATE INDEX ON _player_version (understat_player_id, match_date);
