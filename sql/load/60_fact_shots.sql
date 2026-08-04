-- staging.shots -> gold.fact_shots.
--
-- Grain: one shot, keyed on the Understat shot id. The shooter must resolve to a
-- dim_players version (inner join, asserted below); the assisting player is
-- optional and stays NULL when the shot was unassisted.

INSERT INTO gold.fact_shots (
    understat_shot_id, understat_match_id, match_key, player_key, team_key,
    assist_player_key, minute, is_home, x, y, xg, result, situation, shot_type
)
SELECT DISTINCT ON (s.understat_shot_id)
       s.understat_shot_id, s.understat_match_id,
       m.match_key, pv.player_key, t.team_key, pva.player_key,
       s.minute, s.is_home, s.x, s.y, s.xg, s.result, s.situation, s.shot_type
FROM staging.shots s
JOIN _player_version pv
     ON pv.understat_player_id = s.understat_player_id
    AND pv.match_date = s.match_date
JOIN gold.dim_teams t ON t.canonical_name = s.team_canonical
LEFT JOIN _player_version pva
     ON pva.understat_player_id = s.assist_understat_player_id
    AND pva.match_date = s.match_date
LEFT JOIN gold.fact_matches m ON m.understat_match_id = s.understat_match_id
WHERE s.understat_shot_id IS NOT NULL
  AND s.understat_player_id IS NOT NULL
ORDER BY s.understat_shot_id
ON CONFLICT (understat_shot_id) DO UPDATE SET
    understat_match_id = EXCLUDED.understat_match_id,
    match_key          = COALESCE(EXCLUDED.match_key, fact_shots.match_key),
    player_key         = EXCLUDED.player_key,
    team_key           = EXCLUDED.team_key,
    assist_player_key  = EXCLUDED.assist_player_key,
    minute             = EXCLUDED.minute,
    is_home            = EXCLUDED.is_home,
    x                  = EXCLUDED.x,
    y                  = EXCLUDED.y,
    xg                 = EXCLUDED.xg,
    result             = EXCLUDED.result,
    situation          = EXCLUDED.situation,
    shot_type          = EXCLUDED.shot_type,
    loaded_at          = now();

DO $$
DECLARE
    missing bigint;
BEGIN
    SELECT count(*) INTO missing
    FROM staging.shots s
    LEFT JOIN gold.fact_shots f ON f.understat_shot_id = s.understat_shot_id
    WHERE f.shot_key IS NULL;

    IF missing > 0 THEN
        RAISE EXCEPTION
            'fact_shots load dropped % staged shot(s); check that every shooter reached dim_players and every club reached dim_teams',
            missing;
    END IF;
END $$;
