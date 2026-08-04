-- staging.player_events -> gold.fact_player_events.
--
-- Grain: one player per match, keyed on the Understat pair, so a re-extract
-- corrects a row instead of duplicating it. Understat is the only source for
-- these measures, so an update takes the new values outright rather than
-- COALESCE-ing them: a re-extract is a correction, not a partial view.
--
-- match_key stays NULL when the match is not in fact_matches yet (Understat
-- publishes rosters before every source agrees on the fixture); the natural keys
-- are always present, so a later run fills it in.

INSERT INTO gold.fact_player_events (
    understat_match_id, understat_player_id, match_key, player_key, team_key,
    match_date, position, minutes_played, goals, own_goals, assists, shots,
    key_passes, xg, xa, xg_chain, xg_buildup, yellow_cards, red_cards
)
SELECT DISTINCT ON (s.understat_match_id, s.understat_player_id)
       s.understat_match_id, s.understat_player_id,
       m.match_key, pv.player_key, t.team_key,
       s.match_date, s.position, s.minutes_played, s.goals, s.own_goals, s.assists,
       s.shots, s.key_passes, s.xg, s.xa, s.xg_chain, s.xg_buildup,
       s.yellow_cards, s.red_cards
FROM staging.player_events s
JOIN _player_version pv
     ON pv.understat_player_id = s.understat_player_id
    AND pv.match_date = s.match_date
JOIN gold.dim_teams t ON t.canonical_name = s.team_canonical
LEFT JOIN gold.fact_matches m ON m.understat_match_id = s.understat_match_id
WHERE s.understat_match_id IS NOT NULL
  AND s.understat_player_id IS NOT NULL
ORDER BY s.understat_match_id, s.understat_player_id, s.minutes_played DESC
ON CONFLICT (understat_match_id, understat_player_id) DO UPDATE SET
    match_key      = COALESCE(EXCLUDED.match_key, fact_player_events.match_key),
    player_key     = EXCLUDED.player_key,
    team_key       = EXCLUDED.team_key,
    match_date     = EXCLUDED.match_date,
    position       = EXCLUDED.position,
    minutes_played = EXCLUDED.minutes_played,
    goals          = EXCLUDED.goals,
    own_goals      = EXCLUDED.own_goals,
    assists        = EXCLUDED.assists,
    shots          = EXCLUDED.shots,
    key_passes     = EXCLUDED.key_passes,
    xg             = EXCLUDED.xg,
    xa             = EXCLUDED.xa,
    xg_chain       = EXCLUDED.xg_chain,
    xg_buildup     = EXCLUDED.xg_buildup,
    yellow_cards   = EXCLUDED.yellow_cards,
    red_cards      = EXCLUDED.red_cards,
    loaded_at      = now();

-- The player and team joins are inner: assert they dropped nothing.
DO $$
DECLARE
    missing bigint;
BEGIN
    SELECT count(*) INTO missing
    FROM staging.player_events s
    LEFT JOIN gold.fact_player_events f
           ON f.understat_match_id = s.understat_match_id
          AND f.understat_player_id = s.understat_player_id
    WHERE f.player_event_key IS NULL;

    IF missing > 0 THEN
        RAISE EXCEPTION
            'fact_player_events load dropped % staged row(s); check that every player reached dim_players and every club reached dim_teams',
            missing;
    END IF;
END $$;
