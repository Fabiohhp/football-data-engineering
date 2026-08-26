-- staging.matches -> gold.fact_matches.
--
-- Not a plain ON CONFLICT upsert: the table has two partial unique indexes (one
-- per source id) and ON CONFLICT can only target one of them. A match that
-- landed Understat-only yesterday and gains its Football-Data id today would
-- miss the football_data_match_id index, insert a second row, and only then trip
-- the understat_match_id index — an error, and the wrong shape. So the existing
-- row is resolved on *either* id first, then updated or inserted explicitly.
--
-- Updates COALESCE their measures: a source that has nothing to say about a
-- match must not erase what the other source already said about it. A genuine
-- correction (a re-stated score) still applies, because it is not NULL.

CREATE TEMP TABLE _src_matches ON COMMIT DROP AS
SELECT DISTINCT ON (s.match_date, s.home_team_canonical, s.away_team_canonical)
       s.*,
       ht.team_key AS home_team_key,
       at.team_key AS away_team_key,
       COALESCE(r.referee_key, 0) AS referee_key,
       -- The ground is the *home* club's, reached club -> venue -> dimension.
       -- Both joins are LEFT: a club missing from this run's roster payload
       -- keeps the 'Unknown' member rather than dropping the match.
       COALESCE(st.stadium_key, 0) AS stadium_key,
       f.match_key AS existing_match_key
FROM staging.matches s
JOIN gold.dim_teams ht ON ht.canonical_name = s.home_team_canonical
JOIN gold.dim_teams at ON at.canonical_name = s.away_team_canonical
LEFT JOIN gold.dim_referees r ON r.football_data_id = s.referee_football_data_id
LEFT JOIN staging.stadiums sv ON sv.team_canonical = s.home_team_canonical
LEFT JOIN gold.dim_stadiums st ON st.stadium_name = sv.stadium_name
LEFT JOIN gold.fact_matches f
       ON (s.football_data_match_id IS NOT NULL
           AND f.football_data_match_id = s.football_data_match_id)
       OR (s.understat_match_id IS NOT NULL
           AND f.understat_match_id = s.understat_match_id)
WHERE s.match_date IS NOT NULL
ORDER BY s.match_date, s.home_team_canonical, s.away_team_canonical, f.match_key;

UPDATE gold.fact_matches f
SET football_data_match_id = COALESCE(s.football_data_match_id, f.football_data_match_id),
    understat_match_id     = COALESCE(s.understat_match_id, f.understat_match_id),
    competition_code       = COALESCE(s.competition_code, f.competition_code),
    competition_name       = COALESCE(s.competition_name, f.competition_name),
    season                 = COALESCE(s.season, f.season),
    matchday               = COALESCE(s.matchday, f.matchday),
    match_utc              = COALESCE(s.match_utc, f.match_utc),
    match_date             = s.match_date,
    status                 = COALESCE(s.status, f.status),
    home_team_key          = s.home_team_key,
    away_team_key          = s.away_team_key,
    -- Never downgrade a known official back to the 'Unknown' member.
    referee_key            = CASE WHEN s.referee_key <> 0 THEN s.referee_key ELSE f.referee_key END,
    -- Likewise never downgrade a known ground back to the 'Unknown' member.
    stadium_key            = CASE WHEN s.stadium_key <> 0 THEN s.stadium_key ELSE f.stadium_key END,
    home_goals             = COALESCE(s.home_goals, f.home_goals),
    away_goals             = COALESCE(s.away_goals, f.away_goals),
    home_goals_ht          = COALESCE(s.home_goals_ht, f.home_goals_ht),
    away_goals_ht          = COALESCE(s.away_goals_ht, f.away_goals_ht),
    result                 = COALESCE(s.result, f.result),
    home_xg                = COALESCE(s.home_xg, f.home_xg),
    away_xg                = COALESCE(s.away_xg, f.away_xg),
    home_npxg              = COALESCE(s.home_npxg, f.home_npxg),
    away_npxg              = COALESCE(s.away_npxg, f.away_npxg),
    home_ppda              = COALESCE(s.home_ppda, f.home_ppda),
    away_ppda              = COALESCE(s.away_ppda, f.away_ppda),
    home_deep              = COALESCE(s.home_deep, f.home_deep),
    away_deep              = COALESCE(s.away_deep, f.away_deep),
    home_xpts              = COALESCE(s.home_xpts, f.home_xpts),
    away_xpts              = COALESCE(s.away_xpts, f.away_xpts),
    loaded_at              = now()
FROM _src_matches s
WHERE f.match_key = s.existing_match_key;

INSERT INTO gold.fact_matches (
    football_data_match_id, understat_match_id,
    competition_code, competition_name, season, matchday, match_utc, match_date, status,
    home_team_key, away_team_key, referee_key, stadium_key,
    home_goals, away_goals, home_goals_ht, away_goals_ht, result,
    home_xg, away_xg, home_npxg, away_npxg, home_ppda, away_ppda,
    home_deep, away_deep, home_xpts, away_xpts
)
SELECT s.football_data_match_id, s.understat_match_id,
       s.competition_code, s.competition_name, s.season, s.matchday, s.match_utc,
       s.match_date, s.status,
       s.home_team_key, s.away_team_key, s.referee_key, s.stadium_key,
       s.home_goals, s.away_goals, s.home_goals_ht, s.away_goals_ht, s.result,
       s.home_xg, s.away_xg, s.home_npxg, s.away_npxg, s.home_ppda, s.away_ppda,
       s.home_deep, s.away_deep, s.home_xpts, s.away_xpts
FROM _src_matches s
WHERE s.existing_match_key IS NULL;

-- The joins to dim_teams above are inner, so an unmapped club would silently
-- drop a match instead of failing. Assert that nothing went missing.
DO $$
DECLARE
    missing bigint;
BEGIN
    SELECT count(*) INTO missing
    FROM staging.matches s
    LEFT JOIN gold.fact_matches f
           ON (s.football_data_match_id IS NOT NULL
               AND f.football_data_match_id = s.football_data_match_id)
           OR (s.understat_match_id IS NOT NULL
               AND f.understat_match_id = s.understat_match_id)
    WHERE f.match_key IS NULL;

    IF missing > 0 THEN
        RAISE EXCEPTION
            'fact_matches load dropped % staged match(es); the usual cause is a club whose name did not normalize into dim_teams',
            missing;
    END IF;
END $$;
