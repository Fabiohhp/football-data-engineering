-- staging.teams -> gold.dim_teams (SCD Type 1 upsert).
--
-- `canonical_name` is the business key. A club can appear in this run under one
-- source only (Understat covers the big-5, Football-Data covers more), so an
-- update never overwrites a known value with NULL — it fills gaps.

INSERT INTO gold.dim_teams (
    canonical_name, football_data_name, understat_name, football_data_id, understat_id
)
SELECT s.canonical_name,
       -- The grain is already one row per club (Spark groups them); max() is a
       -- defensive collapse so a duplicate can never abort the whole load.
       max(s.football_data_name),
       max(s.understat_name),
       max(s.football_data_id),
       max(s.understat_id)
FROM staging.teams s
WHERE s.canonical_name IS NOT NULL
GROUP BY s.canonical_name
ON CONFLICT (canonical_name) DO UPDATE SET
    football_data_name = COALESCE(EXCLUDED.football_data_name, dim_teams.football_data_name),
    understat_name     = COALESCE(EXCLUDED.understat_name, dim_teams.understat_name),
    football_data_id   = COALESCE(EXCLUDED.football_data_id, dim_teams.football_data_id),
    understat_id       = COALESCE(EXCLUDED.understat_id, dim_teams.understat_id),
    loaded_at          = now();
