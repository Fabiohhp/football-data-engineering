-- staging.referees -> gold.dim_referees (SCD Type 1 upsert).
--
-- Rows without a Football-Data id are dropped rather than inserted: the id is
-- the business key, and a name alone cannot be deduplicated across runs. Matches
-- whose referee is unknown point at the 'Unknown' member (referee_key 0)
-- instead, which is what the feed's frequent empty `referees` array means.

INSERT INTO gold.dim_referees (football_data_id, referee_name, nationality)
SELECT s.football_data_id,
       max(s.referee_name),
       max(s.nationality)
FROM staging.referees s
WHERE s.football_data_id IS NOT NULL
  AND s.referee_name IS NOT NULL
GROUP BY s.football_data_id
ON CONFLICT (football_data_id) DO UPDATE SET
    referee_name = EXCLUDED.referee_name,
    nationality  = COALESCE(EXCLUDED.nationality, dim_referees.nationality),
    loaded_at    = now();
