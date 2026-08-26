-- staging.stadiums -> gold.dim_stadiums.
--
-- Runs before 30_fact_matches.sql, which resolves each fixture's stadium_key
-- through this table (the numeric filename prefix is the dependency order —
-- see src.transform.gold_sql.load_statements).
--
-- DISTINCT ON, not a plain SELECT: staging.stadiums is one row per *club*, and
-- clubs share grounds (Inter and Milan at San Siro, Roma and Lazio at the
-- Olimpico). Feeding both rows to ON CONFLICT would fail outright — Postgres
-- refuses to affect the same row twice in one INSERT — so the ground is
-- collapsed here. The two clubs still resolve to the same stadium_key, which is
-- the correct answer.
--
-- Updates COALESCE, for the same reason every other load does: a roster payload
-- that omits an address must not erase one we already have.

INSERT INTO gold.dim_stadiums (stadium_name, address, country)
SELECT DISTINCT ON (s.stadium_name)
       s.stadium_name,
       s.address,
       s.country
FROM staging.stadiums s
WHERE s.stadium_name IS NOT NULL
ORDER BY s.stadium_name, s.address NULLS LAST, s.country NULLS LAST
ON CONFLICT (stadium_name) DO UPDATE
SET address   = COALESCE(EXCLUDED.address, gold.dim_stadiums.address),
    country   = COALESCE(EXCLUDED.country, gold.dim_stadiums.country),
    loaded_at = now();
