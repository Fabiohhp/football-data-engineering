-- =============================================================================
-- Gold layer — analytics-ready star schema (facts + dimensions).
--
-- This schema is the CONTRACT between this pipeline (Part 1) and the downstream
-- analytics (Part 2) and ML (Part 3) subprojects. Keep it clean, well-typed,
-- constrained and stable. All Gold objects live in the `gold` schema.
--
-- Files in sql/ddl/ are executed by Postgres (docker-entrypoint-initdb.d) in
-- lexical order, so the numeric prefixes enforce dependency order:
--   00  schema
--   10-19  dimensions
--   20-29  facts (reference dimensions)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;
