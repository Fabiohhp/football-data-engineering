# Football Data Platform

End-to-end batch data platform for football (soccer) data: ingestion →
Bronze/Silver/Gold (Medallion architecture) → PostgreSQL data warehouse.

See `CLAUDE.md` for full architecture, source constraints, and conventions.

## Status

The pipeline runs end-to-end from extraction through to the Gold star schema in
PostgreSQL, and the loaded warehouse passes its data quality checks. Currently
running: PostgreSQL (warehouse), MinIO (Bronze/Silver), and a standalone Spark
cluster. Airflow orchestration is the remaining step — every stage exists as a
re-runnable, date-parameterized entry point waiting to be wired into the DAG.

Implemented so far:

- `src/common/` — config, logging, MinIO Bronze reader/writer, cross-source
  team-name normalization, shared source constants.
- `src/extract/` — Football-Data.org and Understat extractors writing raw JSON
  to Bronze (rate-limited, incremental, idempotent, retrying).
- `src/transform/` — `parsers.py` (pure, JVM-free flatteners),
  `bronze_to_silver.py` (PySpark job producing typed Parquet), and
  `silver_to_gold.py` (PySpark job loading the star schema).
- `src/quality/` — the Gold-layer data quality suite (`checks.py` catalogue,
  `run_checks.py` runner) that fails the pipeline on corrupt data.
- `sql/ddl/` — Gold-layer star schema and the staging tables (see below).
- `sql/load/` — the Silver→Gold merge statements.

## Quickstart

```bash
cp .env.example .env   # fill in FOOTBALL_DATA_API_TOKEN, etc.
docker compose up -d
docker compose ps       # confirm postgres and minio are healthy
```

- MinIO console: http://localhost:9001 (user/pass from `.env`)
- PostgreSQL: localhost:5432 (db/user/pass from `.env`)

```bash
docker compose down     # stop and remove containers (volumes persist)
```

## Running an extraction

Both extractors are incremental: they take an explicit date and fetch only that
day's matches. Run them from the host with `MINIO_ENDPOINT` pointed at the
published port (the value in `.env` is the Compose-internal hostname, which only
resolves inside the network):

```bash
MINIO_ENDPOINT=http://localhost:9000 python -m src.extract.extract_footballdata
MINIO_ENDPOINT=http://localhost:9000 python -m src.extract.extract_understat
```

Both default to yesterday; import `extract_daily_matches` /
`extract_daily_understat` and pass a `date` to backfill a specific day. Understat
takes ~3s per request by design; Football-Data pauses ~60s if it drains its
10-calls/minute budget (the big-5 default stays under it).

### Ingestion scope: the big-5 only

We extract exactly the five competitions **both** sources cover — Premier League,
La Liga, Bundesliga, Serie A, Ligue 1. Football-Data's free tier also grants the
Championship, Eredivisie, Primeira Liga, Brasileirão and the cups, but Understat
serves the big-5 exclusively, so matches from any other competition would reach
Gold with no xG and no player events. The two default lists pair 1:1 and a test
enforces it.

Verified end-to-end against `2025-11-01`: **23 matches from each source, 46 teams
from each source, all 46 joining** — no orphans in either direction.

## Silver layer (Bronze → Parquet)

`src/transform/bronze_to_silver.py` turns one date of raw Bronze JSON into typed,
deduplicated Parquet under `s3a://silver/`:

| Table | Grain | Source |
|-------|-------|--------|
| `fd_matches` | one match | Football-Data (results, referee) |
| `understat_matches` | one match | Understat (xG, plus npxG/PPDA/deep/xPts per side) |
| `player_events` | one player per match | Understat rosters |
| `shots` | one shot | Understat shots |

Run it against the Spark cluster:

```bash
docker compose up -d spark-master spark-worker   # Spark UI: http://localhost:8080
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/football/src/transform/bronze_to_silver.py --date 2025-11-01
```

Design notes:

- **Schemas are declared, never inferred.** Understat returns every scalar as a
  string and nests collections in dicts keyed by `"h"`/`"a"` and by dynamic
  roster ids, so inference would yield all-string columns shaped by whichever
  file Spark happened to sample. Types live in `bronze_to_silver.py` and are
  enforced when the DataFrame is built.
- **Parsing is pure and JVM-free.** The flatteners are in `src/transform/parsers.py`
  with no PySpark import, so CI unit-tests them without installing Spark or a
  JVM. Spark owns schema enforcement, dedup, the fixture/team-history join and
  the write.
- **Idempotent by partition.** Tables are partitioned by `match_date` and written
  with dynamic partition overwrite, so re-running a date replaces exactly that
  date. Verified: a second run of `2025-11-01` leaves counts unchanged.

Verified on `2025-11-01`: 23 / 23 / 715 / 540 rows, the two sources join 23-for-23
on `(match_date, home_team, away_team)`, and they agree on **23 of 23 scorelines** —
an independent check that the team-name canonicalization is joining the right
clubs.

## Gold layer load (Silver → PostgreSQL)

`src/transform/silver_to_gold.py` loads one date of Silver into the star schema:

```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/football/src/transform/silver_to_gold.py --date 2025-11-01
```

Design notes:

- **Stage, then merge.** Spark's JDBC writer only appends or overwrites, and
  neither is an upsert — appending duplicates a re-run, overwriting destroys
  every earlier date. So Spark lands the date's rows in `staging.*`
  (`sql/ddl/30_staging.sql`) and the SQL in `sql/load/` merges them into `gold`.
  Surrogate keys, SCD Type 2 and idempotency are decided in SQL, next to the
  rows they have to reconcile.
- **One transaction.** Every merge statement runs on one connection inside a
  single transaction, so a failure anywhere leaves Gold exactly as it was.
- **Self-verifying.** The fact loads join on team and player keys, so an
  unmapped club would silently shorten a fact table. Each one ends by asserting
  that every staged row reached Gold and raises if not — a failed run rather
  than a quietly incomplete one.
- **Matches are resolved on either source id**, not upserted on one. `fact_matches`
  has two partial unique indexes (one per source); a match that arrived
  Understat-only and later gains its Football-Data id has to update that row,
  not insert a second one. Measures are merged with `COALESCE`, so a source with
  nothing to say about a match never erases what the other source said.
- **SCD Type 2 tracks transfers, not formations.** A new `dim_players` version
  opens when the club or the name changes. Position is a Type 1 attribute: it is
  the position played *in that match*, and a substitute is literally `Sub`, so
  versioning on it would mint a row per matchday and bury the transfers.

Verified on `2025-11-01`:

| Check | Result |
|-------|--------|
| Rows loaded | 23 matches, 715 player events, 540 shots, 46 teams, 715 players, 23 referees |
| Fact FKs resolved | 0 rows on the `Unknown` member for team, player or referee |
| Player ↔ team agreement | 715/715 events where the player's dimension club equals the club he played for |
| Assists resolved | 404/540 shots carry an assisting player (the rest are unassisted) |
| **Idempotency** | second run of the same date: counts identical, `max(match_key)`/`max(player_key)` unchanged — no rows and no surrogate keys burned |
| **SCD Type 2** | simulated transfer on a later date closes the old version at `valid_to = date - 1` and opens the new one; an unchanged player is untouched |

## Gold layer schema

The Gold layer is a **star schema** in the `gold` schema of PostgreSQL, and is
the stable contract consumed by the downstream analytics (Part 2) and ML
(Part 3) subprojects. DDL lives in `sql/ddl/` and is applied automatically by
Postgres on first startup (`docker-entrypoint-initdb.d`, run in filename order:
`00` schema → `1x` dimensions → `2x` facts → `30` staging).

> `docker-entrypoint-initdb.d` only runs when the data directory is **empty**.
> If your `postgres_data` volume predates a DDL change, apply it by hand — every
> file is idempotent (`CREATE TABLE IF NOT EXISTS`, `ON CONFLICT DO NOTHING`):
>
> ```bash
> for f in sql/ddl/*.sql; do docker compose exec -T postgres \
>   psql -v ON_ERROR_STOP=1 -U football -d football_dw -q < "$f"; done
> ```

### Dimensions

| Table | Grain | SCD | Source | Notes |
|-------|-------|-----|--------|-------|
| `dim_teams` | one club | Type 1 | Football-Data + Understat | `canonical_name` is the cross-source join key (see `src/common/team_mapping.py`); each source's own spelling and id are kept beside it, so the conforming decision is auditable from the dimension itself. |
| `dim_players` | one player **version** | **Type 2** | Understat | Tracks club history via `valid_from`/`valid_to`/`is_current`; `understat_player_id` is the durable business key. Position is Type 1 (see the load notes above). |
| `dim_referees` | one official | Type 1 | Football-Data | From `match.referees`. |
| `dim_stadiums` | one venue | Type 1 | — | **Placeholder** (only the `Unknown` member). No live source: Football-Data free tier omits venue, FBref is blocked. |

### Facts

| Table | Grain | Source | Key measures |
|-------|-------|--------|--------------|
| `fact_matches` | one match | Football-Data (results) + Understat (xG) | goals (FT/HT), result, xG/npxG/ppda/deep/xpts per side |
| `fact_player_events` | one player per match | Understat rosters | minutes, goals, shots, key_passes, assists, xG, xA, xGChain, xGBuildup, cards |
| `fact_shots` | one shot | Understat shots | pitch `x`/`y`, xG, result, situation, shot_type (added beyond the base spec — the primary asset for the Part 3 xG model) |

### Design guarantees

- **No silent nulls in fact keys.** Every fact FK is `NOT NULL`; optional
  dimensions (referee, stadium) default to the `Unknown` member (key `0`).
  Genuinely source-dependent *measures* (xG for non-Understat competitions,
  scores for unfinished matches) may be null and are documented in the DDL.
- **Idempotency.** Facts carry source natural keys with unique constraints
  (partial where a source id can be absent), so Silver→Gold loads upsert instead
  of duplicating.
- **Integrity.** `CHECK` constraints enforce valid results, non-negative
  counts, distinct home/away teams, and `0 ≤ shot xG ≤ 1`.
- **Not modeled (FBref-blocked):** possession, total passes, tackles, and real
  stadium data. FBref is the only source for these and is Cloudflare-blocked; we
  deliberately do not ship all-null columns for them.
- **Not modeled (not ingested):** club crest, TLA and colours. They live only on
  Football-Data's `/competitions/{code}/teams` endpoint, which this pipeline does
  not call — same rule: no all-null columns in the contract. Adding that call
  would also give `dim_stadiums` real venues and is the obvious way to retire the
  placeholder.

Validate the DDL against a throwaway Postgres:

```bash
docker run --rm -d --name ddl-check -e POSTGRES_PASSWORD=x -e POSTGRES_DB=football_dw postgres:16
for f in sql/ddl/*.sql; do docker exec -i ddl-check psql -v ON_ERROR_STOP=1 -U postgres -d football_dw < "$f"; done
docker rm -f ddl-check
```

## Data quality checks

`src/quality/` validates the Gold layer and is the pipeline's last step — the
task that decides whether a night's load is fit to publish:

```bash
docker compose exec -w /opt/football spark-master python -m src.quality.run_checks
```

It exits non-zero (and raises `DataQualityError`, which fails an Airflow task) if
any **ERROR** check finds violations. `src/quality/checks.py` holds the catalogue
— each check is one `SELECT` counting offending rows, paired with its severity
and the reason breaking it matters.

Design notes:

- **Only rules the schema cannot express.** Primary keys, foreign keys,
  `NOT NULL` and the `CHECK` constraints are already unbreakable by a committed
  row, so asserting them would just re-prove that Postgres works. Every check
  covers something the DDL *can't* state: an SCD window that overlaps, a fact
  whose two source rows never got paired, a `result` contradicting its own score.
- **ERROR vs WARN.** ERROR means a consumer would be misled, and fails the run.
  WARN means the rule has a legitimate way of being false — a source omitting a
  field, or a state the model explicitly permits (`match_key` is nullable by
  design). Keeping the two apart is what stops a known source quirk from
  reddening the DAG nightly and training everyone to ignore it.
- **Whole warehouse, not one date.** Restricting the checks to the date just
  loaded would miss corruption an earlier run introduced — an SCD version that
  only starts overlapping once a later transfer opens the next one. Gold is small
  and these are invariants about the star schema, not about a partition of it.
- **All checks run before anything raises**, so one red run shows the whole
  shape of the problem rather than just its first symptom.

Coverage: 21 checks — cross-source id collisions and unconformed clubs
(`dim_teams`); overlapping versions, current-flag consistency, every player
having a current version (`dim_players`); fixture-grain duplication, missing
scores on finished matches, result/score agreement (`fact_matches`); the same
player counted twice under two versions, and the club he played for disagreeing
with his dimension version (`fact_player_events`); pitch coordinates, and goals
reconciling against `fact_player_events` (`fact_shots`). Each fact table is
guarded against `Unknown`-member leakage, which is how an unresolved key
surfaces given the `NOT NULL` FKs — a test enforces that guard exists.

**Verified by fault injection.** All 21 pass on the loaded warehouse, but a check
that never fires proves nothing. Each was re-run against a deliberately corrupted
copy of its target table inside a rolled-back transaction — a duplicated fixture,
a nulled score, an overlapping SCD window, an out-of-pitch coordinate, and so on.
**21/21 detected the violation they claim to catch**, and the rollbacks left row
counts unchanged.

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

pytest -q
ruff check src tests
black --check src tests
```
