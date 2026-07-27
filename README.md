# Football Data Platform

End-to-end batch data platform for football (soccer) data: ingestion →
Bronze/Silver/Gold (Medallion architecture) → PostgreSQL data warehouse.

See `CLAUDE.md` for full architecture, source constraints, and conventions.

## Status

Extraction and the Bronze→Silver transform are working end-to-end. Currently
running: PostgreSQL (warehouse), MinIO (Bronze/Silver), and a standalone Spark
cluster. Airflow orchestration and the Silver→Gold load are the next steps.

Implemented so far:

- `src/common/` — config, logging, MinIO Bronze reader/writer, cross-source
  team-name normalization, shared source constants.
- `src/extract/` — Football-Data.org and Understat extractors writing raw JSON
  to Bronze (rate-limited, incremental, idempotent, retrying).
- `src/transform/` — `parsers.py` (pure, JVM-free flatteners) and
  `bronze_to_silver.py` (PySpark job producing typed Parquet).
- `sql/ddl/` — Gold-layer star schema (see below).

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

## Gold layer schema

The Gold layer is a **star schema** in the `gold` schema of PostgreSQL, and is
the stable contract consumed by the downstream analytics (Part 2) and ML
(Part 3) subprojects. DDL lives in `sql/ddl/` and is applied automatically by
Postgres on first startup (`docker-entrypoint-initdb.d`, run in filename order:
`00` schema → `1x` dimensions → `2x` facts).

### Dimensions

| Table | Grain | SCD | Source | Notes |
|-------|-------|-----|--------|-------|
| `dim_teams` | one club | Type 1 | Football-Data + Understat | `canonical_name` is the cross-source join key (see `src/common/team_mapping.py`); both source ids retained. |
| `dim_players` | one player **version** | **Type 2** | Understat | Tracks club/position history via `valid_from`/`valid_to`/`is_current`; `understat_player_id` is the durable business key. |
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

Validate the DDL against a throwaway Postgres:

```bash
docker run --rm -d --name ddl-check -e POSTGRES_PASSWORD=x -e POSTGRES_DB=football_dw postgres:16
for f in sql/ddl/*.sql; do docker exec -i ddl-check psql -v ON_ERROR_STOP=1 -U postgres -d football_dw < "$f"; done
docker rm -f ddl-check
```

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

pytest -q
ruff check src tests
black --check src tests
```
