# How the pipeline works

The detail behind the [README](../README.md): how the DAGs are wired, how each layer is built
and verified, the Gold schema, the quality suite and the development setup.

- [Orchestration (Airflow)](#orchestration-airflow)
- [Backfilling whole seasons](#backfilling-whole-seasons)
- [Running a stage by hand](#running-a-stage-by-hand)
- [Silver layer (Bronze → Parquet)](#silver-layer-bronze--parquet)
- [Gold layer load (Silver → PostgreSQL)](#gold-layer-load-silver--postgresql)
- [Gold layer schema](#gold-layer-schema)
- [Data quality checks](#data-quality-checks)
- [Development](#development)

## Orchestration (Airflow)

**Three DAGs, one per medallion layer, chained by assets.** Only Bronze is on a
clock; each layer publishes the datasets it produces, and the next one is queued
by Airflow the moment they all land. All three are unpaused on startup, so a
fresh stack begins scheduling immediately.

```
bronze_football_matches            0 6 * * *
  check_if_api_is_available
        ├─── extract_football_data ───────► ▣ bronze_football_data_matches
        ├─── extract_football_data_teams ─► ▣ bronze_football_data_teams
        └─── extract_understat ───────────► ▣ bronze_understat_match_data
                                              │  (all three required)
silver_football_matches   ◄───────────────────┘
  resolve_match_date ─► process_bronze_to_silver_spark ─► publish_silver_tables
                                                             │
                                       ▣ silver_fd_matches   │
                                       ▣ silver_fd_teams
                                       ▣ silver_understat_matches
                                       ▣ silver_player_events
                                       ▣ silver_shots
                                          │  (all five required)
gold_football_star_schema ◄───────────────┘
  resolve_match_date ─► load_silver_to_gold_postgres ─► data_quality_check
                                                             │
                                       ▣ gold_dim_teams / dim_players / dim_referees
                                       ▣ gold_dim_stadiums
                                       ▣ gold_fact_matches / fact_player_events / fact_shots
```

Run the whole chain for one date — triggering Bronze is enough, the rest follows
from the assets:

```bash
docker compose exec airflow-scheduler airflow dags trigger bronze_football_matches \
  --conf '{"match_date": "2025-11-01"}'
```

Re-run one layer only, which is how a bad Silver or Gold load is repaired
without re-scraping the sources (every stage is idempotent on its date, so this
corrects rather than duplicates):

```bash
docker compose exec airflow-scheduler airflow dags trigger silver_football_matches \
  --conf '{"match_date": "2025-11-01"}'
```

### Design notes

- **Assets, not a single graph.** Bronze, Silver and Gold each own their
  schedule, retries and failures. A night when Understat is unreachable leaves
  Bronze red and Silver simply never triggered — instead of one long DAG whose
  downstream stages sit in `upstream_failed` — and any layer can be replayed on
  its own. The Assets view draws the medallion graph from
  `airflow/dags/assets/football.py`, so that graph is generated from the
  same definitions the scheduler uses. A list in `schedule=` is an AND: Silver
  waits for *all three* Bronze assets, Gold for *all five* Silver tables.
- **The date travels with the handoff.** An asset-triggered run has no logical
  date of its own, and the wall clock is not the match date anyway (Bronze runs
  at 06:00 for the previous evening's fixtures). So Bronze stamps the date it
  resolved onto every asset event it publishes, and `resolve_match_date` reads it back off the
  triggering events downstream. A run triggered with no date at all, or by
  events carrying two different dates, fails with a message saying how to re-run
  it — inventing one from the clock is the bug this design exists to prevent.
  Any DAG also accepts `--conf '{"match_date": …}'` to aim a run at one day.
- **The date comes from the run, never from `date.today()`.** Every stage already
  took an explicit date, so the DAGs supply one and nothing else. A retry after
  midnight, a manual re-run and a repair therefore all process the day they were
  meant to — a task that computed "yesterday" itself would look correct until the
  first retry and then overwrite a good partition with an empty one. Tests assert
  the templating rather than trusting it.
- **Bronze processes yesterday: `macros.ds_add(ds, -1)`.** Airflow 3's cron
  timetable gives a run a *zero-width* data interval — `data_interval_start ==
  data_interval_end == logical_date`, all the moment it fires — so `{{ ds }}` on the
  06:00 run of day D is D itself, whose fixtures kick off that evening. Asking
  the sources for them at breakfast returns `TIMED` rows from Football-Data and
  nothing at all from Understat, which publishes a match only once it has been
  played. The result is not an error, it is a night of matches loaded with every
  expected-goals measure null. Under Airflow 2 the interval was
  `[06:00 D-1, 06:00 D)` and `ds` was already the previous day, which is why this
  needed to become explicit on the way to Airflow 3.
- **Gold is published by the quality gate, not by the load.** The Gold assets are
  the contract with the analytics and ML subprojects, so their events are emitted
  by `data_quality_check`: an update means "loaded *and* validated", and a future
  consumer subscribing to `gold_fact_matches` never wakes up on a bad night.
- **Scheduled at 06:00 UTC, not midnight.** Combined with the day offset above,
  a run wakes up a full eight hours after the last fixture of the day it is about
  to process. Matches finish around 22:00 UTC and Understat needs a couple of
  hours to publish shot data, so a midnight cut would still be racing the sources
  for the evening that had just ended.
- **Backfills cannot stampede the sources.** `catchup=False` with
  `max_active_runs=1`: a stack switched off for a week does not wake up and fire
  seven concurrent runs at two rate-limited sources. Missed days are re-run
  deliberately, one at a time.
- **One pool guards the Football-Data rate limit.** Per-request politeness is
  not enough once requests run in parallel: the sensor and the two Football-Data
  extractors each build their own client with their own view of the
  10-calls-per-minute budget, but that budget is enforced server-side and shared.
  Running them at once means racing for it with no task able to see another's
  spend. All three therefore hold a slot in the single-slot
  `football_data_api_calls` pool, so only one talks to the API at a time and its
  client's view of the budget is authoritative again. Understat stays out of it —
  different host, different limit, and serialising the two independent sources
  would roughly double a Bronze run for nothing. The pool is registered by
  `airflow-init` (an upsert, so `docker compose up` is safe to repeat); a task
  naming a pool that does not exist sits queued forever instead of failing.
- **Credentials come from Airflow Connections, never from the DAG.** The four
  connections (`spark_default`, `postgres_gold`, `minio_default`,
  `football_data_api`) are defined as `AIRFLOW_CONN_*` environment variables in
  `docker-compose.yml`, so they exist the moment the stack starts, with no
  `airflow connections add` step to forget, and they never reach the metadata
  database. `airflow/dags/football_connections.py` turns each one into the frozen
  config dataclass the pipeline already expects, which keeps `src/` free of any
  Airflow import — the transforms still run under a bare `spark-submit`.
- **The Spark stages get Jinja, not values.** Their driver is a separate process
  configured by environment, so `env_vars` is passed as
  `{{ conn.minio_default.password }}`-style templates that Airflow renders at
  task-run time and masks in the UI. Resolving them at parse time would query the
  backend on every parse cycle and bake the secret into the serialized DAG.
- **The quality gate does not retry.** Every other task retries twice with
  exponential backoff, because their failures are transient — a source having a
  bad minute, a worker restarting. Re-reading corrupt rows just fails again 15
  minutes later with the report buried under two retries.
- **The availability sensor uses `reschedule` mode.** A poke-mode sensor holds an
  executor slot for its whole timeout, and under LocalExecutor that is one of the
  few slots the Spark driver needs later.

### Verified end-to-end

`2025-11-01` re-run through the three-DAG chain, triggering only Bronze — a date
the warehouse had already loaded, so the idempotency claim is measured rather
than asserted:

| Check | Result |
|-------|--------|
| Tasks | 10/10 succeeded, first attempt: Bronze 1m38s, Silver 17s, Gold 15s |
| **Asset handoff** | Bronze ended 23:56:23 → `asset_triggered__…` Silver run created the same second; Silver ended 23:56:39 → Gold run created the same second. No sensor, no trigger operator |
| **Date propagation** | all 15 asset events carry `{"match_date": "2025-11-01"}` — the three Bronze ones with their object counts — and both Spark stages were submitted with `--date 2025-11-01`, from the conf, through XCom |
| Spark placement | both apps `FINISHED` on the standalone cluster with 2 cores — the driver in the Airflow container, executors on `spark-worker` |
| Secrets | rendered `env_vars` masked in the metadata store |
| **Idempotency** | after the re-run: 23 matches / 715 events / 540 shots — unchanged, and `fact_matches_unique_fixture_grain` still passes, so the merge updated in place rather than inserting |
| **Venue resolution** | 24/24 matches across both loaded dates resolve to a real ground (`stadium_key <> 0`); 109 stadiums in `dim_stadiums` |
| Data quality | all 23 checks passed, which is what published the seven Gold assets |
| Empty day | a scheduled run for a pre-season date (no fixtures) succeeded end-to-end through all three DAGs and loaded nothing |

### Where the build departed from the original plan

- **LocalExecutor, not a webserver, scheduler and worker.** That split describes
  CeleryExecutor, which needs a Redis broker and a dedicated worker
  container on top of what is already running, for no benefit at this data
  volume (one date is ~1300 rows) — and the Spark driver runs in the scheduler
  container under client mode, so it needs the headroom. Tasks run as
  subprocesses of the scheduler instead. Measured footprint of the four Airflow
  components as configured: **~1.1 GB** (scheduler 309 MB, dag-processor 299 MB,
  triggerer 278 MB, api-server 232 MB).
- **Airflow 3.1.8, not Airflow 2.** `schedule=` replaces `schedule_interval=`,
  the webserver is now `api-server`, and a `dag-processor` parses DAGs in its own
  process.
- **One DAG per layer, not one DAG for the pipeline.** The stages and their
  order are as planned; they are split across `bronze_`, `silver_` and
  `gold_` DAGs joined by asset events, so each layer can fail, retry and be
  replayed on its own.
- **Three extract tasks, not one.** The sources hit different hosts under
  different rate limits, any can fail alone, and a retry should re-run only the
  one that actually failed — so Football-Data's results and its competition
  rosters are separate tasks too, and a roster outage cannot fail the night's
  fixtures. They publish one asset each, so Silver waits for all three rather
  than for "Bronze".

## Backfilling whole seasons

The schedule loads one date a day; Part 2 needs whole seasons. `scripts/backfill.py` walks the
dates **serially and in chronological order**, for two structural reasons:

- **Serially**, because the layers hand the date to each other through asset events. Trigger
  Bronze twice in quick succession and both days' events land in one Silver run, which then
  fails on purpose rather than picking one.
- **In order**, because `dim_players` is an SCD Type 2 that only ever opens versions going
  forward. A date loaded before a player's first known version resolves to that too-new version
  instead, which attaches the player's goals to a club they had not joined yet. Measured on
  2024-08-17 loaded after the 2026/27 season: **108 violations from one date**, caught by the
  `fact_player_events_team_agrees_with_player_version` ERROR check.

```bash
python -m scripts.backfill snapshot            # record what Gold holds today
python -m scripts.backfill reset --yes         # truncate Gold for a clean rebuild
python -m scripts.backfill plan --seasons 2024 2025
python -m scripts.backfill run                 # the long one: scrape + load, in order
python -m scripts.backfill replay --from 2026-07-01   # re-load Bronze we already have
python -m scripts.backfill status
```

The runner triggers one Bronze run, waits for the whole chain to settle, verifies the date
reached Gold, and only then moves on. `run` and `replay` are resumable: every finished date is
recorded in `.backfill/state.json` and skipped next time. `replay` re-triggers Silver for dates
whose raw payloads are already in Bronze, which reloads them into Gold without calling a source
at all.

`run` calls both sources for every date, so it stops while Understat is unreachable. To see the
chain work without them, load the recorded date instead
([README](../README.md#reproduce-a-real-date)).

A paused DAG queues manual runs without executing them, so the schedule cannot simply be
switched off for the duration. The runner deletes Bronze's `scheduled__` runs while it works,
because a 06:00 run would otherwise load a recent date into the middle of a rebuild, and records
their dates so they can be run afterwards.

## Running a stage by hand

The DAGs are the normal entry point; every stage also stands alone, which is how
they are debugged and how they were built. Nothing below needs Airflow.

### Extraction

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
| `fd_teams` | one club | Football-Data competition rosters (venue, address, country) |
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

- **`fd_teams` is partitioned by the ingestion date, not a fixture date.** It is
  reference data with no match of its own, so the run's date is stamped on it —
  which keeps it partitioned exactly like every other Silver table and means the
  Gold load needs no special case to read it.
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
| Player ↔ team agreement | 715/715 events where the player's dimension club equals the club they played for |
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
| `dim_stadiums` | one ground | Type 1 | Football-Data (`/competitions/{code}/teams`) | `venue` per club, deduped to one row per ground (clubs that share a stadium resolve to the same key). `country` and the raw `address` are populated; `city` and `capacity` have no source and stay null. |

### Facts

| Table | Grain | Source | Key measures |
|-------|-------|--------|--------------|
| `fact_matches` | one match | Football-Data (results, venue) + Understat (xG) | goals (FT/HT), result, xG/npxG/ppda/deep/xpts per side; `stadium_key` resolved from the home club's ground |
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
- **Not modeled (FBref-blocked):** possession, total passes, tackles, and
  stadium `city`/`capacity`. FBref is the only source for these and is
  Cloudflare-blocked; we deliberately do not ship all-null columns for them.
  (`city` and `capacity` are the one exception — the columns exist because
  `dim_stadiums` is otherwise complete, and they are documented as the slot a
  future source fills.)
- **Not modeled (not ingested):** club crest, TLA and colours. The pipeline now
  calls Football-Data's `/competitions/{code}/teams` for venues, so these are
  available and no longer blocked — they are simply not in the contract yet.
- **Venues are resolved per season.** The teams endpoint defaults to the current
  season, so `extract_competition_teams` passes the season the run's date falls
  in. Backfilling November 2025 therefore resolves grounds against the 2025/26
  clubs, not against whoever is in the league today.

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

Coverage: 24 checks — cross-source id collisions and unconformed clubs
(`dim_teams`); a ground missing its country (`dim_stadiums`); overlapping
versions, current-flag consistency, every player
having a current version (`dim_players`); fixture-grain duplication, missing
scores on finished matches, result/score agreement, a finished match Understat
never delivered (`fact_matches`); the same
player counted twice under two versions, and the club they played for disagreeing
with their dimension version (`fact_player_events`); matches still pointing at the
`Unknown` ground (`fact_matches`); pitch coordinates, and goals
reconciling against `fact_player_events` (`fact_shots`). Each fact table is
guarded against `Unknown`-member leakage, which is how an unresolved key
surfaces given the `NOT NULL` FKs — a test enforces that guard exists.

**Verified by fault injection.** All 24 pass on the recorded date, but a check
that never fires proves nothing. Each was re-run against a deliberately corrupted
copy of its target table inside a rolled-back transaction — a duplicated fixture,
a nulled score, an overlapping SCD window, an out-of-pitch coordinate, and so on.
**21/21 detected the violation they claim to catch**, and the rollbacks left row
counts unchanged. (The two venue checks added since are not in that count.)

The xG coverage check came later too, and it was tested on real loads instead. With
Serie A's Understat payloads held back from the recorded date, it reported that
league's 3 matches; with them restored and the date replayed, xG was back on all 23
and it reported none. It is a WARN because Football-Data and Understat date some
fixtures differently: 11 of the 12 finished matches without xG in the 3,733-match
snapshot have their Understat row a day or two away. The other one, Rennes v Paris
Saint-Germain on 2026-08-23, is the only match it reports on that snapshot.

## Development

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

pytest -q
ruff check src tests airflow/dags
black --check src tests airflow/dags
```

`requirements.txt` stays free of Airflow and PySpark so the core suite runs
without a JVM or a scheduler. The DAG tests skip on that install and run in CI,
which adds Airflow separately:

```bash
pip install -r requirements-airflow.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.11.txt
pip install --no-deps apache-airflow-providers-apache-spark==5.5.1

pytest -q tests/test_dag_integrity.py tests/test_football_connections.py \
  tests/test_football_run_date.py
```

The two PySpark transform modules are gated the same way — they need a JVM, so
they skip on the core install and run in their own CI job:

```bash
pip install -r requirements-spark.txt   # pyspark 3.5.3, the cluster's version

pytest -q tests/test_bronze_to_silver_spark.py tests/test_silver_to_gold_spark.py
```

> The Spark provider is installed with `--no-deps` on purpose: it declares
> `pyspark>=3.5.2`, but its operator and hook only shell out to the
> `spark-submit` binary and never import it, so CI skips a 300 MB wheel nothing
> tests touch. The Airflow *image* does install pyspark — pinned to **3.5.3**,
> the cluster's exact version. A Spark 4 client cannot drive a Spark 3.5
> standalone master, and Airflow's own constraints file resolves pyspark to
> 4.1.1, so `docker/airflow/Dockerfile` filters `pyspark` and `py4j` out of those
> constraints before installing.

CI runs three jobs: `lint-and-test` (fast, JVM-free), `dag-tests` (installs
Airflow and asserts the DAGs import and are wired correctly) and `spark-tests`
(installs a JDK and PySpark and executes the two transform modules against real
DataFrames). Each of the latter two first asserts that its dependency actually
imports: all of those test modules begin with `pytest.importorskip`, so without
that guard a degraded install would report "skipped", exit 0, and go green
having verified nothing.

The DAG job earns its keep because a DAG file that raises at import does not
fail loudly — Airflow records an import error and schedules nothing, and the
first symptom is a day with no data. A broken asset
handoff is quieter still: both DAGs parse, and the downstream one simply never
runs again, which is why the tests assert that what each layer publishes is
exactly what the next one waits for.
