# CLAUDE.md

This file gives Claude Code the context it needs to work effectively in this
repository. Read it fully before generating or modifying code.

---

## 1. Project Overview

This project builds an **end-to-end batch data platform for football (soccer)
data**. It ingests match and player statistics daily from public sources,
processes them through a **Medallion architecture (Bronze → Silver → Gold)**,
and serves clean, analytics-ready data into a PostgreSQL data warehouse.

The pipeline is fully containerized and orchestrated, so it can be started by a
reviewer with a single `docker compose up`.

This is one subproject inside a larger repository (see *Scope of this file*
below). This `CLAUDE.md` governs only the data engineering & architecture work.

### ⚠️ Scope of this file — READ THIS FIRST

This belongs to a **larger repository** that will eventually host several
subprojects. **However, this `CLAUDE.md` describes ONLY the Data Engineering &
Architecture subproject (Part 1).** Everything below applies to that subproject
alone — do not treat the analytics or ML phases as part of this file's scope.

| Phase | Scope | Covered by this CLAUDE.md? | Status |
|-------|-------|----------------------------|--------|
| **Part 1 — Data Engineering & Architecture** | Ingestion, orchestration, lakehouse, dimensional modeling, data quality, CI/CD | ✅ Yes — this file | **Complete** |
| Part 2 — Analytics & BI | Dashboards, KPIs, exploratory analysis on top of the Gold layer | ❌ No (separate subproject, own context) | Complete, in `football-data-analytics` |
| Part 3 — Machine Learning | Match outcome / xG prediction models trained on the Gold tables this project produces | ❌ No (separate subproject, own context) | Complete, in `football-data-science` |

**Implications for any code you write under this subproject:**

- The **primary deliverable of Part 1 is a robust, reproducible data pipeline** —
  NOT models, dashboards, or statistical analysis. Do not add ML or BI logic here.
- The **Gold layer is the contract** between this project and the future ones.
  Design Gold tables (facts + dimensions) so they are clean, well-typed,
  documented, and stable enough to be consumed by external analytics/ML repos.
- Favor decisions that make downstream ML/analytics easier later: consistent
  keys, no silent nulls in fact tables, clear grain per table, documented schemas.
- When in doubt, optimize for **data quality and reproducibility**, not for
  squeezing out predictive features. That comes later.

---

## 2. Tech Stack

| Layer | Technology | Notes |
|-------|-----------|-------|
| Infrastructure | **Docker & Docker Compose** | Single-command startup for reviewers |
| Orchestration | **Apache Airflow 3.1.8** | api-server + scheduler + dag-processor + triggerer, **LocalExecutor** |
| Data Lake (object storage) | **MinIO** | Local S3-compatible store for Bronze/Silver |
| Processing | **Apache Spark (PySpark)** | Silver and Gold transformations |
| Data Warehouse | **PostgreSQL** | Serves final Gold tables + Airflow metadata |
| Data Quality | **Great Expectations** (or SQL checks) | Run at end of the DAG |
| CI/CD | **GitHub Actions** | Linting + tests on every commit |

> Spark is intentionally used even though the data volume is small. The goal is
> to demonstrate familiarity with big-data tooling for a portfolio context.

> **Executor choice.** LocalExecutor, not Celery: tasks run as subprocesses of
> the scheduler. A broker and a dedicated worker container buy nothing at this
> data volume (one date is ~1300 rows), and the Spark driver runs in the
> scheduler container under client mode, so it needs the headroom. The four
> Airflow components as configured measure ~1.1 GB.
>
> **PySpark in the Airflow image is pinned to the cluster's version (3.5.3).**
> Airflow's constraints file resolves pyspark to 4.x, and a Spark 4 client
> cannot drive a Spark 3.5 standalone master. `docker/airflow/Dockerfile`
> filters `pyspark` and `py4j` out of the constraints before installing. Keep it
> equal to `docker/spark/requirements.txt`.

---

## 3. Architecture

```
                ┌─────────────┐
   Sources ───► │  EXTRACT    │  Python scripts (incremental, rate-limited)
 (FBref,        └──────┬──────┘
  Understat,          │ raw JSON / HTML
  Football-Data)      ▼
                ┌─────────────┐
                │   BRONZE    │  MinIO  s3://bronze/...   (raw, untouched)
                └──────┬──────┘
                       │ PySpark: clean, cast, standardize
                       ▼
                ┌─────────────┐
                │   SILVER    │  MinIO  s3://silver/...   (Parquet, typed)
                └──────┬──────┘
                       │ PySpark: joins, aggregations, dimensional model
                       ▼
                ┌─────────────┐
                │    GOLD     │  PostgreSQL  (Star Schema: facts + dims)
                └─────────────┘
```

### Layer responsibilities

- **Bronze** — Persist the source response *exactly as received* (raw JSON from
  APIs, raw HTML/tables from scraped pages). **Never transform here.** This lets
  us reprocess without re-scraping if downstream logic changes.
  Partition by ingestion date:
  `s3://bronze/fbref/matches/year=2026/month=06/day=18/`
- **Silver** — Cleaned and standardized with PySpark: deduplicate, cast strings
  to dates/numbers, normalize team names via a mapping table
  (e.g. `"Man Utd"` → `"Manchester United"`), handle nulls. Output **Parquet**.
- **Gold** — **Dimensional model (Star Schema)** written to PostgreSQL:
  - Dimensions: `dim_players`, `dim_teams`, `dim_stadiums`, `dim_referees`
    (treat as **Slowly Changing Dimensions** where appropriate). `dim_stadiums`
    is fed by Football-Data's `/competitions/{code}/teams` endpoint, which is the
    only venue source available — it supplies the ground's name, country and a
    free-form address, but no city or capacity.
  - Facts: `fact_matches` (result, possession, xG),
    `fact_player_events` (passes, shots, tackles per match).

---

## 4. Data Sources & Hard Constraints

> These constraints are **non-negotiable** and must be respected in every
> extraction script. Violating them gets our IP blocked.

### FBref (web scraping) — primary source for player-level stats
- No official API. Data appears a few hours after each match.
- **Rate limit: max ~20 requests/minute.** Always insert
  `time.sleep(3)` (3–5s) between requests.
- A 429 error means we were throttled → back off, do not hammer.
- **Only scrape the current round / yesterday's matches.** Never rebuild full
  history every night.

### Understat (web scraping) — xG / xA / shot data
- No official REST API; community libraries parse JSON injected into the page.
- More tolerant than FBref but **still rate-limit and add delays.**
- Fragile: if their site layout changes, the parser may break until the
  community library updates. Wrap parsing defensively.

### Football-Data.org (real API) — results & standings
- Real API with token auth and documentation.
- **Free tier (Tier 1): 12 competitions, 10 calls/minute.**
- No player-level stats (no shots/passes/tackles) — results & tables only.
- `/competitions/{code}/teams` is the **only venue source** in the project and
  what populates `dim_stadiums`. Always pass `?season=` — it defaults to the
  current season, so a backfill without it resolves grounds against today's
  clubs rather than the ones that played that season.
- Store the API token in `.env`, never commit it.

### Mandatory extraction rules
1. **Incremental load only.** Query the warehouse for which matches happened
   yesterday, then request *only* those specific pages/matches.
2. **Resilience:** use the `tenacity` library to retry on transient failures
   with backoff.
3. **Politeness:** enforce `time.sleep()` delays between every request.
4. **Idempotency:** re-running a day's extraction must not create duplicates
   downstream (use upserts / dedup keys).

---

## 5. Repository Structure

```
football-data-platform/
├── .github/workflows/ci.yml      # GitHub Actions: lint + tests + DAG tests
├── airflow/
│   ├── dags/                     # bronze_/silver_/gold_ DAGs, assets/,
│   │                             # football_conf.py, football_connections.py,
│   │                             # football_run_date.py, .airflowignore
│   ├── plugins/
│   └── config/
├── docker/
│   ├── airflow/Dockerfile        # Airflow + JVM + Spark 3.5.3 client
│   └── spark/Dockerfile
├── src/
│   ├── extract/                  # extract_fbref.py, extract_understat.py, ...
│   ├── transform/                # bronze_to_silver.py, silver_to_gold.py
│   ├── common/                   # config, minio_client, logging, team_mapping
│   └── quality/                  # data quality checks / expectations
├── sql/
│   ├── ddl/                      # CREATE TABLE statements for Gold schema
│   ├── load/                     # Silver -> Gold merge statements
│   └── airflow/                  # Airflow metadata role/database bootstrap
├── tests/                        # pytest unit tests
├── docker-compose.yml
├── requirements.txt              # core dev deps; no Airflow, no PySpark
├── requirements-airflow.txt      # Airflow, for CI's DAG tests only
├── requirements-spark.txt        # PySpark, for CI's Spark transform tests only
├── pyproject.toml                # ruff/black config
├── .env.example                  # template; real .env is gitignored
└── README.md
```

---

## 6. Common Commands

```bash
# Start the whole stack (MinIO, Postgres, Airflow, Spark)
docker compose up -d

# Tail logs
docker compose logs -f airflow-scheduler

# Stop and remove containers
docker compose down

# Run unit tests. The DAG tests and the two PySpark transform suites skip on
# this install by design — requirements.txt carries neither Airflow nor PySpark,
# so the core suite needs no JVM and no scheduler. CI runs them in their own
# jobs (see requirements-airflow.txt / requirements-spark.txt).
pytest -q

# Lint & format (must pass in CI)
ruff check src tests airflow/dags
black --check src tests airflow/dags

# Run the whole chain for one date: Bronze publishes its assets, which trigger
# Silver, which triggers Gold. Nothing else needs to be started by hand.
docker compose exec airflow-scheduler airflow dags trigger bronze_football_matches \
  --conf '{"match_date": "2025-11-01"}'

# The same chain with neither source nor token: load the recorded date, then trigger Silver.
docker compose exec -w /opt/football spark-master python -m src.extract.seed_bronze

# Re-run one layer only (repairing Silver or Gold without re-scraping Bronze).
docker compose exec airflow-scheduler airflow dags trigger silver_football_matches \
  --conf '{"match_date": "2025-11-01"}'
```

> Airflow UI: http://localhost:8088 (not 8080 — the Spark master has that).
> Every DAG takes the same `match_date` conf, and it is the only way to aim a
> run at a specific day: Silver and Gold are triggered by assets, so their runs
> have no logical date to backfill against.

> If a command depends on infrastructure that isn't running, say so instead of
> guessing — do not invent output.

---

## 7. The Airflow DAGs

**One DAG per medallion layer, chained by assets** — not one DAG with five
tasks. Each layer owns its own schedule, retries, replays and failures, and the
handoff between them is an Airflow 3 asset event rather than an edge inside a
single graph. Written with the TaskFlow API (`@dag` / `@task` from
`airflow.sdk`).

| DAG | Trigger | Tasks | Publishes |
|-----|---------|-------|-----------|
| `bronze_football_matches` | `schedule="0 6 * * *"` | `check_if_api_is_available` (sensor, `reschedule`), `extract_football_data`, `extract_football_data_teams`, `extract_understat` | `bronze_*` assets |
| `silver_football_matches` | all three `bronze_*` assets | `resolve_match_date`, `process_bronze_to_silver_spark` (SparkSubmit), `publish_silver_tables` | `silver_*` assets |
| `gold_football_star_schema` | all five `silver_*` assets | `resolve_match_date`, `load_silver_to_gold_postgres` (SparkSubmit), `data_quality_check` | `gold_*` assets |

Rules for anything added here:

- **Assets are declared in `airflow/dags/assets/football.py`, never inline.** A
  producer and a consumer that name assets separately can drift apart by one
  character, and the only symptom is a downstream DAG that silently stops
  running. A list in `schedule=` is an AND: every asset must be updated.
- **Pass the run's date, never compute it.** Bronze takes `{{ ds }}`; Silver and
  Gold read the date off the asset event that triggered them
  (`airflow/dags/football_run_date.py`), because an asset-triggered run has no
  logical date of its own. Every DAG also accepts a `match_date` conf override,
  which is how one layer is replayed for one day. A task that calls
  `date.today()` looks correct until its first retry after midnight.
- **`schedule="0 6 * * *"`** on Bronze — an `@daily` cut at 06:00 UTC rather than
  midnight, so a run does not race the sources for the previous evening's
  fixtures. Airflow 3 uses `schedule=`; `schedule_interval=` was removed.
- **`catchup=False` and `max_active_runs=1`** on every DAG — non-negotiable, or a
  stack that was off for a week fires every missed day at two rate-limited
  sources at once, and two Spark drivers land in the scheduler container.
- **Keep tasks small, retryable and idempotent.** The data-quality gate is the
  one deliberate exception: `retries=0`, because corrupt rows do not improve on
  a second read. It also owns the Gold outlets, so an asset update means
  "loaded *and* validated".
- **Use Airflow Connections, never hardcoded credentials.** They are defined as
  `AIRFLOW_CONN_*` environment variables in `docker-compose.yml`;
  `airflow/dags/football_connections.py` converts them into the config
  dataclasses from `src/common/config.py`. Look them up at task runtime, never
  at DAG-parse time. **`src/` must never import Airflow** — the stages have to
  keep running under a bare `spark-submit`.
- **Every DAG change needs a test in `tests/test_dag_integrity.py`.** A DAG that
  raises at import does not fail loudly; Airflow records an import error and
  schedules nothing. The same applies to a broken asset handoff, which is why
  the tests assert that what one layer publishes is exactly what the next one
  waits for.

---

## 8. Coding Conventions

- **Python 3.11+**, type hints on public functions.
- Formatting: **black**; linting: **ruff**. Code must pass both before commit.
- No secrets in code — read everything from environment variables / `.env`.
- All extraction functions must accept an explicit date and be re-runnable.
- Log with the standard `logging` module (no bare `print` in pipeline code).
- PySpark jobs: read paths and connection details from config, not hardcoded.
- Prefer pure, testable functions; keep I/O at the edges.
- Write a unit test for every non-trivial transformation.

---

## 9. Definition of Done (Part 1)

A feature is complete when:
- It respects all source rate limits and uses incremental loading.
- It is wired into the DAG and runs idempotently end-to-end.
- It has unit tests and passes `ruff` + `black` + `pytest`.
- Gold-layer changes update the `sql/ddl` schema and are documented.
- The data quality task still passes (no unexpected nulls/duplicates).
- The README explains how to run it.

---

## 10. What NOT to do in this repo

- ❌ Do not add machine learning models or training code here (that's the Part 3 subproject).
- ❌ Do not add BI dashboards or exploratory analysis notebooks here (that's the Part 2 subproject).
- ❌ Do not transform data in the Bronze layer.
- ❌ Do not rebuild full historical data on every run.
- ❌ Do not bypass rate limits or remove `time.sleep()` delays.
- ❌ Do not commit secrets, tokens, or `.env`.
