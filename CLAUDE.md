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
| **Part 1 — Data Engineering & Architecture** | Ingestion, orchestration, lakehouse, dimensional modeling, data quality, CI/CD | ✅ Yes — this file | **In progress** |
| Part 2 — Analytics & BI | Dashboards, KPIs, exploratory analysis on top of the Gold layer | ❌ No (separate subproject, own context) | Not started |
| Part 3 — Machine Learning | Match outcome / xG prediction models trained on the Gold tables this project produces | ❌ No (separate subproject, own context) | Not started |

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
| Orchestration | **Apache Airflow** | Webserver + Scheduler + Worker |
| Data Lake (object storage) | **MinIO** | Local S3-compatible store for Bronze/Silver |
| Processing | **Apache Spark (PySpark)** | Silver and Gold transformations |
| Data Warehouse | **PostgreSQL** | Serves final Gold tables + Airflow metadata |
| Data Quality | **Great Expectations** (or SQL checks) | Run at end of the DAG |
| CI/CD | **GitHub Actions** | Linting + tests on every commit |

> Spark is intentionally used even though the data volume is small. The goal is
> to demonstrate familiarity with big-data tooling for a portfolio context.

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
    (treat as **Slowly Changing Dimensions** where appropriate).
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
├── .github/workflows/ci.yml      # GitHub Actions: lint + tests
├── airflow/
│   ├── dags/                     # DAG definitions
│   ├── plugins/
│   └── config/
├── docker/
│   ├── airflow/Dockerfile
│   └── spark/Dockerfile
├── src/
│   ├── extract/                  # extract_fbref.py, extract_understat.py, ...
│   ├── transform/                # bronze_to_silver.py, silver_to_gold.py
│   ├── common/                   # config, minio_client, logging, team_mapping
│   └── quality/                  # data quality checks / expectations
├── sql/ddl/                      # CREATE TABLE statements for Gold schema
├── tests/                        # pytest unit tests
├── docker-compose.yml
├── requirements.txt
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

# Run unit tests
pytest -q

# Lint & format (must pass in CI)
ruff check src tests
black --check src tests

# Trigger the pipeline manually inside the Airflow container
docker compose exec airflow-webserver airflow dags trigger daily_football_pipeline
```

> If a command depends on infrastructure that isn't running, say so instead of
> guessing — do not invent output.

---

## 7. The Airflow DAG

Single daily DAG (`schedule_interval="@daily"`). Expected task flow:

1. `check_if_api_is_available` — Sensor / availability check.
2. `extract_daily_matches_to_bronze` — PythonOperator (rate-limited, incremental).
3. `process_bronze_to_silver_spark` — SparkSubmitOperator.
4. `load_silver_to_gold_postgres` — SparkSubmitOperator or PostgresOperator.
5. `data_quality_check` — Fail the run if nulls/duplicates are detected.

Keep tasks small, retryable, and idempotent. Use Airflow Connections (not
hardcoded credentials) to reach MinIO and Postgres.

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
