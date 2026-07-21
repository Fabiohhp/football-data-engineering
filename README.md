# Football Data Platform

End-to-end batch data platform for football (soccer) data: ingestion →
Bronze/Silver/Gold (Medallion architecture) → PostgreSQL data warehouse.

See `CLAUDE.md` for full architecture, source constraints, and conventions.

## Status

Infra scaffolding in progress. Currently running: PostgreSQL (data warehouse)
and MinIO (Bronze/Silver object storage). Airflow and Spark are added in a
later step.

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

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

pytest -q
ruff check src tests
black --check src tests
```
