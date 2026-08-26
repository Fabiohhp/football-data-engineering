"""Tests for the Airflow-Connection -> pipeline-config adapters.

The interesting failure these guard against is not a wrong string: it is a
credential that never arrives. Every stage reads its configuration from
``src.common.config``, and a missing value there surfaces far from its cause —
as a MinIO 403 inside a Spark executor, or a psycopg2 authentication error in
the middle of a merge. These tests pin the mapping at the point where it is
still cheap to be wrong.
"""

from __future__ import annotations

import json

import pytest

# `airflow.sdk`, not `airflow`: this repository has its own `airflow/` directory
# at the root, which Python happily imports as a namespace package when the real
# one is absent. Skipping on the bare name would therefore never skip — it would
# succeed and then fail on the first real import.
pytest.importorskip(
    "airflow.sdk",
    reason="Airflow is installed from requirements-airflow.txt (CI and the Airflow image only)",
)

from src.common.config import minio_config, postgres_config  # noqa: E402

import football_connections as fc  # noqa: E402


def _connection(**kwargs):
    from airflow.sdk import Connection

    return Connection(**kwargs)


@pytest.fixture
def fake_connections(monkeypatch):
    """Serve canned Connections in place of Airflow's backend."""
    store = {
        fc.MINIO_CONN_ID: _connection(
            conn_id=fc.MINIO_CONN_ID,
            conn_type="aws",
            login="minio-key",
            password="minio-secret",
            extra=json.dumps(
                {
                    "endpoint_url": "http://minio:9000",
                    "bronze_bucket": "bronze",
                    "silver_bucket": "silver",
                }
            ),
        ),
        fc.POSTGRES_CONN_ID: _connection(
            conn_id=fc.POSTGRES_CONN_ID,
            conn_type="postgres",
            host="postgres",
            port=5432,
            login="football",
            password="pg-secret",
            schema="football_dw",
        ),
        fc.FOOTBALL_DATA_CONN_ID: _connection(
            conn_id=fc.FOOTBALL_DATA_CONN_ID,
            conn_type="http",
            host="api.football-data.org",
            schema="https",
            password="api-token",
        ),
    }
    monkeypatch.setattr(fc.BaseHook, "get_connection", staticmethod(lambda conn_id: store[conn_id]))
    return store


def test_minio_config_is_built_from_the_connection(fake_connections):
    config = fc.minio_config_from_connection()

    assert config.endpoint == "http://minio:9000"
    assert config.access_key == "minio-key"
    assert config.secret_key == "minio-secret"
    assert config.bronze_bucket == "bronze"
    assert config.silver_bucket == "silver"


def test_minio_buckets_fall_back_to_the_medallion_defaults(monkeypatch, fake_connections):
    """Bucket names are conventional; only the credentials are mandatory."""
    bare = _connection(
        conn_id=fc.MINIO_CONN_ID,
        conn_type="aws",
        login="k",
        password="s",
        extra=json.dumps({"endpoint_url": "http://minio:9000"}),
    )
    monkeypatch.setattr(fc.BaseHook, "get_connection", staticmethod(lambda conn_id: bare))

    config = fc.minio_config_from_connection()
    assert (config.bronze_bucket, config.silver_bucket) == ("bronze", "silver")


def test_postgres_config_reads_the_database_from_the_schema_field(fake_connections):
    """Airflow's `schema` is the dbname — getting this wrong yields a live
    connection to the wrong database rather than an error."""
    config = fc.postgres_config_from_connection()

    assert config.database == "football_dw"
    assert (config.host, config.port, config.user) == ("postgres", 5432, "football")
    assert config.jdbc_url == "jdbc:postgresql://postgres:5432/football_dw"


def test_football_data_config_builds_the_versioned_base_url(fake_connections):
    """The extractor appends paths like `/competitions/PL/matches` to this."""
    config = fc.football_data_config_from_connection()

    assert config.base_url == "https://api.football-data.org/v4"
    assert config.api_token == "api-token"


def test_a_half_configured_connection_names_what_is_missing(monkeypatch):
    """The error has to identify the connection, or it is a scavenger hunt."""
    # Complete apart from the secret key, so the failure is unambiguously the
    # password rather than whichever field happens to be checked first.
    no_password = _connection(
        conn_id=fc.MINIO_CONN_ID,
        conn_type="aws",
        login="minio-key",
        extra=json.dumps({"endpoint_url": "http://minio:9000"}),
    )
    monkeypatch.setattr(fc.BaseHook, "get_connection", staticmethod(lambda conn_id: no_password))

    with pytest.raises(ValueError, match=r"minio_default.*password"):
        fc.minio_config_from_connection()


def test_a_connection_missing_its_endpoint_names_the_extra_field(monkeypatch):
    """`endpoint_url` lives in the extra blob, where a typo is easy and silent."""
    no_endpoint = _connection(
        conn_id=fc.MINIO_CONN_ID, conn_type="aws", login="k", password="s", extra=json.dumps({})
    )
    monkeypatch.setattr(fc.BaseHook, "get_connection", staticmethod(lambda conn_id: no_endpoint))

    with pytest.raises(ValueError, match=r"minio_default.*extra\.endpoint_url"):
        fc.minio_config_from_connection()


def test_spark_env_templates_defer_every_lookup_to_run_time(fake_connections):
    """Values must be Jinja, not resolved strings.

    Resolving at DAG-parse time would hit the connection backend on every parse
    cycle and write the credential into the serialized DAG, where it is visible
    to anyone who can read the metadata database.
    """
    templates = fc.spark_env_templates()

    for name, value in templates.items():
        assert value.startswith("{{ conn."), f"{name} is not a template: {value!r}"
        assert value.endswith("}}")

    assert templates["MINIO_ROOT_PASSWORD"] == "{{ conn.minio_default.password }}"
    assert templates["POSTGRES_DB"] == "{{ conn.postgres_gold.schema }}"


def test_spark_env_is_enough_to_configure_the_pipeline(monkeypatch):
    """The env a Spark driver receives must satisfy ``src.common.config``.

    This is the regression that matters: ``config.py`` raises on a missing
    required variable, and the Spark stages get their entire configuration from
    this mapping. Adding a new required setting to the config without adding it
    here would break both transforms at run time — in a container, mid-DAG,
    after the extract has already spent its rate-limit budget.
    """
    # Stand in for what Airflow renders; the values are irrelevant, the keys are
    # the contract. POSTGRES_PORT has to parse as an int.
    for key in fc.spark_env_templates():
        monkeypatch.setenv(key, "5432" if key == "POSTGRES_PORT" else f"rendered-{key}")

    minio = minio_config()
    postgres = postgres_config()

    assert minio.access_key == "rendered-MINIO_ROOT_USER"
    assert postgres.database == "rendered-POSTGRES_DB"
    assert postgres.port == 5432
