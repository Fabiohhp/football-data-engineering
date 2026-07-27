"""Tests for the Bronze partition-key builder and JSON writer."""

from __future__ import annotations

from datetime import date

import pytest
from src.common.config import MinioConfig
from src.common.minio_client import BronzeWriter, bronze_key


def test_bronze_key_layout() -> None:
    key = bronze_key("fbref", "matches", date(2026, 6, 18), "PL.json")
    assert key == "fbref/matches/year=2026/month=06/day=18/PL.json"


def test_bronze_key_zero_pads_month_and_day() -> None:
    key = bronze_key("understat", "match_data", date(2026, 1, 5), "22275.json")
    assert key == "understat/match_data/year=2026/month=01/day=05/22275.json"


@pytest.mark.parametrize(
    ("source", "dataset", "filename"),
    [("", "matches", "a.json"), ("s", "", "a.json"), ("s", "matches", "")],
)
def test_bronze_key_rejects_empty_parts(source: str, dataset: str, filename: str) -> None:
    with pytest.raises(ValueError):
        bronze_key(source, dataset, date(2026, 6, 18), filename)


class _FakeS3:
    """Minimal stand-in for a boto3 S3 client."""

    def __init__(self) -> None:
        self.puts: list[dict] = []

    def put_object(self, **kwargs) -> None:
        self.puts.append(kwargs)


def _config() -> MinioConfig:
    return MinioConfig(
        endpoint="http://minio:9000",
        access_key="k",
        secret_key="s",
        bronze_bucket="bronze",
        silver_bucket="silver",
    )


def test_write_json_puts_utf8_body_and_returns_uri() -> None:
    fake = _FakeS3()
    writer = BronzeWriter(config=_config(), client=fake)

    uri = writer.write_json("understat/league_data/x.json", {"team": "Wolves", "n": 1})

    assert uri == "s3://bronze/understat/league_data/x.json"
    assert len(fake.puts) == 1
    put = fake.puts[0]
    assert put["Bucket"] == "bronze"
    assert put["Key"] == "understat/league_data/x.json"
    assert put["ContentType"] == "application/json"
    assert isinstance(put["Body"], bytes)
    assert b'"Wolves"' in put["Body"]


def test_write_json_is_idempotent_on_same_key() -> None:
    fake = _FakeS3()
    writer = BronzeWriter(config=_config(), client=fake)
    key = "understat/match_data/year=2026/month=06/day=18/1.json"

    writer.write_json(key, {"v": 1})
    writer.write_json(key, {"v": 2})

    # Same key both times -> overwrite semantics (no duplicate keys).
    assert {p["Key"] for p in fake.puts} == {key}
