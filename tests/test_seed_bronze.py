"""Tests for the recorded Bronze date and the loader that puts it in MinIO."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from src.common.config import MinioConfig
from src.common.minio_client import BronzeWriter
from src.extract.seed_bronze import SAMPLE_DIR, seed_bronze
from src.transform.parsers import parse_football_data_matches, parse_understat_shots

PARTITION = "year=2025/month=11/day=01"


class _FakeS3:
    """Minimal stand-in for a boto3 S3 client."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def head_bucket(self, **kwargs) -> None:
        return None

    def put_object(self, **kwargs) -> None:
        self.objects[kwargs["Key"]] = kwargs["Body"]


def _writer(client: _FakeS3) -> BronzeWriter:
    config = MinioConfig("http://minio:9000", "user", "secret", "bronze", "silver")
    return BronzeWriter(config=config, client=client)


def _payloads(dataset: str) -> list[dict]:
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((SAMPLE_DIR / dataset / PARTITION).glob("*.json"))
    ]


@pytest.mark.parametrize(
    ("dataset", "files"),
    [
        ("football_data/matches", 5),
        ("football_data/teams", 5),
        ("understat/league_data", 5),
        ("understat/match_data", 23),
    ],
)
def test_sample_holds_the_whole_date_for_every_dataset(dataset: str, files: int) -> None:
    assert len(_payloads(dataset)) == files


def test_sample_reproduces_the_counts_the_readme_states() -> None:
    matches = [
        m for p in _payloads("football_data/matches") for m in parse_football_data_matches(p)
    ]
    shots = [s for p in _payloads("understat/match_data") for s in parse_understat_shots(p)]

    assert len(matches) == 23
    assert len(shots) == 540


def test_seed_stores_each_payload_unchanged_at_its_recorded_key() -> None:
    client = _FakeS3()

    stored = seed_bronze(writer=_writer(client))

    expected = {
        path.relative_to(SAMPLE_DIR).as_posix(): path.read_bytes()
        for path in SAMPLE_DIR.rglob("*.json")
    }
    assert stored == len(expected) == 38
    assert client.objects == expected


def test_seed_refuses_an_empty_sample(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="No recorded payloads"):
        seed_bronze(writer=_writer(_FakeS3()), sample_dir=tmp_path)
