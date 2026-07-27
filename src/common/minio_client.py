"""MinIO / S3 helpers for writing raw payloads to the Bronze layer.

The Bronze layer stores source responses *exactly as received* (see CLAUDE.md).
This module keeps the pure path-building logic (``bronze_key``) separate from the
I/O (``BronzeWriter``) so the key layout can be unit-tested without any network.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from src.common.config import MinioConfig, minio_config
from src.common.logging import get_logger

logger = get_logger(__name__)


def bronze_key(source: str, dataset: str, dt: date, filename: str) -> str:
    """Build a partitioned Bronze object key.

    Example::

        bronze_key("fbref", "matches", date(2026, 6, 18), "PL.json")
        -> "fbref/matches/year=2026/month=06/day=18/PL.json"

    The date partition uses zero-padded month/day so lexical and chronological
    ordering match, which keeps range scans predictable downstream.
    """
    if not source or not dataset or not filename:
        raise ValueError("source, dataset and filename must all be non-empty")
    return (
        f"{source}/{dataset}"
        f"/year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}"
        f"/{filename}"
    )


def ensure_bucket(client: Any, bucket: str) -> None:
    """Create ``bucket`` if it does not already exist.

    Also used for the Silver bucket before a Spark write: the ``s3a`` connector
    can create objects but not buckets, so a first run would otherwise fail with
    a bare 404 from Hadoop.
    """
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        logger.info("Bucket %r missing — creating it", bucket)
        client.create_bucket(Bucket=bucket)


class BronzeWriter:
    """Thin wrapper around an S3 client scoped to the Bronze bucket."""

    def __init__(self, config: MinioConfig | None = None, client: Any | None = None):
        self._config = config or minio_config()
        self._client = client or _build_s3_client(self._config)

    @property
    def bucket(self) -> str:
        return self._config.bronze_bucket

    def ensure_bucket(self) -> None:
        """Create the Bronze bucket if it does not already exist."""
        ensure_bucket(self._client, self.bucket)

    def write_json(self, key: str, payload: Any) -> str:
        """Serialize ``payload`` as UTF-8 JSON and store it at ``key``.

        Writing to a fixed key is idempotent: re-running a day's extraction
        overwrites the same object rather than creating duplicates. Returns the
        ``s3://`` URI of the written object.
        """
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        uri = f"s3://{self.bucket}/{key}"
        logger.info("Wrote %d bytes to %s", len(body), uri)
        return uri


class BronzeReader:
    """Read side of the Bronze layer, used by the Silver transform.

    Bronze holds many small JSON objects; listing and fetching them with boto3
    keeps the Spark job free of the ``s3a`` quirks around wildcard reads, and
    lets the flatteners stay pure Python (and therefore CI-testable without a
    JVM). Spark still owns the typed DataFrames, the joins and the Parquet
    write.
    """

    def __init__(self, config: MinioConfig | None = None, client: Any | None = None):
        self._config = config or minio_config()
        self._client = client or _build_s3_client(self._config)

    @property
    def bucket(self) -> str:
        return self._config.bronze_bucket

    def list_keys(self, prefix: str) -> list[str]:
        """Return every object key under ``prefix`` (sorted for determinism)."""
        paginator = self._client.get_paginator("list_objects_v2")
        keys = [
            obj["Key"]
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            for obj in page.get("Contents", [])
        ]
        return sorted(keys)

    def read_json(self, key: str) -> Any:
        """Load and deserialize one Bronze object."""
        body = self._client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return json.loads(body)


def _build_s3_client(config: MinioConfig) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        config=BotoConfig(signature_version="s3v4"),
    )
