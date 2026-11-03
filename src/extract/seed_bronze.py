"""Load the recorded Bronze payloads into MinIO, without calling either source.

``data/sample_bronze/`` holds one complete match date (2025-11-01) as the extractors stored it,
laid out by ``bronze_key``. Loading it leaves Bronze as a successful extraction would, so Silver
and Gold can run with no API token and with Understat unreachable.

Run it inside the stack, then trigger Silver for that date::

    docker compose exec -w /opt/football spark-master python -m src.extract.seed_bronze
"""

from __future__ import annotations

import json
from pathlib import Path

from src.common.logging import get_logger
from src.common.minio_client import BronzeWriter

logger = get_logger(__name__)

SAMPLE_DIR = Path(__file__).resolve().parents[2] / "data" / "sample_bronze"


def seed_bronze(writer: BronzeWriter | None = None, sample_dir: Path = SAMPLE_DIR) -> int:
    """Store every recorded payload at the key it came from and return how many.

    Idempotent, like an extraction: each payload overwrites its own fixed key.
    """
    paths = sorted(sample_dir.rglob("*.json"))
    if not paths:
        raise FileNotFoundError(f"No recorded payloads under {sample_dir}")

    writer = writer or BronzeWriter()
    writer.ensure_bucket()
    for path in paths:
        key = path.relative_to(sample_dir).as_posix()
        writer.write_json(key, json.loads(path.read_text(encoding="utf-8")))

    logger.info("Seeded %d payloads into s3://%s", len(paths), writer.bucket)
    return len(paths)


if __name__ == "__main__":
    seed_bronze()
