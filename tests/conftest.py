"""Shared fixtures.

The only thing that needs sharing is a SparkSession: building one costs a JVM
start-up, so the Spark-backed tests share a single session for the whole run.
"""

from __future__ import annotations

from typing import Any

import pytest


@pytest.fixture(scope="session")
def spark() -> Any:
    """A local SparkSession, or skip the test if PySpark is not installed.

    ``importorskip`` lives inside the fixture rather than at module scope so
    that requesting this fixture is what opts a test into needing a JVM — the
    default suite (``requirements.txt``, no PySpark, no Java) still collects and
    runs everything else. The ``spark-tests`` CI job is where these actually run.
    """
    pytest.importorskip(
        "pyspark",
        reason="PySpark is installed from requirements-spark.txt (the spark-tests CI job)",
    )
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("football-tests")
        # One partition: these frames are a handful of rows, and the default 200
        # turns every shuffle into seconds of scheduling overhead.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
