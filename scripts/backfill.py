"""Load whole seasons through the medallion chain, one match date at a time.

The pipeline is built for a daily run: one chain, one match date (CLAUDE.md §7).
Part 2 needs whole seasons, so something has to walk the dates — and it has to
walk them **serially and in chronological order**. Two reasons, both structural:

* **Serially**, because the layers hand the date to each other through asset
  events. Trigger Bronze twice in quick succession and both days' events land in
  one Silver run, which then fails on purpose rather than picking one
  (``football_run_date.resolve_match_date_value``).
* **In order**, because ``dim_players`` is an SCD Type 2 that only ever opens
  versions going forward (``sql/load/20_dim_players.sql``). A date loaded before
  its player's first known version resolves to that too-new version instead, and
  ``fact_player_events_team_agrees_with_player_version`` — an ERROR check — sees
  a player's goals attached to a club he had not joined yet. Measured on
  2024-08-17 loaded after the 2026/27 season: 108 violations from one date.

So this runner triggers one Bronze run, waits for the whole chain to settle,
verifies the date reached Gold, and only then moves on. It is deliberately dumb
about time: the sources' rate limits (CLAUDE.md §4) are enforced inside the
extractors, and nothing here tries to overtake them.

Usage::

    python -m scripts.backfill snapshot            # record what Gold holds today
    python -m scripts.backfill reset --yes         # truncate Gold for a clean rebuild
    python -m scripts.backfill plan --seasons 2024 2025
    python -m scripts.backfill run                 # the long one: scrape + load, in order
    python -m scripts.backfill replay --from 2026-07-01   # re-load Bronze we already have
    python -m scripts.backfill status

``run`` and ``replay`` are resumable: every finished date is recorded in
``.backfill/state.json`` and skipped next time, so the job can be interrupted and
restarted. ``replay`` re-triggers **Silver** for dates whose raw payloads are
already in Bronze, which reloads them into Gold without calling a source at all.

A note on the daily schedule: a paused DAG queues manual runs without executing
them, so the schedule cannot simply be switched off for the duration. Instead
this runner deletes Bronze's ``scheduled__`` runs while it works — otherwise a
06:00 run would load a 2026 date into the middle of a 2024 rebuild and reopen
exactly the problem the rebuild exists to fix — and records their dates so they
can be run afterwards.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
import requests
from src.common.config import football_data_config, postgres_config
from src.common.logging import get_logger
from src.extract.extract_footballdata import DEFAULT_COMPETITIONS, FootballDataClient

logger = get_logger("backfill")

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = REPO_ROOT / ".backfill"
PLAN_FILE = STATE_DIR / "plan.json"
STATE_FILE = STATE_DIR / "state.json"
SNAPSHOT_FILE = STATE_DIR / "pre_reset_counts.json"
DEFERRED_FILE = STATE_DIR / "deferred_daily.json"

BRONZE_DAG = "bronze_football_matches"
SILVER_DAG = "silver_football_matches"
GOLD_DAG = "gold_football_star_schema"

# Gold tables, children first: TRUNCATE ... CASCADE would do it, but naming them
# in order keeps the statement honest about what it destroys.
GOLD_TABLES = (
    "gold.fact_shots",
    "gold.fact_player_events",
    "gold.fact_matches",
    "gold.dim_players",
    "gold.dim_teams",
    "gold.dim_referees",
    "gold.dim_stadiums",
)

# How long one date may take before the runner gives up and moves on. A busy
# Saturday measured ~3 minutes (Understat's 3s politeness delay times ~20
# matches, plus two Spark stages). Half an hour means something is wedged.
CHAIN_TIMEOUT_SECONDS = 1800
POLL_SECONDS = 5


# --------------------------------------------------------------------------- #
# Airflow REST API
# --------------------------------------------------------------------------- #


@dataclass
class AirflowApi:
    """The api-server, not the CLI: a `docker compose exec` costs ~4 seconds per
    poll and this job polls thousands of times. Tokens expire within the hour,
    so every call re-authenticates once on a 401."""

    base_url: str
    username: str
    password: str
    _token: str | None = field(default=None, repr=False)

    def _authenticate(self) -> str:
        response = requests.post(
            f"{self.base_url}/auth/token",
            json={"username": self.username, "password": self.password},
            timeout=30,
        )
        response.raise_for_status()
        self._token = response.json()["access_token"]
        return self._token

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        token = self._token or self._authenticate()
        url = f"{self.base_url}{path}"
        response = requests.request(
            method, url, headers={"Authorization": f"Bearer {token}"}, timeout=60, **kwargs
        )
        if response.status_code == 401:
            token = self._authenticate()
            response = requests.request(
                method, url, headers={"Authorization": f"Bearer {token}"}, timeout=60, **kwargs
            )
        return response

    def trigger(self, dag_id: str, run_id: str, match_date: str) -> None:
        """Start a run for one match date.

        A 409 means this run id already exists — a previous attempt got as far as
        triggering — so the runner falls in behind that run rather than failing.
        """
        response = self._request(
            "POST",
            f"/api/v2/dags/{dag_id}/dagRuns",
            json={"dag_run_id": run_id, "logical_date": None, "conf": {"match_date": match_date}},
        )
        if response.status_code == 409:
            logger.warning("Run %s already exists; waiting on it instead", run_id)
            return
        response.raise_for_status()

    def run_state(self, dag_id: str, run_id: str) -> str:
        response = self._request("GET", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}")
        if response.status_code == 404:
            return "missing"
        response.raise_for_status()
        return response.json()["state"]

    def runs_in_progress(self, dag_id: str) -> list[dict[str, Any]]:
        response = self._request(
            "GET",
            f"/api/v2/dags/{dag_id}/dagRuns",
            params=[("state", "queued"), ("state", "running")],
        )
        response.raise_for_status()
        return response.json()["dag_runs"]

    def kill_run(self, dag_id: str, run_id: str) -> None:
        """Stop a run and remove it.

        A *running* run cannot be deleted — Airflow answers 409 — so it is marked
        failed first. Both steps tolerate losing a race with the scheduler:
        suppression must never be the thing that stops a backfill.
        """
        self._request("PATCH", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}", json={"state": "failed"})
        response = self._request("DELETE", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}")
        if response.status_code not in (200, 204, 404, 409):
            logger.warning("Could not delete %s: HTTP %s", run_id, response.status_code)


def suppress_scheduled_runs(api: AirflowApi) -> None:
    """Kill Bronze's scheduled runs, recording the dates they would have loaded.

    The 06:00 schedule keeps firing while this runs, and its date is always
    "yesterday" — far ahead of wherever the rebuild has reached. One that gets
    through creates current player versions for dates the backfill has not
    reached, which is exactly the corruption the chronological order exists to
    avoid. Measured once: a scheduled run slipped through a two-minute gap
    between dates and put four 2026 matches into a warehouse rebuilt to
    2025-01-19.

    Never raises: a failure here is logged and the backfill carries on.
    """
    try:
        runs = api.runs_in_progress(BRONZE_DAG)
    except Exception as exc:  # noqa: BLE001 - suppression must not stop the walk
        logger.warning("Could not list Bronze runs: %s", exc)
        return

    deferred: list[str] = load_json(DEFERRED_FILE, [])
    changed = False
    for run in runs:
        run_id = run["dag_run_id"]
        if not run_id.startswith("scheduled__"):
            continue
        try:
            api.kill_run(BRONZE_DAG, run_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not suppress %s: %s", run_id, exc)
            continue
        # SCHEDULED_MATCH_DATE is the day before the run's logical date.
        logical = date.fromisoformat(run_id.removeprefix("scheduled__")[:10])
        deferred_date = (logical - timedelta(days=1)).isoformat()
        if deferred_date not in deferred:
            deferred.append(deferred_date)
            changed = True
        logger.warning("Suppressed %s; %s deferred to the end", run_id, deferred_date)
    if changed:
        save_json(DEFERRED_FILE, sorted(deferred))


def tick(api: AirflowApi) -> None:
    """One poll interval: wait, then kill any scheduled run that has appeared.

    Checked on every poll rather than once per date. Bronze's scheduled run
    finishes in about a minute, so a check between dates can miss its whole life
    — and by the time it is visible as "done", Silver is already carrying its
    date downstream.
    """
    time.sleep(POLL_SECONDS)
    suppress_scheduled_runs(api)


# --------------------------------------------------------------------------- #
# PostgreSQL helpers
# --------------------------------------------------------------------------- #


def query(sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    config = postgres_config()
    connection = psycopg2.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=config.database,
    )
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall() if cursor.description else []
    finally:
        connection.close()


def gold_match_count(match_date: str) -> int:
    """How many matches Gold holds for a date — the runner's proof of success."""
    rows = query("SELECT count(*) FROM gold.fact_matches WHERE match_date = %s", (match_date,))
    return int(rows[0][0])


def gold_max_date() -> str | None:
    rows = query("SELECT max(match_date)::text FROM gold.fact_matches")
    return rows[0][0]


def gold_counts_by_date() -> dict[str, int]:
    rows = query("SELECT match_date::text, count(*) FROM gold.fact_matches GROUP BY 1 ORDER BY 1")
    return {row[0]: int(row[1]) for row in rows}


def reset_gold() -> None:
    """Empty the star schema, then restore the DDL's `Unknown` members.

    Everything here is re-derivable: the raw payloads are in Bronze, and the two
    seasons being loaded come from the sources. The DDL files are idempotent
    (`CREATE TABLE IF NOT EXISTS`, `INSERT ... ON CONFLICT DO NOTHING`), so
    re-running them after the truncate is what puts key 0 back in each table.
    """
    logger.warning("Truncating %d Gold tables", len(GOLD_TABLES))
    query(f"TRUNCATE {', '.join(GOLD_TABLES)} RESTART IDENTITY CASCADE")
    for path in sorted((REPO_ROOT / "sql" / "ddl").glob("*.sql")):
        query(path.read_text())
        logger.info("Re-applied %s", path.name)


# --------------------------------------------------------------------------- #
# Plan / state files
# --------------------------------------------------------------------------- #


def load_json(path: Path, default: Any) -> Any:
    return json.loads(path.read_text()) if path.exists() else default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def build_plan(seasons: list[str]) -> dict[str, int]:
    """Map every date with big-5 fixtures to the number of matches played on it.

    One Football-Data call per competition per season, so the calendar's empty
    half is never triggered at all, and each date carries the count that proves
    it loaded completely. Only FINISHED matches count: a postponed fixture keeps
    a date the sources have nothing for.
    """
    client = FootballDataClient(football_data_config())
    counts: dict[str, int] = defaultdict(int)
    for season in seasons:
        for competition in DEFAULT_COMPETITIONS:
            payload = client.fetch_matches(
                competition, date(int(season), 7, 1), date(int(season) + 1, 6, 30)
            )
            finished = [m for m in payload.get("matches", []) if m.get("status") == "FINISHED"]
            for match in finished:
                counts[match["utcDate"][:10]] += 1
            logger.info("%s %s: %d finished matches", competition, season, len(finished))
    return dict(sorted(counts.items()))


# --------------------------------------------------------------------------- #
# Running one date
# --------------------------------------------------------------------------- #


def wait_for_run(api: AirflowApi, dag_id: str, run_id: str, deadline: float) -> str:
    while time.monotonic() < deadline:
        state = api.run_state(dag_id, run_id)
        if state in {"success", "failed"}:
            return state
        tick(api)
    return "timeout"


def wait_for_chain(api: AirflowApi, deadline: float) -> bool:
    """Wait until Silver and Gold have nothing queued or running.

    Two consecutive quiet polls are required: between Silver finishing and the
    scheduler creating the Gold run, both DAGs look idle for a moment, and
    stopping there would check Gold before it had loaded anything.
    """
    quiet = 0
    while time.monotonic() < deadline:
        active = len(api.runs_in_progress(SILVER_DAG)) + len(api.runs_in_progress(GOLD_DAG))
        quiet = quiet + 1 if active == 0 else 0
        if quiet >= 2:
            return True
        tick(api)
    return False


def process_date(
    api: AirflowApi, match_date: str, expected: int, *, from_bronze: bool
) -> dict[str, Any]:
    """Run one date through the chain and verify it reached Gold.

    ``from_bronze`` starts at Silver instead of Bronze, for dates whose raw
    payloads are already in the lake: no source is called at all.
    """
    started = time.monotonic()
    deadline = started + CHAIN_TIMEOUT_SECONDS

    dag_id = SILVER_DAG if from_bronze else BRONZE_DAG
    # Not "backfill__": Airflow 3 reserves that prefix for its own native
    # backfill runs and rejects a manual run that uses it.
    prefix = "replay" if from_bronze else "histload"
    run_id = f"{prefix}__{match_date}"

    api.trigger(dag_id, run_id, match_date)
    first_state = wait_for_run(api, dag_id, run_id, deadline)
    if first_state == "success":
        wait_for_chain(api, deadline)
    loaded = gold_match_count(match_date)

    if first_state == "success" and loaded < expected:
        # Usually Silver's own guard firing: it was woken by two dates at once
        # and refused to guess. Replaying Silver for this date recovers it, and
        # Gold follows from Silver's assets.
        logger.warning(
            "%s: Gold has %d of %d matches — replaying Silver", match_date, loaded, expected
        )
        repair_id = f"repair__{match_date}"
        api.trigger(SILVER_DAG, repair_id, match_date)
        wait_for_run(api, SILVER_DAG, repair_id, deadline)
        wait_for_chain(api, deadline)
        loaded = gold_match_count(match_date)

    status = "done" if first_state == "success" and loaded >= expected else "short"
    if first_state != "success":
        status = first_state
    return {
        "status": status,
        "expected": expected,
        "loaded": loaded,
        "seconds": round(time.monotonic() - started, 1),
        "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }


def walk_dates(
    api: AirflowApi, targets: dict[str, int], *, from_bronze: bool, retry_failed: bool
) -> None:
    state: dict[str, dict[str, Any]] = load_json(STATE_FILE, {})

    finished = {"done"} if retry_failed else {"done", "short", "failed", "timeout"}
    pending = [d for d in sorted(targets) if state.get(d, {}).get("status") not in finished]
    logger.info(
        "%d dates pending of %d (%d already done)",
        len(pending),
        len(targets),
        sum(1 for d in targets if state.get(d, {}).get("status") == "done"),
    )

    durations: list[float] = []
    for index, match_date in enumerate(pending, start=1):
        suppress_scheduled_runs(api)
        result = process_date(api, match_date, targets[match_date], from_bronze=from_bronze)
        state[match_date] = result
        save_json(STATE_FILE, state)
        durations.append(result["seconds"])

        # Nothing may exist beyond the date just loaded: the whole rebuild rests
        # on dates arriving in order, so a later one having slipped in means the
        # remaining work would pile on top of a corrupted dimension. Stop and say
        # so rather than spend hours making it worse.
        front = gold_max_date()
        if front and front > match_date:
            sys.exit(
                f"ABORT: Gold holds {front}, past the date just loaded ({match_date}). "
                "A scheduled run probably got through. Purge everything after "
                f"{match_date} and resume."
            )

        eta = timedelta(seconds=int(sum(durations) / len(durations) * (len(pending) - index)))
        logger.info(
            "[%d/%d] %s %s — %d/%d matches in %ss (ETA %s)",
            index,
            len(pending),
            match_date,
            result["status"],
            result["loaded"],
            result["expected"],
            result["seconds"],
            eta,
        )


def status() -> None:
    plan: dict[str, int] = load_json(PLAN_FILE, {})
    state: dict[str, dict[str, Any]] = load_json(STATE_FILE, {})
    deferred: list[str] = load_json(DEFERRED_FILE, [])

    by_status: dict[str, int] = defaultdict(int)
    for match_date in plan:
        by_status[state.get(match_date, {}).get("status", "pending")] += 1

    print(f"Planned dates: {len(plan)}   matches expected: {sum(plan.values())}")
    for name, count in sorted(by_status.items()):
        print(f"  {name:>8}: {count}")
    done = [r for r in state.values() if r.get("status") == "done"]
    if done:
        average = sum(r["seconds"] for r in done) / len(done)
        print(f"Average {average:.0f}s per date; {len(done)} dates loaded")
    try:
        counts = gold_counts_by_date()
        print(f"Gold: {sum(counts.values())} matches across {len(counts)} dates")
        if counts:
            print(f"      {min(counts)} .. {max(counts)}")
    except Exception as exc:  # noqa: BLE001 - status must not fail on a stopped stack
        print(f"Gold: unavailable ({type(exc).__name__})")
    problems = sorted(d for d, r in state.items() if r.get("status") != "done")
    if problems:
        print(f"Needing attention ({len(problems)}): {', '.join(problems[:15])}")
    if deferred:
        print(f"Deferred daily runs ({len(deferred)}): {', '.join(deferred)}")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # This script runs on the host, against the published ports; .env's values
    # name the Compose network's hostnames, which only resolve inside it.
    # Anything already exported still wins (src.common.config never overrides a
    # real environment variable).
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("MINIO_ENDPOINT", "http://localhost:9000")

    parser = argparse.ArgumentParser(description="Backfill whole seasons, one date at a time.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("snapshot", help="record Gold's current match counts per date")
    reset_parser = sub.add_parser("reset", help="truncate Gold for a clean chronological rebuild")
    reset_parser.add_argument("--yes", action="store_true", help="required: this destroys Gold")
    plan_parser = sub.add_parser("plan", help="ask Football-Data which dates had fixtures")
    plan_parser.add_argument("--seasons", nargs="+", required=True, help="season start years")
    run_parser = sub.add_parser("run", help="scrape and load every planned date, in order")
    run_parser.add_argument("--retry-failed", action="store_true")
    replay_parser = sub.add_parser("replay", help="reload dates already in Bronze (no scraping)")
    replay_parser.add_argument("--from", dest="from_date", required=True, help="ISO date")
    replay_parser.add_argument("--retry-failed", action="store_true")
    sub.add_parser("status", help="summarize progress")

    args = parser.parse_args()

    if args.command == "snapshot":
        counts = gold_counts_by_date()
        save_json(SNAPSHOT_FILE, counts)
        logger.info("Recorded %d dates, %d matches", len(counts), sum(counts.values()))
        return
    if args.command == "reset":
        if not args.yes:
            sys.exit("Refusing to truncate Gold without --yes")
        reset_gold()
        return
    if args.command == "plan":
        plan = build_plan(args.seasons)
        save_json(PLAN_FILE, plan)
        logger.info("Planned %d dates, %d matches", len(plan), sum(plan.values()))
        return
    if args.command == "status":
        status()
        return

    api = AirflowApi(
        base_url=os.environ.get("AIRFLOW_API_URL", "http://localhost:8088"),
        username=os.environ.get("AIRFLOW_ADMIN_USER", "airflow"),
        password=os.environ.get("AIRFLOW_ADMIN_PASSWORD", "airflow"),
    )

    if args.command == "run":
        plan = load_json(PLAN_FILE, {})
        if not plan:
            sys.exit("No plan found. Run `plan --seasons ...` first.")
        walk_dates(api, plan, from_bronze=False, retry_failed=args.retry_failed)
        return

    # replay: everything Bronze already holds from --from onwards, using the
    # counts recorded before the reset as the expectation.
    from src.common.minio_client import BronzeReader

    reader = BronzeReader()
    bronze_dates = sorted(
        {
            f"{part[0].removeprefix('year=')}-{part[1].removeprefix('month=')}"
            f"-{part[2].removeprefix('day=')}"
            for key in reader.list_keys("")
            for part in [[p for p in key.split("/") if p.startswith(("year=", "month=", "day="))]]
            if len(part) == 3
        }
    )
    previous: dict[str, int] = load_json(SNAPSHOT_FILE, {})
    targets = {d: previous.get(d, 0) for d in bronze_dates if d >= args.from_date}
    logger.info("Replaying %d dates from Bronze (>= %s)", len(targets), args.from_date)
    walk_dates(api, targets, from_bronze=True, retry_failed=args.retry_failed)


if __name__ == "__main__":
    main()
