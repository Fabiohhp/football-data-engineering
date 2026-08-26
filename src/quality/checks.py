"""Declarative catalogue of Gold-layer data quality checks.

Pure and dependency-free — importing this touches no database, so CI unit-tests
the catalogue itself (see ``tests/test_quality_checks.py``). The runner that
executes it lives in ``src.quality.run_checks``.

Every check is a single ``SELECT`` returning **one row, one column: the number
of offending rows**. Zero means pass. Phrasing every check as "count the rows
that violate this rule" is what lets the runner treat them uniformly and report
*how badly* something broke rather than just that it did.

Why SQL rather than Great Expectations: the Gold layer is a star schema whose
interesting failures are relational — an orphaned fact, an SCD window that
overlaps, a result that contradicts its own score. Those are joins, and a suite
of row-wise column expectations expresses them poorly. Unlike the merge
statements in ``sql/load/``, these stay inline: a check is only meaningful
alongside its severity and the sentence explaining what breaking it implies, so
splitting the four apart would scatter one thought across two files.

What is deliberately **not** checked here: anything the DDL already enforces.
Primary keys, foreign keys, ``NOT NULL``, the ``CHECK`` constraints and the
partial unique indexes cannot be violated by a committed row, so asserting them
would only ever prove Postgres works. Every check below covers a rule the
schema *cannot* express.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Severity(str, Enum):
    """How the pipeline should react to a failing check.

    ``ERROR`` fails the run: the data is wrong in a way that would mislead a
    downstream consumer. ``WARN`` is logged and tolerated: the rule describes
    something we *expect* but that has a legitimate way of being false — an
    upstream source omitting a field, or a state the model explicitly allows.
    Reserving ERROR for genuine corruption is what keeps a red run meaningful.
    """

    ERROR = "ERROR"
    WARN = "WARN"


@dataclass(frozen=True)
class Check:
    """One data quality rule."""

    name: str
    table: str
    severity: Severity
    description: str
    sql: str


# =============================================================================
# dim_teams
# =============================================================================

_DIM_TEAMS = (
    Check(
        name="dim_teams_no_source_id_collision",
        table="gold.dim_teams",
        severity=Severity.ERROR,
        description=(
            "A source id must identify exactly one club. Two canonical names sharing one "
            "football_data_id or understat_id means team-name normalization split a single "
            "club in two, which silently halves that club's facts."
        ),
        sql="""
            SELECT count(*) FROM (
                SELECT football_data_id
                  FROM gold.dim_teams
                 WHERE football_data_id IS NOT NULL
                 GROUP BY football_data_id
                HAVING count(*) > 1
                UNION ALL
                SELECT understat_id
                  FROM gold.dim_teams
                 WHERE understat_id IS NOT NULL
                 GROUP BY understat_id
                HAVING count(*) > 1
            ) AS collisions
        """,
    ),
    Check(
        name="dim_teams_conformed_across_both_sources",
        table="gold.dim_teams",
        severity=Severity.WARN,
        description=(
            "A club seen by only one source never got conformed. Expected for competitions "
            "Understat does not cover, but a big-5 club appearing here means normalize_team_name "
            "is missing an alias — the one failure mode the cross-source join cannot survive."
        ),
        sql="""
            SELECT count(*)
              FROM gold.dim_teams
             WHERE team_key <> 0
               AND (football_data_id IS NULL OR understat_id IS NULL)
        """,
    ),
)


# =============================================================================
# dim_stadiums
# =============================================================================

_DIM_STADIUMS = (
    Check(
        name="dim_stadiums_named_ground_has_a_country",
        table="gold.dim_stadiums",
        severity=Severity.WARN,
        description=(
            "Country is the only stadium attribute Football-Data supplies besides the name and "
            "the free-form address, so a null one means the roster payload arrived without its "
            "area block. Not corruption — the ground still resolves — but it is the signal that "
            "the source shape moved."
        ),
        sql="""
            SELECT count(*)
              FROM gold.dim_stadiums
             WHERE stadium_key <> 0
               AND country IS NULL
        """,
    ),
)


# =============================================================================
# dim_players — the SCD Type 2 invariants no constraint can express
# =============================================================================

_DIM_PLAYERS = (
    Check(
        name="dim_players_no_overlapping_versions",
        table="gold.dim_players",
        severity=Severity.ERROR,
        description=(
            "Two versions of one player must never be valid on the same day, or a fact row "
            "joining 'the version valid on the match date' matches both and duplicates itself. "
            "The unique index only guards is_current, so an overlap in closed history is invisible."
        ),
        sql="""
            SELECT count(*)
              FROM gold.dim_players a
              JOIN gold.dim_players b
                ON a.understat_player_id = b.understat_player_id
               AND a.player_key < b.player_key
             WHERE a.valid_from <= COALESCE(b.valid_to, DATE '9999-12-31')
               AND b.valid_from <= COALESCE(a.valid_to, DATE '9999-12-31')
        """,
    ),
    Check(
        name="dim_players_current_flag_agrees_with_valid_to",
        table="gold.dim_players",
        severity=Severity.ERROR,
        description=(
            "is_current and valid_to encode the same fact twice: the current version is the open "
            "one. If they disagree, 'current squad' and 'squad as of today' return different "
            "answers, and which one a consumer gets depends on which column they filtered on."
        ),
        sql="""
            SELECT count(*)
              FROM gold.dim_players
             WHERE (is_current AND valid_to IS NOT NULL)
                OR (NOT is_current AND valid_to IS NULL)
        """,
    ),
    Check(
        name="dim_players_every_player_has_a_current_version",
        table="gold.dim_players",
        severity=Severity.ERROR,
        description=(
            "Closing a version without opening the replacement leaves a player with history but "
            "no present. Such a player vanishes from every current-squad query while his facts "
            "still reference him."
        ),
        sql="""
            SELECT count(*) FROM (
                SELECT understat_player_id
                  FROM gold.dim_players
                 GROUP BY understat_player_id
                HAVING count(*) FILTER (WHERE is_current) = 0
            ) AS orphaned
        """,
    ),
    Check(
        name="dim_players_team_resolved",
        table="gold.dim_players",
        severity=Severity.ERROR,
        description=(
            "Every player is loaded from a match roster, so his club is always known. A null or "
            "Unknown current_team_key means the roster's club failed to resolve against dim_teams."
        ),
        sql="""
            SELECT count(*)
              FROM gold.dim_players
             WHERE player_key <> 0
               AND (current_team_key IS NULL OR current_team_key = 0)
        """,
    ),
)


# =============================================================================
# fact_matches
# =============================================================================

_FACT_MATCHES = (
    Check(
        name="fact_matches_no_unknown_team",
        table="gold.fact_matches",
        severity=Severity.ERROR,
        description=(
            "The team FKs are NOT NULL and default to the Unknown member, so an unresolved club "
            "cannot show up as a null — it shows up here. A match against Unknown is unusable: it "
            "belongs to no club's record."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_matches
             WHERE home_team_key = 0 OR away_team_key = 0
        """,
    ),
    Check(
        name="fact_matches_unique_fixture_grain",
        table="gold.fact_matches",
        severity=Severity.ERROR,
        description=(
            "The grain is one row per match. The partial unique indexes only stop a repeat of the "
            "same source id, so the real duplication risk is one fixture landing twice — once per "
            "source — when the cross-source join fails to pair them."
        ),
        sql="""
            SELECT count(*) FROM (
                SELECT match_date, home_team_key, away_team_key
                  FROM gold.fact_matches
                 GROUP BY match_date, home_team_key, away_team_key
                HAVING count(*) > 1
            ) AS dups
        """,
    ),
    Check(
        name="fact_matches_finished_have_a_score",
        table="gold.fact_matches",
        severity=Severity.ERROR,
        description=(
            "A finished match has a score by definition. Null goals on a FINISHED row is the "
            "silent-null case the Gold contract forbids: every downstream aggregate would treat "
            "the match as though it had not been played."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_matches
             WHERE status = 'FINISHED'
               AND (home_goals IS NULL OR away_goals IS NULL)
        """,
    ),
    Check(
        name="fact_matches_result_agrees_with_score",
        table="gold.fact_matches",
        severity=Severity.ERROR,
        description=(
            "result is derived from the score, so the two can only disagree if the derivation or "
            "the source mapping broke. Checked only where both are present, since either may be "
            "legitimately null."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_matches
             WHERE home_goals IS NOT NULL
               AND away_goals IS NOT NULL
               AND result IS NOT NULL
               AND result <> CASE
                       WHEN home_goals > away_goals THEN 'HOME_TEAM'
                       WHEN home_goals < away_goals THEN 'AWAY_TEAM'
                       ELSE 'DRAW'
                   END
        """,
    ),
    Check(
        name="fact_matches_not_in_the_future",
        table="gold.fact_matches",
        severity=Severity.WARN,
        description=(
            "The pipeline loads matches that have already happened. A future match_date points at "
            "a date-handling bug — a timezone shift on match_utc, or a run given the wrong date. "
            "A warning, not an error, because ingesting a scheduled fixture is harmless in itself."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_matches
             WHERE match_date > CURRENT_DATE
        """,
    ),
    Check(
        name="fact_matches_stadium_resolved",
        table="gold.fact_matches",
        severity=Severity.WARN,
        description=(
            "A match still pointing at the 'Unknown' ground means its home club was absent from "
            "the competition roster this run staged. Legitimate for a club outside the big-5 "
            "rosters, but a big-5 fixture landing here means the venue join is drifting — either "
            "the club did not normalize or Football-Data dropped its venue."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_matches
             WHERE stadium_key = 0
        """,
    ),
    Check(
        name="fact_matches_xg_plausible",
        table="gold.fact_matches",
        severity=Severity.WARN,
        description=(
            "Team xG for one match sits well under 10; negative is impossible. A breach means "
            "Understat changed units or the field's meaning, which no type or constraint catches "
            "because the values stay perfectly valid numerics."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_matches
             WHERE (home_xg IS NOT NULL AND (home_xg < 0 OR home_xg > 15))
                OR (away_xg IS NOT NULL AND (away_xg < 0 OR away_xg > 15))
        """,
    ),
)


# =============================================================================
# fact_player_events
# =============================================================================

_FACT_PLAYER_EVENTS = (
    Check(
        name="fact_player_events_no_unknown_dimension",
        table="gold.fact_player_events",
        severity=Severity.ERROR,
        description=(
            "Same rule as the match facts: the FKs default to the Unknown member, so unresolved "
            "players or clubs surface here rather than as nulls. An event on Unknown belongs to "
            "nobody and corrupts every per-player and per-club aggregate."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_player_events
             WHERE player_key = 0 OR team_key = 0
        """,
    ),
    Check(
        name="fact_player_events_unique_per_match_and_player",
        table="gold.fact_player_events",
        severity=Severity.ERROR,
        description=(
            "One row per player per match. The unique constraint is on the Understat natural keys, "
            "so it cannot see the surrogate-key duplicate: the same player counted twice in a "
            "match under two different dimension versions."
        ),
        sql="""
            SELECT count(*) FROM (
                SELECT match_key, player_key
                  FROM gold.fact_player_events
                 WHERE match_key IS NOT NULL
                 GROUP BY match_key, player_key
                HAVING count(*) > 1
            ) AS dups
        """,
    ),
    Check(
        name="fact_player_events_team_agrees_with_player_version",
        table="gold.fact_player_events",
        severity=Severity.ERROR,
        description=(
            "player_key points at the version valid on the match date, so that version's club must "
            "be the club he played for. A mismatch means the SCD lookup picked the wrong version — "
            "the specific bug that assigns a transferred player's goals to his former club."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_player_events e
              JOIN gold.dim_players p ON p.player_key = e.player_key
             WHERE p.player_key <> 0
               AND p.current_team_key IS NOT NULL
               AND p.current_team_key <> e.team_key
        """,
    ),
    Check(
        name="fact_player_events_linked_to_a_match",
        table="gold.fact_player_events",
        severity=Severity.WARN,
        description=(
            "match_key is nullable by design — an event can land before its match does — but the "
            "load writes both in one transaction, so in practice it should always resolve. Left as "
            "a warning because the model permits it; a sustained non-zero count means the "
            "Understat fixture never reached fact_matches."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_player_events
             WHERE match_key IS NULL
        """,
    ),
    Check(
        name="fact_player_events_minutes_plausible",
        table="gold.fact_player_events",
        severity=Severity.WARN,
        description=(
            "Minutes played cannot exceed a match with extra time. An out-of-range value means the "
            "field stopped being minutes — the kind of upstream redefinition that a SMALLINT "
            "column accepts without complaint."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_player_events
             WHERE minutes_played IS NOT NULL
               AND (minutes_played < 0 OR minutes_played > 120)
        """,
    ),
)


# =============================================================================
# fact_shots
# =============================================================================

_FACT_SHOTS = (
    Check(
        name="fact_shots_no_unknown_dimension",
        table="gold.fact_shots",
        severity=Severity.ERROR,
        description=(
            "A shot attributed to the Unknown player or club cannot be aggregated by either, which "
            "makes it dead weight in the table the ML subproject cares about most."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_shots
             WHERE player_key = 0 OR team_key = 0
        """,
    ),
    Check(
        name="fact_shots_goals_reconcile_with_player_events",
        table="gold.fact_shots",
        severity=Severity.WARN,
        description=(
            "The two Understat facts must tell the same story: a player's goals in a match should "
            "equal his shots resolving to 'Goal'. This is the one check that compares two fact "
            "tables, so it catches a partial load that every single-table check would pass."
        ),
        sql="""
            SELECT count(*) FROM (
                SELECT understat_match_id,
                       player_key,
                       count(*) FILTER (WHERE result = 'Goal') AS shot_goals
                  FROM gold.fact_shots
                 GROUP BY understat_match_id, player_key
            ) AS s
            JOIN gold.fact_player_events e
              ON e.understat_match_id = s.understat_match_id
             AND e.player_key = s.player_key
             WHERE COALESCE(e.goals, 0) <> s.shot_goals
        """,
    ),
    Check(
        name="fact_shots_coordinates_on_the_pitch",
        table="gold.fact_shots",
        severity=Severity.ERROR,
        description=(
            "x and y are Understat's normalized pitch coordinates and must stay within [0, 1]. "
            "Out-of-range values mean the coordinate system changed, which would silently poison "
            "any location-based xG model built on this table."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_shots
             WHERE (x IS NOT NULL AND (x < 0 OR x > 1))
                OR (y IS NOT NULL AND (y < 0 OR y > 1))
        """,
    ),
    Check(
        name="fact_shots_have_xg",
        table="gold.fact_shots",
        severity=Severity.WARN,
        description=(
            "Per-shot xG is this table's reason to exist. A null means Understat served a shot "
            "record without it — worth surfacing, but not worth failing a load whose remaining "
            "columns are still correct."
        ),
        sql="""
            SELECT count(*)
              FROM gold.fact_shots
             WHERE xg IS NULL
        """,
    ),
)


CHECKS: tuple[Check, ...] = (
    *_DIM_TEAMS,
    *_DIM_STADIUMS,
    *_DIM_PLAYERS,
    *_FACT_MATCHES,
    *_FACT_PLAYER_EVENTS,
    *_FACT_SHOTS,
)
