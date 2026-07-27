"""Tests for cross-source team-name normalization."""

from __future__ import annotations

import pytest
from src.common.team_mapping import canonical_team_names, normalize_team_name


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Football-Data full names -> canonical
        ("Manchester United FC", "Manchester United"),
        ("Brighton & Hove Albion FC", "Brighton & Hove Albion"),
        ("AFC Bournemouth", "AFC Bournemouth"),
        ("Nottingham Forest FC", "Nottingham Forest"),
        # Understat short names -> same canonical form
        ("Man Utd", "Manchester United"),
        ("Brighton", "Brighton & Hove Albion"),
        ("Bournemouth", "AFC Bournemouth"),
        ("Wolves", "Wolverhampton Wanderers"),
        ("Spurs", "Tottenham Hotspur"),
    ],
)
def test_known_aliases_map_to_canonical(raw: str, expected: str) -> None:
    assert normalize_team_name(raw) == expected


def test_football_data_and_understat_names_agree() -> None:
    # The whole point: the two sources must resolve to the same key so joins work.
    assert normalize_team_name("Manchester City FC") == normalize_team_name("Man City")
    assert normalize_team_name("Newcastle United FC") == normalize_team_name("Newcastle")


# Exact spellings observed in Bronze on 2025-11-01 (one pair per big-5 league,
# plus the cases that broke the join before the mapping covered Europe).
# Written as (understat_spelling, football_data_spelling) so a regression shows
# up as a failed join rather than as silently-null xG in fact_matches.
@pytest.mark.parametrize(
    ("understat", "football_data"),
    [
        ("Brighton", "Brighton & Hove Albion FC"),
        ("Atletico Madrid", "Club Atlético de Madrid"),
        ("Bayern Munich", "FC Bayern München"),
        ("RasenBallsport Leipzig", "RB Leipzig"),
        ("Borussia M.Gladbach", "Borussia Mönchengladbach"),
        ("FC Heidenheim", "1. FC Heidenheim 1846"),
        ("Mainz 05", "1. FSV Mainz 05"),
        # Football-Data uses the founding year in match payloads but not in its
        # /teams roster — the spelling that slipped through the first mapping.
        ("St. Pauli", "FC St. Pauli 1910"),
        ("Inter", "FC Internazionale Milano"),
        ("Verona", "Hellas Verona FC"),
        ("Paris Saint Germain", "Paris Saint-Germain FC"),
        ("Marseille", "Olympique de Marseille"),
        ("Lyon", "Olympique Lyonnais"),
        ("Alaves", "Deportivo Alavés"),
    ],
)
def test_cross_source_pairs_resolve_to_one_canonical(understat: str, football_data: str) -> None:
    assert normalize_team_name(understat) == normalize_team_name(football_data)


def test_lookup_is_accent_insensitive() -> None:
    # Sources are inconsistent about diacritics; a join must not hinge on them.
    assert normalize_team_name("Malaga") == normalize_team_name("Málaga CF") == "Málaga"
    assert normalize_team_name("Borussia Moenchengladbach") == "Borussia Mönchengladbach"


def test_ambiguous_alias_is_rejected_at_build_time() -> None:
    # The alias table is hand-maintained; a duplicate alias must fail loudly
    # instead of silently pointing one club's name at another club.
    from src.common import team_mapping

    original = team_mapping._ALIASES
    try:
        team_mapping._ALIASES = {"Real Madrid": ("Los Blancos",), "Barcelona": ("Los Blancos",)}
        with pytest.raises(ValueError, match="Ambiguous team alias"):
            team_mapping._build_index()
    finally:
        team_mapping._ALIASES = original


def test_case_and_whitespace_insensitive() -> None:
    assert normalize_team_name("  man utd  ") == "Manchester United"
    assert normalize_team_name("ARSENAL FC") == "Arsenal"


def test_html_entities_are_unescaped() -> None:
    # Understat HTML-escapes apostrophes, e.g. Nott'm Forest.
    assert normalize_team_name("Nott&#039;m Forest") == "Nottingham Forest"


def test_unknown_team_falls_back_to_cleaned_name() -> None:
    # Unknown club: strip the FC suffix, return best-effort, don't raise.
    assert normalize_team_name("Some New Club FC") == "Some New Club"


def test_empty_name_raises() -> None:
    with pytest.raises(ValueError):
        normalize_team_name("   ")


def test_canonical_names_are_unique_and_sorted() -> None:
    names = canonical_team_names()
    assert names == tuple(sorted(names))
    assert len(names) == len(set(names))
