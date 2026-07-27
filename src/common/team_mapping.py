"""Canonical team-name normalization across data sources.

The two live sources spell clubs differently:

* Football-Data.org uses full legal-ish names — ``"Manchester United FC"``,
  ``"Club Atlético de Madrid"``, ``"FC Bayern München"``.
* Understat uses short forms — ``"Manchester United"``, ``"Atletico Madrid"``,
  ``"Bayern Munich"``, and HTML-escaped apostrophes (``"Nott'm Forest"`` etc.).

To join xG (Understat) onto results (Football-Data) in Silver/Gold we need a
single canonical spelling per club. ``normalize_team_name`` maps any known alias
to that canonical form; unknown inputs fall back to a best-effort cleanup and a
warning so we notice new/renamed clubs instead of silently dropping joins.

Lookup is **accent-insensitive**: ``"Malaga"`` and ``"Málaga CF"`` both resolve
to ``"Málaga"``. Canonical names keep their proper diacritics because
``dim_teams.canonical_name`` is user-facing, but nothing depends on a source
spelling them correctly.

Coverage is the **big-5 domestic leagues** plus the second-tier clubs that
rotate into them via promotion. That matches the pipeline's ingestion scope
exactly: we extract only the five competitions both sources serve, so every club
that reaches Bronze should resolve through the table below rather than through
the fallback. A fallback warning in pipeline logs therefore means a real gap —
a renamed or newly promoted club — and should be fixed here, not ignored.

Note when extending this table: Football-Data's ``/competitions/{code}/teams``
endpoint returns only the **current** season's roster, and its match payloads
sometimes spell a club differently than that roster does (``"FC St. Pauli
1910"`` in a match vs no entry at all once relegated). Harvest aliases from
Bronze match payloads, not just the roster endpoint.

Keep this the single source of truth for club naming; Silver transforms should
call it rather than hand-rolling string fixes.
"""

from __future__ import annotations

import html
import re
import unicodedata

from src.common.logging import get_logger

logger = get_logger(__name__)

# Canonical name -> every alias we have seen from any source (case- and
# accent-insensitive). The canonical form itself does not need to be repeated
# as its own alias. Grouped by league purely for readability.
_ALIASES: dict[str, tuple[str, ...]] = {
    # --- Premier League / England -------------------------------------------
    "Arsenal": ("Arsenal FC",),
    "Aston Villa": ("Aston Villa FC",),
    "AFC Bournemouth": ("Bournemouth",),
    "Brentford": ("Brentford FC",),
    "Brighton & Hove Albion": (
        "Brighton",
        "Brighton & Hove Albion FC",
        "Brighton and Hove Albion",
    ),
    "Burnley": ("Burnley FC",),
    "Chelsea": ("Chelsea FC",),
    "Coventry City": ("Coventry", "Coventry City FC"),
    "Crystal Palace": ("Crystal Palace FC",),
    "Everton": ("Everton FC",),
    "Fulham": ("Fulham FC",),
    "Hull City": ("Hull", "Hull City AFC"),
    "Ipswich Town": ("Ipswich", "Ipswich Town FC"),
    "Leeds United": ("Leeds", "Leeds United FC"),
    "Leicester City": ("Leicester", "Leicester City FC"),
    "Liverpool": ("Liverpool FC",),
    "Luton Town": ("Luton", "Luton Town FC"),
    "Manchester City": ("Man City", "Manchester City FC"),
    "Manchester United": ("Man Utd", "Man United", "Manchester United FC"),
    "Newcastle United": ("Newcastle", "Newcastle United FC", "Newcastle Utd"),
    "Nottingham Forest": ("Nottingham Forest FC", "Nott'm Forest", "Nottm Forest"),
    "Sheffield United": ("Sheffield Utd", "Sheffield United FC"),
    "Southampton": ("Southampton FC",),
    "Sunderland": ("Sunderland AFC",),
    "Tottenham Hotspur": ("Tottenham", "Spurs", "Tottenham Hotspur FC"),
    "West Ham United": ("West Ham", "West Ham United FC"),
    "Wolverhampton Wanderers": ("Wolves", "Wolverhampton Wanderers FC", "Wolverhampton"),
    # --- La Liga / Spain -----------------------------------------------------
    "Real Madrid": ("Real Madrid CF",),
    "Barcelona": ("FC Barcelona", "Barcelona FC"),
    "Atlético Madrid": (
        "Atletico Madrid",
        "Club Atlético de Madrid",
        "Atlético de Madrid",
        "Atletico",
    ),
    "Athletic Club": ("Athletic Bilbao", "Athletic Club de Bilbao"),
    "Real Sociedad": ("Real Sociedad de Fútbol",),
    "Villarreal": ("Villarreal CF",),
    "Real Betis": ("Real Betis Balompié", "Betis"),
    "Sevilla": ("Sevilla FC",),
    "Valencia": ("Valencia CF",),
    "Celta Vigo": ("RC Celta de Vigo", "Celta de Vigo", "Celta"),
    "Osasuna": ("CA Osasuna",),
    "Rayo Vallecano": ("Rayo Vallecano de Madrid",),
    "Getafe": ("Getafe CF",),
    "Espanyol": ("RCD Espanyol de Barcelona", "RCD Espanyol"),
    "Girona": ("Girona FC",),
    "Deportivo Alavés": ("Alaves", "Alavés", "Deportivo Alaves"),
    "Mallorca": ("RCD Mallorca",),
    "Elche": ("Elche CF",),
    "Levante": ("Levante UD",),
    "Real Oviedo": ("Oviedo",),
    "Málaga": ("Malaga", "Málaga CF"),
    "Deportivo La Coruña": (
        "RC Deportivo La Coruña",
        "Deportivo La Coruna",
        "Deportivo",
    ),
    "Racing Santander": ("Real Racing Club de Santander", "Racing de Santander"),
    # --- Bundesliga / Germany ------------------------------------------------
    "Bayern Munich": (
        "FC Bayern München",
        "Bayern München",
        "FC Bayern Muenchen",
        "Bayern",
    ),
    "Borussia Dortmund": ("BVB", "BV Borussia 09 Dortmund"),
    "Bayer Leverkusen": ("Bayer 04 Leverkusen",),
    "RB Leipzig": ("RasenBallsport Leipzig",),
    "Borussia Mönchengladbach": (
        "Borussia M.Gladbach",
        "Borussia Moenchengladbach",
        "Gladbach",
    ),
    "Eintracht Frankfurt": ("Eintracht Frankfurt FC",),
    "VfB Stuttgart": ("Stuttgart",),
    "Werder Bremen": ("SV Werder Bremen",),
    "Union Berlin": ("1. FC Union Berlin",),
    "Mainz 05": ("1. FSV Mainz 05", "Mainz"),
    "FC Köln": ("1. FC Köln", "1. FC Koeln", "FC Cologne", "Cologne"),
    "Freiburg": ("SC Freiburg",),
    "Hoffenheim": ("TSG 1899 Hoffenheim", "TSG Hoffenheim"),
    "Augsburg": ("FC Augsburg",),
    "Heidenheim": ("FC Heidenheim", "1. FC Heidenheim 1846"),
    "St. Pauli": ("FC St. Pauli", "FC St. Pauli 1910", "St Pauli"),
    "Wolfsburg": ("VfL Wolfsburg",),
    "Hamburger SV": ("Hamburg", "HSV"),
    "Schalke 04": ("FC Schalke 04", "Schalke"),
    "Paderborn": ("SC Paderborn 07", "SC Paderborn"),
    "Elversberg": ("SV 07 Elversberg", "SV Elversberg"),
    # --- Serie A / Italy -----------------------------------------------------
    "Inter Milan": ("Inter", "FC Internazionale Milano", "Internazionale"),
    "AC Milan": ("Milan",),
    "Juventus": ("Juventus FC",),
    "Napoli": ("SSC Napoli",),
    "AS Roma": ("Roma",),
    "Lazio": ("SS Lazio",),
    "Atalanta": ("Atalanta BC",),
    "Fiorentina": ("ACF Fiorentina",),
    "Bologna": ("Bologna FC 1909",),
    "Torino": ("Torino FC",),
    "Udinese": ("Udinese Calcio",),
    "Genoa": ("Genoa CFC",),
    "Cagliari": ("Cagliari Calcio",),
    "Lecce": ("US Lecce",),
    "Sassuolo": ("US Sassuolo Calcio", "Sassuolo Calcio"),
    "Como": ("Como 1907",),
    "Cremonese": ("US Cremonese",),
    "Parma": ("Parma Calcio 1913",),
    "Pisa": ("Pisa SC", "AC Pisa 1909"),
    "Hellas Verona": ("Verona", "Hellas Verona FC"),
    "Monza": ("AC Monza",),
    "Frosinone": ("Frosinone Calcio",),
    "Venezia": ("Venezia FC",),
    # --- Ligue 1 / France ----------------------------------------------------
    "Paris Saint-Germain": ("Paris Saint Germain", "Paris Saint-Germain FC", "PSG"),
    "Marseille": ("Olympique de Marseille", "Olympique Marseille"),
    "Lyon": ("Olympique Lyonnais", "Olympique Lyon"),
    "Monaco": ("AS Monaco FC", "AS Monaco"),
    "Lille": ("Lille OSC", "LOSC Lille"),
    "Nice": ("OGC Nice",),
    "Lens": ("Racing Club de Lens", "RC Lens"),
    "Rennes": ("Stade Rennais FC 1901", "Stade Rennais"),
    "Strasbourg": ("RC Strasbourg Alsace", "RC Strasbourg"),
    "Brest": ("Stade Brestois 29", "Stade Brestois"),
    "Nantes": ("FC Nantes",),
    "Toulouse": ("Toulouse FC",),
    "Auxerre": ("AJ Auxerre",),
    "Angers": ("Angers SCO", "SCO Angers"),
    "Le Havre": ("Le Havre AC",),
    "Lorient": ("FC Lorient",),
    "Metz": ("FC Metz",),
    "Paris FC": (),
    "Troyes": ("ES Troyes AC", "ESTAC Troyes"),
    "Le Mans": ("Le Mans FC",),
}

# Suffixes/prefixes stripped in the best-effort fallback for unknown clubs.
_SUFFIX_RE = re.compile(r"\s+(FC|AFC|CF|SC)$", flags=re.IGNORECASE)
_PREFIX_RE = re.compile(r"^(AFC|FC|CF|SC)\s+", flags=re.IGNORECASE)


def _clean_raw(name: str) -> str:
    """Unescape HTML entities and collapse whitespace."""
    cleaned = html.unescape(name).strip()
    return re.sub(r"\s+", " ", cleaned)


def _fold(name: str) -> str:
    """Case- and accent-insensitive lookup key.

    Decomposes to NFKD and drops combining marks so ``"Málaga"`` and
    ``"Malaga"`` collapse to the same key — sources are inconsistent about
    diacritics and we must not lose a join over an accent.
    """
    decomposed = unicodedata.normalize("NFKD", name)
    return "".join(c for c in decomposed if not unicodedata.combining(c)).casefold()


def _build_index() -> dict[str, str]:
    """Reverse alias -> canonical index, rejecting ambiguous aliases.

    This table is hand-maintained and long enough that a copy-paste slip could
    silently point one club's alias at another club. Raising at import time
    turns that into an immediate, obvious failure instead of a corrupted join.
    """
    index: dict[str, str] = {}
    for canonical, aliases in _ALIASES.items():
        for name in (canonical, *aliases):
            key = _fold(_clean_raw(name))
            existing = index.get(key)
            if existing is not None and existing != canonical:
                raise ValueError(
                    f"Ambiguous team alias {name!r}: maps to both "
                    f"{existing!r} and {canonical!r}"
                )
            index[key] = canonical
    return index


_ALIAS_TO_CANONICAL: dict[str, str] = _build_index()


def normalize_team_name(name: str) -> str:
    """Return the canonical club name for any known source spelling.

    Unknown inputs are cleaned up (HTML-unescaped, ``FC``/``AFC`` affixes
    stripped) and returned as-is with a warning, so an unmapped club degrades
    gracefully instead of raising mid-pipeline.
    """
    if not name or not name.strip():
        raise ValueError("team name must be a non-empty string")

    cleaned = _clean_raw(name)
    canonical = _ALIAS_TO_CANONICAL.get(_fold(cleaned))
    if canonical is not None:
        return canonical

    fallback = _PREFIX_RE.sub("", _SUFFIX_RE.sub("", cleaned)).strip()
    # The de-affixed form may itself be a known alias (e.g. a novel "Foo FC"
    # spelling of an already-mapped "Foo").
    canonical = _ALIAS_TO_CANONICAL.get(_fold(fallback))
    if canonical is not None:
        return canonical

    logger.warning("Unmapped team name %r — using best-effort %r", name, fallback)
    return fallback


def canonical_team_names() -> tuple[str, ...]:
    """All canonical club names known to the mapping (sorted)."""
    return tuple(sorted(_ALIASES))
