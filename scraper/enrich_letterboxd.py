#!/usr/bin/env python3
"""
Letterboxd Enrichment Script (v3 – async, validated matching)

Reads all films_*.json / films.json data files, looks up each unique film
on Letterboxd via direct slug matching, validates page metadata against
the source, and writes back `letterboxd_url` and `letterboxd_rating`
into every matching film entry.

Designed to run as a post-scraping step in GitHub Actions, or locally.

Usage:
    python enrich_letterboxd.py                      # auto-finds public/data/
    python enrich_letterboxd.py -d ./public/data     # explicit data dir
    python enrich_letterboxd.py --dry-run             # preview without writing
    python enrich_letterboxd.py --concurrency 8       # parallel requests (default 5)
"""

import asyncio
import json
import re
import sys
import logging
import argparse
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 12
CONCURRENCY = 5          # parallel Letterboxd requests
DELAY_BETWEEN = 0.25     # seconds between starting each request
MAX_RETRIES = 2
YEAR_TOLERANCE = 2

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

LBOXD_BASE = "https://letterboxd.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("enrich_letterboxd")


# ─── Manual slug overrides ───────────────────────────────────────────
# Maps cleaned title (lowercase) → Letterboxd slug.
# These are normalised via normalize_match_key() at module load time.

_RAW_SLUG_OVERRIDES = {
    # Periods collapse to nothing in Letterboxd slugs (not hyphens)
    "dr. strangelove": "dr-strangelove-or-how-i-learned-to-stop-worrying-and-love-the-bomb",
    "e.t. the extra terrestrial": "et-the-extra-terrestrial",
    "e.t. the extra-terrestrial": "et-the-extra-terrestrial",
    "a.i. artificial intelligence": "ai-artificial-intelligence",
    "d.e.b.s.": "debs",
    "to live and die in l.a.": "to-live-and-die-in-la",
    # Curly apostrophe variants (after clean_title strips accents, these become straight)
    "at midnight i'll take your soul": "at-midnight-ill-take-your-soul",
    # Alternate titles / regional spelling
    "small axe: lovers rock": "lovers-rock",
    "timecode live": "timecode",
    "the colour of pomegranates": "the-color-of-pomegranates",
    "osamu tezuka's metropolis": "metropolis-2001",
    "osamu tezukas metropolis": "metropolis-2001",      # after apostrophe strip
    "boy and the world": "the-boy-and-the-world",
    "rocky horror picture show": "the-rocky-horror-picture-show",
    # Trilogy parts with non-standard slug patterns
    "the human condition - part 1 - no greater love":
        "the-human-condition-i-no-greater-love",
    "the human condition - part 2 - road to eternity":
        "the-human-condition-ii-road-to-eternity",
    "the human condition - part 3 - a soldier's prayer":
        "the-human-condition-iii-a-soldiers-prayer",
    "the human condition - part 3 - a soldier\u2019s prayer":
        "the-human-condition-iii-a-soldiers-prayer",
    # TV specials screened theatrically
    "twin peaks : pilot - northwest passage": "twin-peaks",
    # Miami Vice after anniversary-screening strip
    "miami vice": "miami-vice-2006",
    # ── Same-title disambiguation (no year available in source) ──
    "departures": "departures-2025",
    "love me tender": "love-me-tender-2025",
    "michael": "michael-2026",
    # ── ICA alternate titles ──
    "a ay": "oh-moon",
    # ── Garden Cinema alternate titles ──
    "jules et jim": "jules-and-jim",
    "lift to the scaffold": "elevator-to-the-gallows",
    "xiao wu": "xiao-wu",
    "the photograph": "the-photograph-1986",     # Nico/Nikos Papatakis transliteration
    "walking a tightrope": "walking-a-tightrope", # Nico/Nikos Papatakis transliteration
    # ── Rio Cinema alternate titles / spelling ──
    "murs murs": "mur-murs",                    # Varda film: Letterboxd uses "Mur Murs"
    "appropriate behaviour": "appropriate-behavior", # UK spelling → US slug on Letterboxd
}

# (title, source_year) → forced slug — for cases where the default
# year-suffixed slug resolves to the wrong film on Letterboxd.
_RAW_YEAR_SLUG_OVERRIDES = {
    ("the cannibals", 1969): "the-year-of-the-cannibals",
    ("fly away home", 1996): "fly-away-home",
    ("in the mood for love", 2000): "in-the-mood-for-love",
    ("jurassic park", 1993): "jurassic-park",
    ("pride and prejudice", 2005): "pride-prejudice",
    ("sirat", 2025): "sirat-2025",
    ("sirat", 2026): "sirat-2025",
    ("small axe lovers rock", 2020): "lovers-rock-2020",
    ("super 8½", 1994): "super-8-1994",
    # ── ICA same-year disambiguation ──
    ("dracula", 2025): "dracula-2025-1",
    ("the stranger", 2025): "the-stranger-2025-1",
    # ── Garden Cinema: Nico/Nikos Papatakis transliteration ──
    ("the photograph", 1986): "the-photograph-1986",
    ("walking a tightrope", 1991): "walking-a-tightrope",
}

# (title, source_year) → expected Letterboxd year — for when the source
# data uses a different year than Letterboxd.
_RAW_YEAR_TARGET_OVERRIDES = {
    ("sirat", 2026): 2025,
    ("twin peaks pilot northwest passage", 1990): 1989,
}

# Groups of title variants that should be treated as equivalent when
# validating a fetched page against the source film.
TITLE_EQUIVALENCE_GROUPS = [
    ("apocalypse now final cut", "apocalypse now"),
    ("blade runner the final cut", "blade runner"),
    ("doctor who the movie", "doctor who"),
    ("dr strangelove", "dr strangelove or how i learned to stop worrying and love the bomb"),
    ("man marked for death twenty years later", "twenty years later"),
    ("montreal ma belle", "montreal my beautiful"),
    ("mr hulots holiday", "monsieur hulots holiday"),
    ("neighbouring sounds", "neighboring sounds"),
    ("nightmare alley vision in darkness and light", "nightmare alley"),
    ("osamu tezukas metropolis", "metropolis"),
    ("robocop directors cut", "robocop"),
    ("rocky horror picture show", "the rocky horror picture show"),
    ("small axe lovers rock", "lovers rock"),
    ("the cannibals", "the year of the cannibals"),
    ("the colour of pomegranates", "the color of pomegranates"),
    ("the lodger", "the lodger a story of the london fog"),
    ("timecode live", "timecode"),
    ("twin peaks pilot northwest passage", "twin peaks"),
    # ── Garden Cinema alternate titles ──
    ("jules et jim", "jules and jim"),
    ("lift to the scaffold", "elevator to the gallows"),
    # ── Rio Cinema alternate titles / spelling ──
    ("murs murs", "mur murs"),
    ("appropriate behaviour", "appropriate behavior"),
]


# ─── Title cleaning constants ────────────────────────────────────────

# Known event-series prefixes (case-insensitive).
# If the text BEFORE the colon matches one of these, strip it.
EVENT_PREFIXES = [
    "adults only",
    "camp classics presents",
    "cine-real presents",
    "distorted frame",
    "dog-friendly",
    "exclusive preview",
    "exhibition on screen",
    "fetish friendly",
    "funday",
    "in the scene",
    "late night",
    "lesbian visibility day",
    "lesbian visibility",
    "lost reels presents",
    "nt live",
    "national theatre live",
    "pitchblack mixtapes",
    "pitchblack playback",
    "preview",
    # NOTE: "the male gaze" deliberately NOT here — NQV short film
    # compilations have their own Letterboxd pages and should be
    # looked up as-is (e.g. "The Male Gaze: Heavenly Creatures").
    "uk premiere of 4k restoration",
    "uk premiere",
    "violet hour presents",
    "word space presents",
    # ── ICA-specific event/strand prefixes ──
    "closing night",
    "opening night",
    "in focus",
    "jukebox film club",
    "sürreal sinema",
    "surreal sinema",
    "the cinema of",
    "three films by",
    # ── Garden Cinema event/strand prefixes ──
    "alborada films presents",
    "earth day 2026",
    "earth day 2025",
    "fashion film club presents",
]

# Prefixes that appear WITHOUT a colon — strip them directly
STRIP_PREFIXES_NO_COLON = [
    r"(?i)^sing-a-long-a\s+",
    r"(?i)^solve along a\s+",
    r"(?i)^funeral parade presents\s+",
    r"(?i)^opening\s+night\s+",
    r"(?i)^closing\s+night\s+",
    r"(?i)^(?:LONDON|UK|WORLD|EUROPEAN)\s+PREMIERE\s+",
    # ── Garden Cinema non-colon prefixes ──
    r"(?i)^re:?mind\s+film\s+festival\s+presents?\s+",
    r"(?i)^uk\s+asian\s+film\s+festival\s+presents?\s+",
    r"(?i)^waving\s+kites\s+and\s+re:?mind\s+film\s+festival\s+presents?\s+",
    # ── Possessive-name title prefixes ──
    r"(?i)^osamu\s+tezuka[''\u2019]?s?\s+",
]

# Titles matching these patterns are NOT films → skip entirely
SKIP_PATTERNS = [
    r"(?i)\bnt live\b",
    r"(?i)\bnational theatre live\b",
    r"(?i)\bexhibition on screen\b",
    r"(?i)\bshort films?\b",
    r"(?i)\bshorts\s*[&+]\s*stand-up\b",
    r"(?i)\bday pass\b",
    r"(?i)\bpitchblack (playback|mixtapes)\b",
    r"(?i)\bfilm night\b",
    r"(?i)^festival of britain\b",
    r"(?i)^future forward\b",
    r"(?i)\bfundraiser\b",
    r"(?i)^the quiz of rassilon\b",
    r"(?i)^laura mulvey\b",
    r"(?i)\bdouble feature\b",
    r"(?i)^25 and under:",
    r"(?i)^inferno, purgatory",
    r"(?i)\bmystery movie marathon\b",
    r"(?i)^mystery movie\b",
    r"(?i)\bbleak week\b",
    r"(?i)\bfilm quiz\b",
    r"(?i)\bpoetry\s*#\d",
    r"(?i)\bin conversation\b",
    r"(?i)\bsip and paint\b",
    r"(?i)\bquiz\b(?!.*\bfilm\b)",  # quiz events, but not quiz-titled films
    r"(?i)^creative minds of tomorrow",
    r"(?i)^new writings from\b",
    r"(?i)^ways of seeing archives\b",
    r"(?i)^words, songs and screens\b",
    r"(?i)^music video preservation\b",
    r"(?i)\barchive tour\b",
    r"(?i)^meet the projectionists\b",
    r"(?i)^hitchcock & herrmann\b",
    r"(?i)^melodrama as provocateur\b",
    r"(?i)\bsilent dreams shorts\b",
    r"(?i)^brat summer\b",                    # Charli XCX marathon event
    r"(?i)^mark kermode live\b",              # live event
    r"(?i)^rosie turner\s+q\s*&?\s*a\b",     # Q&A event
    r"(?i)^an introduction to\b",             # intro talks
    r"(?i)^the before trilogy\b",             # compilation
    r"(?i)\btrilogy\s*[-–—]\s*(extended|special)\b", # "Trilogy - Extended Editions"
    r"(?i)^tales of arcadia\b",              # compilation/event
    r"(?i)^two boxes\s*:",                   # double-bill event
    r"(?i)^peckhamplex$",                    # cinema name, not a film
    r"(?i)^guillermo del toro$",             # filmmaker name alone = event
    r"(?i)\bw/\s+\w+.*\bintro\b",           # "w/ Reece Shearsmith intro" suffix
    r"(?i)\bcomedy shorts\b",                # "Lesbian Visibility: Comedy Shorts"
    r"(?i)\banimated shorts\b",              # "Lesbian Visibility: Animated Shorts"
    r"(?i)^solve along a\b",
    # ── Garden Cinema non-film events & compilations ──
    r"(?i)^bar shorts\b",
    r"(?i)^clermont-ferrand\b",
    r"(?i)^offbeat folk film\b",
    r"(?i)\bkabuki salon\b",
    r"(?i)\bkaraoke party\b",
    r"(?i)\bscratch night\b",
    r"(?i)\bmembers[''\u2019]?\s*mingle\b",
    r"(?i)\bseason launch\b",
    r"(?i)^dress-up karaoke\b",
    r"(?i)^baijiu tasting\b",
    # ── Opera/ballet broadcast prefixes ──
    r"(?i)^the royal opera\b",
    r"(?i)^the metropolitan opera\b",
    r"(?i)^royal opera house\b",
    r"(?i)^bolshoi ballet\b",
    r"(?i)^roh\s*:",
]

# Pre-compiled regex for stripping " + Q&A", " + Intro …" etc.
TRAILING_PLUS_SUFFIX_RE = re.compile(
    r"\s*\+\s*("
    r"Q\s*&\s*A"
    r"|intro\b.*"
    r"|director\b.*"
    r"|panel\b.*"
    r"|special\b.*"
    r"|extended\b.*"
    r"|5:40 fantasy.*"
    r"|reece shearsmith.*"
    r")\s*$",
    flags=re.I,
)

# Parentheticals to strip from titles (not part of the actual film name)
NON_FILM_PARENS = [
    r"\(Independent Filmmakers Showcase\)",
    r"\(Short Films?\)",
    r"\(Live Score\)",
    r"\(4K Restoration\)",
    r"\(Black & White version\)",
    r"\(Director['']?s?\s*Cut\)",
    r"\(Extended\s*Cut\)",
    r"\(Theatrical\s*Cut\)",
    r"\(Sedmikr[aá]sky\)",
]


# ─── Utility functions ───────────────────────────────────────────────

def coerce_year(value) -> int | None:
    """Convert JSON year values into integers where possible."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = value.strip()
        if re.fullmatch(r"\d{4}", value):
            return int(value)
    return None


def extract_title_year_hint(title: str) -> int | None:
    """Prefer an explicit film year embedded in the title when present."""
    matches = re.findall(r"\((\d{4})\)", title)
    if len(matches) == 1:
        return int(matches[0])
    return None


def normalize_match_key(title: str | None) -> str:
    """Normalize a title into a punctuation-light comparison key."""
    if not title:
        return ""

    t = unescape(title)
    t = t.replace("½", " 1/2 ")
    t = t.replace("&", " and ")
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    t = t.lower()
    t = re.sub(r"\bpart\b", " ", t)
    t = re.sub(r"\biii\b", " 3 ", t)
    t = re.sub(r"\bii\b", " 2 ", t)
    t = re.sub(r"\biv\b", " 4 ", t)
    t = re.sub(r"\bi\b", " 1 ", t)
    t = re.sub(r"['''`´]", "", t)
    t = re.sub(r"[^a-z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def normalize_director_name(name: str) -> str:
    """Normalize a single director name for fuzzy comparison."""
    if not name:
        return ""
    t = unicodedata.normalize("NFKD", name)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    # Handle common chars that NFKD doesn't decompose
    t = t.replace("ł", "l").replace("Ł", "L")
    t = t.replace("ø", "o").replace("Ø", "O")
    t = t.replace("đ", "d").replace("Đ", "D")
    t = t.lower().strip()
    # Strip apostrophe-like chars WITHOUT creating word boundaries
    # ("Shin'ichirô" → "shinichiro", not "shin ichiro")
    t = re.sub(r"[''\u2018\u2019\u0060\u00B4]", "", t)
    # Replace remaining non-alpha chars with spaces (preserves word
    # boundaries from hyphens: "Kar-Wai" → "kar wai")
    t = re.sub(r"[^a-z ]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _split_director_field(raw: str) -> list[str]:
    """Split a raw director field into individual director names."""
    for sep in ["|", " • ", " · "]:
        if sep in raw:
            return [s.strip() for s in raw.split(sep) if s.strip()]
    return [raw.strip()] if raw.strip() else []


def _names_fuzzy_match(src_norm: str, page_norm: str) -> bool:
    """Check if two normalized director names refer to the same person."""
    if not src_norm or not page_norm:
        return False

    # Exact match
    if src_norm == page_norm:
        return True

    src_words = src_norm.split()
    page_words = page_norm.split()
    src_set = set(src_words)
    page_set = set(page_words)

    # Known first-name transliteration equivalences (normalized form)
    _FIRST_NAME_ALIASES = {
        "nico": "nikos", "nikos": "nico",
        "kate": "catherine", "catherine": "kate",
    }

    # Suffix match: "Sofía Petersen" ⊂ "Olivia Sofía Petersen"
    if src_norm.endswith(page_norm) or page_norm.endswith(src_norm):
        return True

    # Word-set match: "chan wook park" vs "park chan wook"
    if len(src_set) >= 2 and len(page_set) >= 2 and src_set == page_set:
        return True

    # Overlapping full words covering the shorter name entirely
    overlap = src_set & page_set
    if len(overlap) >= 2 and len(overlap) >= min(len(src_set), len(page_set)):
        return True

    # Alias-aware word-set match: replace known aliases and re-check
    if len(src_words) >= 2 and len(page_words) >= 2:
        src_aliased = {_FIRST_NAME_ALIASES.get(w, w) for w in src_set}
        if src_aliased == page_set:
            return True
        page_aliased = {_FIRST_NAME_ALIASES.get(w, w) for w in page_set}
        if src_set == page_aliased:
            return True

    # Initial matching: "r" matches "ribeiro", single-letter words are initials
    # Expand initials to check if they match the first letter of the other name's words
    def expand_initials(words_a, words_b):
        """Check if all words in a match words in b, treating single-char words as initials."""
        for wa in words_a:
            matched = False
            for wb in words_b:
                if wa == wb:
                    matched = True
                    break
                if len(wa) == 1 and wb.startswith(wa):
                    matched = True
                    break
                if len(wb) == 1 and wa.startswith(wb):
                    matched = True
                    break
            if not matched:
                return False
        return True

    if len(src_words) >= 2 and len(page_words) >= 2:
        if expand_initials(src_words, page_words) or expand_initials(page_words, src_words):
            return True

    # Nickname/variant matching: shared last name + first names share prefix ≥4 chars
    # Catches "Charles Chaplin" vs "Charlie Chaplin"
    if len(src_words) >= 2 and len(page_words) >= 2 and src_words[-1] == page_words[-1]:
        src_first = src_words[0]
        page_first = page_words[0]
        prefix_len = min(len(src_first), len(page_first), 4)
        if prefix_len >= 4 and src_first[:prefix_len] == page_first[:prefix_len]:
            return True

    return False


def directors_match(source_director: str | None, page_directors: list[str]) -> bool | None:
    """Check whether the source director matches any of the Letterboxd page directors.

    Returns:
        True  — at least one director matches
        False — source has a director but none of the page directors match
        None  — not enough info to decide (source has no director, or page has none)
    """
    if not source_director:
        return None
    if not page_directors:
        return None

    # Split source into individual directors (handles "Ethan Coen|Joel Coen")
    source_names = _split_director_field(source_director)
    source_norms = [normalize_director_name(n) for n in source_names]
    source_norms = [n for n in source_norms if n]
    if not source_norms:
        return None

    # Deduplicate page directors
    page_norms = []
    seen = set()
    for pd in page_directors:
        pn = normalize_director_name(pd)
        if pn and pn not in seen:
            page_norms.append(pn)
            seen.add(pn)
    if not page_norms:
        return None

    # Check if ANY source director matches ANY page director
    for src_norm in source_norms:
        for page_norm in page_norms:
            if _names_fuzzy_match(src_norm, page_norm):
                return True

    return False


def validate_director(director: str | None) -> str | None:
    """Return None if the director string looks garbled or implausible.

    Some scrapers (notably Close-Up Film Centre) sometimes capture programme
    descriptions, film titles, or other metadata as the director name.
    """
    if not director:
        return None
    d = director.strip()
    if not d:
        return None
    # Too long to be a real director name
    if len(d) > 60:
        return None
    # Contains patterns that indicate scraper confusion
    garbled = [
        r"\(by\s",              # "(by Kelly Gabron)"
        r"^Programme:",         # "Programme: Cairo Streets Abdellah Taïa"
        r"^Opening the",        # "Opening the 19th Century: 1896 Ken Jacobs"
        r"^Lying Spirit",       # "Lying Spirit (by Kelly Gabron) Cauleen Smith"
        r"^Pride:",             # "Pride: From Jericho to Gaza Sven Augustijnen"
        r"^Combined\b",         # "Combined Programme..."
        r"\bProgramme\b",       # any "Programme" reference
        r":\s*\d{4}\s",         # "1896 Ken Jacobs" (year embedded)
        r"\bChild Labor\b",     # "Capitalism: Child Labor Ken Jacobs"
    ]
    for pattern in garbled:
        if re.search(pattern, d, re.I):
            return None
    return d


def build_equivalence_map(groups: list[tuple[str, ...]]) -> dict[str, set[str]]:
    """Create a symmetric title-equivalence map from grouped aliases."""
    result: dict[str, set[str]] = {}
    for group in groups:
        normalized = {normalize_match_key(value) for value in group if value}
        normalized.discard("")
        if not normalized:
            continue
        for key in normalized:
            result.setdefault(key, set()).update(normalized)
    return result


# ─── Build normalised override tables at module load time ────────────

SLUG_OVERRIDES = {
    normalize_match_key(title): slug
    for title, slug in _RAW_SLUG_OVERRIDES.items()
}

YEAR_SLUG_OVERRIDES = {
    (normalize_match_key(title), year): slug
    for (title, year), slug in _RAW_YEAR_SLUG_OVERRIDES.items()
}

YEAR_TARGET_OVERRIDES = {
    (normalize_match_key(title), year): target_year
    for (title, year), target_year in _RAW_YEAR_TARGET_OVERRIDES.items()
}

TITLE_EQUIVALENTS = build_equivalence_map(TITLE_EQUIVALENCE_GROUPS)


def equivalent_title_keys(title: str | None) -> set[str]:
    """Return the normalized title plus any accepted equivalent titles."""
    key = normalize_match_key(title)
    if not key:
        return set()

    keys = set(TITLE_EQUIVALENTS.get(key, {key}))
    expanded = set(keys)
    for value in keys:
        if value.startswith(("the ", "a ", "an ")):
            expanded.add(re.sub(r"^(the|a|an)\s+", "", value))
        else:
            expanded.add(f"the {value}")
    return expanded


# ─── Title cleaning ──────────────────────────────────────────────────

def clean_title_for_lookup(title: str) -> str:
    """
    Strip event prefixes, Q&A / intro suffixes, brackets, and other cruft
    to extract the underlying film title for Letterboxd lookup.
    """
    t = title.strip()

    # Strip [original language title] brackets
    t = re.sub(r"\s*\[[^\]]+\]\s*$", "", t)
    t = re.sub(r"\s*\[[^\]]+\]", "", t)

    # Strip known event-series prefixes (colon-separated)
    if ":" in t:
        before_colon = t.split(":", 1)[0].strip()
        before_lower = before_colon.lower()
        before_lower_clean = re.sub(r"\s+presents$", "", before_lower)
        for prefix in EVENT_PREFIXES:
            if before_lower == prefix or before_lower_clean == prefix:
                t = t.split(":", 1)[1].strip()
                t = t.strip('"').strip("\u201c").strip("\u201d").strip()
                # Strip "with Firstname Lastname" attribution that follows
                # event-prefixed titles (e.g. "Colossal Wreck with Josh Appignanesi")
                t = re.sub(r"\s+with\s+[A-Z][a-z]+(?:\s+[A-Z][a-zÀ-ÿ]+)+\s*$", "", t)
                break

    # Strip non-colon prefixes
    for pattern in STRIP_PREFIXES_NO_COLON:
        t = re.sub(pattern, "", t)

    # Handle 'X presents "Title"' patterns (with quotes, no colon)
    match = re.match(
        r'(?i)^(?:funeral parade|lost reels|word space)\s+presents\s*["\u201c]([^"\u201d]+)["\u201d]',
        t,
    )
    if match:
        t = match.group(1).strip()

    # Strip " + Q&A", " + Intro …" etc.
    plus_match = TRAILING_PLUS_SUFFIX_RE.search(t)
    if plus_match:
        t = t[:plus_match.start()].strip()

    # Strip bullet-separated suffixes (e.g. "Audition • 4K Restoration UK Theatrical Premiere")
    # Only strip if the part after the bullet looks like a non-title event/release descriptor
    bullet_match = re.search(r"\s*[•·]\s+", t)
    if bullet_match:
        after = t[bullet_match.end():]
        if re.search(r"(?i)(restoration|premiere|screening|re-?release|anniversary|4k|imax|hfr|special)", after):
            t = t[:bullet_match.start()].strip()

    # Strip "with X live on stage" suffix
    t = re.sub(r"\s+with\s+\w[\w\s]*\blive on stage\b.*$", "", t, flags=re.I)

    # Strip non-film parentheticals
    for pattern in NON_FILM_PARENS:
        t = re.sub(r"\s*" + pattern, "", t, flags=re.I)

    # Strip year parenthetical (we pass year separately)
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t)

    # Strip re-release / anniversary suffixes
    t = re.sub(r"\s*[-\u2013\u2014]\s*\d+\w*\s*anniversary[\w\s]*$", "", t, flags=re.I)
    t = re.sub(r"\s+\d+\w*\s*anniversary[\w\s]*$", "", t, flags=re.I)
    t = re.sub(r"\s*\(\d+\w*\s*anniversary[^)]*\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(re-?release\)", "", t, flags=re.I)

    # Strip restoration/premiere/director's cut suffixes after colon/dash
    t = re.sub(r"\s*[:\u2013\u2014-]\s*4K\s+Restoration\s*(Premiere)?\s*$", "", t, flags=re.I)
    t = re.sub(r"\s*[-\u2013\u2014]\s*restoration\s+premiere\s*$", "", t, flags=re.I)
    t = re.sub(r"\s*:\s*Director['\u2019]?s?\s*Cut\s*$", "", t, flags=re.I)

    # Strip trailing "Preview" / "Exclusive Preview" / "Screening"
    t = re.sub(r"\s+(Exclusive\s+)?Preview\s*$", "", t, flags=re.I)
    t = re.sub(r"\s+Screening\s*$", "", t, flags=re.I)

    # Strip "w/ Name intro" suffix
    t = re.sub(r"\s+w/\s+\w[\w\s]*\bintro\b.*$", "", t, flags=re.I)

    # Extract English title from parenthesized translation:
    # "Relatos salvajes (Wild Tales)" → "Wild Tales"
    # "El secreto de sus ojos (The Secret in Their Eyes)" → "The Secret in Their Eyes"
    # Triggers when a trailing parenthetical is all-ASCII and starts with a
    # capital letter — by this point, years, anniversaries, and known
    # non-film descriptors have already been stripped.
    paren_match = re.match(r'^(.+?)\s*\(([A-Z][A-Za-z0-9\s\':,!?\-]{2,})\)\s*$', t)
    if paren_match:
        english_part = paren_match.group(2).strip()
        if english_part.isascii() and not re.match(
            r'(?i)^(live|extended|director|theatrical|original|restoration|remaster|special|uncut)',
            english_part,
        ) and not re.match(r'^[A-Z]{2,}\b', english_part):
            t = english_part

    return t.strip('"').strip("\u201c").strip("\u201d").strip('"').strip()


def looks_like_multi_film_title(title: str) -> bool:
    """Heuristic: if a cleaned title still has multiple long parts, skip it."""
    if " + " not in title:
        return False
    if re.search(r"(?i)romeo\s*\+\s*juliet", title):
        return False
    if re.search(r"(?i)\d\+\d", title):
        return False

    parts = [part.strip(" -:") for part in title.split(" + ")]
    if len(parts) < 2:
        return False

    normalized_parts = [normalize_match_key(part) for part in parts]
    return all(len(part) >= 4 for part in normalized_parts[:2])


def should_skip(title: str) -> bool:
    """Return True if this title is an event / compilation, not a single film."""
    cleaned = clean_title_for_lookup(title)
    for candidate in (title, cleaned):
        for pattern in SKIP_PATTERNS:
            if re.search(pattern, candidate):
                return True
    return looks_like_multi_film_title(cleaned)


# ─── Slug construction ───────────────────────────────────────────────

def slugify_title(title: str) -> str:
    """Convert a film title into a Letterboxd-style slug."""
    s = title
    # Handle vulgar fractions before normalization (NFD doesn't decompose these)
    s = s.replace("½", " 1 2 ")
    s = s.replace("¼", " 1 4 ")
    s = s.replace("¾", " 3 4 ")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    # Strip ALL apostrophe-like characters (straight, curly left/right, backtick)
    s = re.sub(r"[''\u2018\u2019\u0060\u00B4]", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s


def generate_title_variants(title: str) -> list[str]:
    """Generate a small set of safe title variants for slug guessing."""
    variants: list[str] = []
    seen: set[str] = set()

    def add(value: str):
        clean = value.strip()
        if clean and clean not in seen:
            seen.add(clean)
            variants.append(clean)

    add(title)
    add(clean_title_for_lookup(title))

    if re.search(r"(?i)\band\b", title):
        add(re.sub(r"(?i)\band\b", "&", title))
        add(re.sub(r"(?i)\band\b", "&", clean_title_for_lookup(title)))
    if "&" in title:
        add(title.replace("&", "and"))
        add(clean_title_for_lookup(title).replace("&", "and"))

    # Try without leading article — Letterboxd sometimes omits "The"/"A"
    # from the slug (e.g. "The Male Gaze: Heavenly Creatures" →
    # /film/male-gaze-heavenly-creatures/)
    for base in list(variants):
        stripped = re.sub(r"^(?:The|A|An)\s+", "", base, flags=re.I)
        if stripped != base:
            add(stripped)

    return variants


def valid_source_slugs(title: str) -> set[str]:
    """Return slugs plausibly derived from the source title itself."""
    slugs = set()
    for variant in generate_title_variants(title):
        slug = slugify_title(variant)
        if slug:
            slugs.add(slug)
    return slugs


def is_specific_title(title: str) -> bool:
    """
    Long or structurally specific titles are safer to accept even when the
    source year is noisy.
    """
    cleaned = normalize_match_key(clean_title_for_lookup(title))
    if len(cleaned.split()) >= 4:
        return True
    return any(token in title for token in (":", "-", "'", "\u2019", ","))


# ─── Letterboxd page parsing ────────────────────────────────────────

def parse_page_title_and_year(soup: BeautifulSoup) -> tuple[str | None, int | None]:
    """Extract the Letterboxd page title and year from page metadata."""
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title:
        raw = unescape(og_title.get("content", "")).strip()
        match = re.match(r"(.+?)\s*\((\d{4})\)\s*$", raw)
        if match:
            return match.group(1).strip(), int(match.group(2))
        if raw:
            return raw, None

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            payload = json.loads(script.string)
        except (json.JSONDecodeError, TypeError, ValueError):
            continue

        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            date_published = item.get("datePublished")
            year = None
            if isinstance(date_published, str):
                year_match = re.match(r"(\d{4})", date_published)
                if year_match:
                    year = int(year_match.group(1))
            if name:
                return str(name).strip(), year

    headline = soup.select_one("h1.headline-1, h1")
    if headline:
        return headline.get_text(" ", strip=True), None

    return None, None


def extract_rating_from_soup(soup: BeautifulSoup) -> float | None:
    """Extract the average Letterboxd rating from the film page."""
    twitter_meta = soup.find("meta", attrs={"name": "twitter:data2"})
    if twitter_meta:
        content = twitter_meta.get("content", "")
        match = re.search(r"([\d.]+)\s*out\s*of\s*5", content)
        if match:
            return float(match.group(1))

    for meta in soup.find_all("meta"):
        content = meta.get("content", "")
        if "out of 5" in content:
            match = re.search(r"([\d.]+)\s*out\s*of\s*5", content)
            if match:
                return float(match.group(1))

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            payload = json.loads(script.string)
        except (json.JSONDecodeError, TypeError, ValueError):
            continue

        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if isinstance(item, dict) and "aggregateRating" in item:
                agg = item["aggregateRating"]
                value = agg.get("ratingValue")
                if value is None:
                    continue
                rating = float(value)
                best = float(agg.get("bestRating", 5))
                if best == 10:
                    rating = round(rating / 2, 2)
                return rating

    avg_el = soup.select_one("a.display-rating, .average-rating")
    if avg_el:
        text = avg_el.get_text(strip=True)
        match = re.search(r"([\d.]+)", text)
        if match:
            return float(match.group(1))

    return None


def extract_slug_from_url(url: str) -> str | None:
    """Extract /film/<slug>/ from a Letterboxd URL."""
    match = re.search(r"/film/([^/?#]+)/?", url)
    return match.group(1) if match else None


def extract_directors_from_soup(soup: BeautifulSoup) -> list[str]:
    """Extract director names from a Letterboxd page.

    Tries multiple sources in order:
      1. ld+json structured data (most reliable when present)
      2. <a href="/director/..."> links in the page (visible on almost all pages)
      3. "Directed by" text in the header area
    """
    directors = []

    # ── Strategy 1: ld+json ──
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            payload = json.loads(script.string)
        except (json.JSONDecodeError, TypeError, ValueError):
            continue

        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            if not isinstance(item, dict):
                continue
            raw = item.get("director")
            if not raw:
                continue
            entries = raw if isinstance(raw, list) else [raw]
            for entry in entries:
                if isinstance(entry, dict):
                    name = entry.get("name")
                    if name:
                        directors.append(str(name).strip())
                elif isinstance(entry, str) and entry.strip():
                    directors.append(entry.strip())

    if directors:
        return directors

    # ── Strategy 2: /director/ links ──
    # Letterboxd pages have <a href="/director/name/"> elements
    for link in soup.find_all("a", href=re.compile(r"^/director/")):
        name = link.get_text(strip=True)
        if name and len(name) > 1:
            directors.append(name)

    if directors:
        return directors

    # ── Strategy 3: "Directed by" visible text ──
    # Sometimes appears in a <span> or <p> near the title
    for el in soup.find_all(string=re.compile(r"Directed by")):
        parent = el.parent
        if parent:
            # The director name is usually in a sibling <a> tag
            link = parent.find("a", href=re.compile(r"/director/"))
            if link:
                name = link.get_text(strip=True)
                if name:
                    directors.append(name)

    return directors


def extract_page_metadata(html: str, final_url: str) -> dict:
    """Extract the metadata needed to validate and persist a Letterboxd page."""
    soup = BeautifulSoup(html, "html.parser")
    title, year = parse_page_title_and_year(soup)
    rating = extract_rating_from_soup(soup)
    directors = extract_directors_from_soup(soup)

    clean_url = final_url.split("?")[0]
    if not clean_url.endswith("/"):
        clean_url += "/"

    return {
        "url": clean_url,
        "slug": extract_slug_from_url(clean_url),
        "title": title,
        "year": year,
        "rating": round(rating, 2) if rating is not None else None,
        "directors": directors,
    }


# ─── HTTP fetch ──────────────────────────────────────────────────────

async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    retries: int = MAX_RETRIES,
) -> dict | None:
    """GET with retries and return HTML plus the final redirected URL."""
    for attempt in range(retries + 1):
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                allow_redirects=True,
            ) as resp:
                if resp.status == 200:
                    return {
                        "html": await resp.text(),
                        "final_url": str(resp.url),
                    }
                if resp.status == 404:
                    return None
                log.warning(f"  HTTP {resp.status} for {url} (attempt {attempt + 1})")
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.warning(f"  Request error for {url}: {exc} (attempt {attempt + 1})")

        if attempt < retries:
            await asyncio.sleep(1.5 * (attempt + 1))

    return None


# ─── Slug candidate generation & validation ──────────────────────────

@dataclass(frozen=True)
class SlugCandidate:
    slug: str
    label: str
    candidate_year: int | None = None


def build_slug_candidates(title: str, year: int | None) -> list[SlugCandidate]:
    """Build an ordered list of slug guesses to try for a source title."""
    candidates: list[SlugCandidate] = []
    seen: set[str] = set()
    title_key = normalize_match_key(title)

    def add(slug: str, label: str, candidate_year: int | None = None):
        slug = slug.strip("/")
        if not slug or slug in seen:
            return
        seen.add(slug)
        candidates.append(SlugCandidate(slug=slug, label=label, candidate_year=candidate_year))

    # Year-specific overrides first
    if year is not None:
        override = YEAR_SLUG_OVERRIDES.get((title_key, year))
        if override:
            target_year = YEAR_TARGET_OVERRIDES.get((title_key, year), year)
            add(override, "year override", target_year)

    # Title-only overrides
    override = SLUG_OVERRIDES.get(title_key)
    if override:
        target_year = YEAR_TARGET_OVERRIDES.get((title_key, year), year) if year is not None else None
        add(override, "override", target_year)

    # Auto-generated slug variants
    for variant in generate_title_variants(title):
        slug = slugify_title(variant)
        if not slug:
            continue
        if year is not None:
            # 1. Exact year (highest confidence)
            add(f"{slug}-{year}", f"{variant} [{year}]", year)
            # 2. Title-only / bare slug (Letterboxd's canonical entry for the
            #    most notable film with this title — preferred over year±1
            #    to avoid matching a different film from an adjacent year)
            add(slug, f"{variant} [title-only]")
            # 3. Adjacent years (lowest confidence — different film risk)
            add(f"{slug}-{year - 1}", f"{variant} [{year - 1}]", year - 1)
            add(f"{slug}-{year + 1}", f"{variant} [{year + 1}]", year + 1)
        else:
            # No year available — try recent years first (cinema listings
            # without year are almost always current/recent releases), then
            # fall back to bare slug for genuine catalogue titles.
            # This ordering means: if mother-mary-2026 exists, it wins over
            # mother-mary (1982). But if debs-2026/2025 both 404, we still
            # correctly match debs (2004) via the bare slug.
            current_year = datetime.now().year
            add(f"{slug}-{current_year}", f"{variant} [{current_year} guess]", current_year)
            add(f"{slug}-{current_year - 1}", f"{variant} [{current_year - 1} guess]", current_year - 1)
            add(slug, f"{variant} [title-only]")

    return candidates


def title_match_strength(source_title: str, page_title: str | None, final_slug: str | None) -> int:
    """Score how confidently a page title looks like the intended source title.

    Returns:
        3 — equivalence group match (strongest)
        2 — slug plausibly derived from source title
        0 — no recognisable connection
    """
    if not page_title:
        return 0

    source_keys = equivalent_title_keys(source_title)
    page_keys = equivalent_title_keys(page_title)
    if source_keys & page_keys:
        return 3

    if final_slug and final_slug in valid_source_slugs(source_title):
        return 2

    # Slug-based fallback: compare slugified titles directly.
    # This catches cases where normalize_match_key diverges due to
    # unusual Unicode (e.g. different apostrophe codepoints between
    # source and page), but the slugified versions still match.
    source_slug = slugify_title(source_title)
    page_slug = slugify_title(page_title)
    if source_slug and page_slug and source_slug == page_slug:
        return 2

    # Also check if the page's final slug (minus any year suffix) matches
    # a slug derived from the source title
    if final_slug:
        bare_slug = re.sub(r"-\d{4}$", "", final_slug)
        if bare_slug in valid_source_slugs(source_title):
            return 2

    return 0


def is_valid_page_match(
    source_title: str,
    source_year: int | None,
    source_director: str | None,
    page: dict,
    candidate: SlugCandidate,
) -> tuple[bool, str]:
    """Validate that a fetched Letterboxd page actually represents the source film."""
    page_title = page.get("title")
    page_year = coerce_year(page.get("year"))
    page_directors = page.get("directors", [])
    final_slug = page.get("slug")

    strength = title_match_strength(source_title, page_title, final_slug)
    if strength == 0:
        page_label = f"{page_title} ({page_year})" if page_title else "unknown page"
        return False, f"title mismatch -> {page_label}"

    # ── Director check ──
    dir_match = directors_match(source_director, page_directors)

    # Director mismatch is a strong rejection signal — even if title+year
    # are identical (e.g. Rocky 1976 Avildsen vs Rocky 1976 McCarthy)
    if dir_match is False:
        page_dir_str = ", ".join(page_directors[:2])
        return False, f"director mismatch: source={source_director!r} page={page_dir_str!r}"

    # ── Year checks ──

    # If director positively matches, trust it over year-tolerance —
    # a confirmed director match is stronger than a year discrepancy
    # (source year metadata can be noisy across territories/festivals)
    if dir_match is True:
        return True, "ok (director confirmed)"

    # If the candidate carried a specific expected year, verify it
    if candidate.candidate_year is not None and page_year is not None and page_year != candidate.candidate_year:
        return False, f"candidate year {candidate.candidate_year} resolved to {page_year}"

    # General year-tolerance check (source has year, director unknown)
    if source_year is not None and page_year is not None and candidate.candidate_year is None:
        delta = abs(source_year - page_year)
        if delta > YEAR_TOLERANCE and not (strength >= 3 and is_specific_title(source_title)):
            return False, f"source year {source_year} resolved to {page_year}"

    return True, "ok"


async def try_direct_slug(
    session: aiohttp.ClientSession,
    title: str,
    year: int | None = None,
    director: str | None = None,
) -> dict | None:
    """
    Try Letterboxd slugs derived from the cleaned title, but only accept pages
    whose resolved metadata still look like the intended film.
    """
    for candidate in build_slug_candidates(title, year):
        url = f"{LBOXD_BASE}/film/{candidate.slug}/"
        payload = await fetch_with_retry(session, url, retries=0)
        if payload is None:
            continue

        page = extract_page_metadata(payload["html"], payload["final_url"])
        is_valid, reason = is_valid_page_match(title, year, director, page, candidate)
        if is_valid:
            return {
                "url": page["url"],
                "rating": page["rating"],
            }

        page_title = page.get("title") or "unknown"
        page_year = page.get("year")
        year_suffix = f" ({page_year})" if page_year else ""
        log.info(
            f"  rejected {candidate.label}: {page.get('url')} -> {page_title}{year_suffix}"
            f" [{reason}]"
        )

    return None


# ─── Normalisation (dedup across scrapers) ────────────────────────────

def normalize_for_lookup(title: str) -> str:
    """Normalise a title for deduplication across cinema data files."""
    t = unicodedata.normalize("NFD", title)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    t = t.lower().strip()
    t = re.sub(r"\s*\[[^\]]+\]", "", t)           # strip [original title]
    t = re.sub(r"\s*[-\u2013\u2014]\s*\d+\w*\s*anniversary.*$", "", t, flags=re.I)
    t = re.sub(r"\s*\(\d+\w*\s*anniversary[^)]*\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(re-?release\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ─── Async lookup pipeline ────────────────────────────────────────────

async def lookup_film(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    title: str,
    year: int | None,
    director: str | None,
    index: int,
    total: int,
) -> dict:
    """Clean the title and look up on Letterboxd. Returns enrichment dict."""
    async with semaphore:
        cleaned = clean_title_for_lookup(title)
        year_str = f" ({year})" if year else ""
        dir_str = f" [dir: {director}]" if director else ""

        if cleaned != title:
            log.info(f"[{index}/{total}] \u21b3 cleaned: \"{title}\" \u2192 \"{cleaned}\"{year_str}{dir_str}")
        else:
            log.info(f"[{index}/{total}] {title}{year_str}{dir_str}")

        result = await try_direct_slug(session, cleaned, year, director)
        if result and result["url"]:
            log.info(f"  \u2713 {result['url']} \u2014 rating: {result['rating']}")
            return {"letterboxd_url": result["url"], "letterboxd_rating": result["rating"]}

        # Fallback: retry with the original title if cleaning changed it
        if cleaned != title:
            await asyncio.sleep(0.3)
            result = await try_direct_slug(session, title, year, director)
            if result and result["url"]:
                log.info(f"  \u2713 (original) {result['url']} \u2014 rating: {result['rating']}")
                return {"letterboxd_url": result["url"], "letterboxd_rating": result["rating"]}

        log.warning(f"  \u2717 no result for: {cleaned}{year_str}")
        return {"letterboxd_url": None, "letterboxd_rating": None}


# ─── Main pipeline ────────────────────────────────────────────────────

def find_data_files(data_dir: Path) -> list[Path]:
    patterns = ["films.json", "films_*.json"]
    files = []
    for pattern in patterns:
        files.extend(data_dir.glob(pattern))
    return sorted(set(files))


def collect_unique_titles(data_files: list[Path]) -> dict[str, dict]:
    """
    Returns {normalised_key: {"title": str, "year": int|None, "director": str|None, "skip": bool}}
    for all unique films. Merges year and director info across cinemas.
    """
    entries = {}
    for path in data_files:
        data = json.loads(path.read_text(encoding="utf-8"))
        for film in data.get("films", []):
            key = normalize_for_lookup(film["title"])
            if key not in entries:
                entries[key] = []
            entries[key].append({
                "title": film["title"],
                "year": extract_title_year_hint(film["title"]) or coerce_year(film.get("year")),
                "director": validate_director(film.get("director")),
            })

    result = {}
    for key, items in entries.items():
        best_title = min(set(item["title"] for item in items), key=len)
        best_year = next((item["year"] for item in items if item.get("year")), None)
        best_director = next((item["director"] for item in items if item.get("director")), None)
        skip = should_skip(best_title)
        result[key] = {"title": best_title, "year": best_year, "director": best_director, "skip": skip}

    return result


def enrich_data_files(data_files: list[Path], lookup_cache: dict, dry_run: bool = False):
    """Write letterboxd_url and letterboxd_rating into each film entry."""
    for path in data_files:
        data = json.loads(path.read_text(encoding="utf-8"))
        modified = False

        for film in data.get("films", []):
            key = normalize_for_lookup(film["title"])
            if key in lookup_cache:
                lb = lookup_cache[key]
                if film.get("letterboxd_url") != lb["letterboxd_url"] or \
                   film.get("letterboxd_rating") != lb["letterboxd_rating"]:
                    film["letterboxd_url"] = lb["letterboxd_url"]
                    film["letterboxd_rating"] = lb["letterboxd_rating"]
                    modified = True

        if modified and not dry_run:
            path.write_text(
                json.dumps(data, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            log.info(f"\u270f\ufe0f  Updated {path.name}")
        elif modified:
            log.info(f"[DRY RUN] Would update {path.name}")
        else:
            log.info(f"  No changes needed for {path.name}")


async def run(args):
    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        candidates = [
            Path(__file__).parent / "public" / "data",
            Path(__file__).parent.parent / "public" / "data",
            Path("public/data"),
            Path("."),
        ]
        data_dir = next((d for d in candidates if d.exists() and any(d.glob("films*.json"))), None)
        if data_dir is None:
            log.error("Could not find data directory. Use -d to specify.")
            sys.exit(1)

    log.info("=== Letterboxd Enrichment Starting ===")
    log.info(f"Data directory: {data_dir.resolve()}")

    data_files = find_data_files(data_dir)
    if not data_files:
        log.error(f"No films*.json files found in {data_dir}")
        sys.exit(1)
    log.info(f"Found {len(data_files)} data file(s): {[f.name for f in data_files]}")

    unique_titles = collect_unique_titles(data_files)

    to_lookup = {k: v for k, v in unique_titles.items() if not v["skip"]}
    skipped = {k: v for k, v in unique_titles.items() if v["skip"]}

    has_year = sum(1 for v in to_lookup.values() if v["year"])
    log.info(f"Found {len(unique_titles)} unique titles total")
    log.info(f"  \u2192 {len(skipped)} skipped (events/compilations/NT Live)")
    log.info(f"  \u2192 {len(to_lookup)} to look up ({has_year} with year, {len(to_lookup) - has_year} without)")

    if skipped:
        log.info("Skipped titles:")
        for v in sorted(skipped.values(), key=lambda x: x["title"]):
            log.info(f"  \u23ed  {v['title']}")

    # ── Deduplicate by cleaned title to avoid redundant HTTP requests ──
    clean_to_keys: dict[str, list[str]] = {}
    for key, info in to_lookup.items():
        cleaned = clean_title_for_lookup(info["title"])
        year = info["year"]
        dedup_key = f"{cleaned.lower()}|{year or ''}"
        clean_to_keys.setdefault(dedup_key, []).append(key)

    # Build the deduped work list
    work_items = []
    for dedup_key, keys in clean_to_keys.items():
        # Pick the representative entry (prefer one with a year)
        rep_key = next((k for k in keys if to_lookup[k]["year"]), keys[0])
        work_items.append((dedup_key, rep_key, keys))

    log.info(f"  \u2192 {len(work_items)} unique lookups after dedup (saved {len(to_lookup) - len(work_items)} requests)")

    # ── Async lookups ──
    concurrency = args.concurrency
    semaphore = asyncio.Semaphore(concurrency)
    log.info(f"  \u2192 concurrency: {concurrency}")

    lookup_cache: dict[str, dict] = {}

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = []
        for i, (dedup_key, rep_key, keys) in enumerate(work_items, 1):
            info = to_lookup[rep_key]
            task = lookup_film(session, semaphore, info["title"], info["year"], info.get("director"), i, len(work_items))
            tasks.append((keys, task))
            # Stagger task creation slightly to avoid burst
            if i % concurrency == 0:
                await asyncio.sleep(DELAY_BETWEEN)

        # Gather results
        results = await asyncio.gather(*(t for _, t in tasks))

        for (keys, _), result in zip(tasks, results):
            for key in keys:
                lookup_cache[key] = result

    # Also mark skipped titles as no-result
    for key in skipped:
        lookup_cache[key] = {"letterboxd_url": None, "letterboxd_rating": None}

    # Summary
    found = sum(1 for k, v in lookup_cache.items() if v["letterboxd_url"] and k not in skipped)
    rated = sum(1 for k, v in lookup_cache.items() if v["letterboxd_rating"] is not None and k not in skipped)
    log.info(f"\n{'='*50}")
    log.info(f"Results: {found}/{len(to_lookup)} films found on Letterboxd")
    log.info(f"         {rated}/{len(to_lookup)} films have ratings")
    log.info(f"         {len(skipped)} titles skipped (not films)")
    log.info(f"{'='*50}\n")

    enrich_data_files(data_files, lookup_cache, dry_run=args.dry_run)
    log.info("=== Enrichment Complete ===")


def main():
    parser = argparse.ArgumentParser(description="Enrich film data with Letterboxd ratings")
    parser.add_argument("-d", "--data-dir", type=str, default=None,
                        help="Directory containing films JSON files")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview lookups without writing to files")
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY,
                        help=f"Number of parallel requests (default {CONCURRENCY})")
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
