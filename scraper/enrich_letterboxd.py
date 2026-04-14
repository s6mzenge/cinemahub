#!/usr/bin/env python3
"""
Letterboxd Enrichment Script

Reads all films_*.json / films.json data files, looks up each unique film
on Letterboxd via direct slug matching, and writes back `letterboxd_url`
and `letterboxd_rating` into every matching film entry.

Designed to run as a post-scraping step in GitHub Actions, or locally.

Usage:
    python enrich_letterboxd.py                      # auto-finds public/data/
    python enrich_letterboxd.py -d ./public/data     # explicit data dir
    python enrich_letterboxd.py --dry-run             # preview without writing
"""

import json
import re
import sys
import time
import logging
import argparse
import unicodedata
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 12
DELAY_BETWEEN_FILMS = 1.0   # seconds between Letterboxd requests (be polite)
MAX_RETRIES = 2

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


# ─── Title cleaning ──────────────────────────────────────────────────

# Known event-series prefixes (case-insensitive).
# If the text BEFORE the colon matches one of these, strip it.
EVENT_PREFIXES = [
    "adults only",
    "camp classics presents",
    "cine-real presents",
    "distorted frame",
    "dog-friendly",
    "exhibition on screen",
    "exclusive preview",
    "fetish friendly",
    "funday",
    "in the scene",
    "late night",
    "lesbian visibility day",
    "lesbian visibility",
    "lost reels presents",
    "memories",
    "nt live",
    "national theatre live",
    "pitchblack mixtapes",
    "pitchblack playback",
    "preview",
    "the male gaze",
    "uk premiere of 4k restoration",
    "uk premiere",
    "violet hour presents",
]

# Titles matching these patterns are NOT films → skip entirely
SKIP_PATTERNS = [
    r"(?i)\bnt live\b",
    r"(?i)\bnational theatre live\b",
    r"(?i)\bexhibition on screen\b",
    r"(?i)\bshort films?\b",             # "Short Films", "Shorts"
    r"(?i)\bshorts\s*[&+]\s*stand-up\b",
    r"(?i)\bday pass\b",
    r"(?i)\bpitchblack (playback|mixtapes)\b",
    r"(?i)\bfilm night\b",               # "Big Bike Film Night"
    r"(?i)^festival of britain\b",
    r"(?i)^future forward\b",
    r"(?i)\bfundraiser\b",
    r"(?i)^the quiz of rassilon\b",
    r"(?i)^laura mulvey\b",
    r"(?i)\bdouble feature\b",
    r"(?i)^25 and under:",               # BFI intro events
    r"(?i)^inferno, purgatory",
]


def should_skip(title: str) -> bool:
    """Return True if this title is an event/pass/compilation, not a film."""
    for pattern in SKIP_PATTERNS:
        if re.search(pattern, title):
            return True
    return False


def clean_title_for_lookup(title: str) -> str:
    """
    Strip event prefixes, Q&A/intro suffixes, and other cruft to extract
    the actual film title for Letterboxd lookup.

    Examples:
        "Preview: Departures + Q&A"          → "Departures"
        "CAMP CLASSICS presents: Hackers (1995)" → "Hackers"
        "Loner (Independent Filmmakers Showcase)" → "Loner"
        "The Devil Wears Prada 2 + Intro"    → "The Devil Wears Prada 2"
        "Mother Mary + Intro from Femmi"     → "Mother Mary"
        "Adults Only: The Devil Wears Prada 2" → "The Devil Wears Prada 2"
        "Cockroach + Q&A"                    → "Cockroach"
        "Kill Bill: The Whole Bloody Affair"  → "Kill Bill: The Whole Bloody Affair" (kept!)
    """
    t = title.strip()

    # ── Step 1: Strip known event-series prefixes ──
    # Only strip if the part before the colon is a known event prefix
    if ":" in t:
        before_colon = t.split(":")[0].strip()
        before_lower = before_colon.lower()
        # Also handle "presents" variants like 'Lost Reels presents "Lianna"'
        before_lower_clean = re.sub(r'\s+presents$', '', before_lower)
        for prefix in EVENT_PREFIXES:
            if before_lower == prefix or before_lower_clean == prefix:
                t = ":".join(t.split(":")[1:]).strip()
                # Strip leading quotes if present (e.g. Lost Reels presents "Lianna")
                t = t.strip('"').strip('"').strip('"').strip()
                break

    # ── Step 2: Handle " + " — strip suffixes like "+ Q&A", "+ Intro", etc. ──
    # But preserve " + " in actual titles like "Romeo + Juliet" or "Orwell: 2+2=5"
    # Strategy: strip if what follows "+" looks like event text (Q&A, Intro, Director, Special, etc.)
    plus_match = re.search(
        r'\s*\+\s*('
        r'Q\s*&\s*A'
        r'|[Ii]ntro\b.*'
        r'|[Dd]irector\b.*'
        r'|[Ss]pecial\b.*'
        r'|[Ee]xtended\b.*'
        r'|5:40 Fantasy.*'       # "I Saw the TV Glow + 5:40 Fantasy Music Video"
        r')\s*$',
        t
    )
    if plus_match:
        t = t[:plus_match.start()].strip()

    # Handle double bills: "Film A + Film B" — take just the first film
    # But only if both sides look like titles (not "Romeo + Juliet")
    # Heuristic: if there's a " + " with >3 chars on each side, and it's not
    # a known compound title, split and take the first
    if " + " in t and not re.search(r'(?i)romeo\s*\+\s*juliet', t) and not re.search(r'(?i)\d\+\d', t):
        parts = t.split(" + ", 1)
        if len(parts[0].strip()) > 3 and len(parts[1].strip()) > 3:
            # Looks like a double bill
            t = parts[0].strip()

    # ── Step 3: Strip non-film parentheticals ──
    # Remove things like "(Independent Filmmakers Showcase)", "(Live Score)"
    # But keep year parentheticals "(1996)" and format ones "(Director's Cut)"
    t = re.sub(r"\s*\(Independent Filmmakers Showcase\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(Short Films?\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(Live Score\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(4K Restoration\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(Black & White version\)", "", t, flags=re.I)

    # ── Step 4: Strip year parenthetical — we pass year separately ──
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t)

    # ── Step 5: Strip re-release / anniversary suffixes ──
    t = re.sub(r"\s*[-–—]\s*\d+\w*\s*anniversary.*$", "", t, flags=re.I)
    t = re.sub(r"\s*\(\d+\w*\s*anniversary[^)]*\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(re-?release\)", "", t, flags=re.I)

    return t.strip()


# ─── Slug & fetch ─────────────────────────────────────────────────────

def slugify_title(title: str) -> str:
    """Convert a film title into a Letterboxd-style slug."""
    s = unicodedata.normalize("NFD", title)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = re.sub(r"[''`]", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s


def fetch_with_retry(url: str, retries: int = MAX_RETRIES) -> requests.Response | None:
    """GET with retries and polite error handling."""
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return None
            log.warning(f"  HTTP {resp.status_code} for {url} (attempt {attempt+1})")
        except requests.RequestException as e:
            log.warning(f"  Request error for {url}: {e} (attempt {attempt+1})")
        if attempt < retries:
            time.sleep(1.5 * (attempt + 1))
    return None


def try_direct_slug(title: str, year: int | None = None) -> dict | None:
    """
    Try to hit the film page directly by guessing the slug.
    Tries variants in order:
      1. title-year  (e.g. /film/amelie-2001/)
      2. title       (e.g. /film/amelie/)
    """
    slug = slugify_title(title)

    if year:
        url = f"{LBOXD_BASE}/film/{slug}-{year}/"
        resp = fetch_with_retry(url, retries=0)
        if resp is not None:
            return extract_rating_from_page(resp.text, url)

    url = f"{LBOXD_BASE}/film/{slug}/"
    resp = fetch_with_retry(url, retries=0)
    if resp is not None:
        return extract_rating_from_page(resp.text, url)

    return None


# ─── Rating extraction ────────────────────────────────────────────────

def extract_rating_from_page(html: str, url: str) -> dict | None:
    """
    Extract the average rating from a Letterboxd film page.
    Tries multiple HTML locations for robustness.
    """
    soup = BeautifulSoup(html, "html.parser")
    rating = None

    # Method 1: <meta name="twitter:data2" content="Average rating: 3.6 out of 5">
    twitter_meta = soup.find("meta", attrs={"name": "twitter:data2"})
    if twitter_meta:
        content = twitter_meta.get("content", "")
        match = re.search(r"([\d.]+)\s*out\s*of\s*5", content)
        if match:
            rating = float(match.group(1))

    # Method 2: any meta tag with "X.X out of 5"
    if rating is None:
        for meta in soup.find_all("meta"):
            content = meta.get("content", "")
            if "out of 5" in content:
                match = re.search(r"([\d.]+)\s*out\s*of\s*5", content)
                if match:
                    rating = float(match.group(1))
                    break

    # Method 3: JSON-LD aggregateRating
    if rating is None:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and "aggregateRating" in data:
                    agg = data["aggregateRating"]
                    val = agg.get("ratingValue")
                    if val is not None:
                        rating = float(val)
                        best = float(agg.get("bestRating", 5))
                        if best == 10:
                            rating = round(rating / 2, 2)
                        break
            except (json.JSONDecodeError, ValueError, TypeError):
                continue

    # Method 4: visible average-rating element
    if rating is None:
        avg_el = soup.select_one("a.display-rating, .average-rating")
        if avg_el:
            text = avg_el.get_text(strip=True)
            match = re.search(r"([\d.]+)", text)
            if match:
                rating = float(match.group(1))

    clean_url = url.split("?")[0]
    if not clean_url.endswith("/"):
        clean_url += "/"

    return {
        "url": clean_url,
        "rating": round(rating, 2) if rating is not None else None,
    }


# ─── Normalisation (dedup across scrapers) ────────────────────────────

def normalize_for_lookup(title: str) -> str:
    """Normalise a title for deduplication across cinema data files."""
    t = unicodedata.normalize("NFD", title)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    t = t.lower().strip()
    t = re.sub(r"\s*[-\u2013\u2014]\s*\d+\w*\s*anniversary.*$", "", t, flags=re.I)
    t = re.sub(r"\s*\(\d+\w*\s*anniversary[^)]*\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(re-?release\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ─── Main pipeline ────────────────────────────────────────────────────

def find_data_files(data_dir: Path) -> list[Path]:
    patterns = ["films.json", "films_*.json"]
    files = []
    for pattern in patterns:
        files.extend(data_dir.glob(pattern))
    return sorted(set(files))


def collect_unique_titles(data_files: list[Path]) -> dict[str, dict]:
    """
    Returns {normalised_key: {"title": str, "year": int|None, "skip": bool}}
    for all unique films. Merges year info across cinemas.
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
                "year": film.get("year"),
            })

    result = {}
    for key, items in entries.items():
        best_title = min(set(item["title"] for item in items), key=len)
        best_year = next((item["year"] for item in items if item.get("year")), None)
        skip = should_skip(best_title)
        result[key] = {"title": best_title, "year": best_year, "skip": skip}

    return result


def lookup_film(title: str, year: int | None = None) -> dict:
    """
    Clean the title and look up on Letterboxd via direct slug.
    Returns {"letterboxd_url": str|null, "letterboxd_rating": float|null}
    """
    cleaned = clean_title_for_lookup(title)
    year_str = f" ({year})" if year else ""

    if cleaned != title:
        log.info(f"  \u21b3 cleaned: \"{title}\" → \"{cleaned}\"")

    log.info(f"  \u21b3 trying slug for: {cleaned}{year_str}")
    result = try_direct_slug(cleaned, year)
    if result and result["url"]:
        log.info(f"    \u2713 hit: {result['url']} — rating: {result['rating']}")
        return {"letterboxd_url": result["url"], "letterboxd_rating": result["rating"]}

    # If cleaned title differs from slug without year, also worth trying
    # the original (uncleaned) slug in case the colon IS part of the title
    if cleaned != title:
        time.sleep(0.8)
        log.info(f"  \u21b3 retrying with original title: {title}")
        result = try_direct_slug(title, year)
        if result and result["url"]:
            log.info(f"    \u2713 hit: {result['url']} — rating: {result['rating']}")
            return {"letterboxd_url": result["url"], "letterboxd_rating": result["rating"]}

    log.warning(f"    \u2717 no result for: {cleaned}{year_str}")
    return {"letterboxd_url": None, "letterboxd_rating": None}


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


def main():
    parser = argparse.ArgumentParser(description="Enrich film data with Letterboxd ratings")
    parser.add_argument("-d", "--data-dir", type=str, default=None,
                        help="Directory containing films JSON files")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview lookups without writing to files")
    args = parser.parse_args()

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

    # Partition into lookups vs skips
    to_lookup = {k: v for k, v in unique_titles.items() if not v["skip"]}
    skipped = {k: v for k, v in unique_titles.items() if v["skip"]}

    has_year = sum(1 for v in to_lookup.values() if v["year"])
    log.info(f"Found {len(unique_titles)} unique titles total")
    log.info(f"  → {len(skipped)} skipped (events/compilations/NT Live)")
    log.info(f"  → {len(to_lookup)} to look up ({has_year} with year, {len(to_lookup) - has_year} without)")

    if skipped:
        log.info("Skipped titles:")
        for v in sorted(skipped.values(), key=lambda x: x["title"]):
            log.info(f"  ⏭  {v['title']}")

    # Look up each film
    lookup_cache = {}
    for i, (key, info) in enumerate(to_lookup.items(), 1):
        title, year = info["title"], info["year"]
        year_label = f" ({year})" if year else " (no year)"
        log.info(f"[{i}/{len(to_lookup)}] {title}{year_label}")
        lookup_cache[key] = lookup_film(title, year)

        if i < len(to_lookup):
            time.sleep(DELAY_BETWEEN_FILMS)

    # Also mark skipped titles as no-result so they don't get stale data
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


if __name__ == "__main__":
    main()
