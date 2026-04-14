#!/usr/bin/env python3
"""
Letterboxd Enrichment Script (v2 – async, smarter cleaning)

Reads all films_*.json / films.json data files, looks up each unique film
on Letterboxd via direct slug matching, and writes back `letterboxd_url`
and `letterboxd_rating` into every matching film entry.

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
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 12
CONCURRENCY = 5          # parallel Letterboxd requests
DELAY_BETWEEN = 0.25     # seconds between starting each request
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


# ─── Manual slug overrides for films whose titles don't slugify cleanly ─
# Maps cleaned title (lowercase) → Letterboxd slug
SLUG_OVERRIDES = {
    "dr. strangelove": "dr-strangelove-or-how-i-learned-to-stop-worrying-and-love-the-bomb",
    "e.t. the extra terrestrial": "e-t-the-extra-terrestrial",
    "e.t. the extra-terrestrial": "e-t-the-extra-terrestrial",
    "a.i. artificial intelligence": "ai-artificial-intelligence",
    "d.e.b.s.": "d-e-b-s",
    "at midnight i'll take your soul": "at-midnight-ill-take-your-soul",
    "small axe: lovers rock": "lovers-rock",
    "timecode live": "timecode",
}


# ─── Title cleaning ──────────────────────────────────────────────────

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
    "the male gaze",
    "uk premiere of 4k restoration",
    "uk premiere",
    "violet hour presents",
    "word space presents",
]

# Prefixes that appear WITHOUT a colon — strip them directly
STRIP_PREFIXES_NO_COLON = [
    r"(?i)^sing-a-long-a\s+",
    r"(?i)^solve along a\s+",
    r"(?i)^funeral parade presents\s+",
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
    r"(?i)\bmystery movie\b",
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
]


def should_skip(title: str) -> bool:
    """Return True if this title is an event/pass/compilation, not a film."""
    for pattern in SKIP_PATTERNS:
        if re.search(pattern, title):
            return True
    return False


def clean_title_for_lookup(title: str) -> str:
    """
    Strip event prefixes, Q&A/intro suffixes, brackets, and other cruft
    to extract the actual film title for Letterboxd lookup.
    """
    t = title.strip()

    # ── Step 0: Strip [original language title] brackets ──
    # e.g. "Oldboy [Oldeuboi]" → "Oldboy"
    # But preserve brackets that are part of the title when there's nothing before them
    t = re.sub(r"\s*\[[^\]]+\]\s*$", "", t)
    # Also handle mid-title brackets like "Cinema Paradiso [Nuovo Cinema Paradiso]"
    t = re.sub(r"\s*\[[^\]]+\]", "", t)

    # ── Step 1: Strip known event-series prefixes (colon-separated) ──
    if ":" in t:
        before_colon = t.split(":")[0].strip()
        before_lower = before_colon.lower()
        before_lower_clean = re.sub(r'\s+presents$', '', before_lower)
        for prefix in EVENT_PREFIXES:
            if before_lower == prefix or before_lower_clean == prefix:
                t = ":".join(t.split(":")[1:]).strip()
                t = t.strip('"').strip('\u201c').strip('\u201d').strip()
                break

    # ── Step 1b: Strip non-colon prefixes ──
    for pat in STRIP_PREFIXES_NO_COLON:
        t = re.sub(pat, "", t)

    # Handle 'X presents "Title"' patterns (with quotes, no colon)
    m = re.match(
        r'(?i)^(?:funeral parade|lost reels|word space)\s+presents\s*'
        r'["\u201c]([^"\u201d]+)["\u201d]',
        t,
    )
    if m:
        t = m.group(1).strip()

    # ── Step 2: Strip " + " suffixes (Q&A, Intro, etc.) ──
    plus_match = re.search(
        r'\s*\+\s*('
        r'Q\s*&\s*A'
        r'|[Ii]ntro\b.*'
        r'|[Dd]irector\b.*'
        r'|[Ss]pecial\b.*'
        r'|[Ee]xtended\b.*'
        r'|5:40 Fantasy.*'
        r'|Reece Shearsmith.*'
        r')\s*$',
        t
    )
    if plus_match:
        t = t[:plus_match.start()].strip()

    # Handle double bills: "Film A + Film B" — take just the first film
    if " + " in t and not re.search(r'(?i)romeo\s*\+\s*juliet', t) and not re.search(r'(?i)\d\+\d', t):
        parts = t.split(" + ", 1)
        if len(parts[0].strip()) > 3 and len(parts[1].strip()) > 3:
            t = parts[0].strip()

    # ── Step 3: Strip "with X live on stage" suffix ──
    t = re.sub(r"\s+with\s+\w[\w\s]*\blive on stage\b.*$", "", t, flags=re.I)

    # ── Step 4: Strip non-film parentheticals ──
    # Remove things like "(Independent Filmmakers Showcase)", "(Live Score)",
    # "(Director's Cut)", "(Extended Cut)", "(Theatrical Cut)", etc.
    strip_parens = [
        r"\(Independent Filmmakers Showcase\)",
        r"\(Short Films?\)",
        r"\(Live Score\)",
        r"\(4K Restoration\)",
        r"\(Black & White version\)",
        r"\(Director'?s?\s*Cut\)",
        r"\(Extended\s*Cut\)",
        r"\(Theatrical\s*Cut\)",
        r"\(Sedmikr[aá]sky\)",     # Daisies (Sedmikrásky)
    ]
    for pat in strip_parens:
        t = re.sub(r"\s*" + pat, "", t, flags=re.I)

    # ── Step 5: Strip year parenthetical — we pass year separately ──
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t)

    # ── Step 6: Strip re-release / anniversary suffixes ──
    t = re.sub(r"\s*[-–—]\s*\d+\w*\s*anniversary.*$", "", t, flags=re.I)
    t = re.sub(r"\s*\(\d+\w*\s*anniversary[^)]*\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(re-?release\)", "", t, flags=re.I)

    # ── Step 7: Strip restoration/premiere suffixes after colon ──
    # "Vampire's Kiss : 4K Restoration Premiere" → "Vampire's Kiss"
    # "Doctor Who: The Movie – 4K Restoration" → "Doctor Who: The Movie"
    # "Mimic: Director's Cut" → "Mimic"
    t = re.sub(r"\s*[:–—-]\s*4K\s+Restoration\s*(Premiere)?\s*$", "", t, flags=re.I)
    t = re.sub(r"\s*:\s*Director'?s?\s*Cut\s*$", "", t, flags=re.I)

    # ── Step 8: Strip any leftover surrounding quotes ──
    t = t.strip('"').strip('\u201c').strip('\u201d').strip('"')

    return t.strip()


# ─── Slug & fetch ─────────────────────────────────────────────────────

def slugify_title(title: str) -> str:
    """Convert a film title into a Letterboxd-style slug."""
    s = unicodedata.normalize("NFD", title)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    # Strip ALL apostrophe-like characters (straight, curly left/right, backtick)
    s = re.sub(r"[''\u2018\u2019\u0060\u00B4]", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s


async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    retries: int = MAX_RETRIES,
) -> str | None:
    """GET with retries. Returns HTML text on 200, None on 404."""
    for attempt in range(retries + 1):
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                allow_redirects=True,
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
                if resp.status == 404:
                    return None
                log.warning(f"  HTTP {resp.status} for {url} (attempt {attempt+1})")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"  Request error for {url}: {e} (attempt {attempt+1})")
        if attempt < retries:
            await asyncio.sleep(1.5 * (attempt + 1))
    return None


async def try_direct_slug(
    session: aiohttp.ClientSession,
    title: str,
    year: int | None = None,
) -> dict | None:
    """
    Try to hit the film page directly by guessing the slug.
    Order: manual override → title-year → title-only → English title variants.
    """
    # Check manual overrides first
    override_slug = SLUG_OVERRIDES.get(title.lower())
    if override_slug:
        url = f"{LBOXD_BASE}/film/{override_slug}/"
        html = await fetch_with_retry(session, url, retries=0)
        if html is not None:
            return extract_rating_from_page(html, url)

    slug = slugify_title(title)

    # Try slug-year first (e.g. /film/amelie-2001/)
    if year:
        url = f"{LBOXD_BASE}/film/{slug}-{year}/"
        html = await fetch_with_retry(session, url, retries=0)
        if html is not None:
            return extract_rating_from_page(html, url)

    # Try slug without year
    url = f"{LBOXD_BASE}/film/{slug}/"
    html = await fetch_with_retry(session, url, retries=0)
    if html is not None:
        return extract_rating_from_page(html, url)

    return None


# ─── Rating extraction ────────────────────────────────────────────────

def extract_rating_from_page(html: str, url: str) -> dict | None:
    """Extract the average rating from a Letterboxd film page."""
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
    index: int,
    total: int,
) -> dict:
    """Clean the title and look up on Letterboxd. Returns enrichment dict."""
    async with semaphore:
        cleaned = clean_title_for_lookup(title)
        year_str = f" ({year})" if year else ""

        if cleaned != title:
            log.info(f"[{index}/{total}] \u21b3 cleaned: \"{title}\" → \"{cleaned}\"{year_str}")
        else:
            log.info(f"[{index}/{total}] {title}{year_str}")

        result = await try_direct_slug(session, cleaned, year)
        if result and result["url"]:
            log.info(f"  ✓ {result['url']} — rating: {result['rating']}")
            return {"letterboxd_url": result["url"], "letterboxd_rating": result["rating"]}

        # Fallback: retry with the original title if cleaning changed it
        if cleaned != title:
            await asyncio.sleep(0.3)
            result = await try_direct_slug(session, title, year)
            if result and result["url"]:
                log.info(f"  ✓ (original) {result['url']} — rating: {result['rating']}")
                return {"letterboxd_url": result["url"], "letterboxd_rating": result["rating"]}

        log.warning(f"  ✗ no result for: {cleaned}{year_str}")
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
            log.info(f"✏️  Updated {path.name}")
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
    log.info(f"  → {len(skipped)} skipped (events/compilations/NT Live)")
    log.info(f"  → {len(to_lookup)} to look up ({has_year} with year, {len(to_lookup) - has_year} without)")

    if skipped:
        log.info("Skipped titles:")
        for v in sorted(skipped.values(), key=lambda x: x["title"]):
            log.info(f"  ⏭  {v['title']}")

    # ── Deduplicate by cleaned title to avoid redundant HTTP requests ──
    # Multiple raw titles can clean to the same slug (e.g. "Departures",
    # "Preview: Departures + Q&A", "Fetish Friendly: Departures" all → "Departures")
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

    log.info(f"  → {len(work_items)} unique lookups after dedup (saved {len(to_lookup) - len(work_items)} requests)")

    # ── Async lookups ──
    concurrency = args.concurrency
    semaphore = asyncio.Semaphore(concurrency)
    log.info(f"  → concurrency: {concurrency}")

    lookup_cache: dict[str, dict] = {}

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = []
        for i, (dedup_key, rep_key, keys) in enumerate(work_items, 1):
            info = to_lookup[rep_key]
            task = lookup_film(session, semaphore, info["title"], info["year"], i, len(work_items))
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
