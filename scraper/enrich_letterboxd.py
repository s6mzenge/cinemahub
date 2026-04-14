#!/usr/bin/env python3
"""
Letterboxd Enrichment Script

Reads all films_*.json / films.json data files, looks up each unique film
on Letterboxd, and writes back `letterboxd_url` and `letterboxd_rating`
into every matching film entry.

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
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 12
SEARCH_DELAY = 1.0        # seconds between Letterboxd requests (be polite)
DETAIL_DELAY = 0.8
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


# ─── Helpers ──────────────────────────────────────────────────────────

def slugify_title(title: str) -> str:
    """
    Best-effort conversion of a film title into a Letterboxd slug.
    e.g. "The Brutalist" → "the-brutalist"
         "California Schemin'" → "california-schemin"
         "Amélie" → "amelie"
    """
    import unicodedata
    s = unicodedata.normalize("NFD", title)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")  # strip accents
    s = s.lower()
    s = re.sub(r"[''`]", "", s)          # remove apostrophes
    s = re.sub(r"[^a-z0-9]+", "-", s)    # non-alphanum → hyphens
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
                return None  # genuinely not found
            log.warning(f"  HTTP {resp.status_code} for {url} (attempt {attempt+1})")
        except requests.RequestException as e:
            log.warning(f"  Request error for {url}: {e} (attempt {attempt+1})")
        if attempt < retries:
            time.sleep(SEARCH_DELAY * (attempt + 1))
    return None


# ─── Strategy 1: Direct slug guess ────────────────────────────────────

def try_direct_slug(title: str) -> dict | None:
    """
    Try to hit the film page directly by guessing the slug.
    Returns {"url": ..., "rating": ...} or None.
    """
    slug = slugify_title(title)
    url = f"{LBOXD_BASE}/film/{slug}/"
    resp = fetch_with_retry(url, retries=1)
    if resp is None:
        return None
    return extract_rating_from_page(resp.text, url)


# ─── Strategy 2: Search Letterboxd ────────────────────────────────────

def search_letterboxd(title: str) -> dict | None:
    """
    Search Letterboxd for the film title.
    Returns {"url": ..., "rating": ...} or None.
    """
    search_url = f"{LBOXD_BASE}/search/films/{quote(title)}/"
    resp = fetch_with_retry(search_url)
    if resp is None:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # The search results are in <ul class="results"> → <li> with links to /film/...
    results = soup.select("ul.results li.search-result")
    if not results:
        # Alternative: look for any /film/ link in the page
        film_link = soup.select_one('a[href*="/film/"]')
        if film_link:
            href = film_link.get("href", "")
            film_url = href if href.startswith("http") else f"{LBOXD_BASE}{href}"
            return fetch_film_page(film_url)
        return None

    # Take the first result
    first = results[0]
    link = first.select_one('a[href*="/film/"]')
    if not link:
        return None

    href = link.get("href", "")
    film_url = href if href.startswith("http") else f"{LBOXD_BASE}{href}"

    return fetch_film_page(film_url)


def fetch_film_page(url: str) -> dict | None:
    """Fetch a Letterboxd film page and extract rating."""
    time.sleep(DETAIL_DELAY)
    resp = fetch_with_retry(url)
    if resp is None:
        return None
    return extract_rating_from_page(resp.text, url)


def extract_rating_from_page(html: str, url: str) -> dict | None:
    """
    Extract the average rating from a Letterboxd film page.
    
    Letterboxd embeds the rating in several places:
    1. <meta name="twitter:data2" content="Average rating: 3.6 out of 5">
    2. A meta tag in <head> with content like "3.6 out of 5 based on ..."
    3. JSON-LD structured data with aggregateRating
    4. <span class="average-rating"> in the visible page
    
    We try multiple strategies for robustness.
    """
    soup = BeautifulSoup(html, "html.parser")
    rating = None

    # ── Method 1: twitter:data2 meta tag ──
    twitter_meta = soup.find("meta", attrs={"name": "twitter:data2"})
    if twitter_meta:
        content = twitter_meta.get("content", "")
        match = re.search(r"([\d.]+)\s*out\s*of\s*5", content)
        if match:
            rating = float(match.group(1))

    # ── Method 2: Head meta tags with "out of 5" pattern ──
    if rating is None:
        for meta in soup.find_all("meta"):
            content = meta.get("content", "")
            if "out of 5" in content:
                match = re.search(r"([\d.]+)\s*out\s*of\s*5", content)
                if match:
                    rating = float(match.group(1))
                    break

    # ── Method 3: JSON-LD aggregateRating ──
    if rating is None:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and "aggregateRating" in data:
                    agg = data["aggregateRating"]
                    val = agg.get("ratingValue")
                    if val is not None:
                        rating = float(val)
                        # Letterboxd uses 0-5 scale, normalise if needed
                        best = float(agg.get("bestRating", 5))
                        if best == 10:
                            rating = round(rating / 2, 2)
                        break
            except (json.JSONDecodeError, ValueError, TypeError):
                continue

    # ── Method 4: visible average-rating element ──
    if rating is None:
        avg_el = soup.select_one("a.display-rating, .average-rating")
        if avg_el:
            text = avg_el.get_text(strip=True)
            match = re.search(r"([\d.]+)", text)
            if match:
                rating = float(match.group(1))

    # Clean up the URL (ensure it ends with /, remove query params)
    clean_url = url.split("?")[0]
    if not clean_url.endswith("/"):
        clean_url += "/"

    return {
        "url": clean_url,
        "rating": round(rating, 2) if rating is not None else None,
    }


# ─── Normalisation (match titles across scrapers) ─────────────────────

def normalize_for_lookup(title: str) -> str:
    """Normalise a title for deduplication across cinema data files."""
    import unicodedata
    t = unicodedata.normalize("NFD", title)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    t = t.lower().strip()
    # Strip common suffixes
    t = re.sub(r"\s*[-–—]\s*\d+\w*\s*anniversary.*$", "", t, flags=re.I)
    t = re.sub(r"\s*\(\d+\w*\s*anniversary[^)]*\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(re-?release\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ─── Main pipeline ────────────────────────────────────────────────────

def find_data_files(data_dir: Path) -> list[Path]:
    """Find all film JSON data files."""
    patterns = ["films.json", "films_*.json"]
    files = []
    for pattern in patterns:
        files.extend(data_dir.glob(pattern))
    return sorted(set(files))


def collect_unique_titles(data_files: list[Path]) -> dict[str, str]:
    """
    Returns {normalised_key: original_title} for all unique films.
    Picks the "cleanest" original title for each key.
    """
    titles = {}  # norm_key → list of original titles
    for path in data_files:
        data = json.loads(path.read_text(encoding="utf-8"))
        for film in data.get("films", []):
            key = normalize_for_lookup(film["title"])
            if key not in titles:
                titles[key] = []
            titles[key].append(film["title"])

    # Pick shortest original title for each key (usually the cleanest)
    result = {}
    for key, originals in titles.items():
        best = min(set(originals), key=len)
        result[key] = best
    return result


def lookup_film(title: str) -> dict:
    """
    Look up a single film on Letterboxd.
    Returns {"letterboxd_url": str|null, "letterboxd_rating": float|null}
    """
    # Strategy 1: try direct slug
    log.info(f"  ↳ trying direct slug for: {title}")
    result = try_direct_slug(title)
    if result and result["url"]:
        log.info(f"    ✓ direct hit: {result['url']} — rating: {result['rating']}")
        return {"letterboxd_url": result["url"], "letterboxd_rating": result["rating"]}

    # Strategy 2: search
    time.sleep(SEARCH_DELAY)
    log.info(f"  ↳ searching Letterboxd for: {title}")
    result = search_letterboxd(title)
    if result and result["url"]:
        log.info(f"    ✓ search hit: {result['url']} — rating: {result['rating']}")
        return {"letterboxd_url": result["url"], "letterboxd_rating": result["rating"]}

    log.warning(f"    ✗ no result for: {title}")
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
            log.info(f"✏️  Updated {path.name}")
        elif modified:
            log.info(f"[DRY RUN] Would update {path.name}")
        else:
            log.info(f"  No changes needed for {path.name}")


def main():
    parser = argparse.ArgumentParser(description="Enrich film data with Letterboxd ratings")
    parser.add_argument(
        "-d", "--data-dir", type=str, default=None,
        help="Directory containing films JSON files (default: auto-detect)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview lookups without writing to files"
    )
    args = parser.parse_args()

    # Auto-detect data directory
    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        # Try common locations
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

    log.info(f"=== Letterboxd Enrichment Starting ===")
    log.info(f"Data directory: {data_dir.resolve()}")

    data_files = find_data_files(data_dir)
    if not data_files:
        log.error(f"No films*.json files found in {data_dir}")
        sys.exit(1)
    log.info(f"Found {len(data_files)} data file(s): {[f.name for f in data_files]}")

    # Collect unique titles across all files
    unique_titles = collect_unique_titles(data_files)
    log.info(f"Found {len(unique_titles)} unique film(s) to look up")

    # Look up each film on Letterboxd
    lookup_cache = {}
    for i, (key, title) in enumerate(unique_titles.items(), 1):
        log.info(f"[{i}/{len(unique_titles)}] {title}")
        lookup_cache[key] = lookup_film(title)

        # Rate limiting between films
        if i < len(unique_titles):
            time.sleep(SEARCH_DELAY)

    # Summary
    found = sum(1 for v in lookup_cache.values() if v["letterboxd_url"])
    rated = sum(1 for v in lookup_cache.values() if v["letterboxd_rating"] is not None)
    log.info(f"\n{'='*50}")
    log.info(f"Results: {found}/{len(lookup_cache)} films found on Letterboxd")
    log.info(f"         {rated}/{len(lookup_cache)} films have ratings")
    log.info(f"{'='*50}\n")

    # Write results back
    enrich_data_files(data_files, lookup_cache, dry_run=args.dry_run)

    log.info("=== Enrichment Complete ===")


if __name__ == "__main__":
    main()
