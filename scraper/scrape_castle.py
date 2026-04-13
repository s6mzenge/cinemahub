#!/usr/bin/env python3
"""
The Castle Cinema (Hackney) Timetable Scraper

Scrapes thecastlecinema.com for current films, showtimes, booking links,
and screen numbers.

Two-phase approach (like Peckhamplex):
  1. Fetch /listings/ to discover all programme URLs
  2. Fetch each programme page for showtimes, runtime, cast, etc.

Outputs a JSON file compatible with the CinemaHub frontend.

Usage:
    # Scrape live from the website
    python scraper/scrape_castle.py

    # Custom output path
    python scraper/scrape_castle.py -o my_output.json
"""

import json
import re
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://thecastlecinema.com"
LISTINGS_URL = f"{BASE_URL}/listings/"

# Polite delay between requests (seconds)
REQUEST_DELAY = 1.0
REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": "CastleCinemaScraper/1.0 (personal project; scrapes twice daily)",
    "Accept": "text/html,application/xhtml+xml",
}

# Known curated event prefixes — stored as tags but kept in title
EVENT_PREFIXES = [
    "CAMP CLASSICS presents:",
    "Cine-Real presents:",
    "Distorted Frame:",
    "Pitchblack Playback:",
    "Pitchblack Mixtapes:",
    "Violet Hour presents:",
    "Word Space presents",
    "NT Live:",
]

# Genre-based color palette
GENRE_COLORS = {
    "Animation":   {"color": "#e53935", "accent": "#ff6f60"},
    "Adventure":   {"color": "#7c4dff", "accent": "#b388ff"},
    "Horror":      {"color": "#546e7a", "accent": "#90a4ae"},
    "Comedy":      {"color": "#d81b60", "accent": "#ff6090"},
    "Romance":     {"color": "#00897b", "accent": "#4db6ac"},
    "Theatre":     {"color": "#1565c0", "accent": "#64b5f6"},
    "Action":      {"color": "#ef6c00", "accent": "#ffb74d"},
    "Documentary": {"color": "#c62828", "accent": "#ef5350"},
    "Drama":       {"color": "#6a1b9a", "accent": "#ba68c8"},
    "Sci-Fi":      {"color": "#00838f", "accent": "#4dd0e1"},
    "Thriller":    {"color": "#37474f", "accent": "#78909c"},
    "Family":      {"color": "#2e7d32", "accent": "#66bb6a"},
    "Musical":     {"color": "#ad1457", "accent": "#f06292"},
    "Crime":       {"color": "#4e342e", "accent": "#8d6e63"},
    "Live":        {"color": "#0277bd", "accent": "#4fc3f7"},
    "Event":       {"color": "#558b2f", "accent": "#9ccc65"},
}
DEFAULT_COLORS = {"color": "#78909c", "accent": "#b0bec5"}

EXTRA_PALETTES = [
    {"color": "#e53935", "accent": "#ff6f60"},
    {"color": "#7c4dff", "accent": "#b388ff"},
    {"color": "#d81b60", "accent": "#ff6090"},
    {"color": "#00897b", "accent": "#4db6ac"},
    {"color": "#1565c0", "accent": "#64b5f6"},
    {"color": "#ef6c00", "accent": "#ffb74d"},
    {"color": "#c62828", "accent": "#ef5350"},
    {"color": "#6a1b9a", "accent": "#ba68c8"},
    {"color": "#00838f", "accent": "#4dd0e1"},
    {"color": "#546e7a", "accent": "#90a4ae"},
    {"color": "#2e7d32", "accent": "#66bb6a"},
    {"color": "#ad1457", "accent": "#f06292"},
    {"color": "#37474f", "accent": "#78909c"},
    {"color": "#4e342e", "accent": "#8d6e63"},
    {"color": "#0277bd", "accent": "#4fc3f7"},
    {"color": "#558b2f", "accent": "#9ccc65"},
]


def fetch(url: str, retries: int = 2) -> BeautifulSoup | None:
    """Fetch a URL and return a BeautifulSoup object."""
    for attempt in range(retries + 1):
        try:
            log.info(f"Fetching: {url}")
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt+1} failed for {url}: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    return None


def parse_runtime(text: str) -> int | None:
    """Extract minutes from runtime text like '105 mins' or '1hr 47 mins'."""
    text = text.strip().lower()
    hours = 0
    minutes = 0
    h_match = re.search(r"(\d+)\s*h", text)
    m_match = re.search(r"(\d+)\s*min", text)
    if h_match:
        hours = int(h_match.group(1))
    if m_match:
        minutes = int(m_match.group(1))
    total = hours * 60 + minutes
    return total if total > 0 else None


def extract_rating_from_bbfc(soup: BeautifulSoup) -> str:
    """Extract BBFC rating from an img with alt like 'BBFC 15'."""
    bbfc_img = soup.select_one("div.bbfc img")
    if bbfc_img:
        alt = bbfc_img.get("alt", "")
        match = re.search(r"BBFC\s+(\S+)", alt)
        if match:
            return match.group(1)
    return "TBC"


def detect_event_prefix(title: str) -> str | None:
    """Check if the title starts with a known curated event prefix."""
    for prefix in EVENT_PREFIXES:
        if title.lower().startswith(prefix.lower()):
            return prefix
    return None


def extract_poster_url(tile) -> str:
    """Extract the best poster image URL from a tile's picture element."""
    # Try the JPEG source first (more compatible)
    source = tile.select_one("picture source[type='image/jpeg']")
    if source:
        srcset = source.get("srcset", "")
        # First URL in srcset (before comma)
        url = srcset.split(",")[0].strip()
        if url and "default" not in url:
            return url
    # Fallback to img src
    img = tile.select_one("picture img")
    if img:
        src = img.get("src", "")
        if src and "default" not in src:
            return src
    return ""


# ─── Phase 1: Scrape listings page ───

def scrape_listings() -> list[dict]:
    """Scrape the listings page to get all programme URLs and basic info."""
    soup = fetch(LISTINGS_URL)
    if not soup:
        log.error(f"Could not fetch listings page: {LISTINGS_URL}")
        return []

    films = []
    for tile in soup.select("div.tile.programme-tile"):
        prog_id = tile.get("data-prog-id", "")
        if not prog_id:
            continue

        # Title
        title_el = tile.select_one("h1.ellipse")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)

        # Programme URL
        link_el = tile.select_one("a[href*='/programme/']")
        if not link_el:
            continue
        prog_url = urljoin(BASE_URL, link_el["href"])

        # Rating from BBFC icon on tile
        rating = extract_rating_from_bbfc(tile)

        # Poster
        poster_url = extract_poster_url(tile)

        # Accessibility flags from tile
        has_ad = bool(tile.select_one("div.audio-described"))

        # Slug from URL
        slug = prog_url.rstrip("/").split("/")[-1]

        films.append({
            "id": slug,
            "prog_id": prog_id,
            "title": title,
            "prog_url": prog_url,
            "rating": rating,
            "poster_url": poster_url,
            "has_audio_described": has_ad,
        })

    log.info(f"Found {len(films)} programmes on listings page")
    return films


# ─── Phase 2: Scrape each programme detail page ───

def scrape_programme_detail(film: dict) -> dict | None:
    """Scrape a programme detail page for showtimes and full metadata."""
    soup = fetch(film["prog_url"])
    if not soup:
        log.error(f"Could not fetch programme page: {film['prog_url']}")
        return None

    title = film["title"]

    # Runtime from detail page
    runtime_el = soup.select_one("div.film-duration")
    runtime = parse_runtime(runtime_el.get_text()) if runtime_el else None

    # Year
    year_el = soup.select_one("div.film-year")
    year = year_el.get_text(strip=True) if year_el else None

    # Rating (detail page may have a more specific one)
    rating = extract_rating_from_bbfc(soup)
    if rating == "TBC":
        rating = film["rating"]  # fall back to listings page rating

    # Director
    director_el = soup.select_one("span.film-director")
    director = director_el.get_text(strip=True) if director_el else None

    # Cast
    cast_el = soup.select_one("span.film-cast")
    cast = cast_el.get_text(strip=True) if cast_el else None

    # Synopsis
    synopsis_el = soup.select_one("span.film-synopsis")
    description = synopsis_el.get_text(strip=True) if synopsis_el else ""

    # Detect curated event series
    event_prefix = detect_event_prefix(title)
    tags = []
    if event_prefix:
        # Clean prefix name for tag (e.g. "CAMP CLASSICS presents:" → "Camp Classics")
        tag_name = event_prefix.rstrip(":").strip()
        tag_name = re.sub(r"\s+presents$", "", tag_name, flags=re.I).strip()
        tags.append(tag_name)

    # Audio described
    if film.get("has_audio_described"):
        tags.append("AD")

    # Genre — Castle doesn't have explicit genres, so infer from context
    genre = "Other"
    if event_prefix and "NT Live" in event_prefix:
        genre = "Theatre"
    elif event_prefix and ("Pitchblack" in event_prefix):
        genre = "Live"
    elif event_prefix and ("Word Space" in event_prefix):
        genre = "Event"

    # ─── Showtimes ───
    showtimes = {}
    for day_block in soup.select("div.day-times"):
        # Date from the .day div
        day_el = day_block.select_one("div.day")
        if not day_el:
            continue

        # Performance buttons
        for perf_btn in day_block.select("a.performance-button"):
            start_time_str = perf_btn.get("data-start-time", "")
            if not start_time_str:
                continue

            try:
                dt = datetime.fromisoformat(start_time_str)
                date_str = dt.strftime("%Y-%m-%d")
                time_str = dt.strftime("%H:%M")
            except ValueError:
                continue

            booking_url = urljoin(BASE_URL, perf_btn.get("href", ""))

            # Screen
            screen_el = perf_btn.select_one("span.screen")
            screen = screen_el.get_text(strip=True) if screen_el else None

            # Screening type (e.g., "camp classics!")
            type_el = perf_btn.select_one("span.screening-type")
            screening_type = type_el.get_text(strip=True) if type_el else None

            # Check sold out
            sold_out_el = perf_btn.select_one("span.sold-out")
            is_sold_out = False
            if sold_out_el:
                style = sold_out_el.get("style", "")
                is_sold_out = "display:none" not in style.replace(" ", "")

            session = {
                "time": time_str,
                "booking_url": booking_url,
                "screen": screen,
                "hoh": False,
            }

            # Add session tags
            session_tags = list(tags)  # copy film-level tags
            if screening_type:
                # Clean up screening type for display
                clean_type = screening_type.strip().rstrip("!").strip()
                if clean_type and clean_type.lower() not in [t.lower() for t in session_tags]:
                    session_tags.append(clean_type)
            if session_tags:
                session["tags"] = session_tags

            if date_str not in showtimes:
                showtimes[date_str] = []
            showtimes[date_str].append(session)

    if not showtimes:
        log.warning(f"No showtimes found for: {title}")
        return None

    return {
        "id": film["id"],
        "title": title,
        "rating": rating,
        "runtime": runtime,
        "genre": genre,
        "year": year,
        "director": director,
        "cast": cast,
        "description": description,
        "film_url": film["prog_url"],
        "poster_url": film["poster_url"],
        "showtimes": showtimes,
    }


def assign_colors(films: list[dict]) -> None:
    """Assign colors to films. Genre colors first, then procedural hue rotation."""
    import colorsys

    def hsl_to_hex(h, s, l):
        r, g, b = colorsys.hls_to_rgb(h, l, s)
        return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"

    def generate_color(index):
        """Use golden angle for max hue separation, with varied saturation/lightness."""
        golden = 0.618033988749895
        hue = (index * golden) % 1.0
        # Alternate between two saturation/lightness bands for variety
        if index % 3 == 0:
            sat, lit = 0.65, 0.38
            asat, alit = 0.55, 0.62
        elif index % 3 == 1:
            sat, lit = 0.55, 0.42
            asat, alit = 0.50, 0.68
        else:
            sat, lit = 0.70, 0.35
            asat, alit = 0.60, 0.58
        return {
            "color": hsl_to_hex(hue, sat, lit),
            "accent": hsl_to_hex(hue, asat, alit),
        }

    used_colors = set()
    palette_idx = 0
    gen_idx = 0

    for film in films:
        genre = film.get("genre", "Other")
        colors = GENRE_COLORS.get(genre, None)

        if colors and colors["color"] not in used_colors:
            film["color"] = colors["color"]
            film["accent"] = colors["accent"]
            used_colors.add(colors["color"])
        else:
            # Try hand-picked extras first
            assigned = False
            while palette_idx < len(EXTRA_PALETTES):
                c = EXTRA_PALETTES[palette_idx]
                palette_idx += 1
                if c["color"] not in used_colors:
                    film["color"] = c["color"]
                    film["accent"] = c["accent"]
                    used_colors.add(c["color"])
                    assigned = True
                    break
            # Fallback: generate procedural colors (never grey)
            if not assigned:
                while True:
                    c = generate_color(gen_idx)
                    gen_idx += 1
                    if c["color"] not in used_colors:
                        film["color"] = c["color"]
                        film["accent"] = c["accent"]
                        used_colors.add(c["color"])
                        break

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Castle Cinema timetable")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Output path for JSON file")
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (
        Path(__file__).parent.parent / "public" / "data" / "films_castle.json"
    )

    log.info("=== Castle Cinema Scraper Starting ===")

    # Step 1: Get programme list from listings page
    all_progs = scrape_listings()

    if not all_progs:
        log.error("No programmes found. Aborting.")
        sys.exit(1)

    # Step 2: Scrape each programme's detail page
    films = []
    for prog in all_progs:
        detail = scrape_programme_detail(prog)
        if detail and detail["showtimes"]:
            films.append(detail)

    log.info(f"Scraped details for {len(films)} programmes with showtimes")

    if not films:
        log.error("No films with showtimes found. Aborting.")
        sys.exit(1)

    # Step 3: Assign colors
    assign_colors(films)

    # Step 4: Write output
    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "thecastlecinema.com",
        "films": films,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info("=== Scraper Complete ===")


if __name__ == "__main__":
    main()
