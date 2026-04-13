#!/usr/bin/env python3
"""
Prince Charles Cinema Timetable Scraper

Scrapes princecharlescinema.com/whats-on/ for current films, showtimes,
booking links, and tags (35mm, 4K, SUB, etc.).

All data lives on the single "What's On" page — no per-film requests needed.

Outputs a JSON file in the same format as the Peckhamplex scraper so the
frontend can consume it without changes.

Usage:
    # Scrape live from the website (default output: public/data/films_pcc.json)
    python scraper/scrape_prince_charles.py

    # Parse a local HTML file instead (for development)
    python scraper/scrape_prince_charles.py --local Prince_Charles_Source_Code

    # Custom output path
    python scraper/scrape_prince_charles.py -o my_output.json
"""

import json
import re
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup

try:
    import requests
except ImportError:
    requests = None  # only needed for live mode

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

LISTINGS_URL = "https://princecharlescinema.com/whats-on/"

HEADERS = {
    "User-Agent": "PrinceCharlesScraper/1.0 (personal project; scrapes twice daily)",
    "Accept": "text/html,application/xhtml+xml",
}

REQUEST_TIMEOUT = 20

# Genre-based color palette (same as Peckhamplex scraper for consistency)
GENRE_COLORS = {
    "Animation":        {"color": "#e53935", "accent": "#ff6f60"},
    "Adventure":        {"color": "#7c4dff", "accent": "#b388ff"},
    "Horror":           {"color": "#546e7a", "accent": "#90a4ae"},
    "Comedy":           {"color": "#d81b60", "accent": "#ff6090"},
    "Romance":          {"color": "#00897b", "accent": "#4db6ac"},
    "Theatre":          {"color": "#1565c0", "accent": "#64b5f6"},
    "Action":           {"color": "#ef6c00", "accent": "#ffb74d"},
    "Action/Adventure": {"color": "#ef6c00", "accent": "#ffb74d"},
    "Documentary":      {"color": "#c62828", "accent": "#ef5350"},
    "Drama":            {"color": "#6a1b9a", "accent": "#ba68c8"},
    "Sci-Fi":           {"color": "#00838f", "accent": "#4dd0e1"},
    "Science Fiction":  {"color": "#00838f", "accent": "#4dd0e1"},
    "Thriller":         {"color": "#37474f", "accent": "#78909c"},
    "Family":           {"color": "#2e7d32", "accent": "#66bb6a"},
    "Musical":          {"color": "#ad1457", "accent": "#f06292"},
    "Crime":            {"color": "#4e342e", "accent": "#8d6e63"},
    "Neo Noir":         {"color": "#37474f", "accent": "#78909c"},
    "Romance/Comedy":   {"color": "#00897b", "accent": "#4db6ac"},
    "Fantasy":          {"color": "#7c4dff", "accent": "#b388ff"},
    "War":              {"color": "#546e7a", "accent": "#90a4ae"},
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


def fetch_live() -> BeautifulSoup | None:
    """Fetch the What's On page from the live website."""
    if requests is None:
        log.error("requests library not installed. Use --local or: pip install requests")
        return None
    try:
        log.info(f"Fetching: {LISTINGS_URL}")
        resp = requests.get(LISTINGS_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        log.error(f"Failed to fetch listings page: {e}")
        return None


def load_local(path: str) -> BeautifulSoup | None:
    """Load HTML from a local file."""
    p = Path(path)
    if not p.exists():
        log.error(f"Local file not found: {path}")
        return None
    log.info(f"Loading local file: {path}")
    return BeautifulSoup(p.read_text(encoding="utf-8"), "html.parser")


def parse_date_heading(text: str) -> str | None:
    """
    Parse date headings like 'Monday 13th April' to '2026-04-13'.

    The page doesn't include the year, so we infer it:
    - Use the current year
    - If the resulting date is more than 30 days in the past, bump to next year
    """
    cleaned = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", text.strip())
    now = datetime.now()

    for fmt in ["%A %d %B", "%d %B"]:
        try:
            parsed = datetime.strptime(cleaned, fmt)
            # Attach year
            candidate = parsed.replace(year=now.year)
            # If it's more than 30 days in the past, it's probably next year
            if (now - candidate).days > 30:
                candidate = parsed.replace(year=now.year + 1)
            return candidate.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_time_to_24h(time_text: str) -> str | None:
    """
    Convert '11:45 am', '8:25 pm', '12:00 pm' etc. to 24h format '11:45', '20:25', '12:00'.
    """
    time_text = time_text.strip().lower()
    for fmt in ["%I:%M %p", "%I:%M%p", "%I.%M %p"]:
        try:
            t = datetime.strptime(time_text, fmt)
            return t.strftime("%H:%M")
        except ValueError:
            continue
    return None


def parse_runtime(text: str) -> int | None:
    """Extract minutes from '125mins' or '1hr 25mins'."""
    text = text.strip().lower()
    m = re.search(r"(\d+)\s*min", text)
    h = re.search(r"(\d+)\s*hr", text)
    total = 0
    if h:
        total += int(h.group(1)) * 60
    if m:
        total += int(m.group(1))
    return total if total > 0 else None


def extract_films(soup: BeautifulSoup) -> list[dict]:
    """Parse all films and their showtimes from the What's On page."""
    films = []

    for event in soup.select("div.jacro-event"):
        # --- Film metadata ---
        title_el = event.select_one("a.liveeventtitle")
        if not title_el:
            continue

        title = title_el.get_text(strip=True)
        film_url = title_el.get("href", "")

        # Derive an ID from the URL: .../film/9899335/the-mummy-1999 → "the-mummy-1999"
        url_parts = film_url.rstrip("/").split("/")
        film_id = url_parts[-1] if url_parts else title.lower().replace(" ", "-")

        # Poster
        poster_el = event.select_one("div.film_img img")
        poster_url = poster_el.get("src", "") if poster_el else ""

        # Running time metadata spans are always in order:
        # year, runtime, country, rating, genre
        running_time_spans = event.select("div.running-time span")
        year = None
        runtime = None
        country = None
        rating = "TBC"
        genre = "Other"

        span_texts = [s.get_text(strip=True) for s in running_time_spans]
        for i, text in enumerate(span_texts):
            if re.match(r"^\d{4}$", text):
                year = text
            elif re.search(r"\d+\s*min", text.lower()):
                runtime = parse_runtime(text)
            elif text.startswith("(") and text.endswith(")"):
                rating = text.strip("()")
            elif i == len(span_texts) - 1 and not re.match(r"^\d", text) and not text.startswith("("):
                # Last span that isn't a year/runtime/rating is the genre
                genre = text
            elif year and runtime is None and not text.startswith("(") and not re.search(r"\d+\s*min", text.lower()):
                # Between year+runtime and rating = country
                country = text
            elif not year:
                country = text

        # Director / Cast
        film_info_spans = event.select("div.film-info span")
        director = None
        cast = None
        for span in film_info_spans:
            text = span.get_text(strip=True)
            if text.lower().startswith("directed by"):
                director = text.replace("Directed by ", "").replace("directed by ", "")
            elif text.lower().startswith("starring"):
                cast = text.replace("Starring ", "").replace("starring ", "")

        # Description
        desc_el = event.select_one("div.jacro-formatted-text")
        description = desc_el.get_text(strip=True) if desc_el else ""

        # --- Showtimes ---
        showtimes = {}  # date_str -> list of sessions
        current_date = None

        perf_list = event.select_one("ul.performance-list-items")
        if perf_list:
            for child in perf_list.children:
                if not hasattr(child, "name") or child.name is None:
                    continue

                # Date heading
                if child.name == "div" and "heading" in child.get("class", []):
                    date_str = parse_date_heading(child.get_text(strip=True))
                    if date_str:
                        current_date = date_str
                    continue

                # Showtime <li>
                if child.name == "li" and current_date:
                    time_el = child.select_one("span.time")
                    if not time_el:
                        continue
                    time_24 = parse_time_to_24h(time_el.get_text(strip=True))
                    if not time_24:
                        continue

                    booking_el = child.select_one("a.film_book_button")
                    booking_url = booking_el.get("href", "") if booking_el else ""

                    # Tags (4K, 35mm, SUB, etc.)
                    tags = []
                    for tag_span in child.select("div.movietag span.tag"):
                        tags.append(tag_span.get_text(strip=True))

                    session = {
                        "time": time_24,
                        "booking_url": booking_url,
                        "screen": None,  # PCC is single-screen
                        "hoh": False,
                    }
                    if tags:
                        session["tags"] = tags

                    if current_date not in showtimes:
                        showtimes[current_date] = []
                    showtimes[current_date].append(session)

        if not showtimes:
            log.warning(f"No showtimes found for: {title}")
            continue

        films.append({
            "id": film_id,
            "title": title,
            "rating": rating,
            "runtime": runtime,
            "genre": genre,
            "year": year,
            "country": country,
            "director": director,
            "cast": cast,
            "description": description,
            "film_url": film_url,
            "poster_url": poster_url,
            "showtimes": showtimes,
        })

    log.info(f"Parsed {len(films)} films with showtimes")
    return films


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
    parser = argparse.ArgumentParser(description="Scrape Prince Charles Cinema timetable")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Output path for JSON file")
    parser.add_argument("--local", type=str, default=None,
                        help="Path to a local HTML file to parse instead of fetching live")
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (
        Path(__file__).parent.parent / "public" / "data" / "films_pcc.json"
    )

    log.info("=== Prince Charles Cinema Scraper Starting ===")

    # Step 1: Get the HTML
    if args.local:
        soup = load_local(args.local)
    else:
        soup = fetch_live()

    if not soup:
        log.error("Could not load HTML. Aborting.")
        sys.exit(1)

    # Step 2: Parse all films + showtimes from the single page
    films = extract_films(soup)

    if not films:
        log.error("No films found. Aborting.")
        sys.exit(1)

    # Step 3: Assign colors
    assign_colors(films)

    # Step 4: Write output
    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "princecharlescinema.com",
        "films": films,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info("=== Scraper Complete ===")


if __name__ == "__main__":
    main()
