#!/usr/bin/env python3
"""
The Garden Cinema (Covent Garden) Timetable Scraper

Scrapes thegardencinema.co.uk homepage for current films, showtimes,
booking links, and metadata.

Single-page approach: the homepage renders all listings by date with
full film info, showtimes, and booking URLs in the HTML. No JavaScript
rendering needed.

Outputs a JSON file compatible with the CinemaHub frontend.

Usage:
    python scrape_garden.py
    python scrape_garden.py --local saved_page.html
    python scrape_garden.py -o my_output.json
"""

import json
import re
import sys
import logging
import colorsys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.thegardencinema.co.uk"
HOMEPAGE_URL = BASE_URL + "/"

REQUEST_TIMEOUT = 20
HEADERS = {
    "User-Agent": "GardenCinemaScraper/1.0 (personal project; scrapes twice daily)",
    "Accept": "text/html,application/xhtml+xml",
}

# ─── Tag mapping: CSS class fragments → human-readable tag names ─────
TAG_MAP = {
    "q_and_a":          "Q&A",
    "intro":            "Intro",
    "pay_what_you_can": "PWYC",
    "discussion":       "Discussion",
    "live_music":       "Live Music",
    "hoh":              "HoH",
    "audio_description": "AD",
    "members":          "Members",
    "free_members":     "Free (Members)",
}

# ─── Known countries for director / country parsing ──────────────────
KNOWN_COUNTRIES = {
    "usa", "uk", "france", "germany", "italy", "japan", "china",
    "spain", "india", "brazil", "argentina", "mexico", "canada",
    "australia", "south korea", "korea", "ireland", "belgium",
    "netherlands", "sweden", "norway", "denmark", "finland",
    "switzerland", "austria", "poland", "czech republic", "hungary",
    "romania", "greece", "turkey", "israel", "iran", "egypt",
    "portugal", "hong kong", "taiwan", "thailand", "vietnam",
    "indonesia", "philippines", "new zealand", "russia", "ukraine",
    "colombia", "chile", "peru", "cuba", "north macedonia",
    "serbia", "croatia", "bangladesh", "pakistan",
    "united states", "united kingdom",
    "various countries",
}

# ─── Non-film items to skip ──────────────────────────────────────────
NON_FILM_TITLES = {
    "members' mingle",
    "member mingle",
    "members mingle",
    "members' scratch night",
    "baijiu tasting with traditional pastry pairings",
    "baijiu tasting",
}

# ─── Color palette ───────────────────────────────────────────────────
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


# ─── Helpers ─────────────────────────────────────────────────────────

def fetch_html(url: str) -> str | None:
    """Fetch the homepage HTML."""
    try:
        log.info(f"Fetching: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        log.error(f"Failed to fetch {url}: {e}")
        return None


def parse_stats(text: str) -> dict:
    """
    Parse the stats line like:
      'Max Walker-Silverman, USA, UK, 2025, 95m.'
      'Jim Jarmusch, USA, Ireland, France, Italy, Japan, 2025, 110m.'
      '120m.'
      'Various Directors, Various Countries, Various Years, 63m.'

    Returns dict with keys: director, year, runtime, countries
    """
    text = text.strip().rstrip(".")
    if not text:
        return {}

    # Extract runtime (e.g. "95m")
    runtime = None
    runtime_match = re.search(r"(\d+)\s*m$", text)
    if runtime_match:
        runtime = int(runtime_match.group(1))
        text = text[: runtime_match.start()].rstrip(", ")

    if not text:
        return {"runtime": runtime}

    # Split into comma-separated parts
    parts = [p.strip() for p in text.split(",") if p.strip()]

    # Find year (4-digit 19xx/20xx)
    year = None
    year_idx = None
    for i, part in enumerate(parts):
        if re.match(r"^(19|20)\d{2}$", part):
            year = part
            year_idx = i
            break

    # Separate director from countries
    # Everything before the year is director + countries
    # Work backwards from year: countries are known strings
    director = None
    countries = []

    if year_idx is not None:
        pre_year = parts[:year_idx]
    else:
        pre_year = parts

    # Walk backwards through pre_year to find countries
    director_parts = []
    found_country = False
    for part in reversed(pre_year):
        if part.lower() in KNOWN_COUNTRIES:
            countries.insert(0, part)
            found_country = True
        elif found_country:
            # Once we've passed countries, everything before is director
            director_parts.insert(0, part)
        else:
            # Haven't found any country yet — could be a country
            # we don't know, or a director. Check heuristically.
            # If it's a single uppercase word or common country pattern
            if re.match(r"^[A-Z][a-z]+$", part) and len(part) <= 12:
                # Might be a country we missed — be conservative, treat as country
                countries.insert(0, part)
            else:
                director_parts.insert(0, part)

    # If we never found a country, all pre-year parts might be director
    if not found_country and not director_parts:
        director_parts = pre_year
        countries = []

    if director_parts:
        director = ", ".join(director_parts)
        # Skip "Various Directors" etc.
        if director.lower().startswith("various"):
            director = None

    result = {"runtime": runtime}
    if director:
        result["director"] = director
    if year:
        result["year"] = year

    return result


def extract_screening_tags(panel) -> list[str]:
    """Extract tags from screening-panel CSS classes and child elements."""
    tags = []
    classes = panel.get("class", [])
    for cls in classes:
        for key, label in TAG_MAP.items():
            if key in cls and label not in tags:
                tags.append(label)
    # Also check span.screening-tag elements
    for span in panel.select("span.screening-tag"):
        span_classes = " ".join(span.get("class", []))
        for key, label in TAG_MAP.items():
            if key in span_classes and label not in tags:
                tags.append(label)
    return tags


def is_sold_out(panel) -> bool:
    """Check if a screening-panel has the sold-out class."""
    classes = panel.get("class", [])
    return "sold-out" in classes


def parse_screening_date(date_title_text: str, reference_year: int = None) -> str | None:
    """
    Parse date from screening panel date title like 'Fri 17 Apr'.
    Returns ISO date string like '2026-04-17'.
    """
    if not date_title_text:
        return None

    # Clean the text
    text = date_title_text.strip()

    # Try parsing "Fri 17 Apr" or "Sat 02 May"
    # We need to guess the year — use current year
    if reference_year is None:
        reference_year = datetime.now().year

    # Try multiple year possibilities
    for year in [reference_year, reference_year + 1]:
        try:
            dt = datetime.strptime(f"{text} {year}", "%a %d %b %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


# ─── Main parsing ────────────────────────────────────────────────────

def parse_homepage(html: str) -> list[dict]:
    """
    Parse the Garden Cinema homepage HTML and return a list of films
    with showtimes in the standard CinemaHub format.
    """
    soup = BeautifulSoup(html, "html.parser")
    films_by_url = {}  # Deduplicate by film URL

    # Find all date blocks in the by-date listings
    date_blocks = soup.select("section.films-list__by-date div.date-block")
    log.info(f"Found {len(date_blocks)} date blocks")

    for date_block in date_blocks:
        block_date = date_block.get("data-date", "")  # e.g. "2026-04-17"

        for film_el in date_block.select("div.films-list__by-date__film"):
            # ─── Title & URL ───
            title_link = film_el.select_one(
                "h1.films-list__by-date__film__title > a"
            )
            if not title_link:
                continue

            film_url = urljoin(BASE_URL, title_link.get("href", ""))

            # Title text (exclude the rating span)
            rating_span = title_link.select_one(
                "span.films-list__by-date__film__rating"
            )
            if rating_span:
                rating = rating_span.get_text(strip=True)
                # Get title without the rating
                title_text = title_link.get_text(strip=True)
                if title_text.endswith(rating):
                    title_text = title_text[: -len(rating)].strip()
            else:
                title_text = title_link.get_text(strip=True)
                rating = "TBC"

            if not title_text:
                continue

            # Skip non-film events
            if title_text.lower().strip() in NON_FILM_TITLES:
                log.info(f"  Skipping non-film: {title_text}")
                continue

            # ─── Poster ───
            poster_img = film_el.select_one("img.films-list__by-date__film__thumb")
            poster_url = poster_img.get("src", "") if poster_img else ""

            # ─── Synopsis ───
            synopsis_el = film_el.select_one(
                "div.films-list__by-date__film__synopsis"
            )
            description = synopsis_el.get_text(strip=True) if synopsis_el else ""

            # ─── Stats (director, year, runtime) ───
            stats_el = film_el.select_one("div.films-list__by-date__film__stats")
            stats_text = stats_el.get_text(strip=True) if stats_el else ""
            stats = parse_stats(stats_text)

            # ─── Season / Strand ───
            season_links = film_el.select(
                "span.films-list__by-date__film__season__link a"
            )
            seasons = [a.get_text(strip=True) for a in season_links]

            # ─── Partner ───
            partner_link = film_el.select_one(
                "span.films-list__by-date__film__partner__link a"
            )
            partner = partner_link.get_text(strip=True) if partner_link else None

            # ─── Showtimes ───
            screenings_for_date = []
            for panel in film_el.select("div.screening-panel"):
                # Time
                time_link = panel.select_one("span.screening-time > a.screening")
                if not time_link:
                    continue
                time_str = time_link.get_text(strip=True)
                booking_url = time_link.get("href", "")

                # Date from screening panel (use block_date as fallback)
                date_title_el = panel.select_one("div.screening-panel__date-title")
                if date_title_el:
                    screening_date = parse_screening_date(
                        date_title_el.get_text(strip=True)
                    )
                else:
                    screening_date = None

                if not screening_date:
                    screening_date = block_date

                # Tags
                tags = extract_screening_tags(panel)

                # Sold out
                sold_out = is_sold_out(panel)

                # HoH from tags
                hoh = "HoH" in tags

                session = {
                    "time": time_str,
                    "booking_url": booking_url,
                    "screen": None,  # Garden Cinema doesn't expose screen numbers in listings
                    "hoh": hoh,
                }
                if tags:
                    session["tags"] = tags
                if sold_out:
                    session["sold_out"] = True

                screenings_for_date.append((screening_date, session))

            # ─── Merge into films_by_url ───
            if film_url not in films_by_url:
                slug = film_url.rstrip("/").split("/")[-1]
                films_by_url[film_url] = {
                    "id": slug,
                    "title": title_text,
                    "rating": rating,
                    "runtime": stats.get("runtime"),
                    "genre": "Other",
                    "year": stats.get("year"),
                    "director": stats.get("director"),
                    "cast": None,
                    "description": description,
                    "film_url": film_url,
                    "poster_url": poster_url,
                    "showtimes": {},
                }
                if seasons:
                    films_by_url[film_url]["seasons"] = seasons
                if partner:
                    films_by_url[film_url]["partner"] = partner

            # Add showtimes
            film = films_by_url[film_url]
            for date_str, session in screenings_for_date:
                if date_str not in film["showtimes"]:
                    film["showtimes"][date_str] = []
                # Avoid duplicates (same time + same booking URL)
                existing = film["showtimes"][date_str]
                is_dup = any(
                    s["time"] == session["time"]
                    and s["booking_url"] == session["booking_url"]
                    for s in existing
                )
                if not is_dup:
                    existing.append(session)

    # Filter to only films with showtimes
    films = [f for f in films_by_url.values() if f["showtimes"]]

    log.info(f"Parsed {len(films)} unique films with showtimes")
    return films


# ─── Color assignment ────────────────────────────────────────────────

def assign_colors(films: list[dict]) -> None:
    """Assign colors using genre palette, extras, then procedural generation."""

    def hsl_to_hex(h, s, l):
        r, g, b = colorsys.hls_to_rgb(h, l, s)
        return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"

    def generate_color(index):
        golden = 0.618033988749895
        hue = (index * golden) % 1.0
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
        colors = GENRE_COLORS.get(genre)

        if colors and colors["color"] not in used_colors:
            film["color"] = colors["color"]
            film["accent"] = colors["accent"]
            used_colors.add(colors["color"])
        else:
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
            if not assigned:
                while True:
                    c = generate_color(gen_idx)
                    gen_idx += 1
                    if c["color"] not in used_colors:
                        film["color"] = c["color"]
                        film["accent"] = c["accent"]
                        used_colors.add(c["color"])
                        break


# ─── Main ────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Garden Cinema timetable")
    parser.add_argument(
        "--local", type=str, default=None,
        help="Path to a saved HTML file (skip network request)",
    )
    parser.add_argument(
        "-o", "--output", type=str, default=None,
        help="Output path for JSON file",
    )
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (
        Path(__file__).parent / "public" / "data" / "films_garden.json"
    )

    log.info("=== Garden Cinema Scraper Starting ===")

    # Step 1: Get HTML
    if args.local:
        local_path = Path(args.local)
        if not local_path.exists():
            log.error(f"Local file not found: {local_path}")
            sys.exit(1)
        html = local_path.read_text(encoding="utf-8")
        log.info(f"Loaded local file: {local_path} ({len(html):,} bytes)")
    else:
        html = fetch_html(HOMEPAGE_URL)
        if not html:
            log.error("Could not fetch homepage. Aborting.")
            sys.exit(1)

    # Step 2: Parse
    films = parse_homepage(html)

    if not films:
        log.error("No films with showtimes found. Aborting.")
        sys.exit(1)

    # Step 3: Assign colors
    assign_colors(films)

    # Step 4: Write output
    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "thegardencinema.co.uk",
        "films": films,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    log.info(f"Wrote {len(films)} films to {output_path}")

    # Summary
    total_screenings = sum(
        len(sessions)
        for f in films
        for sessions in f["showtimes"].values()
    )
    log.info(f"Total screenings: {total_screenings}")
    log.info("=== Scraper Complete ===")


if __name__ == "__main__":
    main()
