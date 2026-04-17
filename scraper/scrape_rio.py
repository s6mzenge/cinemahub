#!/usr/bin/env python3
"""
Rio Cinema (Dalston) Timetable Scraper

Scrapes riocinema.org.uk for current films, showtimes, and booking links.

The Rio uses a Savoy Systems backend that embeds all event data as a JSON
object in a <script> tag on the What's On page. This scraper extracts that
JSON directly — no detail page requests or JavaScript rendering needed.

Outputs a JSON file compatible with the CinemaHub frontend.

Usage:
    # Scrape live (default output: public/data/films_rio.json)
    python scrape_rio.py

    # Parse a local HTML file instead (for development)
    python scrape_rio.py --local rio_raw_response.html

    # Custom output path
    python scrape_rio.py -o my_output.json
"""

import json
import re
import sys
import logging
import colorsys
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    requests = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://riocinema.org.uk"
LISTINGS_URL = f"{BASE_URL}/Rio.dll/WhatsOn"

REQUEST_TIMEOUT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}


# ─── Title cleaning ──────────────────────────────────────────────────

# Known event-series prefixes that appear before a colon.
# Matched case-insensitively. Sorted longest-first at module load.
RIO_EVENT_PREFIXES = sorted([
    "rio forever",
    "rio forever / never ever",
    "rio forever / never ever with category h",
    "rio forever x queer east",
    "rio forever x asif kapadia",
    "rio forever x",
    "carers & babies",
    "carers &amp; babies",
    "carers and babies",
    "classic matinee",
    "saturday morning picture club",
    "pink palace",
    "queer east",
    "doc'n roll",
    "doc 'n roll",
    "doc'n roll x rio",
    "doc 'n roll x rio",
    "kino polonia",
    "never watching movies",
    "talented u",
    "queer horror nights",
    "hackney history festival presents",
    "hackney history festival",
    "dionne edwards",
    "molly manning walker",
    "never ever",
    "dailies x ico",
    "lwl present",
    "little white lies present",
    "varda film club x la cinemaíz present",
    "varda film club x la cinemaíz  present",
    "varda film club",
], key=len, reverse=True)

# Suffixes to strip (applied with re.IGNORECASE, multi-pass)
TRAILING_SUFFIXES = [
    r"\s*\+\s*Q\s*&\s*A\s*$",
    r"\s*\+\s*Q\s*&amp;\s*A\s*$",
    r"\s*\+\s*Panel\s*$",
    r"\s*\+\s*Intro(?:\s+.*)?$",
    r"\s*\+\s*Discussion\s*$",
    r"\s*\+\s*Live\s+Score\s*$",
    r"\s*\+\s*Best\s+of\s+.*$",
    r"\s+UK\s+Premiere\s*$",
    r"\s+Premiere\s*$",
    r"\s+on\s+35\s*mm\s*$",
    r"\s*\(35\s*mm\s*\)\s*$",
    r"\s+35\s*mm\s*$",
    r"\s+with\s+Shadow\s+Cast\s*$",
    r"\s+with\s+[A-Z][a-z]+(?:\s+(?:and|&)\s+[A-Z][a-z]+)*(?:\s+[A-Z][a-zÀ-ÿ]+)+\s*$",
    r"\s+at\s+RIO\s+CINEMA\s*$",
    r"\s+at\s+Rio\s+Cinema\s*$",
]

# Non-film items to skip entirely
NON_FILM_SKIP_PATTERNS = [
    r"ego death.*rave",
    r"wesley gonzalez.*clementine march",
]


def clean_rio_title(raw_title: str) -> str:
    """
    Extract the actual film title from Rio Cinema's event-style listing titles.

    Examples:
        "RIO FOREVER: PURPLE RAIN" → "Purple Rain"
        "Classic Matinee: HARD TRUTHS" → "Hard Truths"
        "Carers &amp; Babies: THE DRAMA" → "The Drama"
        "SURVIVING EARTH + Q&amp;A" → "Surviving Earth"
        "Kino Polonia - Three Colours: White" → "Three Colours: White"
    """
    t = unescape(raw_title).strip()

    # Normalise whitespace (nbsp, multiple spaces)
    t = re.sub(r"[\xa0\u00a0]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    # Handle "Kino Polonia - Three Colours: White" (dash separator)
    dash_match = re.match(r"^(Kino\s+Polonia)\s*[-–—]\s*(.+)$", t, re.IGNORECASE)
    if dash_match:
        t = dash_match.group(2).strip()

    # Strip known event-series prefixes before colon (iterative for nested prefixes)
    prefix_changed = True
    while prefix_changed and ":" in t:
        prefix_changed = False
        for prefix in RIO_EVENT_PREFIXES:
            colon_positions = [i for i, c in enumerate(t) if c == ":"]
            for colon_pos in colon_positions:
                before = t[:colon_pos].strip()
                if before.lower() == prefix or before.lower().rstrip(" -") == prefix:
                    t = t[colon_pos + 1:].strip()
                    prefix_changed = True
                    break
            else:
                continue
            break

    # Strip "RIO STAFF SELECTS" space-separated prefix (no colon)
    t = re.sub(r"^rio\s+staff\s+selects\s+", "", t, flags=re.IGNORECASE).strip()

    # Strip trailing suffixes in a loop until stable
    changed = True
    while changed:
        changed = False
        for pattern in TRAILING_SUFFIXES:
            new_t = re.sub(pattern, "", t, flags=re.IGNORECASE).strip()
            if new_t != t:
                t = new_t
                changed = True

    # Strip "(35MM)" parentheticals
    t = re.sub(r"\s*\(35\s*mm\s*\)\s*$", "", t, flags=re.IGNORECASE).strip()

    # Strip year parenthetical at end
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t).strip()

    # Title-case if ALL CAPS (preserve mixed case)
    if t == t.upper() and len(t) > 3:
        t = _smart_title_case(t)

    return t.strip()


def _smart_title_case(text: str) -> str:
    """Convert ALL CAPS to title case, preserving Roman numerals."""
    small_words = {"a", "an", "the", "and", "but", "or", "for", "nor",
                   "in", "on", "at", "to", "of", "by", "vs", "vs."}
    roman_re = re.compile(r"^(I{1,3}|IV|V|VI{0,3}|IX|X{0,3}|XI{0,3}|XII{0,3})$")

    words = text.split()
    result = []
    for i, word in enumerate(words):
        if roman_re.match(word):
            result.append(word)
        elif i == 0:
            result.append(word.capitalize())
        elif word.lower() in small_words:
            result.append(word.lower())
        else:
            result.append(word.capitalize())

    text_out = " ".join(result)
    # Capitalize word after colon
    text_out = re.sub(r":\s+([a-z])", lambda m: ": " + m.group(1).upper(), text_out)
    return text_out


def make_film_id(title: str) -> str:
    """Generate a URL-friendly ID from a title."""
    slug = title.lower()
    slug = re.sub(r"[''']", "", slug)
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    return slug


# ─── BBFC Rating parsing ─────────────────────────────────────────────

VALID_BBFC = {"U", "PG", "12A", "12", "15", "18", "R18"}


def parse_bbfc_from_html(rating_html: str) -> str:
    """Extract BBFC rating from the HTML string in the Events JSON."""
    if not rating_html:
        return "TBC"
    m = re.search(r"BBFC Rating:\s*\((\w+)\)", rating_html)
    if m and m.group(1) in VALID_BBFC:
        return m.group(1)
    if "No Rating" in rating_html:
        return "TBC"
    # Try bare parenthetical
    m = re.search(r"\((\w+)\)", rating_html)
    if m and m.group(1) in VALID_BBFC:
        return m.group(1)
    return "TBC"


# ─── Performance tag extraction ──────────────────────────────────────

PERF_TAG_MAP = {
    "PP": "Pink Palace",
    "SP": "Special Event",
    "CM": "Classic Matinee",
    "QA": "Q&A",
    "FF": "Family Flicks",
    "HoH": "HoH",
    "RS": "Relaxed",
    "CB": "Carers & Babies",
    "NoAds": "NoAds",
    "RF": "Rio Forever",
}


def extract_perf_tags(perf: dict) -> list[str]:
    """Extract active tags from a performance object."""
    tags = []
    for key, label in PERF_TAG_MAP.items():
        if perf.get(key) == "Y":
            tags.append(label)
    return tags


# ─── JSON extraction ─────────────────────────────────────────────────

def extract_events_json(html: str) -> list[dict]:
    """
    Extract the Events JSON from the embedded <script> tag.

    The page contains a script with:
        var Events = {"Events":[{...},{...},...]}
    or just the JSON object assigned inline.
    """
    # Find the start of the JSON object
    idx = html.find('{"Events":[')
    if idx < 0:
        log.error("Could not find Events JSON in the HTML")
        return []

    # Parse by finding balanced braces
    depth = 0
    for i in range(idx, len(html)):
        if html[i] == "{":
            depth += 1
        elif html[i] == "}":
            depth -= 1
            if depth == 0:
                raw_json = html[idx:i + 1]
                break
    else:
        log.error("Could not find closing brace for Events JSON")
        return []

    try:
        data = json.loads(raw_json)
        events = data.get("Events", [])
        log.info(f"Extracted {len(events)} events from embedded JSON")
        return events
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse Events JSON: {e}")
        return []


# ─── Event → Film conversion ─────────────────────────────────────────

def parse_events(events: list[dict]) -> list[dict]:
    """Convert Savoy Systems events into the CinemaHub film format."""
    films = []

    for event in events:
        raw_title = unescape(event.get("Title", "").strip())
        rio_id = event.get("ID")

        if not raw_title:
            continue

        # Skip non-film events
        skip = False
        for pattern in NON_FILM_SKIP_PATTERNS:
            if re.search(pattern, raw_title, re.IGNORECASE):
                log.info(f"  Skipping non-film: {raw_title}")
                skip = True
                break
        if skip:
            continue

        # Clean the title
        clean_title = clean_rio_title(raw_title)
        film_id = make_film_id(clean_title)

        # Rating
        rating = parse_bbfc_from_html(event.get("Rating", ""))

        # Runtime
        runtime = event.get("RunningTime")
        if isinstance(runtime, str):
            m = re.search(r"\d+", runtime)
            runtime = int(m.group()) if m else None

        # Director (trim leading space)
        director = (event.get("Director") or "").strip() or None

        # Cast
        cast = (event.get("Cast") or "").strip() or None

        # Year
        year = (event.get("Year") or "").strip() or None

        # Country
        country = (event.get("Country") or "").strip() or None

        # Synopsis
        synopsis = (event.get("Synopsis") or "").strip() or None

        # Film URL
        film_url = event.get("URL", "")

        # Poster URL
        poster_url = event.get("ImageURL", "")

        # Season tags
        season_tags = []
        for season in event.get("Seasons", []):
            name = season.get("SeasonName", "").strip()
            if name:
                season_tags.append(name)

        # Build showtimes from Performances
        showtimes = {}
        for perf in event.get("Performances", []):
            date_str = perf.get("StartDate", "")
            time_str = perf.get("StartTimeAndNotes", "").strip().lower()

            if not date_str or not time_str:
                continue

            # Validate time format
            if not re.match(r"\d{1,2}:\d{2}", time_str):
                continue

            # Booking URL
            booking_url = perf.get("URL", "")
            if booking_url and not booking_url.startswith("http"):
                booking_url = urljoin(BASE_URL + "/Rio.dll/", booking_url)

            # Screen name
            screen = perf.get("AuditoriumName") or None
            # Shorten: "Screen 1" → "S1"
            if screen:
                m = re.match(r"Screen\s+(\d+)", screen)
                if m:
                    screen = f"S{m.group(1)}"

            # HoH flag
            hoh = perf.get("HoH") == "Y"

            # Tags
            tags = extract_perf_tags(perf)

            # Sold out
            sold_out = perf.get("IsSoldOut") == "Y"

            session = {
                "time": time_str,
                "booking_url": booking_url,
                "screen": screen,
                "hoh": hoh,
            }
            if tags:
                session["tags"] = tags
            if sold_out:
                session["sold_out"] = True

            if date_str not in showtimes:
                showtimes[date_str] = []
            showtimes[date_str].append(session)

        if not showtimes:
            log.warning(f"  No showtimes for: {raw_title}")
            continue

        film = {
            "id": film_id,
            "title": clean_title,
            "rating": rating,
            "runtime": runtime,
            "genre": "Other",
            "year": year,
            "director": director,
            "cast": cast,
            "country": country,
            "description": synopsis,
            "film_url": film_url,
            "poster_url": poster_url,
            "showtimes": showtimes,
        }

        if season_tags:
            film["season_tags"] = season_tags

        films.append(film)
        perf_count = sum(len(v) for v in showtimes.values())
        log.info(f"  ✓ {clean_title} — {len(showtimes)} date(s), "
                 f"{perf_count} perf(s), {rating}, {runtime}min")

    log.info(f"Parsed {len(films)} films with showtimes")
    return films


# ─── Color assignment ─────────────────────────────────────────────────

GENRE_COLORS = {
    "Animation":   {"color": "#e53935", "accent": "#ff6f60"},
    "Adventure":   {"color": "#7c4dff", "accent": "#b388ff"},
    "Horror":      {"color": "#546e7a", "accent": "#90a4ae"},
    "Comedy":      {"color": "#d81b60", "accent": "#ff6090"},
    "Romance":     {"color": "#00897b", "accent": "#4db6ac"},
    "Action":      {"color": "#ef6c00", "accent": "#ffb74d"},
    "Documentary": {"color": "#c62828", "accent": "#ef5350"},
    "Drama":       {"color": "#6a1b9a", "accent": "#ba68c8"},
    "Sci-Fi":      {"color": "#00838f", "accent": "#4dd0e1"},
    "Thriller":    {"color": "#37474f", "accent": "#78909c"},
    "Family":      {"color": "#2e7d32", "accent": "#66bb6a"},
    "Musical":     {"color": "#ad1457", "accent": "#f06292"},
    "Crime":       {"color": "#4e342e", "accent": "#8d6e63"},
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


def assign_colors(films: list[dict]) -> None:
    """Assign colors using golden-ratio hue stepping."""
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
        colors = GENRE_COLORS.get(genre, None)

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


# ─── Entry point ──────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Rio Cinema timetable")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Output path for JSON file")
    parser.add_argument("--local", type=str, default=None,
                        help="Path to a saved HTML file to parse instead of fetching live")
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (
        Path(__file__).parent.parent / "public" / "data" / "films_rio.json"
    )

    log.info("=== Rio Cinema Scraper Starting ===")

    # Step 1: Get the HTML
    if args.local:
        log.info(f"Loading local file: {args.local}")
        html = Path(args.local).read_text(encoding="utf-8")
    else:
        if requests is None:
            log.error("requests library not installed. Use --local or: pip install requests")
            sys.exit(1)
        log.info(f"Fetching: {LISTINGS_URL}")
        try:
            resp = requests.get(LISTINGS_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            log.error(f"Failed to fetch listings page: {e}")
            sys.exit(1)

    # Step 2: Extract the Events JSON from the embedded <script>
    events = extract_events_json(html)
    if not events:
        log.error("No events found in JSON. Aborting.")
        sys.exit(1)

    # Step 3: Parse events into the standard film format
    films = parse_events(events)
    if not films:
        log.error("No films with showtimes found. Aborting.")
        sys.exit(1)

    # Step 4: Assign colors
    assign_colors(films)

    # Step 5: Write output
    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "riocinema.org.uk",
        "films": films,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info("=== Scraper Complete ===")


if __name__ == "__main__":
    main()
