#!/usr/bin/env python3
"""
The Arzner Cinema (Bermondsey) Timetable Scraper

Scrapes thearzner.com/TheArzner.dll/WhatsOn for current films, showtimes,
and booking links.

The Arzner embeds ALL film data as a JSON blob inside a <script> tag:
    var Events = {"Events": [...]}

This means we only need ONE page fetch — no per-film requests needed.

The Arzner often lists the same film under multiple entries with different
event wrappings (e.g. "Departures", "Departures + Cockroach",
"Fetish Friendly: Departures", "Preview: Departures + Q&A").
Each entry is kept as a separate film since they represent genuinely
different screening experiences (different runtimes, content, or pricing).
The TypeDescription is stored as a tag so the frontend can display it.

Outputs a JSON file compatible with the CinemaHub frontend.

Usage:
    # Scrape live from the website
    python scraper/scrape_arzner.py

    # Parse a local HTML file instead (for development)
    python scraper/scrape_arzner.py --local The_Arzner_-_Overview

    # Custom output path
    python scraper/scrape_arzner.py -o my_output.json
"""

import json
import re
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path
from html import unescape

try:
    import requests
except ImportError:
    requests = None  # only needed for live mode

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

WHATS_ON_URL = "https://thearzner.com/TheArzner.dll/WhatsOn"
BASE_URL = "https://thearzner.com"

HEADERS = {
    "User-Agent": "ArznerScraper/1.0 (personal project; scrapes twice daily)",
    "Accept": "text/html,application/xhtml+xml",
}

REQUEST_TIMEOUT = 20

# ─── Color palette (consistent with other scrapers) ───

GENRE_COLORS = {
    "New Release":             {"color": "#6a1b9a", "accent": "#ba68c8"},
    "Drama":                   {"color": "#6a1b9a", "accent": "#ba68c8"},
    "Short Films":             {"color": "#ef6c00", "accent": "#ffb74d"},
    "Q&A":                     {"color": "#00897b", "accent": "#4db6ac"},
    "Preview":                 {"color": "#1565c0", "accent": "#64b5f6"},
    "UK Premiere":             {"color": "#c62828", "accent": "#ef5350"},
    "Drag Intro":              {"color": "#ad1457", "accent": "#f06292"},
    "Fetish Friendly":         {"color": "#37474f", "accent": "#78909c"},
    "Lesbian Visibility Week": {"color": "#d81b60", "accent": "#ff6090"},
    "Lesbian Visibility Day":  {"color": "#d81b60", "accent": "#ff6090"},
    "Pride Month":             {"color": "#e53935", "accent": "#ff6f60"},
    "Dog-Friendly":            {"color": "#2e7d32", "accent": "#66bb6a"},
    "Adults Only":             {"color": "#546e7a", "accent": "#90a4ae"},
    "TV":                      {"color": "#0277bd", "accent": "#4fc3f7"},
    "Movie Marathon":          {"color": "#4e342e", "accent": "#8d6e63"},
    "Comedy Night":            {"color": "#d81b60", "accent": "#ff6090"},
    "Fundraiser":              {"color": "#00838f", "accent": "#4dd0e1"},
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


# ─── Fetching / loading ───

def fetch_live() -> str | None:
    """Fetch the WhatsOn page from the live website."""
    if requests is None:
        log.error("requests library not installed. Use --local or: pip install requests")
        return None
    try:
        log.info(f"Fetching: {WHATS_ON_URL}")
        resp = requests.get(WHATS_ON_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        log.error(f"Failed to fetch WhatsOn page: {e}")
        return None


def load_local(path: str) -> str | None:
    """Load HTML from a local file."""
    p = Path(path)
    if not p.exists():
        log.error(f"Local file not found: {path}")
        return None
    log.info(f"Loading local file: {path}")
    return p.read_text(encoding="utf-8")


# ─── JSON extraction ───

def extract_events_json(html: str) -> list[dict]:
    """
    Extract the Events JSON blob from the HTML.

    The page contains a <script> block with:
        var Events =
        {"Events": [...]}\n</script>

    We find 'var Events =' then grab everything from the opening '{' up to
    the next '</script>' tag. The JSON blob can be 100KB+.
    """
    marker = "var Events ="
    idx = html.find(marker)
    if idx < 0:
        log.error("Could not find 'var Events =' in the HTML")
        return []

    # Find the opening brace of the JSON object
    brace_start = html.index("{", idx)

    # Find the closing </script> tag after the JSON
    script_end = html.index("</script>", brace_start)

    raw_json = html[brace_start:script_end].strip()

    try:
        data = json.loads(raw_json)
        events = data.get("Events", [])
        log.info(f"Extracted {len(events)} events from JSON blob")
        return events
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse Events JSON: {e}")
        return []


# ─── Parsing helpers ───

def extract_bbfc_rating(rating_html: str) -> str:
    """
    Extract the BBFC rating from the HTML img tag.

    Input:  '<img class="film-rating" src="..." alt="BBFC Rating: (15)"/>'
    Output: '15'
    """
    if not rating_html:
        return "TBC"
    match = re.search(r'BBFC Rating:\s*\((\w+)\)', rating_html)
    if match:
        return match.group(1)
    return "TBC"


def make_slug(event_id: int, title: str) -> str:
    """Create a URL-friendly slug from the event ID and title."""
    # Clean the title: decode HTML entities, lowercase, replace non-alphanum
    clean = unescape(title).lower()
    clean = re.sub(r'[^a-z0-9]+', '-', clean)
    clean = clean.strip('-')
    return f"{clean}-{event_id}"


def build_booking_url(relative_url: str) -> str:
    """Build a full booking URL from a relative path."""
    if not relative_url:
        return ""
    if relative_url.startswith("http"):
        return relative_url
    # The booking URLs are relative to the base: "Booking?Booking=..."
    return f"{BASE_URL}/TheArzner.dll/{relative_url}"


def clean_synopsis(synopsis: str) -> str:
    """Clean up synopsis text — decode HTML entities and trim ellipsis."""
    if not synopsis:
        return ""
    text = unescape(synopsis)
    # Remove trailing "..." from truncated synopses
    text = re.sub(r'\.\.\.\s*$', '…', text)
    return text.strip()


# ─── Main parsing ───

def parse_events(events: list[dict]) -> list[dict]:
    """
    Parse the raw events from the embedded JSON into the standard film format.

    Each event in the Arzner JSON has this structure:
    {
        "ID": 107368,
        "Title": "Night Stage",
        "Rating": '<img ... alt="BBFC Rating: (18)"/>',
        "Type": 43997,
        "TypeDescription": "New Release",
        "Synopsis": "...",
        "RunningTime": 117,
        "ImageURL": "https://images.savoysystems.co.uk/TAL/173573.jpg",
        "URL": "https://thearzner.com/TheArzner.dll/WhatsOn?f=107368",
        "Tags": [{"Format": ""}],
        "Performances": [
            {
                "ID": 175675,
                "IsSoldOut": "N",
                "CC": "N",           # Closed captions: Y/N
                "StartDate": "2026-04-13",
                "StartTimeAndNotes": "11:45",
                "StartTime": "1145",
                "ReadableDate": "Mon 13 Apr",
                "AuditoriumName": "Screening Room",
                "URL": "Booking?Booking=...",
                "IsOpenForSale": true
            }
        ]
    }
    """
    films = []

    for event in events:
        event_id = event.get("ID")
        title = unescape(event.get("Title", "")).strip()
        if not title or not event_id:
            continue

        rating = extract_bbfc_rating(event.get("Rating", ""))
        runtime = event.get("RunningTime", 0) or None
        if runtime and runtime <= 0:
            runtime = None

        type_desc = event.get("TypeDescription", "").strip()
        synopsis = clean_synopsis(event.get("Synopsis", ""))
        image_url = event.get("ImageURL", "")
        film_url = event.get("URL", "")
        slug = make_slug(event_id, title)

        # ─── Parse performances into showtimes ───
        showtimes = {}  # date_str -> list of sessions
        performances = event.get("Performances", [])

        for perf in performances:
            if not perf.get("IsOpenForSale"):
                continue

            date_str = perf.get("StartDate", "")
            time_str = perf.get("StartTimeAndNotes", "")
            if not date_str or not time_str:
                continue

            # Normalise time: "11:45" is already fine, but ensure HH:MM
            time_str = time_str.strip()
            if not re.match(r'^\d{1,2}:\d{2}', time_str):
                # Try to parse from StartTime field like "1145"
                raw_time = perf.get("StartTime", "")
                if len(raw_time) == 4 and raw_time.isdigit():
                    time_str = f"{raw_time[:2]}:{raw_time[2:]}"
                else:
                    continue

            # Just take HH:MM if there are notes after the time
            time_match = re.match(r'^(\d{1,2}:\d{2})', time_str)
            if time_match:
                time_str = time_match.group(1)
                # Zero-pad hour if needed (e.g. "9:00" -> "09:00")
                if len(time_str) == 4:
                    time_str = "0" + time_str

            booking_url = build_booking_url(perf.get("URL", ""))
            is_sold_out = perf.get("IsSoldOut", "N") == "Y"
            has_cc = perf.get("CC", "N") == "Y"

            screen = perf.get("AuditoriumName", None)

            session = {
                "time": time_str,
                "booking_url": booking_url,
                "screen": screen,
                "hoh": has_cc,  # CC = closed captions, closest to HoH
            }

            # Build session-level tags
            session_tags = []
            if type_desc and type_desc != "New Release":
                session_tags.append(type_desc)
            if has_cc:
                session_tags.append("CC")
            if is_sold_out:
                session_tags.append("Sold Out")
            if session_tags:
                session["tags"] = session_tags

            if date_str not in showtimes:
                showtimes[date_str] = []
            showtimes[date_str].append(session)

        if not showtimes:
            log.warning(f"No showtimes found for: {title} (ID {event_id})")
            continue

        films.append({
            "id": slug,
            "title": title,
            "rating": rating,
            "runtime": runtime,
            "genre": type_desc if type_desc else "Other",
            "description": synopsis,
            "film_url": film_url,
            "poster_url": image_url,
            "showtimes": showtimes,
        })

    log.info(f"Parsed {len(films)} films with showtimes")
    return films


def assign_colors(films: list[dict]) -> None:
    """Assign colors to films based on genre/type, avoiding duplicates."""
    used_colors = set()
    palette_idx = 0

    for film in films:
        genre = film.get("genre", "Other")
        colors = GENRE_COLORS.get(genre, None)

        if colors and colors["color"] not in used_colors:
            film["color"] = colors["color"]
            film["accent"] = colors["accent"]
            used_colors.add(colors["color"])
        else:
            while palette_idx < len(EXTRA_PALETTES):
                c = EXTRA_PALETTES[palette_idx]
                palette_idx += 1
                if c["color"] not in used_colors:
                    film["color"] = c["color"]
                    film["accent"] = c["accent"]
                    used_colors.add(c["color"])
                    break
            else:
                film["color"] = DEFAULT_COLORS["color"]
                film["accent"] = DEFAULT_COLORS["accent"]


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape The Arzner cinema timetable")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Output path for JSON file")
    parser.add_argument("--local", type=str, default=None,
                        help="Path to a local HTML file to parse instead of fetching live")
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (
        Path(__file__).parent.parent / "public" / "data" / "films_arzner.json"
    )

    log.info("=== The Arzner Scraper Starting ===")

    # Step 1: Get the HTML
    if args.local:
        html = load_local(args.local)
    else:
        html = fetch_live()

    if not html:
        log.error("Could not load HTML. Aborting.")
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
        "source": "thearzner.com",
        "films": films,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info("=== Scraper Complete ===")


if __name__ == "__main__":
    main()
