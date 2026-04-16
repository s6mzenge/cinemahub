#!/usr/bin/env python3
"""
Close-Up Film Centre Timetable Scraper

Scrapes closeupfilmcentre.com/film_programmes/ for current films, showtimes,
and booking links.

Close-Up embeds ALL showtime data as a JSON string in a <script> tag:
    var shows = '[{"id":"...","title":"...","show_time":"...","blink":"...",...}]';

This means we only need ONE page fetch for showtimes. Optionally, we can
fetch individual film detail pages for director/year/runtime metadata.

Titles may contain double-encoded UTF-8 HTML entities (the server encodes
UTF-8 bytes as Latin-1, then HTML-encodes that). We fix this automatically.

Outputs a JSON file compatible with the CinemaHub frontend.

Usage:
    # Scrape live from the website
    python scrape_closeup.py

    # Parse a local HTML file instead (for development)
    python scrape_closeup.py --local saved_page.html

    # Skip detail page fetches (faster, no runtime/director/year)
    python scrape_closeup.py --no-details

    # Custom output path
    python scrape_closeup.py -o my_output.json
"""

import json
import re
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from html import unescape
from collections import defaultdict

try:
    import requests
except ImportError:
    requests = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.closeupfilmcentre.com"
LISTINGS_URL = f"{BASE_URL}/film_programmes/"

# Polite delay between detail page requests (seconds)
REQUEST_DELAY = 1.0
REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.5",
}

# ─── Color palette ───

GENRE_COLORS = {
    "Documentary":  {"color": "#c62828", "accent": "#ef5350"},
    "Drama":        {"color": "#6a1b9a", "accent": "#ba68c8"},
    "Classic":      {"color": "#00838f", "accent": "#4dd0e1"},
    "Experimental": {"color": "#ef6c00", "accent": "#ffb74d"},
    "Repertory":    {"color": "#1565c0", "accent": "#64b5f6"},
    "Festival":     {"color": "#d81b60", "accent": "#ff6090"},
    "Sci-Fi":       {"color": "#00838f", "accent": "#4dd0e1"},
    "World Cinema": {"color": "#00897b", "accent": "#4db6ac"},
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


# ─── Fetching / loading ───

def fetch_page(url: str) -> str | None:
    """
    Fetch a page. Tries curl_cffi first (Chrome TLS impersonation for
    Cloudflare bypass), then falls back to plain requests.
    """
    # Strategy 1: curl_cffi (Chrome TLS impersonation)
    try:
        from curl_cffi import requests as cffi_req
        log.info(f"Fetching (curl_cffi): {url}")
        resp = cffi_req.get(url, impersonate="chrome", timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200 and "var shows" in resp.text or len(resp.text) > 5000:
            return resp.text
        log.debug(f"curl_cffi got status {resp.status_code}, trying requests...")
    except ImportError:
        log.debug("curl_cffi not installed, using requests")
    except Exception as e:
        log.debug(f"curl_cffi failed: {e}")

    # Strategy 2: plain requests
    if requests is None:
        log.error("requests library not installed. pip install requests")
        return None
    try:
        log.info(f"Fetching (requests): {url}")
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        log.error(f"Failed to fetch {url}: {e}")
        return None


def load_local(path: str) -> str | None:
    """Load HTML from a local file."""
    p = Path(path)
    if not p.exists():
        log.error(f"Local file not found: {path}")
        return None
    log.info(f"Loading local file: {path}")
    return p.read_text(encoding="utf-8")


# ─── Title / encoding fixes ───

def fix_double_encoded_utf8(text: str) -> str:
    """
    Fix double-encoded UTF-8 in HTML entity-decoded text.

    The Close-Up CMS stores UTF-8 bytes but serves them as Latin-1 character
    references. After HTML-entity decoding, we get mojibake like "Ã­" instead
    of "í". Re-encoding as Latin-1 and decoding as UTF-8 fixes this.

    Examples:
        "Cría cuervos"  (after entity decode of "Cr&Atilde;&shy;a cuervos")
        → re-encode latin-1 → b'Cr\xc3\xada cuervos'
        → decode utf-8 → "Cría cuervos"

        "8½" (after entity decode of "8&Acirc;&frac12;")
        → "8½"
    """
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        # Not double-encoded, return as-is
        return text


def clean_title(raw_title: str) -> str:
    """Decode HTML entities and fix double-encoded UTF-8."""
    title = unescape(raw_title)
    title = fix_double_encoded_utf8(title)
    return title.strip()


# ─── JSON extraction ───

def extract_shows_json(html: str) -> list[dict]:
    """
    Extract the shows JSON from the embedded <script> tag.

    The page contains:
        var shows ='[{...},{...}]';

    Note: it's a JSON string assigned to a JS variable, wrapped in single quotes.
    """
    # Match: var shows ='[...]';
    match = re.search(r"var\s+shows\s*=\s*'(\[.*?\])'\s*;", html, re.DOTALL)
    if not match:
        log.error("Could not find 'var shows' in the HTML")
        return []

    raw_json = match.group(1)

    try:
        shows = json.loads(raw_json)
        log.info(f"Extracted {len(shows)} show entries from JSON")
        return shows
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse shows JSON: {e}")
        return []


# ─── Detail page parsing ───

def parse_detail_page(html: str) -> dict:
    """
    Parse a Close-Up film detail page for director, year, and runtime.

    The detail pages have this HTML structure:

        <p>
          <strong><a href="...">Beau travail</a></strong><br/>
          <a href="...">Claire Denis</a>, 1999, 90 min
        </p>
        <p>"Denis and her near-constant collaborator..." — Harvard Film Archive</p>

    The metadata paragraph contains <strong> (the title), then after a <br/>
    comes "Director, YEAR, RUNTIME min". The description is in the next <p>.

    Returns dict with keys: director, year, runtime, description (any may be None).
    """
    if BeautifulSoup is None:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    info = {"director": None, "year": None, "runtime": None, "description": None}

    content = soup.find("div", id="film_program_support")
    if not content:
        return info

    paragraphs = content.find_all("p")

    # ─── Strategy 1: Find the <p> that contains <strong> + metadata ───
    # This is the canonical metadata paragraph. The text AFTER the <br/> tag
    # contains "Director, YEAR, RUNTIME min".
    for p in paragraphs:
        strong = p.find("strong")
        if not strong:
            continue

        # Get the text AFTER the <br/> tag to avoid including the title
        br = p.find("br")
        if br:
            # Collect text from all siblings after <br/>
            after_br_parts = []
            for sibling in br.next_siblings:
                text_part = sibling.string if hasattr(sibling, 'string') and sibling.string else str(sibling)
                if hasattr(sibling, 'get_text'):
                    text_part = sibling.get_text(" ", strip=True)
                after_br_parts.append(text_part.strip())
            after_br_text = " ".join(part for part in after_br_parts if part)
        else:
            # No <br/>, fall back to full paragraph text
            after_br_text = p.get_text(" ", strip=True)

        meta_match = re.search(
            r'([A-Z\u00C0-\u024F][^,]{1,50}),\s*((?:19|20)\d{2}),\s*(\d+)\s*min',
            after_br_text,
        )
        if meta_match:
            info["director"] = fix_double_encoded_utf8(
                unescape(meta_match.group(1).strip())
            )
            info["year"] = int(meta_match.group(2))
            info["runtime"] = int(meta_match.group(3))
            log.debug(
                f"  Metadata: {info['director']}, {info['year']}, {info['runtime']} min"
            )
            break

    # ─── Strategy 2: Scan all <p> elements for "YYYY, NNN min" ───
    # Fallback for pages with a different structure (e.g. compiled programmes)
    if info["year"] is None:
        for p in paragraphs:
            p_text = p.get_text(" ", strip=True)
            # Skip the header-like lines ("3 - 25 April 2026: ...")
            if re.match(r'^\d+\s*-\s*\d+\s+(January|February|March|April|May|June|'
                        r'July|August|September|October|November|December)', p_text):
                continue
            meta_match = re.search(
                r'([A-Z\u00C0-\u024F][^,]{1,50}),\s*((?:19|20)\d{2}),\s*(\d+)\s*min',
                p_text,
            )
            if meta_match:
                info["director"] = fix_double_encoded_utf8(
                    unescape(meta_match.group(1).strip())
                )
                info["year"] = int(meta_match.group(2))
                info["runtime"] = int(meta_match.group(3))
                break

    # ─── Extract description ───
    # Take the first substantial <p> that isn't the metadata line, an image,
    # or the "Screening as part of" footer.
    for p in paragraphs:
        # Skip paragraphs that are just images
        if p.find("img") and not p.get_text(strip=True).replace(p.find("img").get("alt", ""), "").strip():
            continue
        # Skip metadata paragraph (the one with <strong> title)
        if p.find("strong") and p.find("br"):
            continue
        p_text = p.get_text(" ", strip=True)
        # Skip short lines and the "Screening as part of" footer
        if len(p_text) > 60 and not p_text.startswith("Screening as part of"):
            info["description"] = p_text
            break

    return info


# ─── Season / programme detection ───

# Known programme/season names from the URL structure
PROGRAMME_TAGS = {
    "open-city-documentary-festival": "Open City Docs",
    "histoire-s-du-cinema": "Histoire(s) du cinéma",
    "close-up-on-abbas-kiarostami": "Kiarostami Season",
    "against-all-odds": "Against all Odds",
    "anthea-kennedy-and-ian-wiblin": "Berlin Trilogy",
}


def detect_programme(film_url: str) -> str | None:
    """Detect which programme/season a screening belongs to from its URL."""
    for slug, tag in PROGRAMME_TAGS.items():
        if slug in film_url:
            return tag
    return None


# ─── Main parsing ───

def parse_shows(shows: list[dict], fetch_details: bool = True) -> list[dict]:
    """
    Parse the raw shows JSON into the standard CinemaHub film format.

    Shows are grouped by fp_id (film programme ID). Multiple shows with the
    same fp_id are different showtimes of the same film.

    Each show entry:
    {
        "id": "58398",
        "fp_id": "4248",
        "title": "Clay + Lunar Visions II",
        "blink": "https://www.ticketsource.co.uk/...",
        "show_time": "2026-04-16 20:30:00",
        "status": "1",
        "booking_availability": "book",
        "film_url": "/film_programmes/2026/..."
    }
    """
    # Group shows by fp_id
    grouped = defaultdict(list)
    for show in shows:
        fp_id = show.get("fp_id", show.get("id", ""))
        grouped[fp_id].append(show)

    films = []
    detail_cache = {}  # film_url → detail_info

    for fp_id, show_group in grouped.items():
        # Pick the shortest title variant — avoids event suffixes like
        # "Olivia + Q&A with Sofía Petersen" when a plain "Olivia" show exists.
        titles = [clean_title(s.get("title", "").strip()) for s in show_group]
        titles = [t for t in titles if t]
        if not titles:
            continue
        title = min(titles, key=len)

        first = show_group[0]
        film_url_rel = first.get("film_url", "")
        film_url = f"{BASE_URL}{film_url_rel}" if film_url_rel else ""

        # Detect programme/season tag
        programme = detect_programme(film_url_rel)

        # ─── Optionally fetch detail page for metadata ───
        detail_info = {}
        if fetch_details and film_url and film_url not in detail_cache:
            time.sleep(REQUEST_DELAY)
            detail_html = fetch_page(film_url)
            if detail_html:
                detail_info = parse_detail_page(detail_html)
            detail_cache[film_url] = detail_info
        elif film_url in detail_cache:
            detail_info = detail_cache[film_url]

        runtime = detail_info.get("runtime")
        year = detail_info.get("year")
        director = detail_info.get("director")
        description = detail_info.get("description", "")

        # ─── Build showtimes ───
        showtimes = {}
        for show in show_group:
            show_time_str = show.get("show_time", "")
            if not show_time_str:
                continue

            try:
                dt = datetime.strptime(show_time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                log.warning(f"Could not parse show_time: {show_time_str}")
                continue

            date_str = dt.strftime("%Y-%m-%d")
            time_str = dt.strftime("%H:%M")

            booking_url = show.get("blink", "")
            can_book = show.get("booking_availability", "") == "book"

            session = {
                "time": time_str,
                "booking_url": booking_url,
                "screen": None,  # Close-Up is a single-screen cinema
                "hoh": False,
            }

            # Add tags
            session_tags = []
            if programme:
                session_tags.append(programme)
            if not can_book:
                session_tags.append("Sold Out")
            if session_tags:
                session["tags"] = session_tags

            if date_str not in showtimes:
                showtimes[date_str] = []
            showtimes[date_str].append(session)

        if not showtimes:
            log.warning(f"No showtimes for: {title} (fp_id {fp_id})")
            continue

        # Build slug
        slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')
        slug = f"{slug}-{fp_id}"

        film = {
            "id": slug,
            "title": title,
            "rating": "TBC",  # Close-Up doesn't display BBFC ratings
            "runtime": runtime,
            "genre": programme or "Repertory",
            "film_url": film_url,
            "poster_url": "",  # Could be scraped from detail page if needed
            "showtimes": showtimes,
        }

        if year:
            film["year"] = year
        if director:
            film["director"] = director
        if description:
            film["description"] = description

        films.append(film)

    log.info(f"Parsed {len(films)} films with showtimes")
    return films


# ─── Color assignment (same as other scrapers) ───

def assign_colors(films: list[dict]) -> None:
    """Assign colors to films. Genre colors first, then procedural hue rotation."""
    import colorsys

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


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Close-Up Film Centre timetable")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Output path for JSON file")
    parser.add_argument("--local", type=str, default=None,
                        help="Path to a local HTML file to parse instead of fetching live")
    parser.add_argument("--no-details", action="store_true",
                        help="Skip fetching detail pages (faster, no runtime/director/year)")
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (
        Path(__file__).parent.parent / "public" / "data" / "films_closeup.json"
    )

    log.info("=== Close-Up Film Centre Scraper Starting ===")

    # Step 1: Get the HTML
    if args.local:
        html = load_local(args.local)
    else:
        html = fetch_page(LISTINGS_URL)

    if not html:
        log.error("Could not load HTML. Aborting.")
        sys.exit(1)

    # Step 2: Extract the shows JSON from the embedded <script>
    shows = extract_shows_json(html)

    if not shows:
        log.error("No shows found in JSON. Aborting.")
        sys.exit(1)

    # Step 3: Parse shows into the standard film format
    fetch_details = not args.no_details and not args.local
    films = parse_shows(shows, fetch_details=fetch_details)

    if not films:
        log.error("No films with showtimes found. Aborting.")
        sys.exit(1)

    # Step 4: Assign colors
    assign_colors(films)

    # Step 5: Write output
    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "closeupfilmcentre.com",
        "films": films,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info("=== Scraper Complete ===")


if __name__ == "__main__":
    main()
