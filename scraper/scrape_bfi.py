#!/usr/bin/env python3
"""
BFI Southbank Timetable Scraper

Two-phase scraper for whatson.bfi.org.uk:
  1. Parse the A-Z overview page to discover all film permalinks
  2. Fetch each film's detail page (via Playwright, due to Cloudflare)
     and extract showtimes from the embedded `searchResults` JS array,
     plus metadata (director, cast, runtime, rating) from the HTML.

The BFI site uses AudienceView ticketing. Each detail page embeds a
`var articleContext = { searchResults: [...] }` JavaScript object where
each row is a performance (type "P") with date, time, screen, availability,
and booking URL — all positionally mapped via `searchNames`.

Usage:
    # Scrape live (requires Playwright: playwright install chromium)
    python scraper/scrape_bfi.py

    # Parse local HTML files (for development)
    python scraper/scrape_bfi.py --local-overview BFI_Overview.html --local-detail BFI_Ali.html

    # Custom output path
    python scraper/scrape_bfi.py -o my_output.json
"""

import json
import re
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from html import unescape
from urllib.parse import urljoin

from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://whatson.bfi.org.uk/Online/"
OVERVIEW_URL = (
    "https://whatson.bfi.org.uk/Online/default.asp"
    "?BOparam::WScontent::loadArticle::permalink=filmsindex"
    "&BOparam::WScontent::loadArticle::context_id="
)

# Polite delay between requests (seconds)
REQUEST_DELAY = 0.3

# Number of concurrent page fetches
CONCURRENT_WORKERS = 5

# ─── searchNames are parsed dynamically from each page ───
# We build an index map at runtime from the searchNames array.
# These are the field names we look for:
FIELD_ID = "id"
FIELD_OBJECT_TYPE = "object_type"
FIELD_DESCRIPTION = "description"
FIELD_SHORT_DESC = "short_description"
FIELD_TIME = "start_date_time"
FIELD_DATE_DAY = "start_date_date"
FIELD_DATE_MONTH = "start_date_month"
FIELD_DATE_YEAR = "start_date_year"
FIELD_SALES_STATUS = "sales_status"
FIELD_AVAIL_STATUS = "availability_status"
FIELD_KEYWORDS = "keywords"
FIELD_ADDITIONAL_INFO = "additional_info"
FIELD_VENUE_NAME = "venue_name"
FIELD_VENUE_DESC = "venue_description"
FIELD_VENUE_SHORT = "venue_short_description"
FIELD_SERIES = "series_name"
FIELD_MIN_PRICE = "min_price"
FIELD_MAX_PRICE = "max_price"

# Month offset: BFI uses 0-indexed months (Jan=0)
MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

# ─── Color palette ───
GENRE_COLORS = {
    "Drama":       {"color": "#6a1b9a", "accent": "#ba68c8"},
    "Comedy":      {"color": "#d81b60", "accent": "#ff6090"},
    "Horror":      {"color": "#546e7a", "accent": "#90a4ae"},
    "Action":      {"color": "#ef6c00", "accent": "#ffb74d"},
    "Documentary": {"color": "#c62828", "accent": "#ef5350"},
    "Thriller":    {"color": "#37474f", "accent": "#78909c"},
    "Family":      {"color": "#2e7d32", "accent": "#66bb6a"},
    "Animation":   {"color": "#e53935", "accent": "#ff6f60"},
    "Sci-Fi":      {"color": "#00838f", "accent": "#4dd0e1"},
    "Musical":     {"color": "#ad1457", "accent": "#f06292"},
    "Theatre":     {"color": "#1565c0", "accent": "#64b5f6"},
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


# ─── Phase 1: Parse the A-Z overview page ───

def extract_film_permalinks(html: str) -> list[dict]:
    """
    Extract film permalinks and titles from the A-Z overview page.

    The films are listed as <li><a href="article/ali-2026">Ali</a></li>
    inside a div.Rich-text <ul>.
    """
    soup = BeautifulSoup(html, "html.parser")
    films = []
    seen = set()

    # Find the Rich-text div that contains the A-Z list
    for rt_div in soup.select("div.Rich-text"):
        for li in rt_div.select("ul > li > a[href]"):
            href = li.get("href", "")
            title = li.get_text(strip=True)
            if not title or not href:
                continue

            # Extract permalink from href
            # Formats: "article/ali-2026" or full URL with permalink param
            if href.startswith("article/"):
                permalink = href.replace("article/", "")
            elif "permalink=" in href:
                m = re.search(r'permalink=([^&]+)', href)
                permalink = m.group(1) if m else None
            else:
                continue

            if not permalink or permalink in seen:
                continue
            seen.add(permalink)

            # Skip IMAX links (different venue)
            if "imax" in href.lower() and "imax" not in permalink.lower():
                continue

            films.append({
                "permalink": permalink,
                "title": unescape(title),
                "url": f"{BASE_URL}default.asp?BOparam::WScontent::loadArticle::permalink={permalink}"
                       f"&BOparam::WScontent::loadArticle::context_id=",
            })

    log.info(f"Found {len(films)} film permalinks on overview page")
    return films


# ─── Phase 2: Parse a film detail page ───

def safe_get(row: list, idx: int | None, default="") -> str:
    """Safely get a value from a searchResults row by index."""
    if idx is None or idx >= len(row):
        return default
    val = row[idx]
    if isinstance(val, str):
        return val
    return str(val) if val is not None else default


def build_field_map(html: str) -> dict[str, int]:
    """
    Extract the searchNames array from the page and build a field→index map.

    This makes the parser resilient to BFI reordering fields.
    """
    match = re.search(r'searchNames\s*:\s*\[([^\]]+)\]', html, re.DOTALL)
    if not match:
        return {}

    raw = match.group(1).strip()
    try:
        names = json.loads(f"[{raw}]")
        return {name: idx for idx, name in enumerate(names) if name}
    except json.JSONDecodeError:
        return {}


def extract_search_results(html: str) -> list[list]:
    """
    Extract the searchResults array from the articleContext JavaScript.

    Returns a list of rows, each row being a list of values.
    """
    # Find the searchResults array in the JS
    match = re.search(
        r'searchResults\s*:\s*\[\s*\n?(.*?)\s*\],\s*\n?\s*searchFilters',
        html,
        re.DOTALL,
    )
    if not match:
        return []

    raw = match.group(1).strip()
    if not raw:
        return []

    # The searchResults is an array of arrays. Wrap in [] and parse.
    try:
        rows = json.loads(f"[{raw}]")
        return rows
    except json.JSONDecodeError:
        # Try fixing common JS issues (trailing commas, etc.)
        cleaned = re.sub(r',\s*\]', ']', f"[{raw}]")
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            log.warning(f"Could not parse searchResults: {e}")
            return []


def parse_runtime_from_info(text: str) -> int | None:
    """Extract runtime in minutes from text like 'USA 2001. 159min'."""
    m = re.search(r'(\d+)\s*min', text)
    return int(m.group(1)) if m else None


def parse_detail_page(html: str, permalink: str, link_title: str) -> dict | None:
    """
    Parse a BFI film detail page for metadata and showtimes.

    Extracts:
    - Metadata from HTML: title, director, cast, runtime, rating, poster, synopsis
    - Showtimes from searchResults JS: date, time, screen, booking URL, availability
    """
    soup = BeautifulSoup(html, "html.parser")

    # ─── Metadata from HTML ───
    title_el = soup.select_one("h1.Page__heading")
    title = title_el.get_text(strip=True) if title_el else link_title

    desc_el = soup.select_one("p.Page__description")
    description = desc_el.get_text(strip=True) if desc_el else ""

    poster_el = soup.select_one("img.Media__image")
    poster_url = ""
    if poster_el:
        src = poster_el.get("src", "")
        if src:
            poster_url = urljoin("https://whatson.bfi.org.uk/Online/", src)

    # Film info fields
    director = None
    cast = None
    runtime = None
    rating = "TBC"
    format_tags = []

    for wrapper in soup.select("li.Film-info__information__wrapper"):
        heading_el = wrapper.select_one("p.Film-info__information__heading")
        value_el = wrapper.select_one("p.Film-info__information__value")
        if not value_el:
            continue

        value = value_el.get_text(strip=True)
        heading = heading_el.get_text(strip=True) if heading_el else ""

        if heading == "Director":
            director = value
        elif heading == "With":
            cast = value
        elif heading == "Certificate":
            rating = value
        elif not heading:
            # No heading — could be runtime or format
            rt = parse_runtime_from_info(value)
            if rt:
                runtime = rt
            elif value in ("35mm", "70mm", "16mm", "4K", "DCP", "Digital"):
                format_tags.append(value)

    # Synopsis from Rich-text
    synopsis = ""
    for rt_div in soup.select("div.Rich-text"):
        paras = rt_div.select("p")
        texts = [p.get_text(strip=True) for p in paras if p.get_text(strip=True) and not p.select("img")]
        if texts:
            synopsis = " ".join(texts)
            break

    # Season from breadcrumbs
    season = None
    breadcrumbs = soup.select("li.Breadcrumbs__item a.Breadcrumbs__link")
    if len(breadcrumbs) >= 2:
        season = breadcrumbs[-1].get_text(strip=True)

    # ─── Showtimes from searchResults ───
    fm = build_field_map(html)
    rows = extract_search_results(html)
    showtimes = {}

    for row in rows:
        obj_type = safe_get(row, fm.get(FIELD_OBJECT_TYPE))
        if obj_type != "P":
            continue  # Only performances

        # Build date string: month is 0-indexed in BFI
        month_idx_str = safe_get(row, fm.get(FIELD_DATE_MONTH))
        day_str = safe_get(row, fm.get(FIELD_DATE_DAY))
        year_str = safe_get(row, fm.get(FIELD_DATE_YEAR))
        time_str = safe_get(row, fm.get(FIELD_TIME))

        if not (month_idx_str and day_str and year_str and time_str):
            continue

        try:
            month_idx = int(month_idx_str)  # 0-indexed
            day = int(day_str)
            year = int(year_str)
            date_str = f"{year}-{month_idx + 1:02d}-{day:02d}"
        except (ValueError, IndexError):
            continue

        # Screen
        screen = safe_get(row, fm.get(FIELD_VENUE_SHORT)) or safe_get(row, fm.get(FIELD_VENUE_DESC))

        # Booking URL
        booking_path = safe_get(row, fm.get(FIELD_ADDITIONAL_INFO))
        booking_url = urljoin(BASE_URL, booking_path) if booking_path else ""

        # Availability
        avail = safe_get(row, fm.get(FIELD_AVAIL_STATUS))
        is_sold_out = avail == "X" or avail == "0"

        # Keywords as tags
        keywords_raw = safe_get(row, fm.get(FIELD_KEYWORDS))
        tags = [k.strip() for k in keywords_raw.split(",") if k.strip()] if keywords_raw else []
        if is_sold_out:
            tags.append("Sold Out")
        if season and season not in tags:
            tags.insert(0, season)

        session = {
            "time": time_str,
            "booking_url": booking_url,
            "screen": screen if screen else None,
            "hoh": False,
        }
        if tags:
            session["tags"] = tags

        if date_str not in showtimes:
            showtimes[date_str] = []
        showtimes[date_str].append(session)

    if not showtimes:
        log.warning(f"No showtimes found for: {title} ({permalink})")
        return None

    film_url = (
        f"{BASE_URL}default.asp?BOparam::WScontent::loadArticle::permalink={permalink}"
        f"&BOparam::WScontent::loadArticle::context_id="
    )

    return {
        "id": permalink,
        "title": title,
        "rating": rating,
        "runtime": runtime,
        "genre": "Other",
        "director": director,
        "cast": cast,
        "description": description or synopsis,
        "film_url": film_url,
        "poster_url": poster_url,
        "showtimes": showtimes,
    }


# ─── Page fetching ───

# Content markers that prove we got the real BFI page, not a Cloudflare challenge
_REAL_CONTENT_MARKERS = ("Page__heading", "Rich-text", "Film-info", "searchResults")


def _has_real_content(html: str) -> bool:
    return any(marker in html for marker in _REAL_CONTENT_MARKERS)


def _create_session():
    """Create a curl_cffi session with Chrome TLS impersonation."""
    try:
        from curl_cffi import requests as cffi_req
        session = cffi_req.Session(impersonate="chrome")
        return session
    except ImportError:
        return None


def fetch_page(url: str, session=None) -> str | None:
    """
    Fetch a BFI page using curl_cffi with Chrome TLS fingerprint impersonation.

    Cloudflare's primary bot detection checks the TLS fingerprint —
    curl_cffi impersonates a real Chrome browser at the TLS level,
    which is what makes it effective where requests and Playwright fail.
    """
    # Strategy 1: curl_cffi (best Cloudflare bypass)
    try:
        from curl_cffi import requests as cffi_req
        if session:
            resp = session.get(url, timeout=20)
        else:
            resp = cffi_req.get(url, impersonate="chrome", timeout=20)
        resp.raise_for_status()
        if _has_real_content(resp.text):
            return resp.text
        log.debug("curl_cffi got Cloudflare challenge page")
    except ImportError:
        log.warning("curl_cffi not installed — run: pip install curl_cffi")
    except Exception as e:
        log.debug(f"curl_cffi failed for {url}: {e}")

    # Strategy 2: plain requests (unlikely to work if curl_cffi didn't)
    try:
        import requests as req
        resp = req.get(url, headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
        }, timeout=20)
        resp.raise_for_status()
        if _has_real_content(resp.text):
            return resp.text
    except Exception:
        pass

    return None


def fetch_and_parse_all(film_list: list[dict]) -> list[dict]:
    """
    Fetch and parse all film detail pages concurrently.

    Uses ThreadPoolExecutor to fetch CONCURRENT_WORKERS pages at a time,
    each with its own curl_cffi session. ~200 films in ~1-2 minutes
    instead of 10+ minutes sequentially.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    total = len(film_list)
    log.info(f"Fetching {total} detail pages ({CONCURRENT_WORKERS} concurrent workers)...")

    results = []
    done_count = 0

    def _fetch_one(film: dict) -> dict | None:
        """Fetch and parse a single film (runs in a thread)."""
        session = _create_session()
        html = fetch_page(film["url"], session)
        if not html:
            return None
        time.sleep(REQUEST_DELAY)
        return parse_detail_page(html, film["permalink"], film["title"])

    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        future_to_film = {executor.submit(_fetch_one, f): f for f in film_list}

        for future in as_completed(future_to_film):
            done_count += 1
            film = future_to_film[future]
            try:
                detail = future.result()
                if detail and detail.get("showtimes"):
                    results.append(detail)
                    if done_count % 20 == 0 or done_count == total:
                        log.info(f"  Progress: {done_count}/{total} fetched, {len(results)} with showtimes")
            except Exception as e:
                log.warning(f"Error processing {film['title']}: {e}")

    log.info(f"Fetched {total} pages, {len(results)} films with showtimes")
    return results


# ─── Color assignment ───

def assign_colors(films: list[dict]) -> None:
    """Assign colors to films, avoiding duplicates."""
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


# ─── Main ───

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape BFI Southbank timetable")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Output path for JSON file")
    parser.add_argument("--local-overview", type=str, default=None,
                        help="Path to a local overview HTML file")
    parser.add_argument("--local-detail", type=str, nargs="*", default=None,
                        help="Path(s) to local detail HTML file(s) (for testing)")
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (
        Path(__file__).parent.parent / "public" / "data" / "films_bfi.json"
    )

    log.info("=== BFI Southbank Scraper Starting ===")

    films = []

    if args.local_detail:
        # ─── Local detail mode: parse provided HTML files directly ───
        for path_str in args.local_detail:
            p = Path(path_str)
            if not p.exists():
                log.warning(f"File not found: {path_str}")
                continue
            log.info(f"Parsing local detail: {p.name}")
            html = p.read_text(encoding="utf-8")
            permalink = p.stem  # use filename as permalink
            detail = parse_detail_page(html, permalink, permalink)
            if detail and detail["showtimes"]:
                films.append(detail)

    elif args.local_overview:
        # ─── Local overview + live detail pages ───
        p = Path(args.local_overview)
        if not p.exists():
            log.error(f"Overview file not found: {args.local_overview}")
            sys.exit(1)

        log.info(f"Parsing local overview: {p.name}")
        overview_html = p.read_text(encoding="utf-8")
        film_list = extract_film_permalinks(overview_html)

        if not film_list:
            log.error("No films found in overview. Aborting.")
            sys.exit(1)

        films = fetch_and_parse_all(film_list)

    else:
        # ─── Live mode: fetch overview then all detail pages ───
        log.info("Fetching overview page...")
        session = _create_session()
        overview_html = fetch_page(OVERVIEW_URL, session)

        if not overview_html:
            log.error("Could not fetch overview page (Cloudflare blocked all attempts).")
            log.error("Install curl_cffi for best results: pip install curl_cffi")
            sys.exit(1)

        film_list = extract_film_permalinks(overview_html)

        if not film_list:
            log.error("No films found in overview. Aborting.")
            sys.exit(1)

        films = fetch_and_parse_all(film_list)

    log.info(f"Parsed {len(films)} films with showtimes")

    if not films:
        log.error("No films with showtimes found. Aborting.")
        sys.exit(1)

    # Assign colors
    assign_colors(films)

    # Write output
    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "whatson.bfi.org.uk",
        "films": films,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info("=== Scraper Complete ===")


if __name__ == "__main__":
    main()
