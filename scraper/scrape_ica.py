#!/usr/bin/env python3
"""
ICA (Institute of Contemporary Arts) Cinema Scraper

Scrapes ica.art/upcoming for current film screenings, showtimes, and metadata.

NOTE: The ICA server returns HTTP 404 for /upcoming but still serves the full
listings page in the response body. We deliberately ignore the status code.

Step 1: Fetch /upcoming, parse all film showtimes (single request).
Step 2: Fetch all detail pages concurrently (async aiohttp) for metadata
        enrichment (director, year, runtime, rating, screen, booking URL).

Usage:
    python scraper/scrape_ica.py
    python scraper/scrape_ica.py --local saved_page.html
    python scraper/scrape_ica.py --no-details
    python scraper/scrape_ica.py -o my_output.json
"""

import asyncio
import json
import re
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path
from collections import OrderedDict

from bs4 import BeautifulSoup, NavigableString

try:
    import requests
except ImportError:
    requests = None

try:
    import aiohttp
except ImportError:
    aiohttp = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.ica.art"
LISTINGS_URL = f"{BASE_URL}/upcoming"

CONCURRENCY = 5
REQUEST_TIMEOUT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.5",
}

TITLE_PREFIXES = [
    "LONDON PREMIERE",
    "UK PREMIERE",
    "WORLD PREMIERE",
    "EUROPEAN PREMIERE",
    "PREVIEW",
]

# ICA event/strand prefixes that appear before a colon in the title div.
# These are stripped to expose the actual film title underneath.
ICA_EVENT_PREFIXES = [
    "closing night",
    "opening night",
    "jukebox film club",
    "sürreal sinema",
    "surreal sinema",
    "the cinema of olaa zhyzhko",
    "the cinema of",
    "in focus",
    "three films by penny allen",
    "three films by",
]

# Prefixes without colon that appear at the start of titles
ICA_NO_COLON_PREFIXES = [
    r"(?i)^Opening\s+Night\s+",
    r"(?i)^Closing\s+Night\s+",
]

# ─── Color palette ───

GENRE_COLORS = {
    "Documentary":  {"color": "#c62828", "accent": "#ef5350"},
    "Drama":        {"color": "#6a1b9a", "accent": "#ba68c8"},
    "Comedy":       {"color": "#d81b60", "accent": "#ff6090"},
    "Animation":    {"color": "#e53935", "accent": "#ff6f60"},
    "Horror":       {"color": "#546e7a", "accent": "#90a4ae"},
    "Thriller":     {"color": "#37474f", "accent": "#78909c"},
    "Sci-Fi":       {"color": "#00838f", "accent": "#4dd0e1"},
    "Action":       {"color": "#ef6c00", "accent": "#ffb74d"},
    "Family":       {"color": "#2e7d32", "accent": "#66bb6a"},
    "Musical":      {"color": "#ad1457", "accent": "#f06292"},
    "Romance":      {"color": "#00897b", "accent": "#4db6ac"},
    "Crime":        {"color": "#4e342e", "accent": "#8d6e63"},
    "Fantasy":      {"color": "#7c4dff", "accent": "#b388ff"},
    "Experimental": {"color": "#ef6c00", "accent": "#ffb74d"},
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


# ─── Fetching ───

def fetch_page(url: str, allow_404: bool = False) -> str | None:
    """Fetch a page synchronously (used for the single /upcoming request)."""
    if requests is None:
        log.error("requests library not installed.")
        return None
    try:
        log.info(f"Fetching: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 404 and allow_404 and len(resp.text) > 1000:
            log.info(f"  Got 404 but body has {len(resp.text)} bytes of content — using it")
            return resp.text
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        log.error(f"Failed to fetch {url}: {e}")
        return None


def load_local(path: str) -> str | None:
    p = Path(path)
    if not p.exists():
        log.error(f"Local file not found: {path}")
        return None
    log.info(f"Loading local file: {path}")
    return p.read_text(encoding="utf-8")


# ─── Parsers ───

def parse_docket_date(text: str) -> str | None:
    """Parse 'Thursday, 16 April' → '2026-04-16'."""
    text = text.strip()
    now = datetime.now()
    for fmt in ["%A, %d %B", "%d %B"]:
        try:
            parsed = datetime.strptime(text, fmt)
            candidate = parsed.replace(year=now.year)
            if (now - candidate).days > 60:
                candidate = parsed.replace(year=now.year + 1)
            return candidate.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_time_to_24h(time_text: str) -> str | None:
    """Convert '12:00 PM' → '12:00', '8:40 PM' → '20:40'."""
    time_text = time_text.strip()
    for fmt in ["%I:%M %p", "%I:%M%p", "%I.%M %p"]:
        try:
            return datetime.strptime(time_text, fmt).strftime("%H:%M")
        except ValueError:
            continue
    return None


def clean_title(raw_title: str) -> tuple[str, list[str]]:
    """
    Strip ICA event prefixes, premiere labels, and other cruft.
    Returns (clean_title, tags).

    Processing order:
      1. Strip colon-separated event prefixes ("Jukebox Film Club: ...")
      2. Strip non-colon prefixes ("Opening Night ...")
      3. Strip premiere prefixes ("UK PREMIERE ...")
    """
    title = raw_title.strip()
    tags = []

    # Step 1: Strip ICA event prefixes before a colon
    if ":" in title:
        before_colon = title.split(":", 1)[0].strip()
        before_lower = before_colon.lower()
        for prefix in ICA_EVENT_PREFIXES:
            if before_lower == prefix or before_lower.startswith(prefix):
                title = title.split(":", 1)[1].strip()
                break

    # Step 2: Strip non-colon prefixes
    for pattern in ICA_NO_COLON_PREFIXES:
        title = re.sub(pattern, "", title).strip()

    # Step 3: Strip premiere prefixes
    for prefix in TITLE_PREFIXES:
        pattern = re.compile(r"^\s*" + re.escape(prefix) + r"\s*", re.IGNORECASE)
        if pattern.match(title):
            title = pattern.sub("", title).strip()
            tags.append(prefix.title())

    return re.sub(r"\s+", " ", title).strip(), tags


def parse_colophon(colophon_text: str) -> dict:
    """Parse 'dir. Ozon, France 2025, 123 mins, 15' → metadata dict."""
    meta = {"director": None, "year": None, "runtime": None, "rating": "TBC", "country": None}
    if not colophon_text:
        return meta
    # Normalize non-breaking spaces and whitespace
    colophon_text = re.sub(r"[\s\xa0]+", " ", colophon_text)
    for part in [p.strip() for p in colophon_text.split(",")]:
        if re.match(r"dirs?\.?\s*[A-Z]", part, re.IGNORECASE):
            meta["director"] = re.sub(r"^dirs?\.?\s*", "", part, flags=re.IGNORECASE).strip()
        elif re.search(r"(\d+)\s*mins?", part, re.IGNORECASE):
            meta["runtime"] = int(re.search(r"(\d+)\s*mins?", part, re.IGNORECASE).group(1))
        elif re.search(r"\b((?:19|20)\d{2})\b", part):
            m = re.search(r"\b((?:19|20)\d{2})\b", part)
            meta["year"] = m.group(1)
            country = part[:m.start()].strip().rstrip("/").strip()
            if country and not country.lower().startswith("dir"):
                meta["country"] = country
        elif re.match(r"^(U|PG|12A?|15|18|R18|TBC)$", part.strip()):
            meta["rating"] = part.strip()
    return meta


# ─── Listings page parsing ───

def extract_films_from_listings(html: str) -> list[dict]:
    """Parse the /upcoming page for all film items, grouped by slug."""
    soup = BeautifulSoup(html, "html.parser")

    ladder = soup.select_one("#ladder > div")
    if not ladder:
        log.error("Could not find #ladder > div in HTML")
        return []

    current_date = None
    films_by_slug = OrderedDict()

    for element in ladder.children:
        if isinstance(element, NavigableString):
            continue

        # Date heading
        date_el = element.select_one("div.docket-date")
        if date_el:
            parsed = parse_docket_date(date_el.get_text(strip=True))
            if parsed:
                current_date = parsed
            continue

        # Only film items
        classes = element.get("class", [])
        if "item" not in classes or "films" not in classes:
            continue
        if not current_date:
            continue

        # Film link
        link = element.select_one("a[href^='/films/']")
        if not link:
            continue
        film_url_path = link.get("href", "")
        slug = film_url_path.strip("/").split("/")[-1] if film_url_path else None
        if not slug:
            continue

        # Title
        title_container = element.select_one("div.title-container")
        raw_title = ""
        season = None
        if title_container:
            season_el = title_container.select_one("div.title.season-item")
            if season_el:
                for br in season_el.find_all("br"):
                    br.replace_with(" ")
                season = re.sub(r"\s+", " ", season_el.get_text(separator=" ")).strip()
            for td in title_container.select("div.title"):
                if "season-item" in td.get("class", []):
                    continue
                for br in td.find_all("br"):
                    br.replace_with(" ")
                raw_title = td.get_text(separator=" ").strip()
                raw_title = re.sub(r"\s+", " ", raw_title)
                break

        if not raw_title:
            continue
        title, tags = clean_title(raw_title)
        if not title:
            continue

        # Times
        times = []
        for slot in element.select("div.time-slot"):
            t = parse_time_to_24h(slot.get_text(strip=True))
            if t:
                times.append(t)
        if not times:
            continue

        # Description
        desc_el = element.select_one("div.description")
        description = desc_el.get_text(strip=True) if desc_el else ""

        # Poster
        poster_url = ""
        img_el = element.select_one("img[src]")
        if img_el:
            src = img_el.get("src", "")
            poster_url = ("https:" + src) if src.startswith("//") else (BASE_URL + src if src.startswith("/") else src)

        # Create or update film entry
        if slug not in films_by_slug:
            films_by_slug[slug] = {
                "id": slug,
                "title": title,
                "rating": "TBC",
                "runtime": None,
                "genre": "Other",
                "year": None,
                "country": None,
                "director": None,
                "description": description,
                "season": season,
                "film_url": BASE_URL + film_url_path,
                "poster_url": poster_url,
                "showtimes": {},
                "_tags": list(tags),
            }
        else:
            film = films_by_slug[slug]
            if not film["description"] and description:
                film["description"] = description
            if not film["season"] and season:
                film["season"] = season
            for t in tags:
                if t not in film["_tags"]:
                    film["_tags"].append(t)

        # Add showtimes
        film = films_by_slug[slug]
        if current_date not in film["showtimes"]:
            film["showtimes"][current_date] = []

        for t in times:
            session = {
                "time": t,
                "booking_url": BASE_URL + film_url_path,
                "screen": None,
                "hoh": False,
            }
            all_tags = []
            if film["season"]:
                all_tags.append(film["season"])
            all_tags.extend(film["_tags"])
            if all_tags:
                session["tags"] = list(all_tags)
            film["showtimes"][current_date].append(session)

    log.info(f"Found {len(films_by_slug)} unique films from listings page")
    return list(films_by_slug.values())


# ─── Detail page enrichment (sync helper) ───

def enrich_from_detail(film: dict, html: str) -> None:
    """Enrich a film dict in-place with metadata from its detail page."""
    soup = BeautifulSoup(html, "html.parser")

    # Colophon
    colophon_el = soup.select_one("#colophon")
    if colophon_el:
        text = colophon_el.get_text(separator=" ")
        text = re.sub(r"[\s\xa0]+", " ", text).strip()
        italic = colophon_el.select_one("i")
        if italic:
            italic_text = re.sub(r"[\s\xa0]+", " ", italic.get_text(separator=" ")).strip()
            text = text[len(italic_text):].lstrip(",").strip()
        meta = parse_colophon(text)
        if meta["director"]:
            film["director"] = meta["director"]
        if meta["year"]:
            film["year"] = meta["year"]
        if meta["runtime"]:
            film["runtime"] = meta["runtime"]
        if meta["rating"] != "TBC":
            film["rating"] = meta["rating"]
        if meta["country"]:
            film["country"] = meta["country"]

    # Booking URL
    book_el = soup.select_one("div[onclick*='/book/']")
    if book_el:
        m = re.search(r'location\.href="([^"]+)"', book_el.get("onclick", ""))
        if m:
            booking_url = BASE_URL + m.group(1)
            for sessions in film["showtimes"].values():
                for s in sessions:
                    s["booking_url"] = booking_url

    # Screen info from performance list
    perf_lookup = {}
    for perf in soup.select("div.performance"):
        date_el = perf.select_one("div.date")
        venue_el = perf.select_one("div.venue")
        time_el = perf.select_one("div.time")
        if not (date_el and time_el):
            continue
        try:
            dt = datetime.strptime(date_el.get_text(strip=True), "%a, %d %b %Y")
            date_str = dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
        t24 = parse_time_to_24h(time_el.get_text(strip=True))
        if t24 and venue_el:
            perf_lookup[(date_str, t24)] = venue_el.get_text(strip=True)

    if perf_lookup:
        for date_str, sessions in film["showtimes"].items():
            for s in sessions:
                screen = perf_lookup.get((date_str, s["time"]))
                if screen:
                    s["screen"] = screen


# ─── Async detail page fetching ───

async def fetch_detail_async(session: "aiohttp.ClientSession", sem: asyncio.Semaphore,
                              film: dict) -> None:
    """Fetch a single detail page and enrich the film dict."""
    url = film["film_url"]
    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                html = await resp.text()
                enrich_from_detail(film, html)
        except Exception as e:
            log.warning(f"  Failed to fetch {url}: {e}")


async def enrich_all_async(films: list[dict]) -> None:
    """Fetch all detail pages concurrently and enrich films."""
    if aiohttp is None:
        log.error("aiohttp not installed — falling back to sequential fetching")
        enrich_all_sync(films)
        return

    log.info(f"Fetching {len(films)} detail pages (concurrency={CONCURRENCY})...")
    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = [fetch_detail_async(session, sem, film) for film in films]
        await asyncio.gather(*tasks)

    enriched = sum(1 for f in films if f.get("director"))
    log.info(f"  Enriched {enriched}/{len(films)} films with metadata")


def enrich_all_sync(films: list[dict]) -> None:
    """Fallback: sequential fetching with requests."""
    import time
    log.info(f"Fetching {len(films)} detail pages sequentially...")
    for i, film in enumerate(films):
        if i > 0:
            time.sleep(0.5)
        html = fetch_page(film["film_url"])
        if html:
            enrich_from_detail(film, html)
    enriched = sum(1 for f in films if f.get("director"))
    log.info(f"  Enriched {enriched}/{len(films)} films with metadata")


# ─── Color assignment ───

def assign_colors(films: list[dict]) -> None:
    import colorsys

    def hsl_to_hex(h, s, l):
        r, g, b = colorsys.hls_to_rgb(h, l, s)
        return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"

    def generate_color(index):
        golden = 0.618033988749895
        hue = (index * golden) % 1.0
        if index % 3 == 0:     sat, lit, asat, alit = 0.65, 0.38, 0.55, 0.62
        elif index % 3 == 1:   sat, lit, asat, alit = 0.55, 0.42, 0.50, 0.68
        else:                  sat, lit, asat, alit = 0.70, 0.35, 0.60, 0.58
        return {"color": hsl_to_hex(hue, sat, lit), "accent": hsl_to_hex(hue, asat, alit)}

    used, pidx, gidx = set(), 0, 0
    for film in films:
        colors = GENRE_COLORS.get(film.get("genre", "Other"))
        if colors and colors["color"] not in used:
            film["color"], film["accent"] = colors["color"], colors["accent"]
            used.add(colors["color"])
        else:
            assigned = False
            while pidx < len(EXTRA_PALETTES):
                c = EXTRA_PALETTES[pidx]; pidx += 1
                if c["color"] not in used:
                    film["color"], film["accent"] = c["color"], c["accent"]
                    used.add(c["color"]); assigned = True; break
            if not assigned:
                while True:
                    c = generate_color(gidx); gidx += 1
                    if c["color"] not in used:
                        film["color"], film["accent"] = c["color"], c["accent"]
                        used.add(c["color"]); break


# ─── Main ───

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape ICA cinema timetable")
    parser.add_argument("-o", "--output", type=str, default=None)
    parser.add_argument("--local", type=str, default=None,
                        help="Path to a local /upcoming HTML file")
    parser.add_argument("--no-details", action="store_true",
                        help="Skip detail page fetches (faster, less metadata)")
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (
        Path(__file__).parent.parent / "public" / "data" / "films_ica.json"
    )

    log.info("=== ICA Cinema Scraper Starting ===")

    # Step 1: Get the /upcoming listings page
    if args.local:
        html = load_local(args.local)
    else:
        html = fetch_page(LISTINGS_URL, allow_404=True)

    if not html:
        log.error("Could not load listings page. Aborting.")
        sys.exit(1)

    # Step 2: Parse all films + showtimes
    films = extract_films_from_listings(html)
    if not films:
        log.error("No films found. Aborting.")
        sys.exit(1)

    # Step 3: Optionally enrich from detail pages
    if not args.no_details and not args.local:
        asyncio.run(enrich_all_async(films))

    # Clean up internal fields
    for film in films:
        film.pop("season", None)
        film.pop("_tags", None)

    # Step 4: Assign colors
    assign_colors(films)

    # Step 5: Write output
    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "ica.art",
        "films": films,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info("=== Scraper Complete ===")


if __name__ == "__main__":
    main()
