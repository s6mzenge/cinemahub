#!/usr/bin/env python3
"""
Genesis Cinema (Whitechapel) Timetable Scraper

Scrapes genesiscinema.co.uk for current films, showtimes, and booking links.

Genesis uses an Admit One backend with server-rendered HTML panels grouped
by date. Each panel contains film blocks with titles, ratings, runtimes,
descriptions, and showtime buttons linking to the booking system.

Outputs a JSON file compatible with the CinemaHub frontend.

Usage:
    python scraper/scrape_genesis.py
    python scraper/scrape_genesis.py --local genesis_raw.html
    python scraper/scrape_genesis.py -o my_output.json
"""

import json, re, sys, logging
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    requests = None

from colors import assign_colors
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.genesiscinema.co.uk"
LISTINGS_URL = BASE_URL  # The "What's On" is the homepage
REQUEST_TIMEOUT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}

VALID_BBFC = {"U", "PG", "12A", "12", "15", "18", "R18"}

# Non-film events to skip
NON_FILM_SKIP_PATTERNS = [
    r"^emporium pro wrestling",
    r"^poetry slam$",
    r"^some like it swing$",
    r"^slept on comedy",
    r"^bruce lee u\.?k\.? event",
    r"^queen mary film festival$",
    r"positioning your film",
    r"mini-market",
    r"closing party",
    r"^industry panel",
    r"^buff awards",
    r"^awards ceremony",
]

# Event-type prefixes to strip from titles
EVENT_SUFFIXES = [
    r"\s*-\s*LIFF\s*$",
    r"\s*-\s*BUFF\s*$",
    r"\s*-\s*London Bengali Film Festival\s*$",
    r"\s*-\s*Queer East Festival\s*$",
    r"\s*-\s*Queer East\s*$",
    r"\s*-\s*Seasonal Affective Cinema\s*$",
    r"\s*-\s*Mark Jenkin Retrospective\s*$",
    r"\s*Presented by Bloody Mary Film Club\s*$",
    r"\s*[-\u2013\u2014]\s*Films that F[u\W]*ck\b.*$",
    r"\s*\+\s*Q\s*&\s*A\s*$",
    r"\s*\+\s*Q&A\s*$",
    r"\s*\+\s*Directors?\s+Q\s*&\s*A\s*$",
    r"\s*\+\s*Intro(?:\s+.*)?$",
    r"\s*\(preview cast & crew screening\)\s*$",
    r"\s*\(By Invitation Only\)\s*$",
    r"\s*\(Gala Screening\)\s*$",
    r"\s*\(Closing Night Gala Film\)\s*$",
    r"\s*\(40th Anniversary\)\s*$",
    r"\s*\(Live Script Reading\)\s*$",
]

def clean_genesis_title(raw_title: str) -> str:
    """Clean Genesis Cinema event titles to extract the core film title."""
    t = unescape(raw_title).strip()
    # Remove zero-width / invisible Unicode characters
    t = re.sub(r"[\u200b\u200c\u200d\u2060\ufeff]", "", t)
    # Normalize Unicode quotes/apostrophes to ASCII
    t = t.replace("\u2019", "'").replace("\u2018", "'")
    t = t.replace("\u201c", '"').replace("\u201d", '"')
    t = re.sub(r"\s+", " ", t).strip()

    # Strip event suffixes
    changed = True
    while changed:
        changed = False
        for pattern in EVENT_SUFFIXES:
            new_t = re.sub(pattern, "", t, flags=re.IGNORECASE).strip()
            if new_t != t:
                t = new_t
                changed = True

    # Strip year parenthetical
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t).strip()

    return t


def make_film_id(title: str) -> str:
    slug = title.lower()
    slug = re.sub(r"[''']", "", slug)
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


def parse_rating_from_alt(alt_text: str) -> str:
    """Parse BBFC rating from img alt text like '15', 'PG', '12A', 'TBC'."""
    if not alt_text:
        return "TBC"
    alt = alt_text.strip().upper()
    if alt in VALID_BBFC:
        return alt
    return "TBC"


def parse_runtime(text: str) -> int | None:
    """Extract runtime in minutes from text like 'Running time: 125 mins'."""
    m = re.search(r"(\d+)\s*min", text, re.IGNORECASE)
    return int(m.group(1)) if m else None


def parse_date_from_panel_id(panel_id: str) -> str | None:
    """Convert panel ID like 'panel_20260417' to ISO date '2026-04-17'."""
    m = re.search(r"panel_(\d{4})(\d{2})(\d{2})", panel_id)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def parse_listings(html: str) -> list[dict]:
    """Parse the Genesis Cinema homepage HTML into CinemaHub film format."""
    soup = BeautifulSoup(html, "html.parser")

    # Find all date panels
    panels = soup.find_all("div", class_="whatson_panel")
    if not panels:
        log.error("No whatson_panel divs found in HTML")
        return []

    log.info(f"Found {len(panels)} date panels")

    # Accumulate showtimes per film (keyed by event URL/ID)
    films_map = {}  # event_id → film dict (with showtimes being accumulated)

    for panel in panels:
        panel_id = panel.get("id", "")
        date_str = parse_date_from_panel_id(panel_id)
        if not date_str:
            continue

        # Find all film blocks within this panel
        # Each film is in a div with class "grid-container-border"
        film_blocks = panel.find_all("div", class_="grid-container-border")

        for block in film_blocks:
            # Title
            h2 = block.find("h2")
            if not h2:
                continue
            title_link = h2.find("a")
            raw_title = h2.get_text(strip=True)
            if not raw_title:
                continue

            # Event URL → used as unique key
            event_href = title_link.get("href", "") if title_link else ""
            event_id_match = re.search(r"event/(\d+)", event_href)
            event_id = event_id_match.group(1) if event_id_match else make_film_id(raw_title)

            film_url = ""
            if event_href:
                film_url = urljoin(BASE_URL + "/", event_href)

            # Rating — from the rating img alt
            rating_img = block.find("img", class_="object-scale-down")
            rating = "TBC"
            if rating_img:
                rating = parse_rating_from_alt(rating_img.get("alt", ""))

            # Runtime
            runtime = None
            runtime_p = block.find("p", string=re.compile(r"Running time", re.IGNORECASE))
            if not runtime_p:
                # Try finding span with runtime
                for p in block.find_all("p"):
                    if "Running time" in p.get_text():
                        runtime_p = p
                        break
            if runtime_p:
                runtime = parse_runtime(runtime_p.get_text())

            # Poster
            poster_img = block.find("img", class_="object-contain")
            poster_url = ""
            if poster_img:
                src = poster_img.get("src", "")
                if src:
                    poster_url = urljoin(BASE_URL + "/", src)

            # Description — find the longer paragraph text
            description = None
            for p in block.find_all("p", class_="text-black"):
                text = p.get_text(strip=True)
                if len(text) > 80 and "Running time" not in text:
                    description = text[:500]  # Cap at 500 chars
                    break

            # Clean the title
            clean_title = clean_genesis_title(raw_title)

            # Skip non-film events
            skip = False
            for pattern in NON_FILM_SKIP_PATTERNS:
                if re.search(pattern, clean_title, re.IGNORECASE):
                    log.info(f"  Skipping non-film: {raw_title}")
                    skip = True
                    break
            if skip:
                continue

            # Showtimes for this date
            sessions = []

            # Available showtimes: <a> with class "perfButton" and href containing perfCode
            for perf_link in block.find_all("a", class_="perfButton"):
                href = perf_link.get("href", "")
                if "perfCode" not in href:
                    continue
                # Extract time from the span inside
                time_span = perf_link.find("span", class_="rounded-xl")
                if not time_span:
                    # Try any span with a time pattern
                    for s in perf_link.find_all("span"):
                        if re.match(r"\s*\d{1,2}:\d{2}\s*", s.get_text()):
                            time_span = s
                            break
                if not time_span:
                    continue
                time_str = time_span.get_text(strip=True).lower()
                if not re.match(r"\d{1,2}:\d{2}", time_str):
                    continue

                booking_url = href if href.startswith("http") else urljoin(BASE_URL + "/", href)

                session = {
                    "time": time_str,
                    "booking_url": booking_url,
                    "screen": None,
                    "hoh": False,
                }
                sessions.append(session)

            # Sold-out showtimes: <span> with class "soldOutPerformance"
            for sold_span in block.find_all("span", class_="soldOutPerformance"):
                time_str = sold_span.get_text(strip=True).lower()
                if not re.match(r"\d{1,2}:\d{2}", time_str):
                    continue
                session = {
                    "time": time_str,
                    "booking_url": "",
                    "screen": None,
                    "hoh": False,
                    "sold_out": True,
                }
                sessions.append(session)

            if not sessions:
                continue

            # Deduplicate desktop/mobile copies (same time + same perfCode)
            seen_keys = set()
            deduped = []
            for s in sessions:
                key = (s["time"], s.get("booking_url", ""))
                if key not in seen_keys:
                    seen_keys.add(key)
                    deduped.append(s)
            sessions = deduped

            # Create or update film entry
            if event_id not in films_map:
                films_map[event_id] = {
                    "id": make_film_id(clean_title),
                    "title": clean_title,
                    "rating": rating,
                    "runtime": runtime,
                    "genre": "Other",
                    "year": None,
                    "director": None,
                    "cast": None,
                    "country": None,
                    "description": description,
                    "film_url": film_url,
                    "poster_url": poster_url,
                    "showtimes": {},
                }

            films_map[event_id]["showtimes"][date_str] = sessions

    films = list(films_map.values())

    # Remove films with no showtimes
    films = [f for f in films if f["showtimes"]]

    for film in films:
        perf_count = sum(len(v) for v in film["showtimes"].values())
        log.info(f"  ✓ {film['title']} — {len(film['showtimes'])} date(s), "
                 f"{perf_count} perf(s), {film['rating']}, {film['runtime']}min")

    log.info(f"Parsed {len(films)} films with showtimes")
    return films


# ─── Color assignment (golden-ratio hue stepping) ────────────────────


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Genesis Cinema timetable")
    parser.add_argument("-o","--output",type=str,default=None)
    parser.add_argument("--local",type=str,default=None)
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (
        Path(__file__).resolve().parent.parent / "public" / "data" / "films_genesis.json"
    )

    log.info("=== Genesis Cinema Scraper Starting ===")

    if args.local:
        html = Path(args.local).read_text(encoding="utf-8")
    else:
        if requests is None:
            log.error("requests not installed"); sys.exit(1)
        log.info(f"Fetching: {LISTINGS_URL}")
        try:
            resp = requests.get(LISTINGS_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            log.error(f"Failed to fetch: {e}"); sys.exit(1)

    films = parse_listings(html)
    if not films:
        log.error("No films found. Aborting."); sys.exit(1)

    assign_colors(films)

    output = {"scraped_at": datetime.now(timezone.utc).isoformat(), "source": "genesiscinema.co.uk", "films": films}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info("=== Scraper Complete ===")

if __name__ == "__main__":
    main()
