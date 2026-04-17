#!/usr/bin/env python3
"""
Phoenix Cinema (East Finchley) Timetable Scraper

Scrapes phoenixcinema.co.uk for current films, showtimes, and booking links.

The Phoenix uses the same Savoy Systems backend as Rio Cinema, embedding all
event data as a JSON object in a <script> tag on the What's On page. This
scraper extracts that JSON directly — no detail page requests or JavaScript
rendering needed.

Outputs a JSON file compatible with the CinemaHub frontend.

Usage:
    python scraper/scrape_phoenix.py
    python scraper/scrape_phoenix.py --local phoenix_raw.html
    python scraper/scrape_phoenix.py -o my_output.json
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.phoenixcinema.co.uk"
LISTINGS_URL = f"{BASE_URL}/PhoenixCinemaLondon.dll/WhatsOn"
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

PHOENIX_EVENT_PREFIXES = sorted([
    "japanese film club", "phoenix classics",
    "parents and baby screening", "parent and baby screening",
    "parent & baby screening", "watch with baby",
], key=len, reverse=True)

TRAILING_SUFFIXES = [
    r"\s*\+\s*Q\s*&\s*A\s*$", r"\s*\+\s*Q\s*&amp;\s*A\s*$",
    r"\s*\+\s*Panel\s*$", r"\s*\+\s*Intro(?:\s+.*)?$",
    r"\s*\+\s*Discussion\s*$", r"\s*\+\s*Live\s+Score(?:\s+.*)?$",
    r"\s+UK\s+Premiere\s*$", r"\s+Premiere\s*$",
    r"\s+on\s+35\s*mm\s*$", r"\s*\(35\s*mm\s*\)\s*$", r"\s+35\s*mm\s*$",
    r"\s*\(4K\s+Restoration\)\s*$", r"\s+4K\s+Restoration\s*$",
    r"\s*\(\d+(?:st|nd|rd|th)\s+Anniversary\)\s*$",
    r"\s+\d+(?:st|nd|rd|th)\s+Anniversary\s*$",
    r"\s+with\s+Live\s+Score\s+by\s+.*$",
    r"\s+live\s+concert\s*$",
]

NON_FILM_SKIP_PATTERNS = [
    r"^let us take you to",
    r"live\s+concert$",
]


def clean_phoenix_title(raw_title: str) -> str:
    t = unescape(raw_title).strip()
    t = re.sub(r"[\xa0\u00a0]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    prefix_changed = True
    while prefix_changed and ":" in t:
        prefix_changed = False
        for prefix in PHOENIX_EVENT_PREFIXES:
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

    changed = True
    while changed:
        changed = False
        for pattern in TRAILING_SUFFIXES:
            new_t = re.sub(pattern, "", t, flags=re.IGNORECASE).strip()
            if new_t != t:
                t = new_t
                changed = True

    t = re.sub(r"\s*\(\d{4}\)\s*$", "", t).strip()
    if t == t.upper() and len(t) > 3:
        t = _smart_title_case(t)
    return t.strip()


def _smart_title_case(text: str) -> str:
    small_words = {"a","an","the","and","but","or","for","nor","in","on","at","to","of","by","vs","vs."}
    roman_re = re.compile(r"^(I{1,3}|IV|V|VI{0,3}|IX|X{0,3}|XI{0,3}|XII{0,3})$")
    words = text.split()
    result = []
    for i, word in enumerate(words):
        if roman_re.match(word): result.append(word)
        elif i == 0: result.append(word.capitalize())
        elif word.lower() in small_words: result.append(word.lower())
        else: result.append(word.capitalize())
    text_out = " ".join(result)
    text_out = re.sub(r":\s+([a-z])", lambda m: ": " + m.group(1).upper(), text_out)
    return text_out


def make_film_id(title: str) -> str:
    slug = title.lower()
    slug = re.sub(r"[''']", "", slug)
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


VALID_BBFC = {"U", "PG", "12A", "12", "15", "18", "R18"}

def parse_bbfc_from_html(rating_html: str) -> str:
    if not rating_html: return "TBC"
    m = re.search(r"BBFC Rating:\s*\((\w+)\)", rating_html)
    if m and m.group(1) in VALID_BBFC: return m.group(1)
    m = re.search(r"\((\w+)\)", rating_html)
    if m and m.group(1) in VALID_BBFC: return m.group(1)
    return "TBC"


PERF_TAG_MAP = {
    "BB": "Baby Friendly", "CC": "Closed Captions", "AD": "Audio Described",
    "R": "Relaxed", "QA": "Q&A", "SU": "Subtitled", "HoH": "HoH",
}

def extract_perf_tags(perf: dict) -> list[str]:
    return [label for key, label in PERF_TAG_MAP.items() if perf.get(key) == "Y"]


def extract_events_json(html: str) -> list[dict]:
    idx = html.find('{"Events":[')
    if idx < 0:
        log.error("Could not find Events JSON in the HTML")
        return []
    depth = 0
    for i in range(idx, len(html)):
        if html[i] == "{": depth += 1
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


def parse_events(events: list[dict]) -> list[dict]:
    films = []
    for event in events:
        raw_title = unescape(event.get("Title", "").strip())
        if not raw_title: continue

        clean_title = clean_phoenix_title(raw_title)

        skip = False
        for pattern in NON_FILM_SKIP_PATTERNS:
            if re.search(pattern, raw_title, re.IGNORECASE) or \
               re.search(pattern, clean_title, re.IGNORECASE):
                log.info(f"  Skipping non-film: {raw_title}")
                skip = True; break
        if skip: continue

        film_id = make_film_id(clean_title)
        rating = parse_bbfc_from_html(event.get("Rating", ""))

        runtime = event.get("RunningTime")
        if isinstance(runtime, str):
            m = re.search(r"\d+", runtime)
            runtime = int(m.group()) if m else None

        director = (event.get("Director") or "").strip() or None
        cast = (event.get("Cast") or "").strip() or None
        year = (event.get("Year") or "").strip() or None
        country = (event.get("Country") or "").strip() or None
        synopsis = (event.get("Synopsis") or "").strip() or None

        # Validate year — Savoy sometimes swaps Year/Country fields
        if year and not re.match(r"^\d{4}$", year):
            if country and re.match(r"^\d{4}$", country):
                # Swap: year field has country, country field has year
                year, country = country, year
                log.info(f"  Swapped year/country for: {raw_title} → yr={year}, country={country}")
            else:
                log.info(f"  Invalid year '{year}' for: {raw_title} — setting to None")
                year = None

        film_url = event.get("URL", "")
        if film_url and not film_url.startswith("http"):
            film_url = urljoin(BASE_URL + "/PhoenixCinemaLondon.dll/", film_url)
        poster_url = event.get("ImageURL", "")

        season_tags = []
        for season in event.get("Seasons", []):
            name = season.get("SeasonName", "").strip()
            if name: season_tags.append(name)

        showtimes = {}
        for perf in event.get("Performances", []):
            date_str = perf.get("StartDate", "")
            time_str = perf.get("StartTimeAndNotes", "").strip().lower()
            if not date_str or not time_str: continue
            if not re.match(r"\d{1,2}:\d{2}", time_str): continue

            booking_url = perf.get("URL", "")
            if booking_url and not booking_url.startswith("http"):
                booking_url = urljoin(BASE_URL + "/PhoenixCinemaLondon.dll/", booking_url)

            screen = perf.get("AuditoriumName") or None
            hoh = perf.get("HoH") == "Y" or perf.get("CC") == "Y"
            tags = extract_perf_tags(perf)
            sold_out = perf.get("IsSoldOut") == "Y"

            session = {"time": time_str, "booking_url": booking_url, "screen": screen, "hoh": hoh}
            if tags: session["tags"] = tags
            if sold_out: session["sold_out"] = True
            showtimes.setdefault(date_str, []).append(session)

        if not showtimes:
            log.warning(f"  No showtimes for: {raw_title}")
            continue

        film = {
            "id": film_id, "title": clean_title, "rating": rating,
            "runtime": runtime, "genre": "Other", "year": year,
            "director": director, "cast": cast, "country": country,
            "description": synopsis, "film_url": film_url,
            "poster_url": poster_url, "showtimes": showtimes,
        }
        if season_tags: film["season_tags"] = season_tags
        films.append(film)
        perf_count = sum(len(v) for v in showtimes.values())
        log.info(f"  ✓ {clean_title} — {len(showtimes)} date(s), {perf_count} perf(s), {rating}, {runtime}min")

    log.info(f"Parsed {len(films)} films with showtimes")

    # Merge duplicate titles (e.g. "Parents and Baby: Project Hail Mary"
    # becomes a separate entry from "Project Hail Mary" after title cleaning)
    merged = {}
    for film in films:
        key = film["id"]
        if key not in merged:
            merged[key] = film
        else:
            existing = merged[key]
            # Merge showtimes
            for date, sessions in film["showtimes"].items():
                if date in existing["showtimes"]:
                    existing_times = {s["time"] for s in existing["showtimes"][date]}
                    for s in sessions:
                        if s["time"] not in existing_times:
                            existing["showtimes"][date].append(s)
                else:
                    existing["showtimes"][date] = sessions
            # Prefer the entry with more metadata
            for field in ("year", "director", "cast", "country", "description"):
                if not existing.get(field) and film.get(field):
                    existing[field] = film[field]
            log.info(f"  Merged duplicate: {film['title']}")
    films = list(merged.values())

    return films


# ─── Color assignment (golden-ratio hue stepping) ────────────────────


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Phoenix Cinema timetable")
    parser.add_argument("-o","--output",type=str,default=None)
    parser.add_argument("--local",type=str,default=None)
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (
        Path(__file__).resolve().parent.parent / "public" / "data" / "films_phoenix.json"
    )

    log.info("=== Phoenix Cinema Scraper Starting ===")

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

    events = extract_events_json(html)
    if not events: log.error("No events found. Aborting."); sys.exit(1)

    films = parse_events(events)
    if not films: log.error("No films found. Aborting."); sys.exit(1)

    assign_colors(films)

    output = {"scraped_at": datetime.now(timezone.utc).isoformat(), "source": "phoenixcinema.co.uk", "films": films}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info("=== Scraper Complete ===")

if __name__ == "__main__":
    main()
