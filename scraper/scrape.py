#!/usr/bin/env python3
"""
Peckhamplex Timetable Scraper

Scrapes peckhamplex.london for current films, showtimes, booking links,
and screen numbers from the Veezi ticketing system.

Outputs a JSON file that the frontend reads.
"""

import json
import re
import sys
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.peckhamplex.london"
LISTINGS_URL = f"{BASE_URL}/films/out-now"
COMING_SOON_URL = f"{BASE_URL}/films/coming-soon"

# Polite delay between requests (seconds)
REQUEST_DELAY = 1.0
REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": "PeckhamplexTimetable/1.0 (personal project; scrapes twice daily)",
    "Accept": "text/html,application/xhtml+xml",
}

# Genre-based color palette for the frontend
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
}
DEFAULT_COLORS = {"color": "#78909c", "accent": "#b0bec5"}

# Fallback colors for films that share a genre (so they don't look identical)
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
    """Extract minutes from runtime text like '1 hour 47 minutes' or '107 minutes'."""
    text = text.strip().lower()
    hours = 0
    minutes = 0
    h_match = re.search(r"(\d+)\s*hour", text)
    m_match = re.search(r"(\d+)\s*min", text)
    if h_match:
        hours = int(h_match.group(1))
    if m_match:
        minutes = int(m_match.group(1))
    total = hours * 60 + minutes
    return total if total > 0 else None


def extract_rating(soup: BeautifulSoup) -> str:
    """Extract film rating from the info section."""
    rate_el = soup.find("b", string=re.compile(r"Rate", re.I))
    if rate_el and rate_el.parent:
        text = rate_el.parent.get_text(separator=" ").strip()
        # Pattern: "Rate: 15\nstrong language..." or "Rate: PG"
        match = re.search(r"Rate[:\s]+(\w+)", text)
        if match:
            return match.group(1)
    # Fallback: look for rating image
    rating_img = soup.select_one(".access-details-wrapper .rating img")
    if rating_img:
        src = rating_img.get("src", "")
        # e.g. /imgs/ratings/15.png
        match = re.search(r"/(\w+)\.\w+$", src)
        if match:
            return match.group(1)
    return "TBC"


def extract_genre(soup: BeautifulSoup) -> str:
    """Extract genre from film detail page."""
    genre_el = soup.find("b", string=re.compile(r"Genre", re.I))
    if genre_el and genre_el.parent:
        text = genre_el.parent.get_text().strip()
        match = re.search(r"Genre[:\s]+(.+)", text)
        if match:
            return match.group(1).strip()
    return "Other"


def extract_runtime(soup: BeautifulSoup) -> int | None:
    """Extract runtime from film detail page."""
    rt_el = soup.find("b", string=re.compile(r"Running Time", re.I))
    if rt_el and rt_el.parent:
        return parse_runtime(rt_el.parent.get_text())
    return None


def scrape_film_list(url: str) -> list[dict]:
    """Scrape the films listing page and return basic film info with URLs."""
    soup = fetch(url)
    if not soup:
        log.error(f"Could not fetch listings page: {url}")
        return []

    films = []
    for wrapper in soup.select(".title-wrapper"):
        link_el = wrapper.select_one("a[href*='/film/']")
        if not link_el:
            continue
        film_url = urljoin(BASE_URL, link_el["href"])

        title_el = wrapper.select_one(".film-title p")
        title = title_el.get_text(strip=True) if title_el else "Unknown"

        poster_el = wrapper.select_one("img.poster")
        poster_url = ""
        if poster_el:
            poster_url = urljoin(BASE_URL, poster_el.get("src", ""))

        # Check for HoH icon on listing
        has_hoh_listing = bool(wrapper.select_one('.icon[title*="Hard of Hearing"]'))

        # Derive a slug ID from the URL
        slug = film_url.rstrip("/").split("/")[-1]

        films.append({
            "id": slug,
            "title": title,
            "film_url": film_url,
            "poster_url": poster_url,
            "has_hoh_on_listing": has_hoh_listing,
        })

    log.info(f"Found {len(films)} films on {url}")
    return films


def scrape_film_detail(film: dict) -> dict | None:
    """Scrape an individual film page for full details and showtimes."""
    soup = fetch(film["film_url"])
    if not soup:
        log.error(f"Could not fetch film page: {film['film_url']}")
        return None

    rating = extract_rating(soup)
    genre = extract_genre(soup)
    runtime = extract_runtime(soup)

    # Parse showtimes
    showtimes = {}
    for date_wrapper in soup.select(".book-tickets .date-wrapper"):
        date_el = date_wrapper.select_one(".ticket-date")
        if not date_el:
            continue

        date_text = date_el.get_text(strip=True)
        # Parse "Sunday 12th April 2026" -> "2026-04-12"
        date_str = parse_date_text(date_text)
        if not date_str:
            log.warning(f"Could not parse date: {date_text}")
            continue

        sessions = []
        for btn in date_wrapper.select("a.btn"):
            time_el = btn.select_one("time")
            if not time_el:
                continue
            show_time = time_el.get_text(strip=True)
            booking_url = btn.get("href", "")

            # Check if this is a HoH screening (icon next to time)
            is_hoh = False
            parent_text = btn.parent.get_text() if btn.parent else ""
            # HoH icon is typically indicated by fa-volume-up near the button
            hoh_icon = date_wrapper.select_one('.icon[title*="Hard of Hearing"]')
            if hoh_icon:
                is_hoh = True

            sessions.append({
                "time": show_time,
                "booking_url": booking_url,
                "screen": None,  # Will be filled by Veezi scrape
                "hoh": is_hoh,
            })

        if sessions:
            showtimes[date_str] = sessions

    if not showtimes:
        log.warning(f"No showtimes found for: {film['title']}")

    return {
        "id": film["id"],
        "title": film["title"],
        "rating": rating,
        "runtime": runtime,
        "genre": genre,
        "film_url": film["film_url"],
        "poster_url": film["poster_url"],
        "showtimes": showtimes,
    }


def parse_date_text(text: str) -> str | None:
    """Parse 'Sunday 12th April 2026' or 'Monday 13th April 2026' to '2026-04-12'."""
    # Remove ordinal suffixes
    cleaned = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", text.strip())
    for fmt in ["%A %d %B %Y", "%d %B %Y", "%A %d %b %Y"]:
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def scrape_screen_from_veezi(booking_url: str) -> str | None:
    """Scrape the Veezi booking page to get the screen number."""
    if not booking_url or "veezi.com" not in booking_url:
        return None

    soup = fetch(booking_url)
    if not soup:
        return None

    # Look for screen info: <label>Screen</label> <text>Screen 2</text>
    for info_div in soup.select(".showtime-info"):
        label = info_div.select_one("label")
        if label and "screen" in label.get_text(strip=True).lower():
            text_el = info_div.select_one("text")
            if text_el:
                return text_el.get_text(strip=True)
    return None


def assign_colors(films: list[dict]) -> None:
    """Assign colors to films based on genre, avoiding duplicates."""
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
            # Find next unused color from the palette
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
    output_path = Path(__file__).parent.parent / "public" / "data" / "films.json"

    # Allow overriding output path via CLI arg
    if len(sys.argv) > 1:
        output_path = Path(sys.argv[1])

    log.info("=== Peckhamplex Scraper Starting ===")

    # Step 1: Get film list from both "Out Now" and "Coming Soon"
    all_films_raw = []
    seen_ids = set()

    for url in [LISTINGS_URL, COMING_SOON_URL]:
        for film in scrape_film_list(url):
            if film["id"] not in seen_ids:
                all_films_raw.append(film)
                seen_ids.add(film["id"])

    if not all_films_raw:
        log.error("No films found at all. Aborting.")
        sys.exit(1)

    # Step 2: Scrape each film's detail page
    films = []
    for film_raw in all_films_raw:
        detail = scrape_film_detail(film_raw)
        if detail and detail["showtimes"]:
            films.append(detail)

    log.info(f"Scraped details for {len(films)} films with showtimes")

    # Step 3: Scrape screen numbers from Veezi (only first session per film to be polite)
    # We batch unique booking URLs to avoid duplicate requests
    url_to_screen: dict[str, str | None] = {}
    unique_booking_urls = set()

    for film in films:
        for date_str, sessions in film["showtimes"].items():
            for sess in sessions:
                if sess["booking_url"]:
                    unique_booking_urls.add(sess["booking_url"])

    log.info(f"Found {len(unique_booking_urls)} unique booking URLs to check for screens")

    for url in unique_booking_urls:
        screen = scrape_screen_from_veezi(url)
        url_to_screen[url] = screen
        if screen:
            log.info(f"  Screen: {screen} for {url[:80]}...")

    # Apply screen info back to sessions
    for film in films:
        for date_str, sessions in film["showtimes"].items():
            for sess in sessions:
                if sess["booking_url"] in url_to_screen:
                    sess["screen"] = url_to_screen[sess["booking_url"]]

    # Step 4: Assign colors
    assign_colors(films)

    # Step 5: Write output
    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "peckhamplex.london",
        "films": films,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info("=== Scraper Complete ===")


if __name__ == "__main__":
    main()
