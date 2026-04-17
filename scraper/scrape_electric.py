#!/usr/bin/env python3
"""
Electric Cinema Timetable Scraper (Portobello & White City)

Fetches structured JSON from electriccinema.co.uk/data/data.json
— no Playwright needed, the site exposes a clean REST-like data feed.

Outputs TWO JSON files (one per location) compatible with CinemaHub.

Usage:
    python scraper/scrape_electric.py

    # Custom output directory
    python scraper/scrape_electric.py -d my_output_dir/
"""

import json
import re
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from colors import assign_colors

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.electriccinema.co.uk"
DATA_URL = "https://electriccinema.co.uk/data/data.json"

REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": "ElectricCinemaScraper/1.0 (personal project; scrapes twice daily)",
    "Accept": "application/json",
}

# Map Electric Cinema IDs to output config
CINEMA_CONFIG = {
    "603": {
        "output_file": "films_electric_portobello.json",
        "source": "electriccinema.co.uk (Portobello)",
    },
    "602": {
        "output_file": "films_electric_white_city.json",
        "source": "electriccinema.co.uk (White City)",
    },
}

# Screening type codes → human-readable tags
SCREENING_TYPE_TAGS = {
    "SE": "Electric Selects",
    "MF": None,             # Main Feature — no tag needed, it's the default
    "KC": "Kids Club",
    "ES": "Electric Selects",  # fallback
    "EA": "Early Access",
}

# Genre-based color palette (reused from other scrapers)


def fetch_data() -> dict:
    """Fetch the main data.json from Electric Cinema."""
    params = {"a": datetime.now().isoformat()}  # cache buster
    log.info(f"Fetching: {DATA_URL}")
    resp = requests.get(DATA_URL, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    log.info(
        f"Got {len(data.get('films', {}))} films, "
        f"{len(data.get('screenings', {}))} screenings, "
        f"{len(data.get('cinemas', {}))} cinemas"
    )
    return data


def extract_year(premiere: str | None) -> str | None:
    """Extract year from premiere date string like '2001-04-13'."""
    if not premiere:
        return None
    match = re.match(r"(\d{4})", premiere)
    return match.group(1) if match else None


def build_films_for_cinema(data: dict, cinema_id: str) -> list[dict]:
    """Build the CinemaHub film list for a specific cinema location."""
    films_data = data.get("films", {})
    screenings_data = data.get("screenings", {})
    screening_types = data.get("screeningTypes", {})

    cinema_id_int = int(cinema_id)
    films = []

    for film_id, film in films_data.items():
        # Only include films that screen at this cinema
        if cinema_id_int not in film.get("screeningCinemas", []):
            continue

        # Build showtimes for this cinema
        showtimes = {}
        cinema_screenings = (
            film.get("screenings", {})
            .get("byCinema", {})
            .get(cinema_id, {})
        )

        for date_str, screening_ids in cinema_screenings.items():
            sessions = []
            for sid in screening_ids:
                screening = screenings_data.get(str(sid))
                if not screening:
                    continue

                session = {
                    "time": screening["t"],
                    "screen": screening.get("sn"),
                    "hoh": False,
                }

                # Booking URL
                link = screening.get("link")
                if link and screening.get("bookable", True):
                    session["booking_url"] = urljoin(BASE_URL, link)
                else:
                    session["booking_url"] = None

                # Tags from screening type
                st_code = screening.get("st", "")
                tag = SCREENING_TYPE_TAGS.get(st_code)
                if tag:
                    session["tags"] = [tag]

                # Sold out note
                msg = screening.get("message", "")
                if "sold out" in msg.lower():
                    session["sold_out"] = True

                sessions.append(session)

            if sessions:
                # Sort by time
                sessions.sort(key=lambda s: s["time"])
                showtimes[date_str] = sessions

        if not showtimes:
            continue

        # Slug from film link
        film_link = film.get("link", "")
        slug = film_link.strip("/").split("/")[-1] if film_link else f"electric-{film_id}"

        # Poster URL
        image = film.get("image", "")
        poster_url = urljoin(BASE_URL, image) if image else ""

        # Film page URL
        film_url = urljoin(BASE_URL, film_link) if film_link else ""

        films.append({
            "id": slug,
            "title": film["title"],
            "rating": film.get("rating", "TBC"),
            "runtime": None,  # Not available in the API
            "genre": "Other",
            "year": extract_year(film.get("premiere")),
            "director": film.get("director"),
            "cast": None,     # Not available in the API
            "description": film.get("short_synopsis", ""),
            "film_url": film_url,
            "poster_url": poster_url,
            "showtimes": showtimes,
        })

    log.info(f"Cinema {cinema_id}: {len(films)} films with showtimes")
    return films



def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Electric Cinema timetable")
    parser.add_argument("-d", "--output-dir", type=str, default=None,
                        help="Output directory for JSON files")
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else (
        Path(__file__).parent.parent / "public" / "data"
    )

    log.info("=== Electric Cinema Scraper Starting ===")

    # Step 1: Fetch data
    data = fetch_data()

    if not data.get("films"):
        log.error("No films in API response. Aborting.")
        sys.exit(1)

    # Step 2: Build per-cinema film lists
    for cinema_id, config in CINEMA_CONFIG.items():
        cinema_name = data.get("cinemas", {}).get(cinema_id, {}).get("title", cinema_id)
        log.info(f"Processing cinema: {cinema_name} (ID {cinema_id})")

        films = build_films_for_cinema(data, cinema_id)

        if not films:
            log.warning(f"No films with showtimes for {cinema_name}. Skipping.")
            continue

        # Step 3: Assign colors
        assign_colors(films)

        # Step 4: Write output
        output = {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source": config["source"],
            "films": films,
        }

        output_path = output_dir / config["output_file"]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(output, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        log.info(f"Wrote {len(films)} films to {output_path}")

    log.info("=== Scraper Complete ===")


if __name__ == "__main__":
    main()
