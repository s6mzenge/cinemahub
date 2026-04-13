#!/usr/bin/env python3
"""
Peckhamplex Timetable Scraper — FULLY ASYNC

All three steps run asynchronously:
  Step 1: Film list pages fetched concurrently with aiohttp
  Step 2: Film detail pages fetched concurrently (semaphore-gated)
  Step 3: Veezi screen scraping with concurrent Playwright tabs

Typical total time: ~30-40s instead of ~2-3 minutes.

Usage:
    python scrape.py                          # default concurrency
    python scrape.py --concurrency 8          # more Playwright tabs
    python scrape.py --no-screens             # skip Step 3 entirely
    python scrape.py -o my_output.json        # custom output path
"""

import json
import re
import sys
import time
import logging
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.peckhamplex.london"
LISTINGS_URL = f"{BASE_URL}/films/out-now"
COMING_SOON_URL = f"{BASE_URL}/films/coming-soon"

# Max concurrent HTTP requests for the Peckhamplex site
HTTP_CONCURRENCY = 5
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


# ---------------------------------------------------------------------------
# Async HTTP fetching
# ---------------------------------------------------------------------------

async def fetch(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    retries: int = 2,
) -> BeautifulSoup | None:
    """Fetch a URL through the shared session, gated by the semaphore."""
    async with semaphore:
        for attempt in range(retries + 1):
            try:
                log.info(f"Fetching: {url}")
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    resp.raise_for_status()
                    html = await resp.text()
                    return BeautifulSoup(html, "html.parser")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < retries:
                    await asyncio.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# Parsing helpers (unchanged)
# ---------------------------------------------------------------------------

def parse_runtime(text: str) -> int | None:
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
    rate_el = soup.find("b", string=re.compile(r"Rate", re.I))
    if rate_el and rate_el.parent:
        text = rate_el.parent.get_text(separator=" ").strip()
        match = re.search(r"Rate[:\s]+(\w+)", text)
        if match:
            return match.group(1)
    rating_img = soup.select_one(".access-details-wrapper .rating img")
    if rating_img:
        src = rating_img.get("src", "")
        match = re.search(r"/(\w+)\.\w+$", src)
        if match:
            return match.group(1)
    return "TBC"


def extract_genre(soup: BeautifulSoup) -> str:
    genre_el = soup.find("b", string=re.compile(r"Genre", re.I))
    if genre_el and genre_el.parent:
        text = genre_el.parent.get_text().strip()
        match = re.search(r"Genre[:\s]+(.+)", text)
        if match:
            return match.group(1).strip()
    return "Other"


def extract_runtime(soup: BeautifulSoup) -> int | None:
    rt_el = soup.find("b", string=re.compile(r"Running Time", re.I))
    if rt_el and rt_el.parent:
        return parse_runtime(rt_el.parent.get_text())
    return None


def parse_date_text(text: str) -> str | None:
    cleaned = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", text.strip())
    for fmt in ["%A %d %B %Y", "%d %B %Y", "%A %d %b %Y"]:
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Step 1: Film list scraping (async)
# ---------------------------------------------------------------------------

def parse_film_list(soup: BeautifulSoup, url: str) -> list[dict]:
    """Parse a listing page soup into film stubs."""
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

        has_hoh_listing = bool(wrapper.select_one('.icon[title*="Hard of Hearing"]'))
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


async def scrape_film_lists(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Fetch both listing pages concurrently and merge results."""
    urls = [LISTINGS_URL, COMING_SOON_URL]
    soups = await asyncio.gather(
        *(fetch(session, url, semaphore) for url in urls)
    )

    all_films = []
    seen_ids: set[str] = set()
    for soup, url in zip(soups, urls):
        if not soup:
            log.error(f"Could not fetch listings page: {url}")
            continue
        for film in parse_film_list(soup, url):
            if film["id"] not in seen_ids:
                all_films.append(film)
                seen_ids.add(film["id"])

    return all_films


# ---------------------------------------------------------------------------
# Step 2: Film detail scraping (async, concurrent)
# ---------------------------------------------------------------------------

def parse_film_detail(soup: BeautifulSoup, film: dict) -> dict | None:
    """Parse a film detail page soup into a film dict. Pure parsing, no I/O."""
    rating = extract_rating(soup)
    genre = extract_genre(soup)
    runtime = extract_runtime(soup)

    showtimes = {}
    for date_wrapper in soup.select(".book-tickets .date-wrapper"):
        date_el = date_wrapper.select_one(".ticket-date")
        if not date_el:
            continue

        date_text = date_el.get_text(strip=True)
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

            is_hoh = False
            parent_text = btn.parent.get_text() if btn.parent else ""
            if "HoH" in parent_text or film.get("has_hoh_on_listing", False):
                is_hoh = True

            sessions.append({
                "time": show_time,
                "booking_url": booking_url,
                "screen": None,
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


async def scrape_film_detail(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    film: dict,
) -> dict | None:
    """Fetch and parse a single film detail page."""
    soup = await fetch(session, film["film_url"], semaphore)
    if not soup:
        log.error(f"Could not fetch film page: {film['film_url']}")
        return None
    return parse_film_detail(soup, film)


async def scrape_all_details(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    films_raw: list[dict],
) -> list[dict]:
    """Fetch all film detail pages concurrently."""
    results = await asyncio.gather(
        *(scrape_film_detail(session, semaphore, f) for f in films_raw)
    )
    return [r for r in results if r and r["showtimes"]]


# ---------------------------------------------------------------------------
# Step 3: FAST concurrent Veezi screen scraping (unchanged)
# ---------------------------------------------------------------------------

async def _scrape_single_screen(
    context,
    semaphore: asyncio.Semaphore,
    url: str,
    index: int,
    total: int,
) -> tuple[str, str | None]:
    """Scrape a single Veezi URL in its own tab, gated by the semaphore."""
    async with semaphore:
        page = await context.new_page()
        screen = None
        try:
            log.info(f"  Veezi [{index+1}/{total}]: {url[:70]}...")
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)

            # Fast path: wait for the content selector to appear.
            # After the first few requests warm Cloudflare, this resolves in <500ms.
            # Falls back to short-interval polling for Cloudflare challenges.
            try:
                await page.wait_for_selector(
                    ".showtime-info, .error-page, .unavailable",
                    timeout=12000,
                )
            except Exception:
                # Selector didn't appear — may be stuck on Cloudflare challenge.
                # Short-poll as a fallback.
                for tick in range(4):
                    await page.wait_for_timeout(500)
                    try:
                        title = await page.title()
                    except Exception:
                        continue
                    if "moment" not in title.lower():
                        break

            # Check if we landed on an error/unavailable page
            try:
                title = await page.title()
            except Exception:
                title = ""
            if "unavailable" in title.lower() or "error" in title.lower():
                log.info(f"    Session unavailable (past showtime)")
            else:
                # Extract screen info
                infos = await page.query_selector_all(".showtime-info")
                for info in infos:
                    label_el = await info.query_selector("label")
                    text_el = await info.query_selector("text")
                    if label_el and text_el:
                        label_text = (await label_el.inner_text()).strip().lower()
                        if "screen" in label_text:
                            screen = (await text_el.inner_text()).strip()
                            break

            if screen:
                log.info(f"    → {screen}")

        except Exception as e:
            log.warning(f"    Failed: {e}")
        finally:
            await page.close()

        return url, screen


async def scrape_screens_playwright_fast(
    booking_urls: list[str],
    concurrency: int = 8,
) -> dict[str, str | None]:
    """Scrape screen numbers from Veezi using multiple concurrent browser tabs."""
    from playwright.async_api import async_playwright

    if not booking_urls:
        return {}

    veezi_urls = [u for u in booking_urls if u and "veezi.com" in u]
    if not veezi_urls:
        return {}

    log.info(f"Scraping {len(veezi_urls)} Veezi URLs with concurrency={concurrency}")
    start_time = time.time()

    results: dict[str, str | None] = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/134.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="en-GB",
        )

        semaphore = asyncio.Semaphore(concurrency)
        tasks = [
            _scrape_single_screen(context, semaphore, url, i, len(veezi_urls))
            for i, url in enumerate(veezi_urls)
        ]
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in task_results:
            if isinstance(result, Exception):
                log.warning(f"Task failed with exception: {result}")
            else:
                url, screen = result
                results[url] = screen

        await context.close()
        await browser.close()

    elapsed = time.time() - start_time
    screens_found = sum(1 for v in results.values() if v)
    log.info(
        f"Veezi scraping done: {screens_found}/{len(veezi_urls)} screens found "
        f"in {elapsed:.1f}s"
    )
    return results


# ---------------------------------------------------------------------------
# Color assignment (unchanged)
# ---------------------------------------------------------------------------

def assign_colors(films: list[dict]) -> None:
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def async_main(args):
    output_path = Path(args.output) if args.output else (
        Path(__file__).parent.parent / "public" / "data" / "films.json"
    )

    log.info("=== Peckhamplex Scraper Starting (FULLY ASYNC) ===")
    total_start = time.time()

    http_semaphore = asyncio.Semaphore(HTTP_CONCURRENCY)

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        # Step 1: Get film lists (both pages concurrently)
        t0 = time.time()
        all_films_raw = await scrape_film_lists(session, http_semaphore)
        log.info(f"Step 1 done in {time.time()-t0:.1f}s — {len(all_films_raw)} films found")

        if not all_films_raw:
            log.error("No films found at all. Aborting.")
            sys.exit(1)

        # Step 2: Scrape all film detail pages concurrently
        t0 = time.time()
        films = await scrape_all_details(session, http_semaphore, all_films_raw)
        log.info(f"Step 2 done in {time.time()-t0:.1f}s — {len(films)} films with showtimes")

    # Step 3: Scrape screen numbers (Playwright, already concurrent)
    if not args.no_screens:
        t0 = time.time()
        log.info("Scraping Veezi for screen numbers (concurrent)...")
        unique_booking_urls = []
        seen_urls: set[str] = set()

        for film in films:
            for date_str, sessions in film["showtimes"].items():
                for sess in sessions:
                    if sess["booking_url"] and sess["booking_url"] not in seen_urls:
                        unique_booking_urls.append(sess["booking_url"])
                        seen_urls.add(sess["booking_url"])

        log.info(f"Found {len(unique_booking_urls)} unique booking URLs to check")

        url_to_screen = await scrape_screens_playwright_fast(
            unique_booking_urls, args.concurrency
        )

        screens_found = 0
        for film in films:
            for date_str, sessions in film["showtimes"].items():
                for sess in sessions:
                    if sess["booking_url"] in url_to_screen and url_to_screen[sess["booking_url"]]:
                        sess["screen"] = url_to_screen[sess["booking_url"]]
                        screens_found += 1

        log.info(f"Step 3 done in {time.time()-t0:.1f}s — {screens_found} screens applied")
    else:
        log.info("Skipping Veezi screen scraping (--no-screens)")

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

    total_elapsed = time.time() - total_start
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info(f"=== Scraper Complete in {total_elapsed:.1f}s ===")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Peckhamplex timetable (fully async)")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Output path for films.json")
    parser.add_argument("--no-screens", action="store_true",
                        help="Skip scraping Veezi for screen numbers (faster)")
    parser.add_argument("--concurrency", type=int, default=8,
                        help="Number of concurrent Playwright tabs for Veezi (default: 8)")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
