#!/usr/bin/env python3
"""
Peckhamplex Timetable Scraper — FULLY ASYNC + PIPELINED

All steps run asynchronously, and Steps 2+3 are pipelined:
  Step 1: Film list pages fetched concurrently with aiohttp
  Step 2+3: Film detail pages fetched concurrently — as booking URLs are
            discovered, Playwright workers immediately start scraping Veezi
            for screen numbers (no waiting for all detail pages to finish).

Playwright pages block images/CSS/fonts for faster loads.

Typical total time: ~20-30s (down from ~40-60s without pipelining).

Usage:
    python scrape.py                          # default concurrency
    python scrape.py --concurrency 12         # more Playwright tabs
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
HTTP_CONCURRENCY = 10
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

# Site names to reject when extracting film titles — the mobile header <h1>
# contains "Peckhamplex" and must never be mistaken for a film title.
_SITE_NAMES = {"peckhamplex", "peckhamplex multi-screen cinema"}


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
# Parsing helpers
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


def extract_title(soup: BeautifulSoup, film_id: str, listing_title: str) -> str:
    """Extract the film title from a detail page, with multiple fallbacks.

    Selector priority:
      1. h1.page-title  — the actual film title on current Peckhamplex pages
      2. .film-title h1 / .film-title p / h1.film-title — older markup variants
      3. og:title meta tag
      4. <title> tag (split on " - ")
      5. Listing page title (if usable)
      6. Humanised URL slug

    NEVER uses a bare "h1" — the site header contains "Peckhamplex" which
    would silently replace short titles like "Fuze" or "GOAT".
    """
    # Try film-specific selectors
    for sel in ["h1.page-title", ".film-title h1", ".film-title p", "h1.film-title"]:
        el = soup.select_one(sel)
        if el:
            candidate = el.get_text(strip=True)
            if candidate and candidate.lower() not in _SITE_NAMES:
                return candidate

    # Fallback: og:title meta tag
    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content", "").strip():
        candidate = og["content"].strip()
        if candidate.lower() not in _SITE_NAMES:
            return candidate

    # Fallback: <title> tag — format is "Fuze - Peckhamplex Multi-Screen Cinema"
    title_tag = soup.find("title")
    if title_tag:
        parts = title_tag.get_text(strip=True).split(" - ", 1)
        if parts[0].strip() and parts[0].strip().lower() not in _SITE_NAMES:
            return parts[0].strip()

    # Fallback: keep listing title if it's not "Unknown"
    if listing_title and listing_title != "Unknown" and listing_title.lower() not in _SITE_NAMES:
        return listing_title

    # Last resort: humanise the URL slug
    log.warning(f"Could not extract title for {film_id}, falling back to slug")
    return film_id.replace("-", " ").title()


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

        title = "Unknown"
        for title_sel in [".film-title p", ".film-title h1", ".film-title h2", ".film-title"]:
            title_el = wrapper.select_one(title_sel)
            if title_el:
                candidate = title_el.get_text(strip=True)
                if candidate:
                    title = candidate
                    break

        poster_el = wrapper.select_one("img.poster")
        poster_url = ""
        if poster_el:
            poster_url = urljoin(BASE_URL, poster_el.get("src", ""))

        has_hoh_listing = bool(wrapper.select_one('.icon[title*="Hard of Hearing"]'))
        slug = film_url.rstrip("/").split("/")[-1]

        if title == "Unknown":
            log.warning(f"Could not extract title from listing page for slug: {slug}")

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
    detail_title = extract_title(soup, film["id"], film["title"])

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
        log.warning(f"No showtimes found for: {detail_title}")

    return {
        "id": film["id"],
        "title": detail_title,
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
    """Fetch all film detail pages concurrently (used when --no-screens)."""
    results = await asyncio.gather(
        *(scrape_film_detail(session, semaphore, f) for f in films_raw)
    )
    return [r for r in results if r and r["showtimes"]]


# ---------------------------------------------------------------------------
# Step 3: Veezi screen scraping helpers
# ---------------------------------------------------------------------------

async def _block_unnecessary_resources(route):
    """Abort image/CSS/font/media requests in Playwright for faster loads."""
    if route.request.resource_type in {"image", "stylesheet", "font", "media"}:
        await route.abort()
    else:
        await route.continue_()


async def _scrape_single_screen(context, semaphore, url: str) -> tuple[str, str | None]:
    """Scrape a single Veezi URL in its own tab, gated by the semaphore."""
    async with semaphore:
        page = await context.new_page()
        screen = None
        try:
            log.info(f"  Veezi: {url[:70]}...")
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)

            # Wait for the content selector or error indicator.
            try:
                await page.wait_for_selector(
                    ".showtime-info, .error-page, .unavailable",
                    timeout=10000,
                )
            except Exception:
                # Selector didn't appear — may be stuck on Cloudflare challenge.
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


# ---------------------------------------------------------------------------
# Pipelined Steps 2+3: detail pages + Veezi screens concurrently
# ---------------------------------------------------------------------------

async def scrape_details_and_screens(
    session: aiohttp.ClientSession,
    http_semaphore: asyncio.Semaphore,
    films_raw: list[dict],
    pw_concurrency: int,
) -> tuple[list[dict], dict[str, str | None]]:
    """Fetch detail pages and scrape Veezi screens in a producer-consumer pipeline.

    As each detail page is parsed, its booking URLs are immediately queued for
    Playwright workers — so screen scraping starts while detail pages are still
    being fetched. This overlaps the two slowest phases of the scraper.
    """
    from playwright.async_api import async_playwright

    films: list[dict] = []
    url_to_screen: dict[str, str | None] = {}
    seen_urls: set[str] = set()
    queue: asyncio.Queue[str | None] = asyncio.Queue()

    # -- Producers: fetch detail pages, enqueue booking URLs ----------------

    async def detail_producer(film_stub: dict):
        result = await scrape_film_detail(session, http_semaphore, film_stub)
        if result and result["showtimes"]:
            films.append(result)
            for sessions_list in result["showtimes"].values():
                for sess in sessions_list:
                    url = sess["booking_url"]
                    if url and "veezi.com" in url and url not in seen_urls:
                        seen_urls.add(url)
                        await queue.put(url)

    # -- Consumers: scrape Veezi pages from the queue -----------------------

    async def screen_consumer(context, pw_semaphore):
        while True:
            url = await queue.get()
            if url is None:
                break
            try:
                _, screen = await _scrape_single_screen(context, pw_semaphore, url)
                if screen:
                    url_to_screen[url] = screen
            except Exception as e:
                log.warning(f"Screen consumer error: {e}")

    # -- Orchestrate --------------------------------------------------------

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

        # Block images/CSS/fonts — Veezi pages only need HTML+JS
        await context.route("**/*", _block_unnecessary_resources)

        pw_semaphore = asyncio.Semaphore(pw_concurrency)

        # Start consumer workers (they block on queue.get() until URLs arrive)
        consumers = [
            asyncio.create_task(screen_consumer(context, pw_semaphore))
            for _ in range(pw_concurrency)
        ]

        # Run all producers (detail page fetches) — URLs flow to consumers
        # as they're discovered
        await asyncio.gather(
            *(detail_producer(f) for f in films_raw)
        )

        # All detail pages done — send sentinel per consumer to shut them down
        for _ in range(pw_concurrency):
            await queue.put(None)

        # Wait for remaining screen scraping to finish
        await asyncio.gather(*consumers)

        await context.close()
        await browser.close()

    return films, url_to_screen


# ---------------------------------------------------------------------------
# Color assignment
# ---------------------------------------------------------------------------

def assign_colors(films: list[dict]) -> None:
    """Assign colors to films. Genre colors first, then procedural hue rotation."""
    import colorsys

    def hsl_to_hex(h, s, l):
        r, g, b = colorsys.hls_to_rgb(h, l, s)
        return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"

    def generate_color(index):
        """Use golden angle for max hue separation, with varied saturation/lightness."""
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

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def async_main(args):
    output_path = Path(args.output) if args.output else (
        Path(__file__).parent.parent / "public" / "data" / "films.json"
    )

    log.info("=== Peckhamplex Scraper Starting (PIPELINED) ===")
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

        if args.no_screens:
            # Step 2 only (no Playwright)
            t0 = time.time()
            films = await scrape_all_details(session, http_semaphore, all_films_raw)
            log.info(f"Step 2 done in {time.time()-t0:.1f}s — {len(films)} films with showtimes")
        else:
            # Steps 2+3 pipelined: detail pages + Veezi screens concurrently
            t0 = time.time()
            log.info("Steps 2+3 pipelined: fetching details + scraping Veezi concurrently...")
            films, url_to_screen = await scrape_details_and_screens(
                session, http_semaphore, all_films_raw, args.concurrency
            )
            log.info(
                f"Steps 2+3 done in {time.time()-t0:.1f}s — "
                f"{len(films)} films, {sum(1 for v in url_to_screen.values() if v)} screens found"
            )

            # Apply screen numbers to sessions
            screens_applied = 0
            for film in films:
                for date_str, sessions in film["showtimes"].items():
                    for sess in sessions:
                        screen = url_to_screen.get(sess["booking_url"])
                        if screen:
                            sess["screen"] = screen
                            screens_applied += 1
            log.info(f"Applied {screens_applied} screen numbers to sessions")

    # Step 4: Assign colors
    assign_colors(films)

    # Step 5: Write output
    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "peckhamplex.london",
        "films": films,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    total_elapsed = time.time() - total_start
    log.info(f"Wrote {len(films)} films to {output_path}")
    log.info(f"=== Scraper Complete in {total_elapsed:.1f}s ===")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Peckhamplex timetable (pipelined async)")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Output path for films.json")
    parser.add_argument("--no-screens", action="store_true",
                        help="Skip scraping Veezi for screen numbers (faster)")
    parser.add_argument("--concurrency", type=int, default=12,
                        help="Number of concurrent Playwright tabs for Veezi (default: 12)")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
