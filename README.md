# Peckhamplex Timetable

A personal timetable dashboard for [Peckhamplex Cinema](https://www.peckhamplex.london) that auto-updates twice daily via web scraping.

Shows all screenings in a timeline or grid view, with direct booking links and screen numbers.

## How it works

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  GitHub Actions   │────▶│  public/data/     │────▶│  GitHub Pages    │
│  (scraper, 2x/day)│     │  films.json       │     │  (React frontend)│
└──────────────────┘     └──────────────────┘     └──────────────────┘
```

1. **Scraper** (`scraper/scrape.py`) runs at 10:00 and 16:00 UTC via GitHub Actions
2. It crawls peckhamplex.london for films, showtimes, booking URLs, and screen numbers
3. Outputs `public/data/films.json`
4. Commits the updated JSON → triggers a frontend rebuild
5. **Frontend** (React + Vite) deploys to GitHub Pages

## Quick start

### 1. Create the repo

```bash
# Clone or push this folder to a new GitHub repo
git init
git add -A
git commit -m "initial commit"
git remote add origin git@github.com:YOUR_USERNAME/peckhamplex-timetable.git
git push -u origin main
```

### 2. Enable GitHub Pages

1. Go to **Settings → Pages** in your repo
2. Under **Source**, select **GitHub Actions**

### 3. Update the base URL

In `vite.config.js`, update `base` to match your repo name:

```js
base: "/peckhamplex-timetable/",  // ← your repo name
```

If using a custom domain, set `base: "/"`.

### 4. Run the scraper manually (first time)

Go to **Actions → Scrape Peckhamplex Timetable → Run workflow**.

This populates `films.json` with real data and triggers the first deploy.

### 5. You're live!

Visit `https://YOUR_USERNAME.github.io/peckhamplex-timetable/`

The scraper will now run automatically twice daily.

## Local development

```bash
# Frontend
npm install
npm run dev         # → http://localhost:5173

# Scraper (requires Python 3.10+)
pip install -r scraper/requirements.txt
python scraper/scrape.py
```

## Repo structure

```
├── .github/workflows/
│   ├── scrape.yml          # Twice-daily scraper cron
│   └── deploy.yml          # Build & deploy on push
├── scraper/
│   ├── scrape.py           # Python scraper
│   └── requirements.txt
├── src/
│   ├── main.jsx            # React entry
│   └── App.jsx             # Main timetable component
├── public/
│   └── data/
│       └── films.json      # Auto-generated data (committed by scraper)
├── index.html
├── package.json
└── vite.config.js
```

## Data format

The scraper outputs `films.json` in this shape:

```json
{
  "scraped_at": "2026-04-12T10:00:00Z",
  "source": "peckhamplex.london",
  "films": [
    {
      "id": "california-schemin",
      "title": "California Schemin'",
      "rating": "15",
      "runtime": 107,
      "genre": "Comedy",
      "color": "#d81b60",
      "accent": "#ff6090",
      "film_url": "https://www.peckhamplex.london/film/california-schemin",
      "poster_url": "https://www.peckhamplex.london/imgs/posters/medium/california-schemin.jpg",
      "showtimes": {
        "2026-04-12": [
          {
            "time": "20:30",
            "booking_url": "https://ticketing.eu.veezi.com/purchase/79412?siteToken=...",
            "screen": "Screen 2",
            "hoh": false
          }
        ]
      }
    }
  ]
}
```

## Notes

- **Scraping politeness**: The scraper waits 1 second between requests and identifies itself in the User-Agent header.
- **Screen numbers**: Scraped from the Veezi booking pages. If the Veezi page structure changes, screen numbers may come back as `null` — everything else still works.
- **Costs**: Fully free on GitHub Actions + GitHub Pages (scraper uses ~1 min of compute per run, well within the free tier).
- **Timezone**: Scraper cron runs at 10:00 and 16:00 UTC (11:00 and 17:00 BST). Adjust in `.github/workflows/scrape.yml` if needed.
