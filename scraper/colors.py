"""
Shared color assignment for CinemaHub scrapers.

Provides:
  - GENRE_COLORS: base genre → color mapping (superset of all cinema-specific genres)
  - EXTRA_PALETTES: hand-picked fallback palette
  - DEFAULT_COLORS: grey fallback (unused by assign_colors, kept for reference)
  - assign_colors(films, genre_colors=None): assigns "color" and "accent" to each film

Each scraper imports and calls assign_colors(films), optionally passing a custom
genre_colors dict for cinemas with non-standard genre tags (e.g. Arzner, Close-Up).
"""

import colorsys

# ─── Base genre palette (Material Design, muted) ─────────────────────

GENRE_COLORS = {
    # Standard genres
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
    "Event":       {"color": "#558b2f", "accent": "#9ccc65"},
    "Live":        {"color": "#558b2f", "accent": "#9ccc65"},
    # Aliases / composite genres (Prince Charles, ICA, Close-Up)
    "Action/Adventure": {"color": "#ef6c00", "accent": "#ffb74d"},
    "Science Fiction":  {"color": "#00838f", "accent": "#4dd0e1"},
    "Neo Noir":         {"color": "#37474f", "accent": "#78909c"},
    "Romance/Comedy":   {"color": "#00897b", "accent": "#4db6ac"},
    "Fantasy":          {"color": "#7c4dff", "accent": "#b388ff"},
    "War":              {"color": "#546e7a", "accent": "#90a4ae"},
    "Experimental":     {"color": "#ef6c00", "accent": "#ffb74d"},
    "World Cinema":     {"color": "#00897b", "accent": "#4db6ac"},
    "Classic":          {"color": "#00838f", "accent": "#4dd0e1"},
    "Repertory":        {"color": "#1565c0", "accent": "#64b5f6"},
    "Festival":         {"color": "#d81b60", "accent": "#ff6090"},
}

DEFAULT_COLORS = {"color": "#78909c", "accent": "#b0bec5"}

# ─── Hand-picked extras (used before procedural fallback) ────────────

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


# ─── Color assignment ────────────────────────────────────────────────

def _hsl_to_hex(h: float, s: float, l: float) -> str:
    r, g, b = colorsys.hls_to_rgb(h, l, s)
    return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"


def _generate_color(index: int) -> dict:
    """Golden-angle hue stepping with varied saturation/lightness bands."""
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
        "color": _hsl_to_hex(hue, sat, lit),
        "accent": _hsl_to_hex(hue, asat, alit),
    }


def assign_colors(films: list[dict], genre_colors: dict | None = None) -> None:
    """
    Assign 'color' and 'accent' keys to each film dict.

    Priority: genre match → hand-picked extras → procedural golden-ratio.

    Args:
        films: list of film dicts (modified in-place).
        genre_colors: optional custom genre→color map. Falls back to
                      the shared GENRE_COLORS if not provided.
    """
    palette = genre_colors if genre_colors is not None else GENRE_COLORS

    used_colors: set[str] = set()
    palette_idx = 0
    gen_idx = 0

    for film in films:
        genre = film.get("genre", "Other")
        colors = palette.get(genre)

        if colors and colors["color"] not in used_colors:
            film["color"] = colors["color"]
            film["accent"] = colors["accent"]
            used_colors.add(colors["color"])
        else:
            # Try hand-picked extras first
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
            # Fallback: generate procedural colors (never grey)
            if not assigned:
                while True:
                    c = _generate_color(gen_idx)
                    gen_idx += 1
                    if c["color"] not in used_colors:
                        film["color"] = c["color"]
                        film["accent"] = c["accent"]
                        used_colors.add(c["color"])
                        break
