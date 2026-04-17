import { useState, useMemo, useRef, useEffect } from "react";

const DEFAULT_ADS_MIN = 20;
const DATA_BASE = import.meta.env.BASE_URL + "data/";

const DAYS = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const VALID_RATINGS = new Set(["U","PG","12A","12","15","18","R18","TBC"]);
const rBg = { U:"#2e7d32", PG:"#b8960f", "12A":"#c45a00", "12":"#c45a00", "15":"#a12020", "18":"#8b0000", "R18":"#5c0000", TBC:"#666" };

/* ─── Cinema registry (extend this later) ─── */
const CINEMAS = [
  { id:"peckhamplex", name:"Peckhamplex", short:"PKX", barColor:"#b8860b", address:"95a Rye Lane, SE15", url:"https://www.peckhamplex.london", price:"£7.59", dataFile:"films.json", source:"peckhamplex.london", adsMin:20 },
  { id:"prince-charles", name:"Prince Charles", short:"PCC", barColor:"#7b68ee", address:"7 Leicester Place, WC2", url:"https://princecharlescinema.com", price:null, dataFile:"films_pcc.json", source:"princecharlescinema.com", adsMin:20 },
  { id:"castle-hackney", name:"The Castle", short:"CST", barColor:"#e06050", address:"64-66 Brooksby's Walk, E9", url:"https://thecastlecinema.com", price:null, dataFile:"films_castle.json", source:"thecastlecinema.com", adsMin:10 },
  { id:"the-arzner", name:"The Arzner", short:"ARZ", barColor:"#2aa67e", address:"10 Bermondsey Square, SE1", url:"https://thearzner.com", price:null, dataFile:"films_arzner.json", source:"thearzner.com", adsMin:20 },
  { id:"bfi-southbank", name:"BFI Southbank", short:"BFI", barColor:"#378add", address:"Belvedere Rd, SE1", url:"https://whatson.bfi.org.uk", price:null, dataFile:"films_bfi.json", source:"whatson.bfi.org.uk", adsMin:20 },
  { id:"electric-portobello", name:"Electric Portobello", short:"ELP", barColor:"#d4537e", address:"191 Portobello Rd, W11", url:"https://www.electriccinema.co.uk", price:null, dataFile:"films_electric_portobello.json", source:"electriccinema.co.uk", adsMin:20 },
  { id:"electric-white-city", name:"Electric White City", short:"EWC", barColor:"#639922", address:"101 Wood Lane, W12", url:"https://www.electriccinema.co.uk", price:null, dataFile:"films_electric_white_city.json", source:"electriccinema.co.uk", adsMin:20 },
  { id:"close-up", name:"Close-Up", short:"CLU", barColor:"#c17817", address:"97 Sclater St, E1", url:"https://www.closeupfilmcentre.com", price:null, dataFile:"films_closeup.json", source:"closeupfilmcentre.com", adsMin:0 },
  { id:"ica", name:"ICA", short:"ICA", barColor:"#1a1aff", address:"The Mall, SW1Y", url:"https://www.ica.art", price:null, dataFile:"films_ica.json", source:"ica.art", adsMin:10 },
  { id:"garden-cinema", name:"Garden Cinema", short:"GDN", barColor:"#6d8764", address:"39 Parker St, WC2B", url:"https://www.thegardencinema.co.uk", price:null, dataFile:"films_garden.json", source:"thegardencinema.co.uk", adsMin:10 },
  { id:"rio-dalston", name:"Rio Cinema", short:"RIO", barColor:"#9b2d30", address:"107 Kingsland High St, E8", url:"https://riocinema.org.uk", price:null, dataFile:"films_rio.json", source:"riocinema.org.uk", adsMin:15 },
  { id:"genesis", name:"Genesis Cinema", short:"GEN", barColor:"#c44536", address:"93-95 Mile End Rd, E1", url:"https://www.genesiscinema.co.uk", price:null, dataFile:"films_genesis.json", source:"genesiscinema.co.uk", adsMin:15 },
  { id:"phoenix", name:"Phoenix Cinema", short:"PHX", barColor:"#8b5cf6", address:"52 High Rd, N2", url:"https://www.phoenixcinema.co.uk", price:null, dataFile:"films_phoenix.json", source:"phoenixcinema.co.uk", adsMin:15 },
];
const CINEMA_MAP = Object.fromEntries(CINEMAS.map(c => [c.id, c]));
function getAdsMin(cinemaId) { return CINEMA_MAP[cinemaId]?.adsMin ?? DEFAULT_ADS_MIN; }

function timeToMin(t){ const [h,m]=t.split(":").map(Number); return h*60+m; }
function minToTime(m){ const h=Math.floor(m/60)%24; return `${h}:${String(m%60).padStart(2,"0")}`; }
function getToday(){ const d=new Date(); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}-${String(d.getDate()).padStart(2,"0")}`; }
function getNowMin(){ const d=new Date(); return d.getHours()*60+d.getMinutes(); }

function getAllDatesWithScreenings(films) {
  const s = new Set();
  films.forEach(f => { if (f.showtimes) Object.keys(f.showtimes).forEach(d => s.add(d)); });
  return [...s].sort();
}

/* Pick best initial date: skip today if all screenings have ended */
function pickInitialDate(allDates, films, todayStr, isAllCinemas, cinemaId) {
  if (!allDates.length) return todayStr;
  if (!allDates.includes(todayStr)) {
    return allDates.find(d => d >= todayStr) || allDates[0];
  }
  const nowMin = getNowMin();
  let hasActive = false;
  for (const f of films) {
    if (hasActive) break;
    if (isAllCinemas && f.perCinema) {
      for (const [cId, pc] of Object.entries(f.perCinema)) {
        const times = pc.showtimes?.[todayStr];
        if (!times) continue;
        const ads = getAdsMin(cId);
        for (const t of times) {
          if (timeToMin(t) + ads + (f.runtime || 0) > nowMin) { hasActive = true; break; }
        }
        if (hasActive) break;
      }
    } else {
      const times = f.showtimes?.[todayStr];
      if (!times) continue;
      const ads = cinemaId ? getAdsMin(cinemaId) : DEFAULT_ADS_MIN;
      for (const t of times) {
        if (timeToMin(t) + ads + (f.runtime || 0) > nowMin) { hasActive = true; break; }
      }
    }
  }
  if (hasActive) return todayStr;
  const nextDate = allDates.find(d => d > todayStr);
  return nextDate || todayStr;
}

function formatDayTab(dateStr) {
  const d = new Date(dateStr+"T12:00:00");
  return { day:DAYS[d.getDay()], num:d.getDate(), mon:MONTHS[d.getMonth()], full:dateStr };
}

function normalizeFilms(rawFilms) {
  return rawFilms.map(f => {
    const showtimes={}, bookingUrls={}, screens={}, hoh={}, tags={};
    if (f.showtimes) {
      for (const [date, sessions] of Object.entries(f.showtimes)) {
        const times = [];
        sessions.forEach(sess => {
          if (typeof sess === "string") { times.push(sess); }
          else {
            times.push(sess.time);
            if (sess.booking_url) { if(!bookingUrls[date]) bookingUrls[date]={}; bookingUrls[date][sess.time]=sess.booking_url; }
            if (sess.screen) { if(!screens[date]) screens[date]={}; screens[date][sess.time]=sess.screen; }
            if (sess.hoh) { if(!hoh[date]) hoh[date]=[]; hoh[date].push(sess.time); }
            if (sess.tags && sess.tags.length) { if(!tags[date]) tags[date]={}; tags[date][sess.time]=sess.tags; }
          }
        });
        showtimes[date] = times;
      }
    }
    const rawRating = (f.rating||"").trim().toUpperCase();
    return { id:f.id, title:f.title, rating:VALID_RATINGS.has(rawRating)?rawRating:"TBC", runtime:f.runtime||90, genre:f.genre||"Other",
      color:f.color||"#78909c", accent:f.accent||"#b0bec5", film_url:f.film_url||null, poster_url:f.poster_url||null,
      letterboxd_url:f.letterboxd_url||null, letterboxd_rating:f.letterboxd_rating||null,
      showtimes, bookingUrls, screens, hoh:Object.keys(hoh).length>0?hoh:(f.hoh||{}), tags };
  });
}

/* ─── Title normalisation for cross-cinema matching ─── */
// Known event-series prefixes (mirrored from enrich_letterboxd.py EVENT_PREFIXES)
const EVENT_PREFIXES_NORM = [
  "adults only", "camp classics presents", "camp classics",
  "cine-real presents", "cine-real", "distorted frame",
  "dog-friendly", "exhibition on screen", "exclusive preview",
  "fetish friendly", "funday", "in the scene", "late night",
  "lesbian visibility day", "lesbian visibility",
  "lost reels presents", "lost reels", "memories",
  "nt live", "national theatre live",
  "pitchblack mixtapes", "pitchblack playback",
  "preview", "the male gaze",
  "uk premiere of 4k restoration", "uk premiere",
  "violet hour presents", "violet hour",
  "word space presents", "word space",
  "25 and under",
];

function normalizeTitle(title) {
  let t = title;
  // Remove diacritics (é→e, ü→u, etc.)
  t = t.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  // Lowercase
  t = t.toLowerCase().trim();

  // ── Step 1: Strip known event-series prefixes (before colon) ──
  // e.g. "Camp Classics presents: Hackers" → "hackers"
  // but  "Kill Bill: The Whole Bloody Affair" → kept as-is
  if (t.includes(":")) {
    const beforeColon = t.split(":")[0].trim().replace(/\s+presents$/, "");
    if (EVENT_PREFIXES_NORM.includes(beforeColon)) {
      t = t.split(":").slice(1).join(":").trim();
      // Strip leading/trailing quotes (e.g. Lost Reels presents "Lianna")
      t = t.replace(/^["""\u201c\u201d]+|["""\u201c\u201d]+$/g, "").trim();
    }
  }

  // ── Step 2: Strip event suffixes: "+ Q&A", "+ Intro", etc. ──
  // But preserve " + " in actual titles like "Romeo + Juliet"
  t = t.replace(/\s*\+\s*(q\s*&\s*a\b.*|intro\b.*|director\b.*|special\b.*|extended\b.*)\s*$/i, "");

  // ── Step 3: Strip non-film parentheticals ──
  t = t.replace(/\s*\(independent filmmakers showcase\)/gi, "");
  t = t.replace(/\s*\(short films?\)/gi, "");
  t = t.replace(/\s*\(live score\)/gi, "");
  t = t.replace(/\s*\(4k restoration\)/gi, "");
  t = t.replace(/\s*\(black & white version\)/gi, "");

  // ── Step 4: Strip anniversary / re-release / year suffixes ──
  // Normalise "national theatre live:" → "nt live:" prefix
  t = t.replace(/^national theatre live:\s*/i, "nt live: ");
  // Strip " - Nth Anniversary..." suffix  (e.g. "Amelie - 25th Anniversary")
  t = t.replace(/\s*[-\u2013\u2014]\s*\d+\w*\s*anniversary.*$/i, "");
  // Strip "(Nth Anniversary...)" parenthetical  (e.g. "(25th Anniversary Re-release)")
  t = t.replace(/\s*\(\d+\w*\s*anniversary[^)]*\)/gi, "");
  // Strip "(Re-release)" standalone
  t = t.replace(/\s*\(re-?release\)/gi, "");
  // Strip trailing year parenthetical "(1996)" — but ONLY at end of string
  t = t.replace(/\s*\(\d{4}\)\s*$/, "");
  // Normalise ", " → " & "  (matches "You, Me" to "You & Me")
  t = t.replace(/,\s+/g, " & ");
  // Collapse whitespace
  t = t.replace(/\s+/g, " ").trim();
  return t;
}

function pickBestTitle(titles) {
  const unique = [...new Set(titles)];
  if (unique.length === 1) return unique[0];
  // Check if any title is "clean" (no anniversary/re-release/year suffix)
  const hasSuffix = t => /\d+\w*\s*anniversary|re-?release|\(\d{4}\)\s*$/i.test(t);
  const clean = unique.filter(t => !hasSuffix(t));
  if (clean.length > 0) {
    // Among clean titles: prefer diacritics, then shortest
    return clean.sort((a, b) => {
      const aAcc = /[^\x00-\x7F]/.test(a), bAcc = /[^\x00-\x7F]/.test(b);
      if (aAcc !== bAcc) return aAcc ? -1 : 1;
      return a.length - b.length;
    })[0];
  }
  // All have suffixes — strip the best one and use that as display
  const strip = t => t
    .replace(/\s*[-\u2013\u2014]\s*\d+\w*\s*anniversary.*$/i, "")
    .replace(/\s*\(\d+\w*\s*anniversary[^)]*\)/gi, "")
    .replace(/\s*\(re-?release\)/gi, "")
    .replace(/\s*\(\d{4}\)\s*$/, "")
    .trim();
  return unique.map(t => strip(t)).sort((a, b) => {
    const aAcc = /[^\x00-\x7F]/.test(a), bAcc = /[^\x00-\x7F]/.test(b);
    if (aAcc !== bAcc) return aAcc ? -1 : 1;
    return a.length - b.length;
  })[0];
}

/* ─── Merge films across all cinemas by normalised title ─── */
function mergeAllCinemaFilms(allCinemaData) {
  const byKey = {};      // normalised key → merged film object
  const titlesByKey = {}; // normalised key → [original titles]
  allCinemaData.forEach(({ cinemaId, films: cFilms }) => {
    cFilms.forEach(f => {
      const key = normalizeTitle(f.title);
      if (!titlesByKey[key]) titlesByKey[key] = [];
      titlesByKey[key].push(f.title);
      if (!byKey[key]) {
        byKey[key] = {
          id: f.id, title: f.title, rating: f.rating, runtime: f.runtime, genre: f.genre,
          color: f.color, accent: f.accent, film_url: f.film_url, poster_url: f.poster_url,
          letterboxd_url: f.letterboxd_url, letterboxd_rating: f.letterboxd_rating,
          showtimes: {}, bookingUrls: {}, screens: {}, hoh: {}, tags: {},
          perCinema: {},
        };
      }
      const merged = byKey[key];
      if (!merged.perCinema[cinemaId]) {
        merged.perCinema[cinemaId] = { showtimes:{}, bookingUrls:{}, screens:{}, hoh:{}, tags:{}, film_url:null };
      }
      if (f.film_url && !merged.perCinema[cinemaId].film_url) merged.perCinema[cinemaId].film_url = f.film_url;
      const pc = merged.perCinema[cinemaId];
      for (const [date, times] of Object.entries(f.showtimes)) {
        if (!pc.showtimes[date]) pc.showtimes[date] = [];
        pc.showtimes[date].push(...times);
        if (!merged.showtimes[date]) merged.showtimes[date] = [];
        merged.showtimes[date].push(...times);
      }
      for (const [date, urls] of Object.entries(f.bookingUrls)) {
        if (!pc.bookingUrls[date]) pc.bookingUrls[date] = {};
        Object.assign(pc.bookingUrls[date], urls);
        if (!merged.bookingUrls[date]) merged.bookingUrls[date] = {};
        Object.assign(merged.bookingUrls[date], urls);
      }
      for (const [date, scr] of Object.entries(f.screens)) {
        if (!pc.screens[date]) pc.screens[date] = {};
        Object.assign(pc.screens[date], scr);
        if (!merged.screens[date]) merged.screens[date] = {};
        Object.assign(merged.screens[date], scr);
      }
      for (const [date, arr] of Object.entries(f.hoh || {})) {
        if (!pc.hoh[date]) pc.hoh[date] = [];
        pc.hoh[date].push(...arr);
        if (!merged.hoh[date]) merged.hoh[date] = [];
        merged.hoh[date].push(...arr);
      }
      for (const [date, tg] of Object.entries(f.tags)) {
        if (!pc.tags[date]) pc.tags[date] = {};
        Object.assign(pc.tags[date], tg);
        if (!merged.tags[date]) merged.tags[date] = {};
        Object.assign(merged.tags[date], tg);
      }
      if (f.film_url && !merged.film_url) merged.film_url = f.film_url;
      if (f.poster_url && !merged.poster_url) merged.poster_url = f.poster_url;
      if (f.letterboxd_url && !merged.letterboxd_url) merged.letterboxd_url = f.letterboxd_url;
      if (f.letterboxd_rating && !merged.letterboxd_rating) merged.letterboxd_rating = f.letterboxd_rating;
    });
  });
  // Pick the best display title for each merged group
  for (const [key, merged] of Object.entries(byKey)) {
    merged.title = pickBestTitle(titlesByKey[key]);
  }
  return Object.values(byKey);
}

/* ─── Letterboxd rating badge (inline after title) ─── */
function LbRating({ rating, url, size=12, color="#639922" }) {
  if (!rating) return null;
  const star = <svg width={size} height={size} viewBox="0 0 24 24" fill={color} style={{ display:"block", flexShrink:0 }}><path d="M12 2l3.09 6.26L22 9.27l-5 4.87L18.18 22 12 18.27 5.82 22 7 14.14l-5-4.87 6.91-1.01z"/></svg>;
  const inner = (
    <span style={{ display:"inline-flex", alignItems:"center", gap:2, flexShrink:0, whiteSpace:"nowrap" }}>
      {star}
      <span style={{ fontSize:size, fontWeight:700, color, fontFamily:"'Space Mono', monospace", lineHeight:1 }}>{rating}</span>
    </span>
  );
  if (url) return <a href={url} target="_blank" rel="noopener" style={{ textDecoration:"none" }} title="Letterboxd rating">{inner}</a>;
  return inner;
}

/* ─── Theme palettes ─── */
const fonts = { mono:"'Space Mono', monospace", serif:"'Playfair Display', serif", sans:"'DM Sans', sans-serif" };

const themes = {
  dark: {
    ...fonts, bg:"#06060b", surface:"#0c0c14", surfaceAlt:"#0a0a11",
    sidebarBg:"#09090f", sidebarBorder:"#14141e",
    rowEven:"rgba(255,255,255,0.008)", rowOdd:"transparent",
    border:"#16161f", borderLight:"#1e1e2a",
    text:"#e8e4dc", textSub:"#d5d0c8", textMuted:"#8a857c", textDim:"#504c46", textFaint:"#2e2c28",
    accent:"#d4a053", accentGlow:"#d4a05355",
    accentSoft:"rgba(212,160,83,0.08)", accentMed:"rgba(212,160,83,0.15)",
    headerBg:"linear-gradient(180deg,#0e0c08 0%,#06060b 100%)",
    grainOpacity:0.018, spotlightColor:"rgba(212,160,83,0.04)",
    barBookBg:"rgba(255,255,255,0.12)", barBookBorder:"rgba(255,255,255,0.18)", barBookHover:"rgba(255,255,255,0.35)",
    barText:"#fff", barSubText:"rgba(255,255,255,0.65)",
    gridStickyBg1:"#0d0c12", gridStickyBg2:"#0a0a10",
    gridDashColor:"#16161f", gridCellBorder:"#06060b",
    cardBorder:c=>`${c}28`, cardBg:c=>`linear-gradient(135deg,${c}08 0%,transparent 100%)`,
    mobileSideBg:"#06060b", mobileConnector:a=>`${a}18`,
    ccBg:"rgba(255,255,255,0.05)", ccBorder:"rgba(255,255,255,0.08)",
    stickyShadow:"4px 0 12px rgba(0,0,0,0.6)",
    pillBgAlpha:"15", pillBorderAlpha:"30",
  },
  light: {
    ...fonts, bg:"#f4f1ec", surface:"#ffffff", surfaceAlt:"#faf8f5",
    sidebarBg:"#efe9e0", sidebarBorder:"#ddd6cb",
    rowEven:"rgba(0,0,0,0.018)", rowOdd:"transparent",
    border:"#e0dbd3", borderLight:"#d5cfc6",
    text:"#1a1814", textSub:"#2e2a24", textMuted:"#6b655c", textDim:"#9a948a", textFaint:"#c5bfb5",
    accent:"#a07430", accentGlow:"#a0743044",
    accentSoft:"rgba(160,116,48,0.07)", accentMed:"rgba(160,116,48,0.13)",
    headerBg:"linear-gradient(180deg,#faf8f4 0%,#f4f1ec 100%)",
    grainOpacity:0.012, spotlightColor:"rgba(160,116,48,0.035)",
    barBookBg:"rgba(255,255,255,0.35)", barBookBorder:"rgba(255,255,255,0.5)", barBookHover:"rgba(255,255,255,0.6)",
    barText:"#fff", barSubText:"rgba(255,255,255,0.75)",
    gridStickyBg1:"#faf8f5", gridStickyBg2:"#f6f3ee",
    gridDashColor:"#e0dbd3", gridCellBorder:"#e0dbd3",
    cardBorder:c=>`${c}30`, cardBg:c=>`linear-gradient(135deg,${c}0c 0%,transparent 100%)`,
    mobileSideBg:"#f4f1ec", mobileConnector:a=>`${a}22`,
    ccBg:"rgba(0,0,0,0.04)", ccBorder:"rgba(0,0,0,0.08)",
    stickyShadow:"4px 0 12px rgba(0,0,0,0.06)",
    pillBgAlpha:"18", pillBorderAlpha:"40",
  },
};

const SIDEBAR_W = 220;

export default function App() {
  const [films, setFilms] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [scrapedAt, setScrapedAt] = useState(null);

  const today = getToday();
  const [selDate, setSelDate] = useState(today);
  const [hovBar, setHovBar] = useState(null);
  const [view, setView] = useState("day");
  const [selWeekStart, setSelWeekStart] = useState(null);
  const tlRef = useRef(null);
  const [isMobile, setIsMobile] = useState(window.innerWidth < 768);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [selCinema, setSelCinema] = useState("all");

  const isAllCinemas = selCinema === "all";
  const cinema = isAllCinemas ? null : (CINEMAS.find(c => c.id === selCinema) || CINEMAS[0]);

  const [theme, setTheme] = useState(() => {
    if (typeof window !== "undefined" && window.matchMedia?.("(prefers-color-scheme: light)").matches) return "light";
    return "dark";
  });
  const T = themes[theme];
  const isDark = theme === "dark";

  useEffect(() => {
    const onResize = () => setIsMobile(window.innerWidth < 768);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  // Close sidebar on outside tap (mobile)
  useEffect(() => {
    if (!isMobile || !sidebarOpen) return;
    const close = () => setSidebarOpen(false);
    const timer = setTimeout(() => document.addEventListener("click", close), 10);
    return () => { clearTimeout(timer); document.removeEventListener("click", close); };
  }, [isMobile, sidebarOpen]);

  useEffect(() => {
    setLoading(true); setError(null);
    if (isAllCinemas) {
      Promise.all(CINEMAS.map(c =>
        fetch(DATA_BASE + c.dataFile)
          .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status} for ${c.name}`); return r.json(); })
          .then(data => ({ cinemaId: c.id, films: normalizeFilms(data.films || []), scrapedAt: data.scraped_at }))
          .catch(() => ({ cinemaId: c.id, films: [], scrapedAt: null }))
      )).then(allData => {
        const merged = mergeAllCinemaFilms(allData);
        setFilms(merged);
        const latestScraped = allData.map(d => d.scrapedAt).filter(Boolean).sort().pop();
        setScrapedAt(latestScraped || null);
        const allDates = getAllDatesWithScreenings(merged);
        const todayStr = getToday();
        setSelDate(pickInitialDate(allDates, merged, todayStr, true, null));
        setSelWeekStart(null);
        setLoading(false);
      });
    } else {
      const c = CINEMAS.find(c => c.id === selCinema) || CINEMAS[0];
      fetch(DATA_BASE + c.dataFile)
        .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
        .then(data => {
          const normalized = normalizeFilms(data.films || []);
          setFilms(normalized); setScrapedAt(data.scraped_at || null);
          const allDates = getAllDatesWithScreenings(normalized);
          const todayStr = getToday();
          setSelDate(pickInitialDate(allDates, normalized, todayStr, false, c.id));
          setSelWeekStart(null);
          setLoading(false);
        })
        .catch(err => { setError(err.message); setLoading(false); });
    }
  }, [selCinema]);

  const allDates = useMemo(() => getAllDatesWithScreenings(films), [films]);

  const weeks = useMemo(() => {
    if (!allDates.length) return [];
    const wks = [], seen = new Set();
    allDates.forEach(d => {
      const dt = new Date(d+"T12:00:00"), day = dt.getDay(), diff = day===0?-6:1-day;
      const mon = new Date(dt); mon.setDate(mon.getDate()+diff);
      const monStr = `${mon.getFullYear()}-${String(mon.getMonth()+1).padStart(2,"0")}-${String(mon.getDate()).padStart(2,"0")}`;
      if (!seen.has(monStr)) {
        seen.add(monStr);
        const sun = new Date(mon); sun.setDate(sun.getDate()+6);
        wks.push({ monStr, monDate:mon, sunDate:sun,
          dates: allDates.filter(ad => { const adt=new Date(ad+"T12:00:00"); return adt>=mon&&adt<=sun; }),
          label: `${mon.getDate()} ${MONTHS[mon.getMonth()]} – ${sun.getDate()} ${MONTHS[sun.getMonth()]}`,
        });
      }
    });
    return wks;
  }, [allDates]);

  useEffect(() => { if (weeks.length && !selWeekStart) { const target=allDates.includes(today)?today:selDate; const wk=weeks.find(w=>w.dates.includes(target))||weeks[0]; setSelWeekStart(wk.monStr); } }, [weeks]);

  const selWeekIdx = weeks.findIndex(w => w.monStr === selWeekStart);
  const selWeek = weeks[selWeekIdx] || weeks[0];
  const selDateIdx = allDates.indexOf(selDate);
  const canPrevDay = selDateIdx > 0, canNextDay = selDateIdx < allDates.length - 1;
  const goDay = dir => { const ni=selDateIdx+dir; if(ni>=0&&ni<allDates.length) setSelDate(allDates[ni]); };
  const canPrevWeek = selWeekIdx > 0, canNextWeek = selWeekIdx < weeks.length - 1;
  const goWeek = dir => { const ni=selWeekIdx+dir; if(ni>=0&&ni<weeks.length) setSelWeekStart(weeks[ni].monStr); };

  const dayFilms = useMemo(() => {
    if (!isAllCinemas) {
      const cAds = cinema?.adsMin ?? DEFAULT_ADS_MIN;
      return films.filter(f => f.showtimes[selDate]).map(f => ({
        ...f, times:f.showtimes[selDate],
        sessions: f.showtimes[selDate].map(t => ({
          time:t, startMin:timeToMin(t), adsEnd:timeToMin(t)+cAds, filmEnd:timeToMin(t)+cAds+f.runtime, adsMin:cAds,
          isHoh:f.hoh?.[selDate]?.includes(t), bookingUrl:f.bookingUrls?.[selDate]?.[t]||null, screen:f.screens?.[selDate]?.[t]||null,
          tags:f.tags?.[selDate]?.[t]||[],
        })),
      }));
    }
    // All-cinemas mode: group sessions by cinema within each film
    return films.filter(f => f.showtimes[selDate]).map(f => {
      const cinemaEntries = [];
      const allSessions = [];
      for (const cId of Object.keys(f.perCinema || {})) {
        const pc = f.perCinema[cId];
        const times = pc.showtimes[selDate];
        if (!times || !times.length) continue;
        const cin = CINEMA_MAP[cId];
        const cAds = cin?.adsMin ?? DEFAULT_ADS_MIN;
        const sessions = times.map(t => ({
          time:t, startMin:timeToMin(t), adsEnd:timeToMin(t)+cAds, filmEnd:timeToMin(t)+cAds+f.runtime, adsMin:cAds,
          isHoh:pc.hoh?.[selDate]?.includes(t), bookingUrl:pc.bookingUrls?.[selDate]?.[t]||null, screen:pc.screens?.[selDate]?.[t]||null,
          tags:pc.tags?.[selDate]?.[t]||[], cinemaId:cId,
        }));
        cinemaEntries.push({ cinemaId:cId, cinemaName:cin?.name||cId, cinemaShort:cin?.short||cId.slice(0,3).toUpperCase(), cinemaColor:cin?.barColor||"#888", filmUrl:pc.film_url||null, sessions });
        allSessions.push(...sessions);
      }
      return {
        ...f, times:f.showtimes[selDate],
        sessions: allSessions,
        cinemaEntries,
        cinemaCount: cinemaEntries.length,
      };
    }).sort((a,b) => (b.cinemaCount||0) - (a.cinemaCount||0) || a.title.localeCompare(b.title));
  }, [selDate, films, isAllCinemas]);

  const { axisStart, axisEnd } = useMemo(() => {
    if(!dayFilms.length) return { axisStart:17*60, axisEnd:24*60 };
    let mn=Infinity, mx=-Infinity;
    dayFilms.forEach(f=>f.sessions.forEach(s=>{ mn=Math.min(mn,s.startMin); mx=Math.max(mx,s.filmEnd); }));
    return { axisStart:Math.floor(mn/60)*60, axisEnd:Math.ceil(mx/60)*60 };
  }, [dayFilms]);

  const axisDuration = axisEnd-axisStart;
  const hourMarks=[]; for(let m=axisStart;m<=axisEnd;m+=60) hourMarks.push(m);
  const halfMarks=[]; for(let m=axisStart+30;m<axisEnd;m+=60) halfMarks.push(m);
  const selDayInfo = formatDayTab(selDate);
  useEffect(()=>{ if(tlRef.current) tlRef.current.scrollLeft=0; },[selDate]);
  const pct = min => ((min-axisStart)/axisDuration)*100;

  /* ─── Ticker: upcoming sessions across all dates ─── */
  const tickerItems = useMemo(() => {
    const nowMin = getNowMin();
    const items = [];
    const futureDates = allDates.filter(d => d >= today).slice(0, 7);
    futureDates.forEach(date => {
      const dayInfo = formatDayTab(date);
      films.forEach(film => {
        if (isAllCinemas && film.perCinema) {
          for (const [cId, pc] of Object.entries(film.perCinema)) {
            const times = pc.showtimes[date];
            if (!times) continue;
            const cin = CINEMA_MAP[cId];
            const cAds = cin?.adsMin ?? DEFAULT_ADS_MIN;
            times.forEach(t => {
              const startMin = timeToMin(t);
              const filmEnd = startMin + cAds + (film.runtime || 0);
              if (date === today && filmEnd < nowMin) return;
              items.push({
                film: film.title, filmUrl: film.film_url || null, time: t, date,
                dateLabel: date === today ? "Today" : `${dayInfo.day} ${dayInfo.num}`,
                startMin, color: cin?.barColor || film.color,
                cinemaName: cin?.name || cId,
                screen: pc.screens?.[date]?.[t] || null,
              });
            });
          }
        } else {
          const times = film.showtimes[date];
          if (!times) return;
          const cAds = cinema?.adsMin ?? DEFAULT_ADS_MIN;
          times.forEach(t => {
            const startMin = timeToMin(t);
            const filmEnd = startMin + cAds + (film.runtime || 0);
            if (date === today && filmEnd < nowMin) return;
            items.push({
              film: film.title, filmUrl: film.film_url || null, time: t, date,
              dateLabel: date === today ? "Today" : `${dayInfo.day} ${dayInfo.num}`,
              startMin, color: film.color,
              cinemaName: cinema?.name || "",
              screen: film.screens?.[date]?.[t] || null,
            });
          });
        }
      });
    });
    items.sort((a, b) => a.date === b.date ? a.startMin - b.startMin : a.date.localeCompare(b.date));
    return items.slice(0, 50);
  }, [films, allDates, today, cinema, isAllCinemas]);

  const fontLink = <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700;800;900&family=DM+Sans:wght@300;400;500;600;700&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet" />;

  /* ─── Sidebar ─── */
  const Sidebar = () => (
    <aside
      onClick={e => e.stopPropagation()}
      style={{
        width: SIDEBAR_W,
        flexShrink: 0,
        background: T.sidebarBg,
        borderRight: `1px solid ${T.sidebarBorder}`,
        display: "flex", flexDirection: "column",
        height: "100vh",
        position: isMobile ? "fixed" : "sticky",
        top: 0, left: 0, zIndex: 100,
        transform: isMobile && !sidebarOpen ? `translateX(-${SIDEBAR_W + 1}px)` : "translateX(0)",
        transition: "transform 0.3s cubic-bezier(0.4,0,0.2,1)",
        boxShadow: isMobile && sidebarOpen ? "4px 0 24px rgba(0,0,0,0.4)" : "none",
      }}
    >
      {/* Brand */}
      <div style={{ padding:"24px 20px 20px", borderBottom:`1px solid ${T.sidebarBorder}` }}>
        <div style={{ fontSize:9, letterSpacing:4, textTransform:"uppercase", color:T.accent, fontFamily:T.mono, fontWeight:700, opacity:0.6, marginBottom:4 }}>Timetable</div>
        <div style={{ fontSize:20, fontWeight:900, fontFamily:T.serif, color:T.text, letterSpacing:"-0.3px" }}>
          CinemaHub
        </div>
      </div>

      {/* Cinema list */}
      <div style={{ flex:1, padding:"12px 10px", overflowY:"auto" }}>
        {/* All Cinemas button */}
        <button onClick={() => { setSelCinema("all"); if(isMobile) setSidebarOpen(false); }}
          style={{
            display:"flex", alignItems:"center", gap:10, width:"100%",
            padding:"10px 12px", borderRadius:8, border:"none", cursor:"pointer",
            fontFamily:T.sans, fontSize:13, fontWeight:isAllCinemas?700:500, textAlign:"left",
            background: isAllCinemas ? T.accentSoft : "transparent",
            color: isAllCinemas ? T.accent : T.textMuted,
            transition:"all 0.2s",
            outline: isAllCinemas ? `1px solid ${T.accent}33` : "1px solid transparent",
            marginBottom:4,
          }}
        >
          <div style={{
            width:32, height:32, borderRadius:8, flexShrink:0,
            background: isAllCinemas ? T.accentMed : (isDark ? "rgba(255,255,255,0.03)" : "rgba(0,0,0,0.03)"),
            border:`1px solid ${isAllCinemas ? T.accent+"44" : T.border}`,
            display:"flex", alignItems:"center", justifyContent:"center",
            fontSize:11, fontFamily:T.mono, fontWeight:700, color: isAllCinemas ? T.accent : T.textDim,
          }}>ALL</div>
          <div>
            <div>All Cinemas</div>
            <div style={{ fontSize:10, color:T.textDim, fontWeight:400, marginTop:1 }}>{CINEMAS.length} venues</div>
          </div>
        </button>

        <div style={{ height:1, background:T.sidebarBorder, margin:"6px 10px 10px" }} />

        <div style={{ fontSize:9, letterSpacing:2, textTransform:"uppercase", color:T.textDim, fontFamily:T.mono, fontWeight:600, padding:"4px 10px", marginBottom:4 }}>
          Cinemas
        </div>
        {CINEMAS.map(c => {
          const isActive = c.id === selCinema;
          return (
            <button key={c.id} onClick={() => { setSelCinema(c.id); if(isMobile) setSidebarOpen(false); }}
              style={{
                display:"flex", alignItems:"center", gap:10, width:"100%",
                padding:"10px 12px", borderRadius:8, border:"none", cursor:"pointer",
                fontFamily:T.sans, fontSize:13, fontWeight:isActive?700:500, textAlign:"left",
                background: isActive ? T.accentSoft : "transparent",
                color: isActive ? T.accent : T.textMuted,
                transition:"all 0.2s",
                outline: isActive ? `1px solid ${T.accent}33` : "1px solid transparent",
              }}
            >
              {/* Cinema color dot */}
              <div style={{
                width:32, height:32, borderRadius:8, flexShrink:0,
                background: isActive ? T.accentMed : (isDark ? "rgba(255,255,255,0.03)" : "rgba(0,0,0,0.03)"),
                border:`1px solid ${isActive ? T.accent+"44" : T.border}`,
                display:"flex", alignItems:"center", justifyContent:"center",
              }}>
                <div style={{ width:10, height:10, borderRadius:3, background:c.barColor }} />
              </div>
              <div>
                <div>{c.name}</div>
                <div style={{ fontSize:10, color:T.textDim, fontWeight:400, marginTop:1 }}>{c.address}</div>
              </div>
            </button>
          );
        })}

      </div>

      {/* Bottom: theme toggle */}
      <div style={{ padding:"14px 16px", borderTop:`1px solid ${T.sidebarBorder}`, display:"flex", alignItems:"center", justifyContent:"space-between" }}>
        <span style={{ fontSize:10, color:T.textDim, fontFamily:T.mono, letterSpacing:0.5 }}>
          {isDark ? "Dark" : "Light"} mode
        </span>
        <button
          onClick={() => setTheme(t => t==="dark"?"light":"dark")}
          aria-label="Toggle theme"
          style={{
            display:"flex", alignItems:"center", justifyContent:"center",
            width:34, height:34, borderRadius:8,
            background: isDark ? "rgba(255,255,255,0.04)" : "rgba(0,0,0,0.04)",
            border:`1px solid ${T.border}`, cursor:"pointer", transition:"all 0.25s",
            color:T.textMuted, fontSize:16, lineHeight:1,
          }}
        >{isDark ? "☀" : "☽"}</button>
      </div>
    </aside>
  );

  /* ─── Mobile overlay backdrop ─── */
  const Overlay = () => isMobile && sidebarOpen ? (
    <div style={{
      position:"fixed", inset:0, zIndex:99,
      background:"rgba(0,0,0,0.5)", backdropFilter:"blur(2px)",
      transition:"opacity 0.3s",
    }} onClick={() => setSidebarOpen(false)} />
  ) : null;

  /* ─── Hamburger button (mobile only) ─── */
  const HamburgerBtn = () => isMobile ? (
    <button onClick={(e) => { e.stopPropagation(); setSidebarOpen(o => !o); }} style={{
      display:"flex", flexDirection:"column", justifyContent:"center", gap:4,
      width:34, height:34, padding:6, borderRadius:8,
      background: isDark ? "rgba(255,255,255,0.04)" : "rgba(0,0,0,0.04)",
      border:`1px solid ${T.border}`, cursor:"pointer",
    }}>
      <div style={{ width:16, height:1.5, background:T.textMuted, borderRadius:1 }} />
      <div style={{ width:12, height:1.5, background:T.textMuted, borderRadius:1 }} />
      <div style={{ width:16, height:1.5, background:T.textMuted, borderRadius:1 }} />
    </button>
  ) : null;

  /* ─── Film grain overlay ─── */
  const grainOverlay = (
    <div style={{ position:"fixed", top:0, left:0, right:0, bottom:0, zIndex:0, pointerEvents:"none" }}>
      <div style={{ position:"absolute", top:"-20%", left:"50%", transform:"translateX(-50%)", width:"120%", height:"50%", background:`radial-gradient(ellipse,${T.spotlightColor} 0%,transparent 70%)` }} />
      <div style={{ position:"absolute", inset:0, opacity:T.grainOpacity, backgroundImage:`url("data:image/svg+xml,%3Csvg viewBox='0 0 512 512' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E")` }} />
    </div>
  );

  /* ─── Loading / Error states ─── */
  if (loading) return (
    <div style={{ fontFamily:T.sans, background:T.bg, color:T.text, minHeight:"100vh", display:"flex", alignItems:"center", justifyContent:"center" }}>
      {fontLink}<div style={{ textAlign:"center" }}>
        <div style={{ fontSize:28, fontWeight:800, color:T.accent, marginBottom:12, fontFamily:T.serif }}>CinemaHub</div>
        <div style={{ color:T.textDim, fontSize:13, fontFamily:T.mono, letterSpacing:1 }}>Loading timetable…</div>
      </div>
    </div>
  );
  if (error) return (
    <div style={{ fontFamily:T.sans, background:T.bg, color:T.text, minHeight:"100vh", display:"flex", alignItems:"center", justifyContent:"center" }}>
      {fontLink}<div style={{ textAlign:"center", maxWidth:400, padding:20 }}>
        <div style={{ fontSize:28, fontWeight:800, color:T.accent, marginBottom:12, fontFamily:T.serif }}>CinemaHub</div>
        <div style={{ color:isDark?"#cf7b72":"#9e342c", fontSize:14, marginBottom:8 }}>Failed to load timetable data</div>
        <div style={{ color:T.textDim, fontSize:12, fontFamily:T.mono }}>{error}</div>
        <button onClick={()=>window.location.reload()} style={{ marginTop:20, padding:"10px 28px", background:T.accent, color:isDark?T.bg:"#fff", border:"none", borderRadius:6, cursor:"pointer", fontFamily:T.sans, fontWeight:700, fontSize:13 }}>Retry</button>
      </div>
    </div>
  );

  /* ═══════════════════════════════════════════════════
     MAIN RENDER
     ═══════════════════════════════════════════════════ */
  return (
    <div style={{ fontFamily:T.sans, background:T.bg, color:T.text, minHeight:"100vh", position:"relative", transition:"background 0.35s, color 0.35s", display:"flex" }}>
      {fontLink}
      {grainOverlay}

      <style>{`
        @keyframes goldPulse { 0%,100%{opacity:0.5} 50%{opacity:1} }
        @keyframes tickerScroll { 0% { transform: translateX(0); } 100% { transform: translateX(-50%); } }
        *::-webkit-scrollbar { height:4px; width:4px; }
        *::-webkit-scrollbar-track { background:${T.bg}; }
        *::-webkit-scrollbar-thumb { background:${T.border}; border-radius:2px; }
        .view-btn:hover { border-color:${T.accent} !important; color:${T.accent} !important; }
        .book-btn:hover { background:${T.barBookHover} !important; }
        .book-btn:active { opacity:0.7; transform:scale(0.95); }
        .ticker-link:hover { color:${T.accent} !important; border-bottom-color:${T.accent} !important; }

        /* ── Ticket shape: mask-based concave notches ── */
        .tkt-bar {
          -webkit-mask:
            radial-gradient(circle 7px at 0 50%, transparent 6px, #000 6.5px) 0 0 / 51% 100% no-repeat,
            radial-gradient(circle 7px at 100% 50%, transparent 6px, #000 6.5px) 100% 0 / 51% 100% no-repeat;
          mask:
            radial-gradient(circle 7px at 0 50%, transparent 6px, #000 6.5px) 0 0 / 51% 100% no-repeat,
            radial-gradient(circle 7px at 100% 50%, transparent 6px, #000 6.5px) 100% 0 / 51% 100% no-repeat;
        }

        .tkt-card {
          -webkit-mask:
            radial-gradient(circle 9px at 0 50%, transparent 8px, #000 8.5px) 0 0 / 51% 100% no-repeat,
            radial-gradient(circle 9px at 100% 50%, transparent 8px, #000 8.5px) 100% 0 / 51% 100% no-repeat;
          mask:
            radial-gradient(circle 9px at 0 50%, transparent 8px, #000 8.5px) 0 0 / 51% 100% no-repeat,
            radial-gradient(circle 9px at 100% 50%, transparent 8px, #000 8.5px) 100% 0 / 51% 100% no-repeat;
        }

        .tkt-pill {
          -webkit-mask:
            radial-gradient(circle 5px at 0 50%, transparent 4px, #000 4.5px) 0 0 / 51% 100% no-repeat,
            radial-gradient(circle 5px at 100% 50%, transparent 4px, #000 4.5px) 100% 0 / 51% 100% no-repeat;
          mask:
            radial-gradient(circle 5px at 0 50%, transparent 4px, #000 4.5px) 0 0 / 51% 100% no-repeat,
            radial-gradient(circle 5px at 100% 50%, transparent 4px, #000 4.5px) 100% 0 / 51% 100% no-repeat;
        }
        .tkt-cell { }

        /* ── ADS label: show only when container has enough real pixels ── */
        .ads-zone { container-type: inline-size; }
        .ads-zone .ads-label { display: none; }
        @container (min-width: 28px) {
          .ads-zone .ads-label { display: block; }
        }
      `}</style>

      <Overlay />
      <Sidebar />

      {/* ═══════ MAIN CONTENT ═══════ */}
      <div style={{
        flex:1, position:"relative", zIndex:1, minWidth:0,
        marginLeft: isMobile ? 0 : 0, /* sidebar is sticky, content flows naturally */
      }}>

        {/* ═══════ TICKER BANNER ═══════ */}
        {tickerItems.length > 0 && (
          <div style={{
            background: isDark
              ? `linear-gradient(90deg, ${T.accent}0a 0%, ${T.accent}05 50%, ${T.accent}0a 100%)`
              : `linear-gradient(90deg, ${T.accent}12 0%, ${T.accent}08 50%, ${T.accent}12 100%)`,
            borderBottom: `1px solid ${T.accent}22`,
            borderTop: `1px solid ${T.accent}11`,
            overflow: "hidden",
            position: "relative",
            height: 32,
            display: "flex",
            alignItems: "center",
          }}>
            {/* Label */}
            <div style={{
              position: "relative", zIndex: 3,
              display: "flex", alignItems: "center", gap: 6,
              padding: "0 14px", height: "100%", flexShrink: 0,
              background: isDark
                ? `linear-gradient(135deg, ${T.accent}30 0%, ${T.accent}20 100%)`
                : `linear-gradient(135deg, ${T.accent}28 0%, ${T.accent}18 100%)`,
              fontFamily: T.mono, fontSize: 9, fontWeight: 700,
              color: T.accent, letterSpacing: 2, textTransform: "uppercase",
              clipPath: "polygon(0 0, calc(100% - 8px) 0, 100% 50%, calc(100% - 8px) 100%, 0 100%)",
              paddingRight: 22,
              borderRight: `1px solid ${T.accent}22`,
            }}>
              <span style={{ width: 5, height: 5, borderRadius: "50%", background: T.accent, animation: "goldPulse 1.2s ease infinite" }} />
              COMING UP
            </div>
            {/* Scrolling track */}
            <div style={{ flex: 1, overflow: "hidden", height: "100%", display: "flex", alignItems: "center", maskImage: "linear-gradient(90deg, transparent 0%, #000 3%, #000 97%, transparent 100%)", WebkitMaskImage: "linear-gradient(90deg, transparent 0%, #000 3%, #000 97%, transparent 100%)" }}>
              <div style={{
                display: "flex", alignItems: "center", gap: 0,
                animation: `tickerScroll ${Math.max(tickerItems.length * 3, 20)}s linear infinite`,
                whiteSpace: "nowrap",
              }}>
                {[...tickerItems, ...tickerItems].map((item, i) => (
                  <span key={i} style={{ display: "inline-flex", alignItems: "center", gap: 0, paddingRight: 8 }}>
                    <span style={{ fontSize: 10, color: T.accent, fontFamily: T.mono, fontWeight: 700, opacity: 0.85 }}>{item.dateLabel}</span>
                    <span style={{ fontSize: 10, color: T.textFaint, fontFamily: T.mono, margin: "0 6px" }}>|</span>
                    <span style={{ fontSize: 10, color: T.text, fontFamily: T.mono, fontWeight: 700 }}>{item.time}</span>
                    {item.filmUrl ? (
                      <a href={item.filmUrl} target="_blank" rel="noopener" className="ticker-link" style={{ fontSize: 10, color: T.textSub, fontFamily: T.serif, fontStyle: "italic", margin: "0 4px 0 8px", textDecoration: "none", borderBottom: `1px dotted ${T.textFaint}` }}>{item.film}</a>
                    ) : (
                      <span style={{ fontSize: 10, color: T.textMuted, fontFamily: T.serif, fontStyle: "italic", margin: "0 4px 0 8px" }}>{item.film}</span>
                    )}
                    <span style={{ fontSize: 8, color: T.textDim, fontFamily: T.mono, fontWeight: 600, letterSpacing: 0.5 }}>@ {item.cinemaName}</span>
                    {item.screen && <span style={{ fontSize: 8, color: T.textFaint, fontFamily: T.mono, marginLeft: 4 }}>({item.screen})</span>}
                    <span style={{ display: "inline-block", width: 3, height: 3, borderRadius: "50%", background: item.color, margin: "0 12px 0 12px", opacity: 0.6 }} />
                  </span>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* ═══════ HEADER ═══════ */}
        <div style={{ background:T.headerBg, padding:"20px 24px 18px", borderBottom:`1px solid ${T.accent}33` }}>
          <div style={{ maxWidth:1000, margin:"0 auto" }}>
            <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", flexWrap:"wrap", gap:10 }}>
              <div style={{ display:"flex", alignItems:"center", gap:12 }}>
                <HamburgerBtn />
                <div>
                  <div style={{ fontSize:9, letterSpacing:3, textTransform:"uppercase", color:T.accent, fontFamily:T.mono, fontWeight:700, opacity:0.6, marginBottom:2 }}>{isAllCinemas ? "All Venues" : "Now Showing"}</div>
                  <h1 style={{ fontFamily:T.serif, fontSize:26, fontWeight:900, margin:0, letterSpacing:"-0.3px", lineHeight:1, color:T.text }}>
                    {isAllCinemas ? "All Cinemas" : cinema.name}
                  </h1>
                </div>
              </div>
              <div style={{ display:"flex", alignItems:"center", gap:10, flexWrap:"wrap" }}>
                {!isAllCinemas && cinema.price && (
                <div style={{
                  display:"inline-flex", alignItems:"center", gap:6,
                  padding:"5px 14px", borderRadius:20,
                  background:T.accentSoft, border:`1px solid ${T.accent}22`,
                }}>
                  <div style={{ width:5, height:5, borderRadius:"50%", background:T.accent, animation:"goldPulse 2.5s ease infinite" }} />
                  <span style={{ fontSize:13, color:T.accent, fontWeight:700, fontFamily:T.mono }}>{cinema.price}</span>
                  <span style={{ fontSize:10, color:`${T.accent}88` }}>all tickets</span>
                </div>
                )}
                {!isAllCinemas && (
                <a href={cinema.url} target="_blank" rel="noopener" style={{ color:T.textMuted, textDecoration:"none", fontSize:11, fontFamily:T.mono, letterSpacing:0.5 }}>
                  {cinema.address} ↗
                </a>
                )}
              </div>
            </div>
            {/* Cinema color legend for "all" view */}
            {isAllCinemas && (
              <div style={{ display:"flex", gap:12, flexWrap:"wrap", marginTop:12 }}>
                {CINEMAS.map(c => (
                  <div key={c.id} style={{ display:"flex", alignItems:"center", gap:5, cursor:"pointer", opacity:0.8, transition:"opacity 0.2s" }} onClick={() => setSelCinema(c.id)}>
                    <div style={{ width:8, height:8, borderRadius:3, background:c.barColor, flexShrink:0 }} />
                    <span style={{ fontSize:10, color:T.textMuted, fontFamily:T.mono, fontWeight:500 }}>{c.name}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        <div style={{ maxWidth:1000, margin:"0 auto", padding:"20px 20px 40px" }}>

          {/* ═══════ VIEW TOGGLE + NAV ═══════ */}
          <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", gap:8, marginBottom:20, flexWrap:"wrap" }}>
            <div style={{ display:"flex", gap:6 }}>
              {[["day",isMobile?"Day":"Timeline"],["grid","Week"]].map(([v,label])=>(
                <button key={v} className="view-btn" onClick={()=>setView(v)} style={{
                  padding:"7px 18px", borderRadius:6, fontSize:11, fontWeight:600, cursor:"pointer",
                  fontFamily:T.mono, letterSpacing:0.5, textTransform:"uppercase",
                  border:view===v?`1.5px solid ${T.accent}`:`1.5px solid ${T.border}`,
                  background:view===v?T.accentSoft:"transparent",
                  color:view===v?T.accent:T.textDim, transition:"all 0.25s",
                }}>{label}</button>
              ))}
            </div>

            {view==="day" ? (
              <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                <button onClick={()=>goDay(-1)} disabled={!canPrevDay} className="view-btn" style={{ padding:"6px 12px", borderRadius:6, fontSize:16, cursor:canPrevDay?"pointer":"default", border:`1.5px solid ${T.border}`, background:"transparent", color:canPrevDay?T.text:T.textFaint, fontFamily:T.serif, fontWeight:600, transition:"all 0.25s", lineHeight:1 }}>‹</button>
                <div style={{ textAlign:"center", minWidth:140 }}>
                  <div style={{ fontSize:17, fontWeight:700, color:T.text, fontFamily:T.serif, letterSpacing:"-0.3px" }}>{selDayInfo.day} {selDayInfo.num} {selDayInfo.mon}</div>
                  <div style={{ fontSize:10, color:T.textDim, fontFamily:T.mono, marginTop:2 }}>{selDate===today?"Today · ":""}{dayFilms.length} film{dayFilms.length!==1?"s":""}{isAllCinemas&&dayFilms.length>0?` across ${new Set(dayFilms.flatMap(f=>(f.cinemaEntries||[]).map(ce=>ce.cinemaId))).size} venues`:""}</div>
                </div>
                <button onClick={()=>goDay(1)} disabled={!canNextDay} className="view-btn" style={{ padding:"6px 12px", borderRadius:6, fontSize:16, cursor:canNextDay?"pointer":"default", border:`1.5px solid ${T.border}`, background:"transparent", color:canNextDay?T.text:T.textFaint, fontFamily:T.serif, fontWeight:600, transition:"all 0.25s", lineHeight:1 }}>›</button>
              </div>
            ) : selWeek ? (
              <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                <button onClick={()=>goWeek(-1)} disabled={!canPrevWeek} className="view-btn" style={{ padding:"6px 12px", borderRadius:6, fontSize:16, cursor:canPrevWeek?"pointer":"default", border:`1.5px solid ${T.border}`, background:"transparent", color:canPrevWeek?T.text:T.textFaint, fontFamily:T.serif, fontWeight:600, transition:"all 0.25s", lineHeight:1 }}>‹</button>
                <div style={{ textAlign:"center", minWidth:170 }}>
                  <div style={{ fontSize:17, fontWeight:700, color:T.text, fontFamily:T.serif, letterSpacing:"-0.3px" }}>{selWeek.label}</div>
                  <div style={{ fontSize:10, color:T.textDim, fontFamily:T.mono, marginTop:2 }}>{selWeek.dates.length} screening day{selWeek.dates.length!==1?"s":""}</div>
                </div>
                <button onClick={()=>goWeek(1)} disabled={!canNextWeek} className="view-btn" style={{ padding:"6px 12px", borderRadius:6, fontSize:16, cursor:canNextWeek?"pointer":"default", border:`1.5px solid ${T.border}`, background:"transparent", color:canNextWeek?T.text:T.textFaint, fontFamily:T.serif, fontWeight:600, transition:"all 0.25s", lineHeight:1 }}>›</button>
              </div>
            ) : null}
          </div>

          {view==="day" ? (
            <>
              {dayFilms.length===0 ? (
                <div style={{ textAlign:"center", padding:"60px 20px", color:T.textDim }}>
                  <div style={{ fontSize:40, marginBottom:12, opacity:0.25 }}>◇</div>
                  <p style={{ fontSize:15, fontFamily:T.serif, fontStyle:"italic" }}>No screenings on this day.</p>
                </div>
              ) : isMobile ? (
                /* ═══════ MOBILE FEED ═══════ */
                (() => {
                  const allSessions=[];
                  if (isAllCinemas) {
                    dayFilms.forEach(film => {
                      (film.cinemaEntries||[]).forEach(ce => {
                        ce.sessions.forEach(sess => { allSessions.push({...sess, film, cinemaName:ce.cinemaName, cinemaColor:ce.cinemaColor, cinemaShort:ce.cinemaShort, cinemaFilmUrl:ce.filmUrl}); });
                      });
                    });
                  } else {
                    dayFilms.forEach(film=>{film.sessions.forEach(sess=>{allSessions.push({...sess,film});});});
                  }
                  allSessions.sort((a,b)=>a.startMin-b.startMin);
                  const groups=[]; allSessions.forEach(sess=>{ const last=groups[groups.length-1]; if(last&&last.time===sess.time) last.sessions.push(sess); else groups.push({time:sess.time,startMin:sess.startMin,sessions:[sess]}); });
                  const nowMin = selDate===today?getNowMin():null;
                  return (
                    <div style={{ display:"flex", flexDirection:"column", gap:0, paddingTop:6 }}>
                      {groups.map((group,gi) => {
                        const isPast = nowMin!==null && group.sessions.every(s => s.filmEnd<nowMin);
                        return (
                          <div key={group.time+gi} style={{ opacity:isPast?0.35:1, transition:"opacity 0.3s" }}>
                            {group.sessions.map((sess,si) => {
                              const film=sess.film;
                              return (
                                <div key={`${film.id}-${si}`} style={{ display:"flex", alignItems:"center", gap:0, marginBottom:si<group.sessions.length-1?8:0 }}>
                                  <div style={{ width:56, flexShrink:0, display:"flex", justifyContent:"center" }}>
                                    {si===0 && <div style={{ fontSize:13, fontWeight:700, color:isPast?T.textFaint:T.accent, fontFamily:T.mono }}>{group.time}</div>}
                                  </div>
                                  <div style={{ flex:1 }}>
                                    <div className="tkt-card" style={{ display:"flex", alignItems:"center", gap:0, borderRadius:12, border:`1px solid ${T.cardBorder(isAllCinemas&&sess.cinemaColor?sess.cinemaColor:film.color)}`, background:T.cardBg(isAllCinemas&&sess.cinemaColor?sess.cinemaColor:film.color) }}>
                                      <div style={{ width:4, alignSelf:"stretch", background:`linear-gradient(180deg,${isAllCinemas&&sess.cinemaColor?sess.cinemaColor:film.color},${isAllCinemas&&sess.cinemaColor?sess.cinemaColor:film.color}66)`, flexShrink:0, borderRadius:"12px 0 0 12px" }} />
                                      <div style={{ flex:1, padding:"11px 14px" }}>
                                        <div style={{ display:"flex", alignItems:"center", gap:6 }}>
                                          <div style={{ fontSize:14, fontWeight:700, color:T.text, lineHeight:1.25, fontFamily:T.serif, minWidth:0, overflow:"hidden", display:"-webkit-box", WebkitLineClamp:2, WebkitBoxOrient:"vertical" }}>
                                            {!isAllCinemas && film.film_url ? <a href={film.film_url} target="_blank" rel="noopener" style={{ color:T.text, textDecoration:"none" }}>{film.title}</a> : film.title}
                                          </div>
                                          <LbRating rating={film.letterboxd_rating} url={film.letterboxd_url} size={13} />
                                        </div>
                                        <div style={{ display:"flex", gap:6, marginTop:5, alignItems:"center", flexWrap:"wrap" }}>
                                          {isAllCinemas && sess.cinemaName && (
                                            sess.cinemaFilmUrl ? <a href={sess.cinemaFilmUrl} target="_blank" rel="noopener" style={{ textDecoration:"none" }}><span style={{ fontSize:9, padding:"2px 6px", borderRadius:3, fontWeight:700, background:`${sess.cinemaColor}22`, color:sess.cinemaColor, fontFamily:T.mono, letterSpacing:0.3, border:`1px solid ${sess.cinemaColor}33`, cursor:"pointer" }}>{sess.cinemaShort||sess.cinemaName}</span></a>
                                            : <span style={{ fontSize:9, padding:"2px 6px", borderRadius:3, fontWeight:700, background:`${sess.cinemaColor}22`, color:sess.cinemaColor, fontFamily:T.mono, letterSpacing:0.3, border:`1px solid ${sess.cinemaColor}33` }}>{sess.cinemaShort||sess.cinemaName}</span>
                                          )}
                                          <span style={{ fontSize:9, padding:"2px 6px", borderRadius:3, fontWeight:700, background:rBg[film.rating]||"#444", color:"#fff", fontFamily:T.mono, letterSpacing:0.5 }}>{film.rating}</span>
                                          <span style={{ fontSize:10, color:T.textMuted, fontFamily:T.mono }}>{film.runtime}min</span>
                                          <span style={{ fontSize:10, color:T.textDim, fontFamily:T.mono }}>ends ~{minToTime(sess.filmEnd)}</span>
                                          {sess.screen && <span style={{ fontSize:10, color:T.textDim, fontFamily:T.mono }}>{sess.screen}</span>}
                                          {sess.isHoh && <span style={{ fontSize:9, color:T.textMuted, fontFamily:T.mono, padding:"1px 4px", borderRadius:3, background:T.ccBg, border:`1px solid ${T.ccBorder}` }}>CC</span>}
                                          {sess.tags?.map((tag,ti) => <span key={ti} style={{ fontSize:9, color:T.accent, fontFamily:T.mono, padding:"1px 5px", borderRadius:3, background:T.accentSoft, border:`1px solid ${T.accent}22`, fontWeight:600 }}>{tag}</span>)}
                                        </div>
                                      </div>
                                      {sess.bookingUrl && (<>
                                        <div style={{ width:6, alignSelf:"stretch", flexShrink:0, background:`radial-gradient(circle 2px at center,${T.bg} 1.5px,${film.color}22 2px) center top / 4px 7px repeat-y` }} />
                                        <a href={sess.bookingUrl} target="_blank" rel="noopener" className="book-btn" style={{ display:"flex", alignItems:"center", justifyContent:"center", padding:"0 19px 0 11px", alignSelf:"stretch", background:`${film.color}10`, textDecoration:"none", cursor:"pointer", transition:"background 0.2s", borderRadius:"0 12px 12px 0" }}>
                                          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke={film.accent} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M2 9a3 3 0 0 1 0 6v2a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-2a3 3 0 0 1 0-6V7a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2Z"/><path d="M13 5v2"/><path d="M13 17v2"/><path d="M13 11v2"/></svg>
                                        </a>
                                      </>)}
                                    </div>
                                  </div>
                                </div>
                              );
                            })}
                            {gi<groups.length-1 && (
                              <div style={{ display:"flex" }}>
                                <div style={{ width:56, display:"flex", justifyContent:"center" }}>
                                  <div style={{ width:1, height:18, background:T.mobileConnector(T.accent) }} />
                                </div>
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  );
                })()
              ) : (
                /* ═══════ DESKTOP TIMELINE ═══════ */
                <div style={{ position:"relative" }}>
                  {isAllCinemas ? (
                    <div style={{ display:"flex", height:30, marginBottom:4, borderLeft:"3px solid transparent" }}>
                      <div style={{ width:177, flexShrink:0, padding:"0 14px", borderRight:`1px solid transparent`, boxSizing:"border-box" }} />
                      <div style={{ flex:1, display:"flex" }}>
                        <div style={{ width:60, flexShrink:0, padding:"0 2px", boxSizing:"border-box" }} />
                        <div style={{ flex:1, position:"relative" }}>
                          {hourMarks.map(m => <div key={m} style={{ position:"absolute", left:`${pct(m)}%`, transform:"translateX(-50%)", fontSize:10, fontFamily:T.mono, color:T.textDim, fontWeight:400, letterSpacing:0.5 }}>{minToTime(m)}</div>)}
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div style={{ display:"flex", height:30, marginBottom:4 }}>
                      <div style={{ width:180, flexShrink:0, padding:"0 14px", borderRight:`1px solid transparent`, boxSizing:"border-box" }} />
                      <div style={{ flex:1, position:"relative" }}>
                        {hourMarks.map(m => <div key={m} style={{ position:"absolute", left:`${pct(m)}%`, transform:"translateX(-50%)", fontSize:10, fontFamily:T.mono, color:T.textDim, fontWeight:400, letterSpacing:0.5 }}>{minToTime(m)}</div>)}
                      </div>
                    </div>
                  )}
                  <div ref={tlRef} style={{ position:"relative" }}>
                    {dayFilms.map((film,fi) => {

                      /* ─── ALL CINEMAS: sub-rows per cinema ─── */
                      if (isAllCinemas && film.cinemaEntries) {
                        const isMulti = film.cinemaEntries.length > 1;
                        return (
                          <div key={film.title+fi} style={{ display:"flex", alignItems:"stretch", marginBottom:4, background:fi%2===0?T.rowEven:T.rowOdd, borderRadius:8, borderLeft:isMulti?`3px solid ${T.accent}`:"3px solid transparent" }}>
                            {/* Film label spanning all sub-rows */}
                            <div style={{ width:177, flexShrink:0, padding:"10px 14px", display:"flex", flexDirection:"column", justifyContent:"center", borderRight:`1px solid ${T.border}`, boxSizing:"border-box" }}>
                              <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:3 }}>
                                <div style={{ minWidth:0 }}>
                                  <div style={{ display:"flex", alignItems:"baseline", gap:5, minWidth:0 }}>
                                    <div style={{ fontSize:12, fontWeight:700, color:T.textSub, lineHeight:1.2, fontFamily:T.serif, whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis", minWidth:0 }}>
                                      {film.title}
                                    </div>
                                    <LbRating rating={film.letterboxd_rating} url={film.letterboxd_url} size={11} />
                                  </div>
                                  <div style={{ display:"flex", gap:5, marginTop:4, alignItems:"center", flexWrap:"wrap" }}>
                                    <span style={{ fontSize:8, padding:"1px 5px", borderRadius:3, fontWeight:700, background:rBg[film.rating]||"#444", color:"#fff", fontFamily:T.mono, letterSpacing:0.5 }}>{film.rating}</span>
                                    <span style={{ fontSize:9, color:T.textDim, fontFamily:T.mono }}>{film.runtime}m</span>
                                    {isMulti && <span style={{ fontSize:8, color:T.accent, fontFamily:T.mono, padding:"1px 5px", borderRadius:3, background:T.accentSoft, border:`1px solid ${T.accent}22`, fontWeight:700 }}>{film.cinemaEntries.length} venues</span>}
                                  </div>
                                </div>
                              </div>
                            </div>
                            {/* Sub-rows container */}
                            <div style={{ flex:1, display:"flex", flexDirection:"column", position:"relative" }}>
                              {/* Hour marks spanning all sub-rows */}
                              {hourMarks.map(m=><div key={m} style={{ position:"absolute", left:`calc(60px + (100% - 60px) * ${pct(m)/100})`, top:0, bottom:0, width:1, background:isDark?`${T.accent}0a`:`${T.accent}12`, zIndex:0, pointerEvents:"none" }} />)}
                              {film.cinemaEntries.map((ce,ci) => (
                                <div key={ce.cinemaId} style={{ display:"flex", alignItems:"stretch", flex:1, minHeight:42, borderTop:ci>0?`1px dashed ${T.border}`:"none", position:"relative", zIndex:1 }}>
                                  {/* Cinema label */}
                                  <div style={{ width:60, flexShrink:0, padding:"4px 2px", display:"flex", alignItems:"center", justifyContent:"center", boxSizing:"border-box" }}>
                                    {ce.filmUrl ? <a href={ce.filmUrl} target="_blank" rel="noopener" style={{ textDecoration:"none", display:"inline-flex" }}><span style={{ fontSize:8, fontWeight:700, color:ce.cinemaColor, fontFamily:T.mono, letterSpacing:0.3, padding:"2px 5px", borderRadius:3, background:`${ce.cinemaColor}18`, cursor:"pointer", whiteSpace:"nowrap", display:"inline-flex", alignItems:"center", gap:3 }}><span style={{ width:4, height:4, borderRadius:1.5, background:ce.cinemaColor, flexShrink:0 }} />{ce.cinemaShort}</span></a>
                                    : <span style={{ fontSize:8, fontWeight:600, color:T.textMuted, fontFamily:T.mono, letterSpacing:0.3, padding:"2px 5px", borderRadius:3, background:`${ce.cinemaColor}10`, whiteSpace:"nowrap", display:"inline-flex", alignItems:"center", gap:3 }}><span style={{ width:4, height:4, borderRadius:1.5, background:ce.cinemaColor, flexShrink:0 }} />{ce.cinemaShort}</span>}
                                  </div>
                                  {/* Bars for this cinema */}
                                  <div style={{ flex:1, position:"relative", minHeight:42 }}>
                                    {ce.sessions.map((sess,si) => {
                                      const barLeft=pct(sess.startMin), adsWidth=pct(sess.adsEnd)-barLeft, totalWidth=pct(sess.filmEnd)-barLeft;
                                      const bKey=`${film.title}-${ce.cinemaId}-${si}`, isHov=hovBar===bKey;
                                      const bc = ce.cinemaColor;
                                      return (
                                        <div key={si} className="tkt-bar" onMouseEnter={()=>setHovBar(bKey)} onMouseLeave={()=>setHovBar(null)}
                                          style={{ position:"absolute", left:`${barLeft}%`, width:`${totalWidth}%`, top:"50%", height:isHov?38:28, transform:"translateY(-50%)", display:"flex", borderRadius:4, overflow:"hidden", zIndex:isHov?10:2, transition:"height 0.2s cubic-bezier(0.4,0,0.2,1),box-shadow 0.2s cubic-bezier(0.4,0,0.2,1)",
                                            boxShadow: isHov ? `0 6px 24px ${bc}44,0 0 0 1px ${bc}55` : isDark ? `0 1px 4px rgba(0,0,0,0.3)` : `0 1px 3px rgba(0,0,0,0.08)`,
                                          }}>
                                          {sess.adsMin > 0 && <>
                                          <div className="ads-zone" style={{ width:`${(adsWidth/totalWidth)*100}%`, background:`repeating-linear-gradient(120deg,${bc}30,${bc}30 3px,${bc}18 3px,${bc}18 6px)`, display:"flex", alignItems:"center", justifyContent:"center", flexShrink:0, borderRadius:"4px 0 0 4px", overflow:"hidden", boxSizing:"border-box", paddingLeft:6, paddingRight:2 }}>
                                            <span className="ads-label" style={{ fontSize:6, fontWeight:700, color:`${bc}`, letterSpacing:1, textTransform:"uppercase", opacity:0.5, fontFamily:T.mono }}>ADS</span>
                                          </div>
                                          <div style={{ width:6, flexShrink:0, background:`radial-gradient(circle 2px at center,${T.bg} 1.5px,${bc}55 2px) center top / 4px 6px repeat-y` }} />
                                          </>}
                                          <div style={{ flex:1, background:`linear-gradient(135deg,${bc}bb 0%,${bc}88 100%)`, padding:sess.adsMin>0?"2px 10px 2px 8px":"2px 10px 2px 20px", display:"flex", alignItems:"center", gap:4, minWidth:0, borderRadius:sess.adsMin>0?"0 4px 4px 0":"4px", overflow:"hidden" }}>
                                            <div style={{ flex:1, minWidth:0 }}>
                                              <div style={{ fontSize:9.5, fontWeight:700, color:T.barText, whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis", textShadow:"0 1px 3px rgba(0,0,0,0.5)", fontFamily:T.mono }}>
                                                {sess.time} – ~{minToTime(sess.filmEnd)}{sess.isHoh?"  CC":""}
                                              </div>
                                              {isHov && <div style={{ fontSize:8, color:T.barSubText, marginTop:1, fontFamily:T.mono }}>{ce.cinemaName}{sess.screen?` · ${sess.screen}`:""}</div>}
                                            </div>
                                            {sess.bookingUrl && (
                                              <a href={sess.bookingUrl} target="_blank" rel="noopener" onClick={e=>e.stopPropagation()} className="book-btn" title={`Book at ${ce.cinemaName}`} style={{ display:"flex", alignItems:"center", justifyContent:"center", padding:"3px 6px", borderRadius:3, background:T.barBookBg, border:`1px solid ${T.barBookBorder}`, textDecoration:"none", flexShrink:0, cursor:"pointer", transition:"background 0.2s" }}>
                                                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M2 9a3 3 0 0 1 0 6v2a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-2a3 3 0 0 1 0-6V7a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2Z"/><path d="M13 5v2"/><path d="M13 17v2"/><path d="M13 11v2"/></svg>
                                              </a>
                                            )}
                                          </div>
                                        </div>
                                      );
                                    })}
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                        );
                      }

                      /* ─── SINGLE CINEMA: original timeline row ─── */
                      const anyHov = film.sessions.some((_,si)=>hovBar===`${film.id}-${si}`);
                      return (
                        <div key={film.id} style={{ display:"flex", alignItems:"stretch", marginBottom:4, background:fi%2===0?T.rowEven:T.rowOdd, borderRadius:8, minHeight:anyHov?74:56, transition:"min-height 0.25s cubic-bezier(0.4,0,0.2,1)" }}>
                          <div style={{ width:180, flexShrink:0, padding:"10px 14px", display:"flex", flexDirection:"column", justifyContent:"center", borderRight:`1px solid ${T.border}`, boxSizing:"border-box" }}>
                            <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:3 }}>
                              <div style={{ width:3, height:28, borderRadius:1.5, background:`linear-gradient(180deg,${film.color},${film.color}55)`, flexShrink:0 }} />
                              <div style={{ minWidth:0 }}>
                                <div style={{ display:"flex", alignItems:"baseline", gap:5, minWidth:0 }}>
                                  <div style={{ fontSize:12, fontWeight:700, color:T.textSub, lineHeight:1.2, fontFamily:T.serif, whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis", minWidth:0 }}>
                                    {film.film_url ? <a href={film.film_url} target="_blank" rel="noopener" style={{ color:T.textSub, textDecoration:"none" }}>{film.title}</a> : film.title}
                                  </div>
                                  <LbRating rating={film.letterboxd_rating} url={film.letterboxd_url} size={11} />
                                </div>
                                <div style={{ display:"flex", gap:5, marginTop:4, alignItems:"center" }}>
                                  <span style={{ fontSize:8, padding:"1px 5px", borderRadius:3, fontWeight:700, background:rBg[film.rating]||"#444", color:"#fff", fontFamily:T.mono, letterSpacing:0.5 }}>{film.rating}</span>
                                  <span style={{ fontSize:9, color:T.textDim, fontFamily:T.mono }}>{film.runtime}m</span>
                                  <span style={{ fontSize:9, color:T.textFaint, fontFamily:T.mono }}>{film.genre}</span>
                                </div>
                              </div>
                            </div>
                          </div>
                          <div style={{ flex:1, position:"relative", padding:"6px 0" }}>
                            {hourMarks.map(m=><div key={m} style={{ position:"absolute", left:`${pct(m)}%`, top:0, bottom:0, width:1, background:isDark?`${T.accent}0a`:`${T.accent}12`, zIndex:0 }} />)}
                            {halfMarks.map(m=><div key={m} style={{ position:"absolute", left:`${pct(m)}%`, top:0, bottom:0, width:1, background:isDark?`${T.accent}05`:`${T.accent}08`, zIndex:0 }} />)}
                            {film.sessions.map((sess,si) => {
                              const barLeft=pct(sess.startMin), adsWidth=pct(sess.adsEnd)-barLeft, totalWidth=pct(sess.filmEnd)-barLeft;
                              const bKey=`${film.id}-${si}`, isHov=hovBar===bKey;
                              return (
                                <div key={si} className="tkt-bar" onMouseEnter={()=>setHovBar(bKey)} onMouseLeave={()=>setHovBar(null)}
                                  style={{ position:"absolute", left:`${barLeft}%`, width:`${totalWidth}%`, top:"50%", height:isHov?54:36, transform:"translateY(-50%)", display:"flex", borderRadius:5, overflow:"hidden", zIndex:isHov?10:2, transition:"height 0.2s cubic-bezier(0.4,0,0.2,1),box-shadow 0.2s cubic-bezier(0.4,0,0.2,1)",
                                    boxShadow: isHov ? `0 8px 32px ${film.color}44,0 0 0 1px ${film.color}55,inset 0 1px 0 rgba(255,255,255,0.06)` : isDark ? `0 1px 6px rgba(0,0,0,0.3),inset 0 1px 0 rgba(255,255,255,0.03)` : `0 1px 4px rgba(0,0,0,0.08),inset 0 1px 0 rgba(255,255,255,0.5)`,
                                  }}>
                                  {sess.adsMin > 0 && <>
                                  <div className="ads-zone" style={{ width:`${(adsWidth/totalWidth)*100}%`, background:`repeating-linear-gradient(120deg,${film.color}30,${film.color}30 3px,${film.color}18 3px,${film.color}18 6px)`, display:"flex", alignItems:"center", justifyContent:"center", flexShrink:0, borderRadius:"5px 0 0 5px", overflow:"hidden", boxSizing:"border-box", paddingLeft:7, paddingRight:2 }}>
                                    <span className="ads-label" style={{ fontSize:7, fontWeight:700, color:film.accent, letterSpacing:1, textTransform:"uppercase", opacity:0.6, fontFamily:T.mono }}>ADS</span>
                                  </div>
                                  <div style={{ width:8, flexShrink:0, position:"relative", zIndex:3, background:`radial-gradient(circle 2.5px at center,${T.bg} 2px,${film.color}55 2.5px) center top / 5px 8px repeat-y` }} />
                                  </>}
                                  <div style={{ flex:1, background:`linear-gradient(135deg,${film.color}bb 0%,${film.color}88 100%)`, padding:sess.adsMin>0?"4px 10px":"4px 10px 4px 20px", display:"flex", alignItems:"center", gap:6, minWidth:0, borderRadius:sess.adsMin>0?"0 5px 5px 0":"5px", overflow:"hidden" }}>
                                    <div style={{ flex:1, minWidth:0 }}>
                                      <div style={{ fontSize:10.5, fontWeight:700, color:T.barText, whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis", textShadow:"0 1px 4px rgba(0,0,0,0.5)", fontFamily:T.mono }}>
                                        {sess.time} – ~{minToTime(sess.filmEnd)}{sess.isHoh?"  CC":""}{sess.tags?.length?`  ${sess.tags.join(" · ")}`:""}
                                      </div>
                                      {isHov && <div style={{ fontSize:9, color:T.barSubText, marginTop:3, fontFamily:T.mono }}>{film.runtime}min{sess.adsMin>0?` + ~${sess.adsMin}min ads`:""}{sess.screen?` · ${sess.screen}`:""}</div>}
                                    </div>
                                    {sess.bookingUrl && (
                                      <a href={sess.bookingUrl} target="_blank" rel="noopener" onClick={e=>e.stopPropagation()} className="book-btn" title="Book tickets" style={{ display:"flex", alignItems:"center", justifyContent:"center", padding:"4px 8px", borderRadius:4, background:T.barBookBg, border:`1px solid ${T.barBookBorder}`, textDecoration:"none", flexShrink:0, cursor:"pointer", transition:"background 0.2s" }}>
                                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M2 9a3 3 0 0 1 0 6v2a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-2a3 3 0 0 1 0-6V7a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2Z"/><path d="M13 5v2"/><path d="M13 17v2"/><path d="M13 11v2"/></svg>
                                      </a>
                                    )}
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                  {selDate===today&&(()=>{ const nowMin=getNowMin(); if(nowMin>=axisStart&&nowMin<=axisEnd){ const tlOffset=isAllCinemas?240:180; return (
                    <div style={{ position:"absolute", left:`calc(${tlOffset}px + (100% - ${tlOffset}px) * ${(nowMin-axisStart)/axisDuration})`, top:30, bottom:0, width:2, background:T.accent, zIndex:20, boxShadow:`0 0 12px ${T.accentGlow}` }}>
                      <div style={{ position:"absolute", top:-8, left:-14, fontSize:8, fontWeight:700, color:T.accent, background:T.bg, padding:"1px 5px", borderRadius:3, fontFamily:T.mono, letterSpacing:1 }}>NOW</div>
                    </div>
                  ); } return null; })()}
                </div>
              )}
            </>
          ) : (
            /* ═══════ GRID VIEW ═══════ */
            <>
              <div style={{ overflowX:"auto", borderRadius:8, border:`1px solid ${T.border}` }}>
                <table style={{ width:"100%", borderCollapse:"separate", borderSpacing:0, minWidth:400 }}>
                  <thead>
                    <tr>
                      <th style={{ padding:"12px 14px", textAlign:"left", fontSize:10, color:T.textDim, fontWeight:600, borderBottom:`1px solid ${T.border}`, position:"sticky", left:0, background:T.surface, zIndex:5, width:160, maxWidth:180, boxShadow:T.stickyShadow, fontFamily:T.mono, letterSpacing:1, textTransform:"uppercase" }}>Film</th>
                      {(selWeek?selWeek.dates:allDates).map(d => {
                        const info=formatDayTab(d), isT=d===today;
                        return (
                          <th key={d} onClick={()=>{setSelDate(d);setView("day");}} style={{ padding:"10px 6px", textAlign:"center", fontSize:10, fontWeight:600, borderBottom:`1px solid ${T.border}`, cursor:"pointer", color:isT?T.accent:T.textDim, background:isT?T.accentSoft:"transparent", borderBottomColor:isT?`${T.accent}55`:T.border, minWidth:72, transition:"color 0.2s", fontFamily:T.mono }}>
                            <div style={{ letterSpacing:1 }}>{info.day}</div>
                            <div style={{ fontSize:18, fontWeight:700, lineHeight:1.1, fontFamily:T.serif, color:isT?T.accent:T.text }}>{info.num}</div>
                            <div style={{ fontSize:9, color:T.textFaint, letterSpacing:1 }}>{info.mon}</div>
                          </th>
                        );
                      })}
                    </tr>
                  </thead>
                  <tbody>
                    {(()=>{ const weekDates=selWeek?selWeek.dates:allDates;
                      const sortedFilms = isAllCinemas
                        ? [...films].filter(f=>weekDates.some(d=>f.showtimes[d])).sort((a,b) => {
                            const aCnt = Object.keys(a.perCinema||{}).length, bCnt = Object.keys(b.perCinema||{}).length;
                            return bCnt - aCnt || a.title.localeCompare(b.title);
                          })
                        : films.filter(f=>weekDates.some(d=>f.showtimes[d]));
                      return sortedFilms.map((film,fi)=>(
                      <tr key={film.title+fi} style={{ background:fi%2===0?T.rowEven:T.rowOdd }}>
                        <td style={{ padding:"10px 12px", borderBottom:`1px solid ${T.gridCellBorder}`, position:"sticky", left:0, background:fi%2===0?T.gridStickyBg1:T.gridStickyBg2, zIndex:4, boxShadow:T.stickyShadow, width:160, maxWidth:180 }}>
                          <div style={{ display:"flex", alignItems:"center", gap:7, maxWidth:160 }}>
                            {!isAllCinemas && <div style={{ width:3, height:24, borderRadius:1.5, background:`linear-gradient(180deg,${film.color},${film.color}44)` }} />}
                            {isAllCinemas && (()=>{
                              const cinIds = Object.keys(film.perCinema||{});
                              return <div style={{ display:"flex", flexDirection:"column", gap:3, alignItems:"center", justifyContent:"center" }}>
                                {cinIds.slice(0,3).map(cId => {
                                  const cin = CINEMA_MAP[cId];
                                  const bc = cin?.barColor||"#888";
                                  const filmUrl = film.perCinema[cId]?.film_url;
                                  const pill = <span key={cId} style={{ fontSize:7, fontWeight:700, color:bc, fontFamily:T.mono, letterSpacing:0.3, lineHeight:1, padding:"2px 4px", borderRadius:2, background:`${bc}18`, cursor:filmUrl?"pointer":"default", whiteSpace:"nowrap" }}>{cin?.short||cId.slice(0,3).toUpperCase()}</span>;
                                  return filmUrl ? <a key={cId} href={filmUrl} target="_blank" rel="noopener" style={{ textDecoration:"none", display:"block" }}>{pill}</a> : pill;
                                })}
                                {cinIds.length>3 && <div style={{ fontSize:7, color:T.textFaint, fontFamily:T.mono }}>+{cinIds.length-3}</div>}
                              </div>;
                            })()}
                            <div style={{ minWidth:0 }}>
                              <div style={{ display:"flex", alignItems:"baseline", gap:4, minWidth:0 }}>
                                <div style={{ fontSize:11, fontWeight:700, color:T.textSub, fontFamily:T.serif, whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis", minWidth:0 }}>{!isAllCinemas&&film.film_url?<a href={film.film_url} target="_blank" rel="noopener" style={{ color:T.textSub, textDecoration:"none" }}>{film.title}</a>:film.title}</div>
                                <LbRating rating={film.letterboxd_rating} url={film.letterboxd_url} size={10} />
                              </div>
                              <div style={{ display:"flex", gap:4, marginTop:3, flexWrap:"wrap" }}>
                                <span style={{ fontSize:8, padding:"0px 5px", borderRadius:2, background:rBg[film.rating]||"#444", color:"#fff", fontWeight:700, fontFamily:T.mono }}>{film.rating}</span>
                                <span style={{ fontSize:9, color:T.textFaint, fontFamily:T.mono }}>{film.runtime}m</span>
                                {isAllCinemas && Object.keys(film.perCinema||{}).length>1 && <span style={{ fontSize:8, color:T.accent, fontFamily:T.mono, padding:"0px 4px", borderRadius:2, background:T.accentSoft, fontWeight:700 }}>{Object.keys(film.perCinema).length} venues</span>}
                              </div>
                            </div>
                          </div>
                        </td>
                        {weekDates.map(d => {
                          const times=film.showtimes[d], isToday=d===today;
                          return (
                            <td key={d} className="tkt-cell" style={{ padding:"6px 6px", textAlign:"center", borderBottom:`1px solid ${T.gridCellBorder}`, borderLeft:`1px solid ${T.gridCellBorder}`, background:isToday?T.accentSoft:"transparent" }}>
                              {times ? (
                                <div style={{ display:"flex", flexDirection:"column", gap:5, alignItems:"center" }}>
                                  {isAllCinemas ? (
                                    /* All-cinemas grid: group pills by cinema */
                                    Object.entries(film.perCinema||{}).filter(([,pc])=>pc.showtimes[d]).map(([cId,pc]) => {
                                      const cin = CINEMA_MAP[cId];
                                      const bc = cin?.barColor||"#888";
                                      return pc.showtimes[d].map((t,i) => {
                                        const bookingUrl=pc.bookingUrls?.[d]?.[t];
                                        const pill = <span key={`${cId}-${i}`} className="tkt-pill" style={{ fontSize:10, fontWeight:600, padding:"3px 8px", borderRadius:4, background:`${bc}${T.pillBgAlpha}`, border:`1.5px solid ${bc}${T.pillBorderAlpha}`, color:isDark?`${bc}`:`${bc}`, fontFamily:T.mono, whiteSpace:"nowrap", transition:"all 0.2s", cursor:bookingUrl?"pointer":"default", display:"inline-flex", alignItems:"center", gap:3 }} title={`${cin?.name||cId}`}>
                                          <span style={{ width:5, height:5, borderRadius:2, background:bc, flexShrink:0 }} />{t}
                                        </span>;
                                        return bookingUrl ? <a key={`${cId}-${i}`} href={bookingUrl} target="_blank" rel="noopener" style={{ textDecoration:"none" }}>{pill}</a> : pill;
                                      });
                                    })
                                  ) : (
                                    /* Single cinema grid: original pills */
                                    times.map((t,i) => {
                                      const isHoh=film.hoh?.[d]?.includes(t), bookingUrl=film.bookingUrls?.[d]?.[t], sessTags=film.tags?.[d]?.[t]||[];
                                      const pill = <span className="tkt-pill" style={{ fontSize:11, fontWeight:600, padding:"4px 12px", borderRadius:4, background:`${film.color}${T.pillBgAlpha}`, border:`1.5px solid ${film.color}${T.pillBorderAlpha}`, color:isDark?film.accent:film.color, fontFamily:T.mono, whiteSpace:"nowrap", transition:"all 0.2s", cursor:bookingUrl?"pointer":"default", display:"inline-flex", alignItems:"center", gap:3 }} title={film.screens?.[d]?.[t]?`${film.screens[d][t]}${isHoh?" · HoH":""}`:(isHoh?"Hard of Hearing":"")}>{t}{isHoh?" CC":""}</span>;
                                      return (
                                        <div key={i} style={{ display:"flex", flexDirection:"column", alignItems:"center", gap:3 }}>
                                          {bookingUrl ? <a href={bookingUrl} target="_blank" rel="noopener" style={{ textDecoration:"none" }}>{pill}</a> : pill}
                                          {sessTags.length>0 && (
                                            <div style={{ display:"flex", gap:2, flexWrap:"wrap", justifyContent:"center", maxWidth:100 }}>
                                              {sessTags.slice(0,2).map((tag,ti) => <span key={ti} style={{ fontSize:7, color:T.textDim, fontFamily:T.mono, padding:"1px 4px", borderRadius:2, background:`${film.color}${T.pillBgAlpha}`, whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis", maxWidth:90 }}>{tag}</span>)}
                                              {sessTags.length>2 && <span style={{ fontSize:7, color:T.textFaint, fontFamily:T.mono }}>+{sessTags.length-2}</span>}
                                            </div>
                                          )}
                                        </div>
                                      );
                                    })
                                  )}
                                </div>
                              ) : <span style={{ color:T.gridDashColor, fontSize:14 }}>—</span>}
                            </td>
                          );
                        })}
                      </tr>
                    )); })()}
                  </tbody>
                </table>
              </div>
              <p style={{ fontSize:10, color:T.textFaint, marginTop:10, fontFamily:T.mono, letterSpacing:0.3 }}>Click any time to book directly. Click a date header to switch to day view.</p>
            </>
          )}

          {/* ═══════ LEGEND ═══════ */}
          <div style={{ marginTop:24, padding:"16px 18px", borderRadius:8, background:isDark?"linear-gradient(135deg,rgba(212,160,83,0.02) 0%,transparent 100%)":"linear-gradient(135deg,rgba(160,116,48,0.03) 0%,transparent 100%)", border:`1px solid ${T.border}` }}>
            {!isMobile && (
              <div style={{ display:"flex", gap:18, flexWrap:"wrap", fontSize:10, color:T.textDim, fontFamily:T.mono }}>
                <div style={{ display:"flex", alignItems:"center", gap:5 }}>
                  <div style={{ width:22, height:10, borderRadius:2, background:`repeating-linear-gradient(120deg,${T.accent}30,${T.accent}30 3px,${T.accent}18 3px,${T.accent}18 6px)` }} />
                  <span>= ~{DEFAULT_ADS_MIN}min ads/trailers</span>
                </div>
                <span>CC = Subtitled (Hard of Hearing)</span>
                <span>Bar length = full session (ads + film)</span>
              </div>
            )}
            <div style={{ display:"flex", gap:14, flexWrap:"wrap", fontSize:10, color:T.textDim, fontFamily:T.mono, marginTop:isMobile?0:8 }}>
              {isMobile && <span>CC = Hard of Hearing</span>}
              <span style={{ display:"flex", alignItems:"center", gap:4 }}>
                <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke={T.textMuted} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M2 9a3 3 0 0 1 0 6v2a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-2a3 3 0 0 1 0-6V7a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2Z"/><path d="M13 5v2"/><path d="M13 17v2"/><path d="M13 11v2"/></svg>
                = Book tickets
              </span>
            </div>
            {scrapedAt && (
              <div style={{ fontSize:9, color:T.textFaint, marginTop:8, fontFamily:T.mono, paddingTop:8, borderTop:`1px solid ${T.border}`, letterSpacing:0.3 }}>
                Data from {isAllCinemas ? `${CINEMAS.length} cinemas` : (cinema.source || cinema.url.replace(/^https?:\/\/(www\.)?/,""))} · Updated {new Date(scrapedAt).toLocaleString("en-GB",{dateStyle:"medium",timeStyle:"short"})} · Always confirm at the cinema
              </div>
            )}
          </div>
        </div>

      </div>
    </div>
  );
}
