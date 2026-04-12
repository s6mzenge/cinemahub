import { useState, useMemo, useRef, useEffect } from "react";

const ADS_MIN = 20;
const DATA_URL = import.meta.env.BASE_URL + "data/films.json";

const DAYS = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const rBg = { U:"#2e7d32", PG:"#b8960f", "12A":"#c45a00", "15":"#a12020", TBC:"#666", very:"#b8960f" };

/* ─── Cinema registry (extend this later) ─── */
const CINEMAS = [
  { id:"peckhamplex", name:"Peckhamplex", address:"95a Rye Lane, Peckham", url:"https://www.peckhamplex.london", price:"£7.59" },
];

function timeToMin(t){ const [h,m]=t.split(":").map(Number); return h*60+m; }
function minToTime(m){ return `${Math.floor(m/60)}:${String(m%60).padStart(2,"0")}`; }
function getToday(){ const d=new Date(); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}-${String(d.getDate()).padStart(2,"0")}`; }
function getNowMin(){ const d=new Date(); return d.getHours()*60+d.getMinutes(); }

function getAllDatesWithScreenings(films) {
  const s = new Set();
  films.forEach(f => { if (f.showtimes) Object.keys(f.showtimes).forEach(d => s.add(d)); });
  return [...s].sort();
}

function formatDayTab(dateStr) {
  const d = new Date(dateStr+"T12:00:00");
  return { day:DAYS[d.getDay()], num:d.getDate(), mon:MONTHS[d.getMonth()], full:dateStr };
}

function normalizeFilms(rawFilms) {
  return rawFilms.map(f => {
    const showtimes={}, bookingUrls={}, screens={}, hoh={};
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
          }
        });
        showtimes[date] = times;
      }
    }
    return { id:f.id, title:f.title, rating:f.rating||"TBC", runtime:f.runtime||90, genre:f.genre||"Other",
      color:f.color||"#78909c", accent:f.accent||"#b0bec5", film_url:f.film_url||null, poster_url:f.poster_url||null,
      showtimes, bookingUrls, screens, hoh:Object.keys(hoh).length>0?hoh:(f.hoh||{}) };
  });
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
  const [selCinema, setSelCinema] = useState(CINEMAS[0].id);

  const cinema = CINEMAS.find(c => c.id === selCinema) || CINEMAS[0];

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
    fetch(DATA_URL)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(data => {
        const normalized = normalizeFilms(data.films || []);
        setFilms(normalized); setScrapedAt(data.scraped_at || null);
        const allDates = getAllDatesWithScreenings(normalized);
        if (allDates.length > 0 && !allDates.includes(today)) { const future = allDates.find(d => d >= today); setSelDate(future || allDates[0]); }
        setLoading(false);
      })
      .catch(err => { setError(err.message); setLoading(false); });
  }, []);

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
    return films.filter(f => f.showtimes[selDate]).map(f => ({
      ...f, times:f.showtimes[selDate],
      sessions: f.showtimes[selDate].map(t => ({
        time:t, startMin:timeToMin(t), adsEnd:timeToMin(t)+ADS_MIN, filmEnd:timeToMin(t)+ADS_MIN+f.runtime,
        isHoh:f.hoh?.[selDate]?.includes(t), bookingUrl:f.bookingUrls?.[selDate]?.[t]||null, screen:f.screens?.[selDate]?.[t]||null,
      })),
    }));
  }, [selDate, films]);

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
              {/* Cinema icon */}
              <div style={{
                width:32, height:32, borderRadius:8, flexShrink:0,
                background: isActive ? T.accentMed : (isDark ? "rgba(255,255,255,0.03)" : "rgba(0,0,0,0.03)"),
                border:`1px solid ${isActive ? T.accent+"44" : T.border}`,
                display:"flex", alignItems:"center", justifyContent:"center",
                fontSize:14,
              }}>🎬</div>
              <div>
                <div>{c.name}</div>
                <div style={{ fontSize:10, color:T.textDim, fontWeight:400, marginTop:1 }}>{c.address}</div>
              </div>
            </button>
          );
        })}

        {/* Placeholder for future cinemas */}
        <button style={{
          display:"flex", alignItems:"center", gap:10, width:"100%",
          padding:"10px 12px", borderRadius:8, border:`1px dashed ${T.border}`, cursor:"default",
          fontFamily:T.mono, fontSize:10, textAlign:"left", marginTop:4,
          background:"transparent", color:T.textFaint, letterSpacing:0.5,
        }}>
          <div style={{
            width:32, height:32, borderRadius:8, flexShrink:0,
            border:`1px dashed ${T.border}`,
            display:"flex", alignItems:"center", justifyContent:"center",
            fontSize:14, opacity:0.4,
          }}>+</div>
          <div>More cinemas soon</div>
        </button>
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
        <div style={{ color:"#c0392b", fontSize:14, marginBottom:8 }}>Failed to load timetable data</div>
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
        *::-webkit-scrollbar { height:4px; width:4px; }
        *::-webkit-scrollbar-track { background:${T.bg}; }
        *::-webkit-scrollbar-thumb { background:${T.border}; border-radius:2px; }
        .view-btn:hover { border-color:${T.accent} !important; color:${T.accent} !important; }
        .book-btn:hover { background:${T.barBookHover} !important; }
        .book-btn:active { opacity:0.7; transform:scale(0.95); }

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
      `}</style>

      <Overlay />
      <Sidebar />

      {/* ═══════ MAIN CONTENT ═══════ */}
      <div style={{
        flex:1, position:"relative", zIndex:1, minWidth:0,
        marginLeft: isMobile ? 0 : 0, /* sidebar is sticky, content flows naturally */
      }}>

        {/* ═══════ HEADER ═══════ */}
        <div style={{ background:T.headerBg, padding:"20px 24px 18px", borderBottom:`1px solid ${T.accent}33` }}>
          <div style={{ maxWidth:1000, margin:"0 auto" }}>
            <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", flexWrap:"wrap", gap:10 }}>
              <div style={{ display:"flex", alignItems:"center", gap:12 }}>
                <HamburgerBtn />
                <div>
                  <div style={{ fontSize:9, letterSpacing:3, textTransform:"uppercase", color:T.accent, fontFamily:T.mono, fontWeight:700, opacity:0.6, marginBottom:2 }}>Now Showing</div>
                  <h1 style={{ fontFamily:T.serif, fontSize:26, fontWeight:900, margin:0, letterSpacing:"-0.3px", lineHeight:1, color:T.text }}>
                    {cinema.name}
                  </h1>
                </div>
              </div>
              <div style={{ display:"flex", alignItems:"center", gap:10, flexWrap:"wrap" }}>
                <div style={{
                  display:"inline-flex", alignItems:"center", gap:6,
                  padding:"5px 14px", borderRadius:20,
                  background:T.accentSoft, border:`1px solid ${T.accent}22`,
                }}>
                  <div style={{ width:5, height:5, borderRadius:"50%", background:T.accent, animation:"goldPulse 2.5s ease infinite" }} />
                  <span style={{ fontSize:13, color:T.accent, fontWeight:700, fontFamily:T.mono }}>{cinema.price}</span>
                  <span style={{ fontSize:10, color:`${T.accent}88` }}>all tickets</span>
                </div>
                <a href={cinema.url} target="_blank" rel="noopener" style={{ color:T.textMuted, textDecoration:"none", fontSize:11, fontFamily:T.mono, letterSpacing:0.5 }}>
                  {cinema.address} ↗
                </a>
              </div>
            </div>
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
                  <div style={{ fontSize:10, color:T.textDim, fontFamily:T.mono, marginTop:2 }}>{selDate===today?"Today · ":""}{dayFilms.length} film{dayFilms.length!==1?"s":""}</div>
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
                  const allSessions=[]; dayFilms.forEach(film=>{film.sessions.forEach(sess=>{allSessions.push({...sess,film});});});
                  allSessions.sort((a,b)=>a.startMin-b.startMin);
                  const groups=[]; allSessions.forEach(sess=>{ const last=groups[groups.length-1]; if(last&&last.time===sess.time) last.sessions.push(sess); else groups.push({time:sess.time,startMin:sess.startMin,sessions:[sess]}); });
                  const nowMin = selDate===today?getNowMin():null;
                  return (
                    <div style={{ display:"flex", flexDirection:"column", gap:0 }}>
                      {groups.map((group,gi) => {
                        const isPast = nowMin!==null && group.startMin+ADS_MIN<nowMin;
                        return (
                          <div key={group.time+gi} style={{ display:"flex", gap:0, opacity:isPast?0.35:1, transition:"opacity 0.3s" }}>
                            <div style={{ width:56, flexShrink:0, display:"flex", flexDirection:"column", alignItems:"center", position:"relative" }}>
                              <div style={{ fontSize:13, fontWeight:700, color:isPast?T.textFaint:T.accent, fontFamily:T.mono, padding:"4px 0", zIndex:2, background:T.bg }}>{group.time}</div>
                              {gi<groups.length-1 && <div style={{ width:1, flex:1, background:T.mobileConnector(T.accent), minHeight:8 }} />}
                            </div>
                            <div style={{ flex:1, display:"flex", flexDirection:"column", gap:8, paddingBottom:18 }}>
                              {group.sessions.map((sess,si) => {
                                const film=sess.film;
                                return (
                                  <div key={`${film.id}-${si}`} className="tkt-card" style={{ display:"flex", alignItems:"center", gap:0, borderRadius:12, border:`1px solid ${T.cardBorder(film.color)}`, background:T.cardBg(film.color) }}>
                                    <div style={{ width:4, alignSelf:"stretch", background:`linear-gradient(180deg,${film.color},${film.color}66)`, flexShrink:0, borderRadius:"12px 0 0 12px" }} />
                                    <div style={{ flex:1, padding:"11px 14px" }}>
                                      <div style={{ fontSize:14, fontWeight:700, color:T.text, lineHeight:1.25, fontFamily:T.serif }}>
                                        {film.film_url ? <a href={film.film_url} target="_blank" rel="noopener" style={{ color:T.text, textDecoration:"none" }}>{film.title}</a> : film.title}
                                      </div>
                                      <div style={{ display:"flex", gap:6, marginTop:5, alignItems:"center", flexWrap:"wrap" }}>
                                        <span style={{ fontSize:9, padding:"2px 6px", borderRadius:3, fontWeight:700, background:rBg[film.rating]||"#444", color:"#fff", fontFamily:T.mono, letterSpacing:0.5 }}>{film.rating}</span>
                                        <span style={{ fontSize:10, color:T.textMuted, fontFamily:T.mono }}>{film.runtime}min</span>
                                        <span style={{ fontSize:10, color:T.textDim, fontFamily:T.mono }}>ends {minToTime(sess.startMin+film.runtime)}</span>
                                        {sess.screen && <span style={{ fontSize:10, color:T.textDim, fontFamily:T.mono }}>{sess.screen}</span>}
                                        {sess.isHoh && <span style={{ fontSize:9, color:T.textMuted, fontFamily:T.mono, padding:"1px 4px", borderRadius:3, background:T.ccBg, border:`1px solid ${T.ccBorder}` }}>CC</span>}
                                      </div>
                                    </div>
                                    {sess.bookingUrl && (<>
                                      <div style={{ width:6, alignSelf:"stretch", flexShrink:0, background:`radial-gradient(circle 2px at center,${T.bg} 1.5px,${film.color}22 2px) center top / 4px 7px repeat-y` }} />
                                      <a href={sess.bookingUrl} target="_blank" rel="noopener" className="book-btn" style={{ display:"flex", alignItems:"center", justifyContent:"center", padding:"0 14px", alignSelf:"stretch", background:`${film.color}10`, textDecoration:"none", cursor:"pointer", transition:"background 0.2s", borderRadius:"0 12px 12px 0" }}>
                                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke={film.accent} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M2 9a3 3 0 0 1 0 6v2a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-2a3 3 0 0 1 0-6V7a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2Z"/><path d="M13 5v2"/><path d="M13 17v2"/><path d="M13 11v2"/></svg>
                                      </a>
                                    </>)}
                                  </div>
                                );
                              })}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  );
                })()
              ) : (
                /* ═══════ DESKTOP TIMELINE ═══════ */
                <div style={{ position:"relative" }}>
                  <div style={{ marginLeft:180, position:"relative", height:30, marginBottom:4 }}>
                    {hourMarks.map(m => <div key={m} style={{ position:"absolute", left:`${pct(m)}%`, transform:"translateX(-50%)", fontSize:10, fontFamily:T.mono, color:T.textDim, fontWeight:400, letterSpacing:0.5 }}>{minToTime(m)}</div>)}
                  </div>
                  <div ref={tlRef} style={{ position:"relative" }}>
                    {dayFilms.map((film,fi) => {
                      const anyHov = film.sessions.some((_,si)=>hovBar===`${film.id}-${si}`);
                      return (
                        <div key={film.id} style={{ display:"flex", alignItems:"stretch", marginBottom:4, background:fi%2===0?T.rowEven:T.rowOdd, borderRadius:8, minHeight:anyHov?74:56, transition:"min-height 0.25s cubic-bezier(0.4,0,0.2,1)" }}>
                          <div style={{ width:180, flexShrink:0, padding:"10px 14px", display:"flex", flexDirection:"column", justifyContent:"center", borderRight:`1px solid ${T.border}` }}>
                            <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:3 }}>
                              <div style={{ width:3, height:28, borderRadius:1.5, background:`linear-gradient(180deg,${film.color},${film.color}55)`, flexShrink:0 }} />
                              <div>
                                <div style={{ fontSize:12, fontWeight:700, color:T.textSub, lineHeight:1.2, fontFamily:T.serif }}>
                                  {film.film_url ? <a href={film.film_url} target="_blank" rel="noopener" style={{ color:T.textSub, textDecoration:"none" }}>{film.title}</a> : film.title}
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
                                  <div style={{ width:`${(adsWidth/totalWidth)*100}%`, background:`repeating-linear-gradient(120deg,${film.color}30,${film.color}30 3px,${film.color}18 3px,${film.color}18 6px)`, display:"flex", alignItems:"center", justifyContent:"center", flexShrink:0, borderRadius:"5px 0 0 5px", overflow:"hidden" }}>
                                    <span style={{ fontSize:7, fontWeight:700, color:film.accent, letterSpacing:1, textTransform:"uppercase", opacity:0.6, fontFamily:T.mono }}>ADS</span>
                                  </div>
                                  <div style={{ width:8, flexShrink:0, position:"relative", zIndex:3, background:`radial-gradient(circle 2.5px at center,${T.bg} 2px,${film.color}55 2.5px) center top / 5px 8px repeat-y` }} />
                                  <div style={{ flex:1, background:`linear-gradient(135deg,${film.color}bb 0%,${film.color}88 100%)`, padding:"4px 10px", display:"flex", alignItems:"center", gap:6, minWidth:0, borderRadius:"0 5px 5px 0", overflow:"hidden" }}>
                                    <div style={{ flex:1, minWidth:0 }}>
                                      <div style={{ fontSize:10.5, fontWeight:700, color:T.barText, whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis", textShadow:"0 1px 4px rgba(0,0,0,0.5)", fontFamily:T.mono }}>
                                        {sess.time} – {minToTime(sess.startMin+film.runtime)}{sess.isHoh?"  CC":""}
                                      </div>
                                      {isHov && <div style={{ fontSize:9, color:T.barSubText, marginTop:3, fontFamily:T.mono }}>Ends ~{minToTime(sess.filmEnd)} with ads{sess.screen?` · ${sess.screen}`:""}</div>}
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
                  {selDate===today&&(()=>{ const nowMin=getNowMin(); if(nowMin>=axisStart&&nowMin<=axisEnd){ return (
                    <div style={{ position:"absolute", left:`calc(180px + ${((nowMin-axisStart)/axisDuration)*100}% * (100% - 180px) / 100%)`, top:30, bottom:0, width:2, background:T.accent, zIndex:20, boxShadow:`0 0 12px ${T.accentGlow}` }}>
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
                      <th style={{ padding:"12px 14px", textAlign:"left", fontSize:10, color:T.textDim, fontWeight:600, borderBottom:`1px solid ${T.border}`, position:"sticky", left:0, background:T.surface, zIndex:5, minWidth:150, boxShadow:T.stickyShadow, fontFamily:T.mono, letterSpacing:1, textTransform:"uppercase" }}>Film</th>
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
                    {(()=>{ const weekDates=selWeek?selWeek.dates:allDates; return films.filter(f=>weekDates.some(d=>f.showtimes[d])).map((film,fi)=>(
                      <tr key={film.id} style={{ background:fi%2===0?T.rowEven:T.rowOdd }}>
                        <td style={{ padding:"10px 12px", borderBottom:`1px solid ${T.gridCellBorder}`, position:"sticky", left:0, background:fi%2===0?T.gridStickyBg1:T.gridStickyBg2, zIndex:4, boxShadow:T.stickyShadow }}>
                          <div style={{ display:"flex", alignItems:"center", gap:7 }}>
                            <div style={{ width:3, height:24, borderRadius:1.5, background:`linear-gradient(180deg,${film.color},${film.color}44)` }} />
                            <div>
                              <div style={{ fontSize:11, fontWeight:700, color:T.textSub, fontFamily:T.serif }}>{film.film_url?<a href={film.film_url} target="_blank" rel="noopener" style={{ color:T.textSub, textDecoration:"none" }}>{film.title}</a>:film.title}</div>
                              <div style={{ display:"flex", gap:4, marginTop:3 }}><span style={{ fontSize:8, padding:"0px 5px", borderRadius:2, background:rBg[film.rating]||"#444", color:"#fff", fontWeight:700, fontFamily:T.mono }}>{film.rating}</span><span style={{ fontSize:9, color:T.textFaint, fontFamily:T.mono }}>{film.runtime}m</span></div>
                            </div>
                          </div>
                        </td>
                        {weekDates.map(d => {
                          const times=film.showtimes[d], isToday=d===today;
                          return (
                            <td key={d} className="tkt-cell" style={{ padding:"6px 6px", textAlign:"center", borderBottom:`1px solid ${T.gridCellBorder}`, borderLeft:`1px solid ${T.gridCellBorder}`, background:isToday?T.accentSoft:"transparent" }}>
                              {times ? (
                                <div style={{ display:"flex", flexDirection:"column", gap:5, alignItems:"center" }}>
                                  {times.map((t,i) => {
                                    const isHoh=film.hoh?.[d]?.includes(t), bookingUrl=film.bookingUrls?.[d]?.[t];
                                    const pill = <span key={i} className="tkt-pill" style={{ fontSize:11, fontWeight:600, padding:"4px 12px", borderRadius:4, background:`${film.color}${T.pillBgAlpha}`, border:`1.5px solid ${film.color}${T.pillBorderAlpha}`, color:isDark?film.accent:film.color, fontFamily:T.mono, whiteSpace:"nowrap", transition:"all 0.2s", cursor:bookingUrl?"pointer":"default", display:"inline-flex", alignItems:"center", gap:3 }} title={film.screens?.[d]?.[t]?`${film.screens[d][t]}${isHoh?" · HoH":""}`:(isHoh?"Hard of Hearing":"")}>{t}{isHoh?" CC":""}</span>;
                                    return bookingUrl ? <a key={i} href={bookingUrl} target="_blank" rel="noopener" style={{ textDecoration:"none" }}>{pill}</a> : pill;
                                  })}
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
                  <span>= ~{ADS_MIN}min ads/trailers</span>
                </div>
                <span>CC = Subtitled (Hard of Hearing)</span>
                <span>Bar length = full session (ads + film)</span>
              </div>
            )}
            <div style={{ display:"flex", gap:14, flexWrap:"wrap", fontSize:10, color:T.textDim, fontFamily:T.mono, marginTop:isMobile?0:undefined }}>
              {isMobile && <span>CC = Hard of Hearing</span>}
              <span style={{ display:"flex", alignItems:"center", gap:4 }}>
                <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke={T.textMuted} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M2 9a3 3 0 0 1 0 6v2a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-2a3 3 0 0 1 0-6V7a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2Z"/><path d="M13 5v2"/><path d="M13 17v2"/><path d="M13 11v2"/></svg>
                = Book tickets
              </span>
            </div>
            {scrapedAt && (
              <div style={{ fontSize:9, color:T.textFaint, marginTop:8, fontFamily:T.mono, paddingTop:8, borderTop:`1px solid ${T.border}`, letterSpacing:0.3 }}>
                Data from peckhamplex.london · Updated {new Date(scrapedAt).toLocaleString("en-GB",{dateStyle:"medium",timeStyle:"short"})} · Always confirm at the cinema
              </div>
            )}
          </div>
        </div>

      </div>
    </div>
  );
}
