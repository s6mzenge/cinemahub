import { useState, useMemo, useRef, useEffect } from "react";

const ADS_MIN = 20;
const DATA_URL = import.meta.env.BASE_URL + "data/films.json";

const DAYS = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const rBg = { U:"#2e7d32", PG:"#b8960f", "12A":"#c45a00", "15":"#a12020", TBC:"#444", very:"#b8960f" };

function timeToMin(t){ const [h,m]=t.split(":").map(Number); return h*60+m; }
function minToTime(m){ return `${Math.floor(m/60)}:${String(m%60).padStart(2,"0")}`; }

function getToday() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}-${String(d.getDate()).padStart(2,"0")}`;
}

function getNowMin() {
  const d = new Date();
  return d.getHours() * 60 + d.getMinutes();
}

function getAllDatesWithScreenings(films) {
  const s = new Set();
  films.forEach(f => {
    if (f.showtimes) Object.keys(f.showtimes).forEach(d => s.add(d));
  });
  return [...s].sort();
}

function formatDayTab(dateStr) {
  const d = new Date(dateStr+"T12:00:00");
  return { day: DAYS[d.getDay()], num: d.getDate(), mon: MONTHS[d.getMonth()], full: dateStr };
}

/** Convert scraped JSON format to the shape our UI expects */
function normalizeFilms(rawFilms) {
  return rawFilms.map(f => {
    const showtimes = {};
    const bookingUrls = {};
    const screens = {};
    const hoh = {};

    if (f.showtimes) {
      for (const [date, sessions] of Object.entries(f.showtimes)) {
        const times = [];
        sessions.forEach(sess => {
          if (typeof sess === "string") {
            times.push(sess);
          } else {
            times.push(sess.time);
            if (sess.booking_url) {
              if (!bookingUrls[date]) bookingUrls[date] = {};
              bookingUrls[date][sess.time] = sess.booking_url;
            }
            if (sess.screen) {
              if (!screens[date]) screens[date] = {};
              screens[date][sess.time] = sess.screen;
            }
            if (sess.hoh) {
              if (!hoh[date]) hoh[date] = [];
              hoh[date].push(sess.time);
            }
          }
        });
        showtimes[date] = times;
      }
    }

    return {
      id: f.id,
      title: f.title,
      rating: f.rating || "TBC",
      runtime: f.runtime || 90,
      genre: f.genre || "Other",
      color: f.color || "#78909c",
      accent: f.accent || "#b0bec5",
      film_url: f.film_url || null,
      poster_url: f.poster_url || null,
      showtimes,
      bookingUrls,
      screens,
      hoh: Object.keys(hoh).length > 0 ? hoh : (f.hoh || {}),
    };
  });
}

/* ─── Shared palette tokens ─── */
const C = {
  bg: "#06060b",
  surface: "#0c0c14",
  surfaceAlt: "#0a0a11",
  border: "#16161f",
  borderLight: "#1e1e2a",
  text: "#e8e4dc",
  textMuted: "#8a857c",
  textDim: "#504c46",
  textFaint: "#2e2c28",
  accent: "#d4a053",       // warm gold
  accentGlow: "#d4a05355",
  accentSoft: "rgba(212,160,83,0.08)",
  accentMed: "rgba(212,160,83,0.15)",
  mono: "'Space Mono', monospace",
  serif: "'Playfair Display', serif",
  sans: "'DM Sans', sans-serif",
};

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
  const [isMobile, setIsMobile] = useState(window.innerWidth < 640);

  useEffect(() => {
    const onResize = () => setIsMobile(window.innerWidth < 640);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  // Fetch data
  useEffect(() => {
    fetch(DATA_URL)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(data => {
        const normalized = normalizeFilms(data.films || []);
        setFilms(normalized);
        setScrapedAt(data.scraped_at || null);
        const allDates = getAllDatesWithScreenings(normalized);
        if (allDates.length > 0 && !allDates.includes(today)) {
          const future = allDates.find(d => d >= today);
          setSelDate(future || allDates[0]);
        }
        setLoading(false);
      })
      .catch(err => {
        console.error("Failed to load film data:", err);
        setError(err.message);
        setLoading(false);
      });
  }, []);

  const allDates = useMemo(() => getAllDatesWithScreenings(films), [films]);

  const dateGroups = useMemo(() => {
    const groups = {};
    allDates.forEach(d => {
      const dt = new Date(d+"T12:00:00");
      const key = `${MONTHS[dt.getMonth()]} ${dt.getFullYear()}`;
      if(!groups[key]) groups[key] = [];
      groups[key].push(d);
    });
    return groups;
  }, [allDates]);

  const weeks = useMemo(() => {
    if (!allDates.length) return [];
    const wks = [];
    const seen = new Set();
    allDates.forEach(d => {
      const dt = new Date(d+"T12:00:00");
      const day = dt.getDay();
      const diff = day === 0 ? -6 : 1 - day;
      const mon = new Date(dt);
      mon.setDate(mon.getDate() + diff);
      const monStr = `${mon.getFullYear()}-${String(mon.getMonth()+1).padStart(2,"0")}-${String(mon.getDate()).padStart(2,"0")}`;
      if (!seen.has(monStr)) {
        seen.add(monStr);
        const sun = new Date(mon);
        sun.setDate(sun.getDate() + 6);
        wks.push({
          monStr,
          monDate: mon,
          sunDate: sun,
          dates: allDates.filter(ad => {
            const adt = new Date(ad+"T12:00:00");
            return adt >= mon && adt <= sun;
          }),
          label: `${mon.getDate()} ${MONTHS[mon.getMonth()]} – ${sun.getDate()} ${MONTHS[sun.getMonth()]}`,
        });
      }
    });
    return wks;
  }, [allDates]);

  useEffect(() => {
    if (weeks.length && !selWeekStart) {
      const target = allDates.includes(today) ? today : selDate;
      const wk = weeks.find(w => w.dates.includes(target)) || weeks[0];
      setSelWeekStart(wk.monStr);
    }
  }, [weeks]);

  const selWeekIdx = weeks.findIndex(w => w.monStr === selWeekStart);
  const selWeek = weeks[selWeekIdx] || weeks[0];

  const selDateIdx = allDates.indexOf(selDate);
  const canPrevDay = selDateIdx > 0;
  const canNextDay = selDateIdx < allDates.length - 1;
  const goDay = (dir) => {
    const ni = selDateIdx + dir;
    if (ni >= 0 && ni < allDates.length) setSelDate(allDates[ni]);
  };

  const canPrevWeek = selWeekIdx > 0;
  const canNextWeek = selWeekIdx < weeks.length - 1;
  const goWeek = (dir) => {
    const ni = selWeekIdx + dir;
    if (ni >= 0 && ni < weeks.length) setSelWeekStart(weeks[ni].monStr);
  };

  const dayFilms = useMemo(() => {
    return films.filter(f => f.showtimes[selDate]).map(f => ({
      ...f,
      times: f.showtimes[selDate],
      sessions: f.showtimes[selDate].map(t => ({
        time: t,
        startMin: timeToMin(t),
        adsEnd: timeToMin(t) + ADS_MIN,
        filmEnd: timeToMin(t) + ADS_MIN + f.runtime,
        isHoh: f.hoh?.[selDate]?.includes(t),
        bookingUrl: f.bookingUrls?.[selDate]?.[t] || null,
        screen: f.screens?.[selDate]?.[t] || null,
      })),
    }));
  }, [selDate, films]);

  const { axisStart, axisEnd } = useMemo(() => {
    if(!dayFilms.length) return { axisStart: 17*60, axisEnd: 24*60 };
    let mn = Infinity, mx = -Infinity;
    dayFilms.forEach(f => f.sessions.forEach(s => {
      mn = Math.min(mn, s.startMin);
      mx = Math.max(mx, s.filmEnd);
    }));
    return { axisStart: Math.floor(mn/60)*60, axisEnd: Math.ceil(mx/60)*60 };
  }, [dayFilms]);

  const axisDuration = axisEnd - axisStart;
  const hourMarks = [];
  for(let m = axisStart; m <= axisEnd; m += 60) hourMarks.push(m);
  const halfMarks = [];
  for(let m = axisStart + 30; m < axisEnd; m += 60) halfMarks.push(m);

  const selDayInfo = formatDayTab(selDate);

  useEffect(() => {
    if(tlRef.current) tlRef.current.scrollLeft = 0;
  }, [selDate]);

  const pct = (min) => ((min - axisStart) / axisDuration) * 100;

  /* ─── FONT LINK (shared across all states) ─── */
  const fontLink = <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700;800;900&family=DM+Sans:wght@300;400;500;600;700&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet" />;

  /* ─── Film grain overlay (reusable) ─── */
  const grainOverlay = (
    <div style={{
      position:"fixed", top:0, left:0, right:0, bottom:0, zIndex:0, pointerEvents:"none",
    }}>
      <div style={{
        position:"absolute", top:"-20%", left:"50%", transform:"translateX(-50%)",
        width:"120%", height:"50%",
        background:`radial-gradient(ellipse, ${C.accentGlow} 0%, transparent 70%)`,
        opacity:0.5,
      }} />
      <div style={{
        position:"absolute", inset:0, opacity:0.018,
        backgroundImage:`url("data:image/svg+xml,%3Csvg viewBox='0 0 512 512' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E")`,
      }} />
    </div>
  );

  if (loading) {
    return (
      <div style={{ fontFamily:C.sans, background:C.bg, color:C.text, minHeight:"100vh", display:"flex", alignItems:"center", justifyContent:"center" }}>
        {fontLink}
        <div style={{ textAlign:"center" }}>
          <div style={{ fontSize:28, fontWeight:800, color:C.accent, marginBottom:12, fontFamily:C.serif, letterSpacing:"-0.5px" }}>Peckhamplex</div>
          <div style={{ color:C.textDim, fontSize:13, fontFamily:C.mono, letterSpacing:1 }}>Loading timetable…</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ fontFamily:C.sans, background:C.bg, color:C.text, minHeight:"100vh", display:"flex", alignItems:"center", justifyContent:"center" }}>
        {fontLink}
        <div style={{ textAlign:"center", maxWidth:400, padding:20 }}>
          <div style={{ fontSize:28, fontWeight:800, color:C.accent, marginBottom:12, fontFamily:C.serif }}>Peckhamplex</div>
          <div style={{ color:"#c0392b", fontSize:14, marginBottom:8, fontFamily:C.sans }}>Failed to load timetable data</div>
          <div style={{ color:C.textDim, fontSize:12, fontFamily:C.mono }}>{error}</div>
          <button onClick={() => window.location.reload()} style={{
            marginTop:20, padding:"10px 28px", background:C.accent, color:C.bg, border:"none",
            borderRadius:6, cursor:"pointer", fontFamily:C.sans, fontWeight:700, fontSize:13,
            letterSpacing:0.5,
          }}>Retry</button>
        </div>
      </div>
    );
  }

  return (
    <div style={{ fontFamily:C.sans, background:C.bg, color:C.text, minHeight:"100vh", position:"relative" }}>
      {fontLink}
      {grainOverlay}

      <style>{`
        @keyframes goldPulse {
          0%, 100% { opacity:0.5; }
          50% { opacity:1; }
        }
        *::-webkit-scrollbar { height:4px; width:4px; }
        *::-webkit-scrollbar-track { background:${C.bg}; }
        *::-webkit-scrollbar-thumb { background:#2a2a34; border-radius:2px; }
        *::-webkit-scrollbar-thumb:hover { background:#3a3a44; }
        .day-btn:hover { background:${C.accentSoft} !important; border-color:${C.accent}66 !important; }
        .view-btn:hover { border-color:${C.accent} !important; color:${C.accent} !important; }
        .booking-link { text-decoration:none; color:inherit; }
        .booking-link:hover { filter:brightness(1.2); }
        .book-btn:hover { background:rgba(255,255,255,0.35) !important; }
        .book-btn:active { opacity:0.7; transform:scale(0.95); }
        a:hover { opacity:0.85; }
      `}</style>

      <div style={{ position:"relative", zIndex:1 }}>

      {/* ═══════ HEADER ═══════ */}
      <div style={{
        background:`linear-gradient(180deg, #0e0c08 0%, ${C.bg} 100%)`,
        padding:"28px 24px 20px",
        borderBottom:`1px solid ${C.accent}33`,
      }}>
        <div style={{ maxWidth:1000, margin:"0 auto" }}>
          <div style={{ display:"flex", alignItems:"flex-end", justifyContent:"space-between", flexWrap:"wrap", gap:12 }}>
            <div>
              <div style={{ fontSize:9, letterSpacing:4, textTransform:"uppercase", color:C.accent, fontFamily:C.mono, fontWeight:700, marginBottom:4, opacity:0.7 }}>Now Showing</div>
              <h1 style={{
                fontFamily:C.serif, fontSize:32, fontWeight:900, margin:0, letterSpacing:"-0.5px", lineHeight:1,
                background:`linear-gradient(135deg, #f0ece4 0%, ${C.accent} 150%)`,
                WebkitBackgroundClip:"text", WebkitTextFillColor:"transparent",
              }}>Peckhamplex</h1>
            </div>
            <div style={{ display:"flex", alignItems:"center", gap:12, flexWrap:"wrap" }}>
              <div style={{
                display:"inline-flex", alignItems:"center", gap:6,
                padding:"5px 14px", borderRadius:20,
                background:C.accentSoft,
                border:`1px solid ${C.accent}22`,
              }}>
                <div style={{ width:5, height:5, borderRadius:"50%", background:C.accent, animation:"goldPulse 2.5s ease infinite" }} />
                <span style={{ fontSize:13, color:C.accent, fontWeight:700, fontFamily:C.mono }}>£7.59</span>
                <span style={{ fontSize:10, color:`${C.accent}88` }}>all tickets</span>
              </div>
              <a href="https://www.peckhamplex.london" target="_blank" rel="noopener" style={{
                color:C.textMuted, textDecoration:"none", fontSize:11, fontFamily:C.mono, letterSpacing:0.5,
              }}>
                95a Rye Lane, Peckham ↗
              </a>
            </div>
          </div>
        </div>
      </div>

      <div style={{ maxWidth:1000, margin:"0 auto", padding:"20px 20px 40px" }}>

        {/* ═══════ VIEW TOGGLE + NAV ═══════ */}
        <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", gap:8, marginBottom:20, flexWrap:"wrap" }}>
          <div style={{ display:"flex", gap:6 }}>
            {[["day", isMobile ? "Day" : "Timeline"],["grid","Week"]].map(([v,label]) => (
              <button key={v} className="view-btn" onClick={() => setView(v)} style={{
                padding:"7px 18px", borderRadius:6, fontSize:11, fontWeight:600, cursor:"pointer",
                fontFamily:C.mono, letterSpacing:0.5, textTransform:"uppercase",
                border: view===v ? `1.5px solid ${C.accent}` : `1.5px solid ${C.border}`,
                background: view===v ? C.accentSoft : "transparent",
                color: view===v ? C.accent : C.textDim,
                transition:"all 0.25s",
              }}>{label}</button>
            ))}
          </div>

          {/* Arrow navigation */}
          {view === "day" ? (
            <div style={{ display:"flex", alignItems:"center", gap:10 }}>
              <button onClick={() => goDay(-1)} disabled={!canPrevDay} className="view-btn" style={{
                padding:"6px 12px", borderRadius:6, fontSize:16, cursor:canPrevDay?"pointer":"default",
                border:`1.5px solid ${C.border}`, background:"transparent",
                color:canPrevDay?C.text:C.textFaint,
                fontFamily:C.serif, fontWeight:600, transition:"all 0.25s", lineHeight:1,
              }}>‹</button>
              <div style={{ textAlign:"center", minWidth:140 }}>
                <div style={{ fontSize:17, fontWeight:700, color:C.text, fontFamily:C.serif, letterSpacing:"-0.3px" }}>
                  {selDayInfo.day} {selDayInfo.num} {selDayInfo.mon}
                </div>
                <div style={{ fontSize:10, color:C.textDim, fontFamily:C.mono, marginTop:2 }}>
                  {selDate === today ? "Today · " : ""}{dayFilms.length} film{dayFilms.length !== 1 ? "s" : ""}
                </div>
              </div>
              <button onClick={() => goDay(1)} disabled={!canNextDay} className="view-btn" style={{
                padding:"6px 12px", borderRadius:6, fontSize:16, cursor:canNextDay?"pointer":"default",
                border:`1.5px solid ${C.border}`, background:"transparent",
                color:canNextDay?C.text:C.textFaint,
                fontFamily:C.serif, fontWeight:600, transition:"all 0.25s", lineHeight:1,
              }}>›</button>
            </div>
          ) : selWeek ? (
            <div style={{ display:"flex", alignItems:"center", gap:10 }}>
              <button onClick={() => goWeek(-1)} disabled={!canPrevWeek} className="view-btn" style={{
                padding:"6px 12px", borderRadius:6, fontSize:16, cursor:canPrevWeek?"pointer":"default",
                border:`1.5px solid ${C.border}`, background:"transparent",
                color:canPrevWeek?C.text:C.textFaint,
                fontFamily:C.serif, fontWeight:600, transition:"all 0.25s", lineHeight:1,
              }}>‹</button>
              <div style={{ textAlign:"center", minWidth:170 }}>
                <div style={{ fontSize:17, fontWeight:700, color:C.text, fontFamily:C.serif, letterSpacing:"-0.3px" }}>
                  {selWeek.label}
                </div>
                <div style={{ fontSize:10, color:C.textDim, fontFamily:C.mono, marginTop:2 }}>
                  {selWeek.dates.length} screening day{selWeek.dates.length !== 1 ? "s" : ""}
                </div>
              </div>
              <button onClick={() => goWeek(1)} disabled={!canNextWeek} className="view-btn" style={{
                padding:"6px 12px", borderRadius:6, fontSize:16, cursor:canNextWeek?"pointer":"default",
                border:`1.5px solid ${C.border}`, background:"transparent",
                color:canNextWeek?C.text:C.textFaint,
                fontFamily:C.serif, fontWeight:600, transition:"all 0.25s", lineHeight:1,
              }}>›</button>
            </div>
          ) : null}
        </div>

        {view === "day" ? (
          /* ═══════════════════ DAY VIEW ═══════════════════ */
          <>
            {dayFilms.length === 0 ? (
              <div style={{ textAlign:"center", padding:"60px 20px", color:C.textDim }}>
                <div style={{ fontSize:40, marginBottom:12, opacity:0.25 }}>◇</div>
                <p style={{ fontSize:15, fontFamily:C.serif, fontStyle:"italic" }}>No screenings on this day.</p>
              </div>
            ) : isMobile ? (
              /* ═══════ MOBILE CHRONOLOGICAL FEED ═══════ */
              (() => {
                const allSessions = [];
                dayFilms.forEach(film => {
                  film.sessions.forEach(sess => {
                    allSessions.push({ ...sess, film });
                  });
                });
                allSessions.sort((a, b) => a.startMin - b.startMin);

                const groups = [];
                allSessions.forEach(sess => {
                  const last = groups[groups.length - 1];
                  if (last && last.time === sess.time) {
                    last.sessions.push(sess);
                  } else {
                    groups.push({ time: sess.time, startMin: sess.startMin, sessions: [sess] });
                  }
                });

                const nowMin = selDate === today ? getNowMin() : null;

                return (
                  <div style={{ display:"flex", flexDirection:"column", gap:0 }}>
                    {groups.map((group, gi) => {
                      const isPast = nowMin !== null && group.startMin + ADS_MIN < nowMin;
                      return (
                        <div key={group.time + gi} style={{ display:"flex", gap:0, opacity: isPast ? 0.35 : 1, transition:"opacity 0.3s" }}>
                          {/* Time gutter */}
                          <div style={{ width:56, flexShrink:0, display:"flex", flexDirection:"column", alignItems:"center", position:"relative" }}>
                            <div style={{
                              fontSize:13, fontWeight:700, color: isPast ? C.textFaint : C.accent,
                              fontFamily:C.mono,
                              padding:"4px 0", zIndex:2, background:C.bg,
                            }}>{group.time}</div>
                            {gi < groups.length - 1 && (
                              <div style={{ width:1, flex:1, background:`${C.accent}18`, minHeight:8 }} />
                            )}
                          </div>
                          {/* Session cards */}
                          <div style={{ flex:1, display:"flex", flexDirection:"column", gap:8, paddingBottom:18 }}>
                            {group.sessions.map((sess, si) => {
                              const film = sess.film;
                              return (
                                <div key={`${film.id}-${si}`} style={{
                                  display:"flex", alignItems:"center", gap:0,
                                  borderRadius:10, overflow:"hidden",
                                  border:`1px solid ${film.color}28`,
                                  background:`linear-gradient(135deg, ${film.color}08 0%, transparent 100%)`,
                                  backdropFilter:"blur(4px)",
                                }}>
                                  {/* Color accent bar */}
                                  <div style={{ width:4, alignSelf:"stretch", background:`linear-gradient(180deg, ${film.color}, ${film.color}66)`, flexShrink:0 }} />
                                  {/* Info */}
                                  <div style={{ flex:1, padding:"11px 14px" }}>
                                    <div style={{ fontSize:14, fontWeight:700, color:C.text, lineHeight:1.25, fontFamily:C.serif }}>
                                      {film.film_url ? (
                                        <a href={film.film_url} target="_blank" rel="noopener" style={{ color:C.text, textDecoration:"none" }}>{film.title}</a>
                                      ) : film.title}
                                    </div>
                                    <div style={{ display:"flex", gap:6, marginTop:5, alignItems:"center", flexWrap:"wrap" }}>
                                      <span style={{
                                        fontSize:9, padding:"2px 6px", borderRadius:3, fontWeight:700,
                                        background:rBg[film.rating]||"#444", color:"#fff", fontFamily:C.mono, letterSpacing:0.5,
                                      }}>{film.rating}</span>
                                      <span style={{ fontSize:10, color:C.textMuted, fontFamily:C.mono }}>{film.runtime}min</span>
                                      <span style={{ fontSize:10, color:C.textDim, fontFamily:C.mono }}>ends {minToTime(sess.startMin + film.runtime)}</span>
                                      {sess.screen && <span style={{ fontSize:10, color:C.textDim, fontFamily:C.mono }}>{sess.screen}</span>}
                                      {sess.isHoh && <span style={{ fontSize:9, color:C.textMuted, fontFamily:C.mono, padding:"1px 4px", borderRadius:3, background:"rgba(255,255,255,0.05)", border:"1px solid rgba(255,255,255,0.08)" }}>CC</span>}
                                    </div>
                                  </div>
                                  {/* Book button */}
                                  {sess.bookingUrl && (
                                    <a href={sess.bookingUrl} target="_blank" rel="noopener"
                                      className="book-btn"
                                      style={{
                                        display:"flex", alignItems:"center", justifyContent:"center",
                                        padding:"0 14px", alignSelf:"stretch",
                                        background:`${film.color}10`,
                                        borderLeft:`1px solid ${film.color}22`,
                                        textDecoration:"none", cursor:"pointer",
                                        transition:"background 0.2s",
                                      }}>
                                      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke={film.accent} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                                        <path d="M2 9a3 3 0 0 1 0 6v2a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-2a3 3 0 0 1 0-6V7a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2Z"/>
                                        <path d="M13 5v2"/><path d="M13 17v2"/><path d="M13 11v2"/>
                                      </svg>
                                    </a>
                                  )}
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
                {/* Time axis header */}
                <div style={{ marginLeft:180, position:"relative", height:30, marginBottom:4 }}>
                  {hourMarks.map(m => (
                    <div key={m} style={{
                      position:"absolute", left:`${pct(m)}%`, transform:"translateX(-50%)",
                      fontSize:10, fontFamily:C.mono, color:C.textDim, fontWeight:400, letterSpacing:0.5,
                    }}>{minToTime(m)}</div>
                  ))}
                </div>

                {/* Film rows */}
                <div ref={tlRef} style={{ position:"relative" }}>
                  {dayFilms.map((film, fi) => (
                    <div key={film.id} style={{
                      display:"flex", alignItems:"stretch", marginBottom:4,
                      background: fi % 2 === 0 ? "rgba(255,255,255,0.008)" : "transparent",
                      borderRadius:8, overflow:"hidden",
                    }}>
                      {/* Film label */}
                      <div style={{
                        width:180, flexShrink:0, padding:"10px 14px",
                        display:"flex", flexDirection:"column", justifyContent:"center",
                        borderRight:`1px solid ${C.border}`,
                      }}>
                        <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:3 }}>
                          <div style={{ width:3, height:28, borderRadius:1.5, background:`linear-gradient(180deg, ${film.color}, ${film.color}55)`, flexShrink:0 }} />
                          <div>
                            <div style={{ fontSize:12, fontWeight:700, color:"#d5d0c8", lineHeight:1.2, fontFamily:C.serif }}>
                              {film.film_url ? (
                                <a href={film.film_url} target="_blank" rel="noopener" style={{ color:"#d5d0c8", textDecoration:"none" }}>{film.title}</a>
                              ) : film.title}
                            </div>
                            <div style={{ display:"flex", gap:5, marginTop:4, alignItems:"center" }}>
                              <span style={{
                                fontSize:8, padding:"1px 5px", borderRadius:3, fontWeight:700,
                                background:rBg[film.rating]||"#444", color:"#fff", fontFamily:C.mono, letterSpacing:0.5,
                              }}>{film.rating}</span>
                              <span style={{ fontSize:9, color:C.textDim, fontFamily:C.mono }}>{film.runtime}m</span>
                              <span style={{ fontSize:9, color:C.textFaint, fontFamily:C.mono }}>{film.genre}</span>
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Timeline area */}
                      <div style={{ flex:1, position:"relative", minHeight:52, padding:"6px 0" }}>
                        {/* Grid lines */}
                        {hourMarks.map(m => (
                          <div key={m} style={{
                            position:"absolute", left:`${pct(m)}%`, top:0, bottom:0,
                            width:1, background:`${C.accent}0a`, zIndex:0,
                          }} />
                        ))}
                        {halfMarks.map(m => (
                          <div key={m} style={{
                            position:"absolute", left:`${pct(m)}%`, top:0, bottom:0,
                            width:1, background:`${C.accent}05`, zIndex:0,
                          }} />
                        ))}

                        {/* Session bars */}
                        {film.sessions.map((sess, si) => {
                          const barLeft = pct(sess.startMin);
                          const adsWidth = pct(sess.adsEnd) - barLeft;
                          const filmWidth = pct(sess.filmEnd) - pct(sess.adsEnd);
                          const totalWidth = pct(sess.filmEnd) - barLeft;
                          const bKey = `${film.id}-${si}`;
                          const isHov = hovBar === bKey;

                          return (
                            <div key={si}
                              onMouseEnter={() => setHovBar(bKey)}
                              onMouseLeave={() => setHovBar(null)}
                              style={{
                                position:"absolute",
                                left:`${barLeft}%`,
                                width:`${totalWidth}%`,
                                top: "50%",
                                height:36,
                                display:"flex",
                                borderRadius:5,
                                overflow:"hidden",
                                zIndex: isHov ? 10 : 2,
                                transform: isHov ? "translateY(-50%) scaleY(1.15)" : "translateY(-50%) scaleY(1)",
                                transition:"transform 0.2s cubic-bezier(0.4,0,0.2,1), box-shadow 0.2s cubic-bezier(0.4,0,0.2,1)",
                                boxShadow: isHov
                                  ? `0 6px 28px ${film.color}44, 0 0 0 1px ${film.color}66, inset 0 1px 0 rgba(255,255,255,0.06)`
                                  : `0 1px 6px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.03)`,
                              }}>
                              {/* Ads portion */}
                              <div style={{
                                width:`${(adsWidth / totalWidth) * 100}%`,
                                background:`repeating-linear-gradient(120deg, ${film.color}30, ${film.color}30 3px, ${film.color}18 3px, ${film.color}18 6px)`,
                                display:"flex", alignItems:"center", justifyContent:"center",
                                borderRight:`1px dashed ${film.color}55`,
                                flexShrink:0,
                              }}>
                                <span style={{ fontSize:7, fontWeight:700, color:film.accent, letterSpacing:1, textTransform:"uppercase", opacity:0.6, fontFamily:C.mono }}>ADS</span>
                              </div>
                              {/* Film portion */}
                              <div style={{
                                flex:1,
                                background:`linear-gradient(135deg, ${film.color}bb 0%, ${film.color}88 100%)`,
                                padding:"3px 8px",
                                display:"flex", alignItems:"center", gap:6,
                                minWidth:0,
                              }}>
                                <div style={{ flex:1, minWidth:0 }}>
                                  <div style={{
                                    fontSize:10.5, fontWeight:700, color:"#fff",
                                    whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis",
                                    textShadow:"0 1px 4px rgba(0,0,0,0.5)",
                                    fontFamily:C.mono,
                                  }}>
                                    {sess.time} – {minToTime(sess.startMin + film.runtime)}
                                    {sess.isHoh ? "  CC" : ""}
                                    {sess.screen ? `  · ${sess.screen}` : ""}
                                  </div>
                                  {isHov && (
                                    <div style={{ fontSize:8.5, color:"rgba(255,255,255,0.65)", marginTop:1, fontFamily:C.mono }}>
                                      Ends ~{minToTime(sess.filmEnd)} with ads
                                      {sess.screen ? ` · ${sess.screen}` : ""}
                                    </div>
                                  )}
                                </div>
                                {/* Book button */}
                                {sess.bookingUrl && (
                                  <a href={sess.bookingUrl} target="_blank" rel="noopener"
                                    onClick={(e) => e.stopPropagation()}
                                    className="book-btn"
                                    title="Book tickets"
                                    style={{
                                      display:"flex", alignItems:"center", justifyContent:"center",
                                      padding:"4px 8px", borderRadius:4,
                                      background:"rgba(255,255,255,0.12)", border:"1px solid rgba(255,255,255,0.18)",
                                      textDecoration:"none", flexShrink:0,
                                      cursor:"pointer", transition:"background 0.2s",
                                    }}>
                                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                      <path d="M2 9a3 3 0 0 1 0 6v2a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-2a3 3 0 0 1 0-6V7a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2Z"/>
                                      <path d="M13 5v2"/>
                                      <path d="M13 17v2"/>
                                      <path d="M13 11v2"/>
                                    </svg>
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

                {/* Now-line if today */}
                {selDate === today && (() => {
                  const nowMin = getNowMin();
                  if(nowMin >= axisStart && nowMin <= axisEnd){
                    return (
                      <div style={{
                        position:"absolute", left:`calc(180px + ${((nowMin-axisStart)/axisDuration)*100}% * (100% - 180px) / 100%)`,
                        top:30, bottom:0, width:2, background:C.accent, zIndex:20,
                        boxShadow:`0 0 12px ${C.accentGlow}`,
                      }}>
                        <div style={{
                          position:"absolute", top:-8, left:-14,
                          fontSize:8, fontWeight:700, color:C.accent,
                          background:C.bg, padding:"1px 5px", borderRadius:3,
                          fontFamily:C.mono, letterSpacing:1,
                        }}>NOW</div>
                      </div>
                    );
                  }
                  return null;
                })()}
              </div>
            )}
          </>
        ) : (
          /* ═══════════════════ GRID OVERVIEW ═══════════════════ */
          <>
            <div style={{ overflowX:"auto", borderRadius:8, border:`1px solid ${C.border}` }}>
              <table style={{ width:"100%", borderCollapse:"separate", borderSpacing:0, minWidth:400 }}>
                <thead>
                  <tr>
                    <th style={{
                      padding:"12px 14px", textAlign:"left", fontSize:10, color:C.textDim, fontWeight:600,
                      borderBottom:`1px solid ${C.border}`, position:"sticky", left:0,
                      background:C.surface, zIndex:5, minWidth:150,
                      boxShadow:"4px 0 12px rgba(0,0,0,0.6)",
                      fontFamily:C.mono, letterSpacing:1, textTransform:"uppercase",
                    }}>Film</th>
                    {(selWeek ? selWeek.dates : allDates).map(d => {
                      const info = formatDayTab(d);
                      const isT = d === today;
                      return (
                        <th key={d} onClick={() => { setSelDate(d); setView("day"); }} style={{
                          padding:"10px 6px", textAlign:"center", fontSize:10, fontWeight:600,
                          borderBottom:`1px solid ${C.border}`, cursor:"pointer",
                          color: isT ? C.accent : C.textDim,
                          background: isT ? C.accentSoft : "transparent",
                          borderBottomColor: isT ? `${C.accent}55` : C.border,
                          minWidth:72, transition:"color 0.2s",
                          fontFamily:C.mono,
                        }}>
                          <div style={{ letterSpacing:1 }}>{info.day}</div>
                          <div style={{ fontSize:18, fontWeight:700, lineHeight:1.1, fontFamily:C.serif }}>{info.num}</div>
                          <div style={{ fontSize:9, color:C.textFaint, letterSpacing:1 }}>{info.mon}</div>
                        </th>
                      );
                    })}
                  </tr>
                </thead>
                <tbody>
                  {(() => { const weekDates = selWeek ? selWeek.dates : allDates; return films.filter(f => weekDates.some(d => f.showtimes[d])).map((film, fi) => (
                    <tr key={film.id} style={{ background: fi%2===0 ? "rgba(255,255,255,0.008)" : "transparent" }}>
                      <td style={{
                        padding:"10px 12px", borderBottom:`1px solid ${C.bg}`,
                        position:"sticky", left:0,
                        background: fi%2===0 ? "#0d0c12" : C.surfaceAlt,
                        zIndex:4, boxShadow:"4px 0 12px rgba(0,0,0,0.6)",
                      }}>
                        <div style={{ display:"flex", alignItems:"center", gap:7 }}>
                          <div style={{ width:3, height:24, borderRadius:1.5, background:`linear-gradient(180deg, ${film.color}, ${film.color}44)` }} />
                          <div>
                            <div style={{ fontSize:11, fontWeight:700, color:"#c8c4bb", fontFamily:C.serif }}>
                              {film.film_url ? (
                                <a href={film.film_url} target="_blank" rel="noopener" style={{ color:"#c8c4bb", textDecoration:"none" }}>{film.title}</a>
                              ) : film.title}
                            </div>
                            <div style={{ display:"flex", gap:4, marginTop:3 }}>
                              <span style={{ fontSize:8, padding:"0px 5px", borderRadius:2, background:rBg[film.rating]||"#444", color:"#fff", fontWeight:700, fontFamily:C.mono }}>{film.rating}</span>
                              <span style={{ fontSize:9, color:C.textFaint, fontFamily:C.mono }}>{film.runtime}m</span>
                            </div>
                          </div>
                        </div>
                      </td>
                      {weekDates.map(d => {
                        const times = film.showtimes[d];
                        const isT = d === today;
                        return (
                          <td key={d}
                            style={{
                              padding:"6px 4px", textAlign:"center",
                              borderBottom:`1px solid ${C.bg}`,
                              borderLeft:`1px solid ${C.bg}`,
                              background: isT ? C.accentSoft : "transparent",
                            }}>
                            {times ? (
                              <div style={{ display:"flex", flexDirection:"column", gap:3, alignItems:"center" }}>
                                {times.map((t,i) => {
                                  const isHoh = film.hoh?.[d]?.includes(t);
                                  const bookingUrl = film.bookingUrls?.[d]?.[t];
                                  const screen = film.screens?.[d]?.[t];
                                  const pill = (
                                    <span key={i} style={{
                                      fontSize:11, fontWeight:600, padding:"3px 8px", borderRadius:4,
                                      background:`${film.color}15`, border:`1px solid ${film.color}30`,
                                      color:film.accent, fontFamily:C.mono,
                                      whiteSpace:"nowrap", transition:"all 0.2s",
                                      cursor: bookingUrl ? "pointer" : "default",
                                      display:"inline-flex", alignItems:"center", gap:3,
                                    }}
                                    title={screen ? `${screen}${isHoh ? " · HoH" : ""}` : (isHoh ? "Hard of Hearing" : "")}
                                    >
                                      {t}{isHoh ? " CC" : ""}
                                    </span>
                                  );
                                  return bookingUrl ? (
                                    <a key={i} href={bookingUrl} target="_blank" rel="noopener" style={{ textDecoration:"none" }}>{pill}</a>
                                  ) : pill;
                                })}
                              </div>
                            ) : (
                              <span style={{ color:C.border, fontSize:14 }}>—</span>
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  )); })()}
                </tbody>
              </table>
            </div>
            <p style={{ fontSize:10, color:C.textFaint, marginTop:10, fontFamily:C.mono, letterSpacing:0.3 }}>Click any time to book directly. Click a date header to switch to day view.</p>
          </>
        )}

        {/* ═══════ LEGEND ═══════ */}
        <div style={{
          marginTop:24, padding:"16px 18px", borderRadius:8,
          background:`linear-gradient(135deg, rgba(212,160,83,0.02) 0%, transparent 100%)`,
          border:`1px solid ${C.border}`,
        }}>
          {!isMobile && (
            <div style={{ display:"flex", gap:18, flexWrap:"wrap", fontSize:10, color:C.textDim, fontFamily:C.mono }}>
              <div style={{ display:"flex", alignItems:"center", gap:5 }}>
                <div style={{ width:22, height:10, borderRadius:2, background:`repeating-linear-gradient(120deg, ${C.accent}30, ${C.accent}30 3px, ${C.accent}18 3px, ${C.accent}18 6px)` }} />
                <span>= ~{ADS_MIN}min ads/trailers</span>
              </div>
              <span style={{ display:"flex", alignItems:"center", gap:3 }}>CC = Subtitled (Hard of Hearing)</span>
              <span>Bar length = full session (ads + film)</span>
            </div>
          )}
          <div style={{ display:"flex", gap:14, flexWrap:"wrap", fontSize:10, color:C.textDim, fontFamily:C.mono, marginTop: isMobile ? 0 : undefined }}>
            {isMobile && <span style={{ display:"flex", alignItems:"center", gap:3 }}>CC = Hard of Hearing</span>}
            <span style={{ display:"flex", alignItems:"center", gap:4 }}>
              <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke={C.textMuted} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M2 9a3 3 0 0 1 0 6v2a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-2a3 3 0 0 1 0-6V7a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2Z"/>
                <path d="M13 5v2"/><path d="M13 17v2"/><path d="M13 11v2"/>
              </svg>
              = Book tickets
            </span>
          </div>
          {scrapedAt && (
            <div style={{
              fontSize:9, color:C.textFaint, marginTop:8, fontFamily:C.mono,
              paddingTop:8, borderTop:`1px solid ${C.border}`,
              letterSpacing:0.3,
            }}>
              Data from peckhamplex.london · Updated {new Date(scrapedAt).toLocaleString("en-GB", { dateStyle:"medium", timeStyle:"short" })} · Always confirm at the cinema
            </div>
          )}
        </div>
      </div>

      </div>
    </div>
  );
}
