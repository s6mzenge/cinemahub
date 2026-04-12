import { useState, useMemo, useRef, useEffect } from "react";

const ADS_MIN = 20;
const DATA_URL = import.meta.env.BASE_URL + "data/films.json";

const DAYS = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const rBg = { U:"#2e7d32", PG:"#ef8f00", "12A":"#e65100", "15":"#c62828", TBC:"#555" };

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
        // sessions can be either:
        //   - array of strings (legacy static format): ["18:00","20:30"]
        //   - array of objects (scraped format): [{time, booking_url, screen, hoh}]
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

export default function App() {
  const [films, setFilms] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [scrapedAt, setScrapedAt] = useState(null);

  const today = getToday();
  const [selDate, setSelDate] = useState(today);
  const [hovBar, setHovBar] = useState(null);
  const [view, setView] = useState("day");
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
        // Default to today if today has screenings, otherwise first available date
        const allDates = getAllDatesWithScreenings(normalized);
        if (allDates.length > 0 && !allDates.includes(today)) {
          // Find the nearest future date
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

  // Group dates by month
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

  // Films showing on selected date
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

  // Time axis range
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

  if (loading) {
    return (
      <div style={{ fontFamily:"'Outfit',sans-serif", background:"#08080e", color:"#e8e6e1", minHeight:"100vh", display:"flex", alignItems:"center", justifyContent:"center" }}>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap" rel="stylesheet" />
        <div style={{ textAlign:"center" }}>
          <div style={{ fontSize:24, fontWeight:800, color:"#f918ac", marginBottom:12 }}>PECKHAMPLEX</div>
          <div style={{ color:"#555", fontSize:14 }}>Loading timetable...</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ fontFamily:"'Outfit',sans-serif", background:"#08080e", color:"#e8e6e1", minHeight:"100vh", display:"flex", alignItems:"center", justifyContent:"center" }}>
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap" rel="stylesheet" />
        <div style={{ textAlign:"center", maxWidth:400, padding:20 }}>
          <div style={{ fontSize:24, fontWeight:800, color:"#f918ac", marginBottom:12 }}>PECKHAMPLEX</div>
          <div style={{ color:"#e53935", fontSize:14, marginBottom:8 }}>Failed to load timetable data</div>
          <div style={{ color:"#555", fontSize:12 }}>{error}</div>
          <button onClick={() => window.location.reload()} style={{
            marginTop:16, padding:"8px 20px", background:"#f918ac", color:"#fff", border:"none",
            borderRadius:8, cursor:"pointer", fontFamily:"'Outfit',sans-serif", fontWeight:600,
          }}>Retry</button>
        </div>
      </div>
    );
  }

  return (
    <div style={{ fontFamily:"'Outfit',sans-serif", background:"#08080e", color:"#e8e6e1", minHeight:"100vh" }}>
      <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet" />

      <style>{`
        *::-webkit-scrollbar { height:6px; width:6px; }
        *::-webkit-scrollbar-track { background:#111; border-radius:3px; }
        *::-webkit-scrollbar-thumb { background:#333; border-radius:3px; }
        *::-webkit-scrollbar-thumb:hover { background:#555; }
        .day-btn:hover { background:rgba(249,24,172,0.08) !important; border-color:#f918ac !important; }
        .view-btn:hover { border-color:#f918ac !important; color:#f918ac !important; }
        .booking-link { text-decoration:none; color:inherit; }
        .booking-link:hover { filter:brightness(1.2); }
        .book-btn:hover { background:rgba(255,255,255,0.4) !important; }
        .book-btn:active { opacity:0.7; transform:scale(0.95); }
      `}</style>

      {/* Header */}
      <div style={{ background:"linear-gradient(180deg,#100818 0%,#0c0c14 100%)", padding:"20px 20px 16px", borderBottom:"2px solid #f918ac" }}>
        <div style={{ maxWidth:1000, margin:"0 auto" }}>
          <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", flexWrap:"wrap", gap:10 }}>
            <div style={{ display:"flex", alignItems:"baseline", gap:10 }}>
              <h1 style={{ fontFamily:"'Outfit',sans-serif", fontSize:24, fontWeight:800, color:"#f918ac", margin:0, letterSpacing:"-0.5px" }}>PECKHAMPLEX</h1>
              <span style={{ fontSize:11, color:"#555", fontWeight:500, letterSpacing:1 }}>SCREENINGS</span>
            </div>
            <div style={{ fontSize:12, color:"#777" }}>
              All tickets <span style={{ color:"#f918ac", fontWeight:700 }}>£7.59</span>
              <span style={{ color:"#333", margin:"0 6px" }}>|</span>
              <a href="https://www.peckhamplex.london" target="_blank" rel="noopener" style={{ color:"#777", textDecoration:"none" }}>
                95a Rye Lane, Peckham
              </a>
            </div>
          </div>
        </div>
      </div>

      <div style={{ maxWidth:1000, margin:"0 auto", padding:"16px 16px 32px" }}>

        {/* View toggle */}
        <div style={{ display:"flex", gap:6, marginBottom:16 }}>
          {[["day", isMobile ? "Today" : "Timeline View"],["grid","Week Overview"]].map(([v,label]) => (
            <button key={v} className="view-btn" onClick={() => setView(v)} style={{
              padding:"7px 16px", borderRadius:8, fontSize:12, fontWeight:600, cursor:"pointer",
              fontFamily:"'Outfit',sans-serif",
              border: view===v ? "1.5px solid #f918ac" : "1.5px solid #222",
              background: view===v ? "rgba(249,24,172,0.1)" : "transparent",
              color: view===v ? "#f918ac" : "#666",
              transition:"all 0.2s",
            }}>{label}</button>
          ))}
        </div>

        {/* Day selector */}
        <div style={{ marginBottom:20 }}>
          {Object.entries(dateGroups).map(([monthLabel, dates]) => (
            <div key={monthLabel} style={{ marginBottom:10 }}>
              <div style={{ fontSize:10, color:"#444", fontWeight:600, letterSpacing:1.5, textTransform:"uppercase", marginBottom:6, fontFamily:"'JetBrains Mono',monospace" }}>{monthLabel}</div>
              <div style={{ display:"flex", gap:5, overflowX:isMobile?"auto":"visible", flexWrap:isMobile?"nowrap":"wrap", paddingBottom:isMobile?6:0 }}>
                {dates.map(d => {
                  const info = formatDayTab(d);
                  const active = d === selDate;
                  const isToday = d === today;
                  return (
                    <button key={d} className="day-btn" onClick={() => setSelDate(d)} style={{
                      display:"flex", flexDirection:"column", alignItems:"center",
                      padding:"6px 10px", borderRadius:10, minWidth:48, cursor:"pointer", flexShrink:0,
                      border: active ? "1.5px solid #f918ac" : isToday ? "1.5px solid #444" : "1.5px solid #1a1a24",
                      background: active ? "rgba(249,24,172,0.12)" : "rgba(255,255,255,0.015)",
                      color: active ? "#f918ac" : "#aaa",
                      transition:"all 0.2s", fontFamily:"'Outfit',sans-serif",
                    }}>
                      <span style={{ fontSize:9, fontWeight:600, color:active?"#f918ac":"#555", textTransform:"uppercase" }}>{info.day}</span>
                      <span style={{ fontSize:18, fontWeight:700, lineHeight:1.1 }}>{info.num}</span>
                      {isToday && <span style={{ fontSize:7, color:"#f918ac", fontWeight:700, marginTop:1 }}>TODAY</span>}
                    </button>
                  );
                })}
              </div>
            </div>
          ))}
        </div>

        {view === "day" ? (
          /* ==================== DAY VIEW ==================== */
          <>
            <div style={{ fontSize:16, fontWeight:700, marginBottom:14, color:"#ddd" }}>
              {selDayInfo.day} {selDayInfo.num} {selDayInfo.mon}
              <span style={{ fontSize:12, fontWeight:400, color:"#555", marginLeft:8 }}>
                {dayFilms.length} film{dayFilms.length !== 1 ? "s" : ""} screening
              </span>
            </div>

            {dayFilms.length === 0 ? (
              <div style={{ textAlign:"center", padding:"50px 20px", color:"#444" }}>
                <p style={{ fontSize:15 }}>No screenings on this day.</p>
              </div>
            ) : isMobile ? (
              /* ========== MOBILE CHRONOLOGICAL FEED ========== */
              (() => {
                // Collect and sort all sessions by start time
                const allSessions = [];
                dayFilms.forEach(film => {
                  film.sessions.forEach(sess => {
                    allSessions.push({ ...sess, film });
                  });
                });
                allSessions.sort((a, b) => a.startMin - b.startMin);

                // Group by start time
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
                        <div key={group.time + gi} style={{ display:"flex", gap:0, opacity: isPast ? 0.45 : 1, transition:"opacity 0.3s" }}>
                          {/* Time gutter */}
                          <div style={{ width:52, flexShrink:0, display:"flex", flexDirection:"column", alignItems:"center", position:"relative" }}>
                            <div style={{
                              fontSize:13, fontWeight:700, color: isPast ? "#444" : "#f918ac",
                              fontFamily:"'JetBrains Mono',monospace",
                              padding:"4px 0", zIndex:2, background:"#08080e",
                            }}>{group.time}</div>
                            {/* Vertical connector line */}
                            {gi < groups.length - 1 && (
                              <div style={{ width:2, flex:1, background:"#1a1a24", minHeight:8 }} />
                            )}
                          </div>
                          {/* Session cards */}
                          <div style={{ flex:1, display:"flex", flexDirection:"column", gap:8, paddingBottom:16 }}>
                            {group.sessions.map((sess, si) => {
                              const film = sess.film;
                              return (
                                <div key={`${film.id}-${si}`} style={{
                                  display:"flex", alignItems:"center", gap:0,
                                  borderRadius:10, overflow:"hidden",
                                  border:`1px solid ${film.color}33`,
                                  background:"rgba(255,255,255,0.015)",
                                }}>
                                  {/* Color accent bar */}
                                  <div style={{ width:5, alignSelf:"stretch", background:film.color, flexShrink:0 }} />
                                  {/* Info */}
                                  <div style={{ flex:1, padding:"10px 12px" }}>
                                    <div style={{ fontSize:14, fontWeight:700, color:"#eee", lineHeight:1.25 }}>
                                      {film.film_url ? (
                                        <a href={film.film_url} target="_blank" rel="noopener" style={{ color:"#eee", textDecoration:"none" }}>{film.title}</a>
                                      ) : film.title}
                                    </div>
                                    <div style={{ display:"flex", gap:6, marginTop:5, alignItems:"center", flexWrap:"wrap" }}>
                                      <span style={{
                                        fontSize:10, padding:"1px 5px", borderRadius:3, fontWeight:700,
                                        background:rBg[film.rating]||"#555", color:"#fff",
                                      }}>{film.rating}</span>
                                      <span style={{ fontSize:11, color:"#666" }}>{film.runtime}min</span>
                                      <span style={{ fontSize:11, color:"#555" }}>ends {minToTime(sess.startMin + film.runtime)}</span>
                                      {sess.screen && <span style={{ fontSize:11, color:"#555" }}>{sess.screen}</span>}
                                      {sess.isHoh && <span style={{ fontSize:11 }}>🔊</span>}
                                    </div>
                                  </div>
                                  {/* Book button */}
                                  {sess.bookingUrl && (
                                    <a href={sess.bookingUrl} target="_blank" rel="noopener"
                                      className="book-btn"
                                      style={{
                                        display:"flex", alignItems:"center", justifyContent:"center",
                                        padding:"0 14px", alignSelf:"stretch",
                                        background:`${film.color}22`,
                                        borderLeft:`1px solid ${film.color}33`,
                                        textDecoration:"none", cursor:"pointer",
                                        transition:"background 0.15s",
                                      }}>
                                      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke={film.accent} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
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
              <div style={{ position:"relative" }}>
                {/* Time axis header */}
                <div style={{ marginLeft:160, position:"relative", height:28, marginBottom:2 }}>
                  {hourMarks.map(m => (
                    <div key={m} style={{
                      position:"absolute", left:`${pct(m)}%`, transform:"translateX(-50%)",
                      fontSize:11, fontFamily:"'JetBrains Mono',monospace", color:"#555", fontWeight:500,
                    }}>{minToTime(m)}</div>
                  ))}
                </div>

                {/* Film rows */}
                <div ref={tlRef} style={{ position:"relative" }}>
                  {dayFilms.map((film, fi) => (
                    <div key={film.id} style={{
                      display:"flex", alignItems:"stretch", marginBottom:6,
                      background: fi % 2 === 0 ? "rgba(255,255,255,0.012)" : "transparent",
                      borderRadius:10, overflow:"hidden",
                    }}>
                      {/* Film label */}
                      <div style={{
                        width:160, flexShrink:0, padding:"10px 12px",
                        display:"flex", flexDirection:"column", justifyContent:"center",
                        borderRight:"1px solid #1a1a24",
                      }}>
                        <div style={{ display:"flex", alignItems:"center", gap:6, marginBottom:3 }}>
                          <div style={{ width:4, height:28, borderRadius:2, background:film.color, flexShrink:0 }} />
                          <div>
                            <div style={{ fontSize:12, fontWeight:700, color:"#ddd", lineHeight:1.2 }}>
                              {film.film_url ? (
                                <a href={film.film_url} target="_blank" rel="noopener" style={{ color:"#ddd", textDecoration:"none" }}>{film.title}</a>
                              ) : film.title}
                            </div>
                            <div style={{ display:"flex", gap:4, marginTop:3, alignItems:"center" }}>
                              <span style={{
                                fontSize:9, padding:"1px 5px", borderRadius:3, fontWeight:700,
                                background:rBg[film.rating]||"#555", color:"#fff",
                              }}>{film.rating}</span>
                              <span style={{ fontSize:10, color:"#555" }}>{film.runtime}min</span>
                              <span style={{ fontSize:10, color:"#444" }}>{film.genre}</span>
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
                            width:1, background:"#151520", zIndex:0,
                          }} />
                        ))}
                        {halfMarks.map(m => (
                          <div key={m} style={{
                            position:"absolute", left:`${pct(m)}%`, top:0, bottom:0,
                            width:1, background:"#0e0e16", zIndex:0,
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
                                borderRadius:6,
                                overflow:"hidden",
                                zIndex: isHov ? 10 : 2,
                                transform: isHov ? "translateY(-50%) scaleY(1.12)" : "translateY(-50%) scaleY(1)",
                                transition:"transform 0.15s ease, box-shadow 0.15s ease",
                                boxShadow: isHov ? `0 4px 24px ${film.color}55, 0 0 0 1px ${film.color}88` : `0 1px 4px rgba(0,0,0,0.3)`,
                              }}>
                              {/* Ads portion */}
                              <div style={{
                                width:`${(adsWidth / totalWidth) * 100}%`,
                                background:`repeating-linear-gradient(120deg, ${film.color}40, ${film.color}40 4px, ${film.color}25 4px, ${film.color}25 8px)`,
                                display:"flex", alignItems:"center", justifyContent:"center",
                                borderRight:`1px dashed ${film.color}80`,
                                flexShrink:0,
                              }}>
                                <span style={{ fontSize:7, fontWeight:700, color:film.accent, letterSpacing:0.5, textTransform:"uppercase", opacity:0.8 }}>ADS</span>
                              </div>
                              {/* Film portion */}
                              <div style={{
                                flex:1,
                                background:`linear-gradient(135deg, ${film.color}cc 0%, ${film.color}99 100%)`,
                                padding:"3px 8px",
                                display:"flex", alignItems:"center", gap:6,
                                minWidth:0,
                              }}>
                                <div style={{ flex:1, minWidth:0 }}>
                                  <div style={{
                                    fontSize:11, fontWeight:700, color:"#fff",
                                    whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis",
                                    textShadow:"0 1px 3px rgba(0,0,0,0.5)",
                                  }}>
                                    {sess.time} – {minToTime(sess.startMin + film.runtime)}
                                    {sess.isHoh ? "  🔊" : ""}
                                    {sess.screen ? `  📍${sess.screen}` : ""}
                                  </div>
                                  {isHov && (
                                    <div style={{ fontSize:9, color:"rgba(255,255,255,0.7)", marginTop:1 }}>
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
                                      background:"rgba(255,255,255,0.18)", border:"1px solid rgba(255,255,255,0.25)",
                                      textDecoration:"none", flexShrink:0,
                                      cursor:"pointer", transition:"background 0.15s",
                                    }}>
                                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
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
                        position:"absolute", left:`calc(160px + ${((nowMin-axisStart)/axisDuration)*100}% * (100% - 160px) / 100%)`,
                        top:28, bottom:0, width:2, background:"#f918ac", zIndex:20,
                        boxShadow:"0 0 8px #f918ac88",
                      }}>
                        <div style={{
                          position:"absolute", top:-8, left:-12,
                          fontSize:8, fontWeight:700, color:"#f918ac",
                          background:"#08080e", padding:"1px 4px", borderRadius:3,
                          fontFamily:"'JetBrains Mono',monospace",
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
          /* ==================== GRID OVERVIEW ==================== */
          <>
            <div style={{ fontSize:16, fontWeight:700, marginBottom:14, color:"#ddd" }}>
              Full Schedule Overview
              <span style={{ fontSize:12, fontWeight:400, color:"#555", marginLeft:8 }}>
                All upcoming screenings
              </span>
            </div>

            <div style={{ overflowX:"auto", borderRadius:10, border:"1px solid #1a1a24" }}>
              <table style={{ width:"100%", borderCollapse:"collapse", minWidth:600 }}>
                <thead>
                  <tr>
                    <th style={{ padding:"10px 12px", textAlign:"left", fontSize:11, color:"#555", fontWeight:600, borderBottom:"1px solid #1a1a24", position:"sticky", left:0, background:"#0c0c14", zIndex:5, minWidth:140 }}>Film</th>
                    {allDates.map(d => {
                      const info = formatDayTab(d);
                      const isT = d === today;
                      const isSel = d === selDate;
                      return (
                        <th key={d} onClick={() => { setSelDate(d); setView("day"); }} style={{
                          padding:"8px 6px", textAlign:"center", fontSize:10, fontWeight:600,
                          borderBottom:"1px solid #1a1a24", cursor:"pointer",
                          color: isT ? "#f918ac" : isSel ? "#f918ac" : "#666",
                          background: isT ? "rgba(249,24,172,0.05)" : "transparent",
                          borderBottomColor: isT ? "#f918ac" : "#1a1a24",
                          minWidth:70, transition:"color 0.15s",
                        }}>
                          <div>{info.day}</div>
                          <div style={{ fontSize:16, fontWeight:700, lineHeight:1.1 }}>{info.num}</div>
                          <div style={{ fontSize:9, color:"#444" }}>{info.mon}</div>
                        </th>
                      );
                    })}
                  </tr>
                </thead>
                <tbody>
                  {films.filter(f => allDates.some(d => f.showtimes[d])).map((film, fi) => (
                    <tr key={film.id} style={{ background: fi%2===0 ? "rgba(255,255,255,0.01)" : "transparent" }}>
                      <td style={{
                        padding:"8px 10px", borderBottom:"1px solid #111118",
                        position:"sticky", left:0, background: fi%2===0 ? "#0d0d13" : "#0a0a10", zIndex:4,
                      }}>
                        <div style={{ display:"flex", alignItems:"center", gap:6 }}>
                          <div style={{ width:3, height:22, borderRadius:2, background:film.color }} />
                          <div>
                            <div style={{ fontSize:11, fontWeight:600, color:"#ccc" }}>
                              {film.film_url ? (
                                <a href={film.film_url} target="_blank" rel="noopener" style={{ color:"#ccc", textDecoration:"none" }}>{film.title}</a>
                              ) : film.title}
                            </div>
                            <div style={{ display:"flex", gap:3, marginTop:2 }}>
                              <span style={{ fontSize:8, padding:"0px 4px", borderRadius:2, background:rBg[film.rating]||"#555", color:"#fff", fontWeight:700 }}>{film.rating}</span>
                              <span style={{ fontSize:9, color:"#444" }}>{film.runtime}m</span>
                            </div>
                          </div>
                        </div>
                      </td>
                      {allDates.map(d => {
                        const times = film.showtimes[d];
                        const isT = d === today;
                        return (
                          <td key={d}
                            style={{
                              padding:"6px 4px", textAlign:"center",
                              borderBottom:"1px solid #111118",
                              borderLeft:"1px solid #111118",
                              background: isT ? "rgba(249,24,172,0.03)" : "transparent",
                            }}>
                            {times ? (
                              <div style={{ display:"flex", flexDirection:"column", gap:3, alignItems:"center" }}>
                                {times.map((t,i) => {
                                  const isHoh = film.hoh?.[d]?.includes(t);
                                  const bookingUrl = film.bookingUrls?.[d]?.[t];
                                  const screen = film.screens?.[d]?.[t];
                                  const pill = (
                                    <span key={i} style={{
                                      fontSize:11, fontWeight:600, padding:"3px 8px", borderRadius:5,
                                      background:`${film.color}22`, border:`1px solid ${film.color}44`,
                                      color:film.accent, fontFamily:"'JetBrains Mono',monospace",
                                      whiteSpace:"nowrap", transition:"background 0.15s",
                                      cursor: bookingUrl ? "pointer" : "default",
                                      display:"inline-flex", alignItems:"center", gap:3,
                                    }}
                                    title={screen ? `${screen}${isHoh ? " · HoH" : ""}` : (isHoh ? "Hard of Hearing" : "")}
                                    >
                                      {t}{isHoh ? " 🔊" : ""}
                                    </span>
                                  );
                                  return bookingUrl ? (
                                    <a key={i} href={bookingUrl} target="_blank" rel="noopener" style={{ textDecoration:"none" }}>{pill}</a>
                                  ) : pill;
                                })}
                              </div>
                            ) : (
                              <span style={{ color:"#1a1a24", fontSize:14 }}>—</span>
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p style={{ fontSize:10, color:"#444", marginTop:8 }}>Click any time to book directly. Click a date header to switch to Timeline View.</p>
          </>
        )}

        {/* Legend */}
        <div style={{ marginTop:20, padding:"14px 16px", borderRadius:10, background:"rgba(255,255,255,0.015)", border:"1px solid #151520" }}>
          <div style={{ display:"flex", gap:12, flexWrap:"wrap", marginBottom:8 }}>
            {films.filter(f => allDates.some(d => f.showtimes[d])).map(f => (
              <div key={f.id} style={{ display:"flex", alignItems:"center", gap:5 }}>
                <div style={{ width:10, height:10, borderRadius:3, background:f.color }} />
                <span style={{ fontSize:10, color:"#777" }}>{f.title}</span>
              </div>
            ))}
          </div>
          {!isMobile && (
            <div style={{ display:"flex", gap:16, flexWrap:"wrap", fontSize:10, color:"#555" }}>
              <div style={{ display:"flex", alignItems:"center", gap:4 }}>
                <div style={{ width:20, height:10, borderRadius:2, background:"repeating-linear-gradient(120deg, #f918ac40, #f918ac40 3px, #f918ac25 3px, #f918ac25 6px)" }} />
                <span>= ~{ADS_MIN}min ads/trailers</span>
              </div>
              <span style={{ display:"flex", alignItems:"center", gap:3 }}>🔊 = Subtitled (Hard of Hearing)</span>
              <span style={{ display:"flex", alignItems:"center", gap:3 }}>📍 = Screen number</span>
              <span>Bar length = full session (ads + film)</span>
            </div>
          )}
          <div style={{ display:"flex", gap:12, flexWrap:"wrap", fontSize:10, color:"#555", marginTop: isMobile ? 0 : undefined }}>
            {isMobile && <span style={{ display:"flex", alignItems:"center", gap:3 }}>🔊 = Hard of Hearing</span>}
            <span style={{ display:"flex", alignItems:"center", gap:3 }}>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#777" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M2 9a3 3 0 0 1 0 6v2a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-2a3 3 0 0 1 0-6V7a2 2 0 0 0-2-2H4a2 2 0 0 0-2 2Z"/>
                <path d="M13 5v2"/><path d="M13 17v2"/><path d="M13 11v2"/>
              </svg>
              = Book tickets
            </span>
          </div>
          {scrapedAt && (
            <p style={{ fontSize:9, color:"#333", marginTop:6 }}>
              Data from peckhamplex.london · Last updated {new Date(scrapedAt).toLocaleString("en-GB", { dateStyle:"medium", timeStyle:"short" })} · Always confirm at the cinema
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
