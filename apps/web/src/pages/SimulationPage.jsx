import { useCallback, useEffect, useState } from "react";
import { getSimulationLive, toSameOriginMedia } from "../api";

const EVENT_META = {
  like: { label: "♥ LIKE", cls: "ev-like" },
  share: { label: "↗ SHARE", cls: "ev-share" },
  skip: { label: "✕ SKIP", cls: "ev-skip" },
  watch: { label: "▶ WATCH", cls: "ev-watch" },
  view: { label: "👁 VIEW", cls: "ev-view" },
};

function fmtTime(ts) {
  if (!ts) return "";
  try {
    return new Date(ts).toLocaleTimeString("en-GB", { hour12: false });
  } catch {
    return "";
  }
}

export default function SimulationPage() {
  const [data, setData] = useState({ customers: [], providers: [], events_per_sec: 0, extract_stats: {} });
  const [paused, setPaused] = useState(false);
  const [hoveredKey, setHoveredKey] = useState("");
  const [error, setError] = useState("");

  const poll = useCallback(async () => {
    try {
      const res = await getSimulationLive();
      setData(res);
      setError("");
    } catch (e) {
      setError(String(e.message || e));
    }
  }, []);

  useEffect(() => {
    poll();
    if (paused) return undefined;
    const t = setInterval(poll, 1600);
    return () => clearInterval(t);
  }, [poll, paused]);

  const customers = data.customers || [];
  const providers = data.providers || [];
  const ext = data.extract_stats || {};

  return (
    <section className="sim-wall">
      <header className="sim-header">
        <div>
          <h1>Live Simulation Wall · 5 Customers &amp; 5 Providers</h1>
          <p>
            Trực quan hóa 100% dòng dữ liệu thật từ <code>traffic-simulator</code> (hàng trên) và{" "}
            <code>upload-simulator</code> (hàng dưới) — rê chuột vào bất kỳ điện thoại nào để phát thử video
          </p>
        </div>

        <div className="sim-header-actions">
          <span className="sim-stat-pill">
            Kafka <strong>{data.events_per_sec ?? 0}</strong> ev/s
          </span>
          <span className="sim-stat-pill ok">
            Extracted <strong>{(ext.completed_total ?? 0).toLocaleString()}</strong>
            {ext.last_ms ? ` · ${ext.last_ms}ms` : ""}
          </span>
          <button
            type="button"
            className={`sim-pause-btn ${paused ? "paused" : ""}`}
            onClick={() => setPaused((p) => !p)}
          >
            <span className="tk-live-dot" aria-hidden="true" />
            <span>{paused ? "Paused" : "Live (1.6s)"}</span>
          </button>
        </div>
      </header>

      {error && <div className="tk-empty-msg err">{error}</div>}

      {/* TOP ROW: 5 CUSTOMER PHONES */}
      <div className="sim-band">
        <div className="sim-band-head">
          <h2>Hàng trên · 5 Customers đang lướt For You</h2>
          <code>traffic-simulator → POST /api/events → Kafka events.user</code>
        </div>

        <div className="sim-grid-5">
          {customers.map((c, idx) => {
            const meta = EVENT_META[c.event_type] || { label: c.event_type, cls: "ev-watch" };
            const hoverId = `c-${idx}-${c.video_id}`;
            const isHover = hoveredKey === hoverId;
            const pct =
              c.event_type === "watch" && c.duration_sec > 0
                ? Math.min(100, Math.round((c.watch_time / c.duration_sec) * 100))
                : c.event_type === "like" || c.event_type === "share"
                  ? 78
                  : c.event_type === "skip"
                    ? 12
                    : 30;
            return (
              <div
                key={`slot-c-${idx}`}
                className="sim-phone"
                onMouseEnter={() => setHoveredKey(hoverId)}
                onMouseLeave={() => setHoveredKey("")}
              >
                <div className="sim-phone-top">
                  <span className="sim-handle">@{c.user_id}</span>
                  <span className="sim-ts">{fmtTime(c.ts)}</span>
                </div>

                <div className="sim-screen">
                  {isHover && c.media_url ? (
                    <video
                      src={toSameOriginMedia(c.media_url)}
                      className="sim-media"
                      autoPlay
                      muted
                      loop
                      playsInline
                    />
                  ) : c.thumb_url ? (
                    <img src={toSameOriginMedia(c.thumb_url)} alt="" className="sim-media" />
                  ) : (
                    <div className="sim-media placeholder">▶</div>
                  )}

                  <div className="sim-vignette" />

                  <div key={c.event_id} className={`sim-action-badge ${meta.cls}`}>
                    <span>{meta.label}</span>
                    {c.event_type === "watch" && c.watch_time > 0 && <small>{c.watch_time}s</small>}
                    <small>{c.weight > 0 ? `+${c.weight}` : c.weight}</small>
                  </div>

                  <div className="sim-phone-bottom">
                    <div className="sim-creator">@{c.creator_id}</div>
                    <div className="sim-title">{c.title}</div>
                    <div className="sim-cat">#{c.category}</div>
                    <div className="sim-bar-track">
                      <div className="sim-bar-fill" style={{ width: `${pct}%` }} />
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* BOTTOM ROW: 5 PROVIDER PHONES */}
      <div className="sim-band">
        <div className="sim-band-head">
          <h2>Hàng dưới · 5 Providers đang đăng Video mới (Cold-start Pool)</h2>
          <code>upload-simulator (5 vid / 15s) → POST /api/upload → Kafka events.extract → ffprobe</code>
        </div>

        <div className="sim-grid-5">
          {providers.map((p, idx) => {
            const hoverId = `p-${idx}-${p.video_id}`;
            const isHover = hoveredKey === hoverId;
            const st = p.status || "enriched";
            return (
              <div
                key={`slot-p-${idx}`}
                className="sim-phone provider"
                onMouseEnter={() => setHoveredKey(hoverId)}
                onMouseLeave={() => setHoveredKey("")}
              >
                <div className="sim-phone-top">
                  <span className="sim-handle">@{p.creator_id}</span>
                  <span className={`sim-st-pill st-${st}`}>{st}</span>
                </div>

                <div className="sim-screen">
                  {isHover && p.media_url ? (
                    <video
                      src={toSameOriginMedia(p.media_url)}
                      className="sim-media"
                      autoPlay
                      muted
                      loop
                      playsInline
                    />
                  ) : p.thumb_url ? (
                    <img src={toSameOriginMedia(p.thumb_url)} alt="" className="sim-media" />
                  ) : (
                    <div className="sim-media placeholder">⬆</div>
                  )}

                  <div className="sim-vignette" />

                  <div key={p.video_id} className="sim-upload-badge">
                    {st === "enriched" ? "✓ ENRICHED · Cold-start" : "⚙ EXTRACTING ffprobe…"}
                  </div>

                  <div className="sim-phone-bottom">
                    <div className="sim-title">{p.title}</div>
                    <div className="sim-cat">
                      #{p.category}
                      {p.duration_sec ? ` · ${p.duration_sec}s` : ""}
                    </div>
                    <div className="sim-pipe-steps">
                      <span className="done">Upload</span>
                      <span className={st !== "draft" ? "done" : ""}>Extract</span>
                      <span className={st === "enriched" ? "done" : ""}>Enriched</span>
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </section>
  );
}
