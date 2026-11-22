import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getMetrics, getTrending, postEvent, toSameOriginMedia } from "../api";

export default function TrendingPage({ userId, searchQuery = "", inspectorOpen, onCloseInspector }) {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [active, setActive] = useState(null);
  const [muted, setMuted] = useState(true);
  const [selectedCat, setSelectedCat] = useState("All");
  const [trendWindow, setTrendWindow] = useState("1m");
  const [hotTags, setHotTags] = useState([]);
  const [updatedAt, setUpdatedAt] = useState("");
  const videoRef = useRef(null);

  const reload = useCallback(async () => {
    setLoading(true);
    try {
      const d = await getTrending(24, trendWindow);
      setItems(d.items || []);
      setError("");
      setUpdatedAt(new Date().toLocaleTimeString("en-GB", { hour12: false }));
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setLoading(false);
    }
  }, [trendWindow]);

  useEffect(() => {
    reload();
    if (trendWindow !== "1m") return undefined;
    const t = setInterval(reload, 5000);
    return () => clearInterval(t);
  }, [reload, trendWindow]);

  useEffect(() => {
    if (!inspectorOpen) return undefined;
    const loadHot = () => {
      getMetrics()
        .then((m) => setHotTags(m.hot_tags || []))
        .catch(() => {});
    };
    loadHot();
    const t = setInterval(loadHot, 4000);
    return () => clearInterval(t);
  }, [inspectorOpen]);

  useEffect(() => {
    const v = videoRef.current;
    if (!v || !active) return;
    v.muted = muted;
    v.play().catch(() => {});
    postEvent({ user_id: userId, video_id: active.video_id, event_type: "view", watch_time: 0 }).catch(() => {});
  }, [active, muted, userId]);

  const categories = useMemo(() => {
    const set = new Set(["All"]);
    items.forEach((it) => {
      if (it.category) set.add(it.category);
    });
    return Array.from(set).slice(0, 10);
  }, [items]);

  const filtered = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();
    return items.filter((it) => {
      if (selectedCat !== "All" && it.category !== selectedCat) return false;
      if (!q) return true;
      const hay = `${it.title || ""} ${it.creator_id || ""} ${it.category || ""}`.toLowerCase();
      return hay.includes(q);
    });
  }, [items, selectedCat, searchQuery]);

  const maxHot = Math.max(1, ...hotTags.map((t) => Number(t.score) || 0));

  return (
    <div className="tk-split-layout">
      <section className="tk-explore">
        <div className="tk-explore-head">
          <div className="tk-win-switch" role="tablist" aria-label="Trending window">
            <button
              type="button"
              className={`tk-win-btn ${trendWindow === "1m" ? "active" : ""}`}
              onClick={() => setTrendWindow("1m")}
            >
              🔥 Trending 1 phút (Live)
            </button>
            <button
              type="button"
              className={`tk-win-btn ${trendWindow === "24h" ? "active" : ""}`}
              onClick={() => setTrendWindow("24h")}
            >
              📅 Trong ngày (24h)
            </button>
          </div>

          <div className="tk-cat-pills">
            {categories.map((c) => (
              <button
                key={c}
                type="button"
                className={`tk-cat-pill ${selectedCat === c ? "active" : ""}`}
                onClick={() => setSelectedCat(c)}
              >
                {c}
              </button>
            ))}
          </div>
          <button type="button" className="tk-ghost-btn" onClick={reload} disabled={loading}>
            {loading ? "Loading…" : `↻ ${updatedAt || "Refresh"}`}
          </button>
        </div>

        {loading && !items.length && <div className="tk-empty-msg">Loading Explore…</div>}
        {error && <div className="tk-empty-msg err">{error}</div>}
        {!loading && !filtered.length && <div className="tk-empty-msg">Không tìm thấy video phù hợp.</div>}

        <div className="tk-explore-grid">
          {filtered.map((it, i) => (
            <button
              key={it.video_id}
              type="button"
              className="tk-exp-card"
              onClick={() => setActive(it)}
            >
              <div className="tk-exp-thumb-box">
                {it.thumb_url ? (
                  <img src={toSameOriginMedia(it.thumb_url)} alt="" className="tk-exp-thumb" loading="lazy" />
                ) : (
                  <div className="tk-exp-thumb placeholder">▶</div>
                )}
                <span className="tk-exp-rank">#{i + 1}</span>
                <span className="tk-exp-stats">♥ {it.likes ?? 0} · ▶ {it.views ?? 0}</span>
              </div>
              <div className="tk-exp-meta">
                <strong className="tk-exp-title">{it.title || it.video_id}</strong>
                <span className="tk-exp-author">
                  @{it.creator_id || "creator"}
                  {it.category ? ` · #${it.category}` : ""}
                </span>
              </div>
            </button>
          ))}
        </div>

        {active && (
          <div className="tk-modal" role="dialog" aria-label="Watch video" onClick={() => setActive(null)}>
            <div className="tk-modal-inner" onClick={(e) => e.stopPropagation()}>
              <button type="button" className="tk-modal-close" onClick={() => setActive(null)}>
                ✕
              </button>
              <video
                key={active.video_id}
                ref={videoRef}
                className="tk-modal-video"
                src={toSameOriginMedia(active.media_url)}
                poster={toSameOriginMedia(active.thumb_url) || undefined}
                playsInline
                autoPlay
                controls
                muted={muted}
                onClick={() => setMuted((m) => !m)}
              />
              <div className="tk-modal-cap">
                <strong>@{active.creator_id}</strong>
                <span>{active.title}</span>
                <em>heat {active.heat} · ♥ {active.likes} · ▶ {active.views}</em>
              </div>
            </div>
          </div>
        )}
      </section>

      {inspectorOpen && (
        <aside className="tk-inspector">
          <div className="tk-ins-head">
            <div>
              <h3>Explore · Speed Layer Inspector</h3>
              <p>Hot Tags từ cửa sổ Tumbling 60s đang xếp hạng trang Explore</p>
            </div>
            <button type="button" className="tk-ins-close" onClick={onCloseInspector}>
              ✕
            </button>
          </div>
          <div className="tk-ins-body">
            <div className="tk-ins-card">
              <div className="tk-ins-card-head">
                <strong>Hot Tags (Redis ZSET)</strong>
                <code>Window 60s</code>
              </div>
              {hotTags.length === 0 ? (
                <p className="tk-ins-muted">Chưa có hot tags trong cửa sổ hiện tại.</p>
              ) : (
                <div className="tk-aff-list">
                  {hotTags.slice(0, 12).map((t) => (
                    <div key={t.tag} className="tk-aff-row">
                      <div className="tk-aff-meta">
                        <span>#{t.tag}</span>
                        <code>{Math.round(t.score)}</code>
                      </div>
                      <div className="tk-aff-track">
                        <div
                          className="tk-aff-fill warn"
                          style={{ width: `${Math.min(100, (Number(t.score) / maxHot) * 100)}%` }}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </aside>
      )}
    </div>
  );
}
