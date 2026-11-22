import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  followCreator,
  getFeed,
  postEvent,
  putWeights,
  toSameOriginMedia,
  unfollowCreator,
} from "../api";

const BATCH_SIZE = 20;

const TAG_NOISE = new Set([
  "shortvideo",
  "short_video",
  "demo",
  "auto_upload",
  "coldstart",
  "cold_start",
  "short_form",
  "mid_form",
  "long_form",
  "vertical",
  "landscape",
  "square",
  "hd",
  "high_fps",
]);

function nowTime() {
  return new Date().toLocaleTimeString("en-GB", { hour12: false });
}

export default function FeedPage({ userId, inspectorOpen, onCloseInspector }) {
  const [items, setItems] = useState([]);
  const [idx, setIdx] = useState(0);
  const [muted, setMuted] = useState(true);
  const [liked, setLiked] = useState({});
  const [followedMap, setFollowedMap] = useState({});
  const [sharedFlash, setSharedFlash] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [progress, setProgress] = useState(0);
  const [timeLabel, setTimeLabel] = useState("0.0s");

  // Contextual Inspector state (hydrated from Redis Bloom Filter + user affinity + Top 10 live diff)
  const [bloomSeenCount, setBloomSeenCount] = useState(0);
  const [sessionEvents, setSessionEvents] = useState([]);
  const [sessionAffinity, setSessionAffinity] = useState({});
  const [weights, setWeights] = useState({ alpha: 0.35, beta: 0.35, gamma: 0.3 });
  const [savingWeights, setSavingWeights] = useState(false);
  const [rankDiffMap, setRankDiffMap] = useState({});
  const [top10Loading, setTop10Loading] = useState(false);
  const [top10UpdatedAt, setTop10UpdatedAt] = useState("");

  const videoRef = useRef(null);
  const watchStart = useRef(Date.now());
  const touchY = useRef(null);
  const wheelLock = useRef(false);
  const lastViewed = useRef("");
  const itemsRef = useRef([]);
  const idxRef = useRef(0);
  const diffTimerRef = useRef(null);
  const prefetchingRef = useRef(false);

  useEffect(() => {
    itemsRef.current = items;
    idxRef.current = idx;
  }, [items, idx]);

  const pushSessionEvent = useCallback((event_type, video, detail = "") => {
    setSessionEvents((prev) => [
      { ts: nowTime(), event_type, video_id: video.video_id, detail },
      ...prev.slice(0, 7),
    ]);
    const cat =
      video.category && !TAG_NOISE.has(String(video.category).toLowerCase())
        ? String(video.category).toLowerCase()
        : null;
    const rawTags = [...(video.tags || []), ...(cat ? [cat] : [])]
      .map((t) => String(t || "").toLowerCase().trim())
      .filter((t) => t && !TAG_NOISE.has(t));
    if (rawTags.length) {
      const delta =
        event_type === "like" ? 3.0 : event_type === "share" ? 4.0 : event_type === "skip" ? -0.5 : 0.8;
      setSessionAffinity((prev) => {
        const next = { ...prev };
        rawTags.slice(0, 3).forEach((t) => {
          next[t] = Math.max(0, Number(((next[t] || 0) + delta).toFixed(1)));
        });
        return next;
      });
    }
  }, []);

  /**
   * TikTok-style real-time queue re-ranking:
   * Keeps items[0..idx] untouched (zero playback glitch),
   * replaces items[idx+1...] with freshly ranked unseen batch from API,
   * and computes exact rank diffs vs previous upcoming Top 10.
   */
  const refreshUpcomingQueue = useCallback(async () => {
    if (prefetchingRef.current) return;
    prefetchingRef.current = true;
    setTop10Loading(true);
    try {
      const data = await getFeed(userId, BATCH_SIZE);
      const curIdx = idxRef.current;
      const curList = itemsRef.current;
      const curPlayingId = curList[curIdx]?.video_id;

      // Snapshot previous upcoming Top 10 ranks before splicing
      const prevRankMap = {};
      curList.slice(curIdx + 1, curIdx + 11).forEach((v, i) => {
        prevRankMap[v.video_id] = i + 1;
      });

      const freshUpcoming = (data.items || []).filter((v) => v.video_id !== curPlayingId);
      const nextDiffs = {};
      if (Object.keys(prevRankMap).length > 0) {
        freshUpcoming.slice(0, 10).forEach((v, i) => {
          const newRank = i + 1;
          if (prevRankMap[v.video_id] == null) nextDiffs[v.video_id] = "NEW";
          else nextDiffs[v.video_id] = prevRankMap[v.video_id] - newRank;
        });
      }

      setItems((prev) => {
        const safeIdx = Math.min(idxRef.current, Math.max(prev.length - 1, 0));
        const head = prev.slice(0, safeIdx + 1);
        return [...head, ...freshUpcoming];
      });
      setRankDiffMap(nextDiffs);
      setTop10UpdatedAt(nowTime());

      if (typeof data.bloom_seen_count === "number") {
        setBloomSeenCount(data.bloom_seen_count);
      }
      if (data.user_affinity && typeof data.user_affinity === "object") {
        setSessionAffinity(data.user_affinity);
      }
    } catch {
      /* ignore */
    } finally {
      setTop10Loading(false);
      prefetchingRef.current = false;
    }
  }, [userId]);

  const scheduleQueueRerank = useCallback(() => {
    if (diffTimerRef.current) clearTimeout(diffTimerRef.current);
    diffTimerRef.current = setTimeout(() => {
      refreshUpcomingQueue();
    }, 3500);
  }, [refreshUpcomingQueue]);

  useEffect(() => {
    return () => {
      if (diffTimerRef.current) clearTimeout(diffTimerRef.current);
    };
  }, []);

  const reload = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const data = await getFeed(userId, BATCH_SIZE);
      const list = data.items || [];
      setItems(list);
      setIdx(0);
      lastViewed.current = "";
      setRankDiffMap({});
      setTop10UpdatedAt(nowTime());
      const fMap = {};
      list.forEach((v) => {
        if (v.creator_id) fMap[v.creator_id] = Boolean(v.followed);
      });
      setFollowedMap(fMap);
      if (list[0]?.explain?.weights) {
        setWeights(list[0].explain.weights);
      }
      if (typeof data.bloom_seen_count === "number") {
        setBloomSeenCount(data.bloom_seen_count);
      }
      if (data.user_affinity && typeof data.user_affinity === "object") {
        setSessionAffinity(data.user_affinity);
      }
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setLoading(false);
    }
  }, [userId]);

  useEffect(() => {
    setSessionEvents([]);
    setSessionAffinity({});
    setBloomSeenCount(0);
    setRankDiffMap({});
    reload();
  }, [reload]);

  // Infinite Feed: auto-replenish whenever <= 5 videos remain ahead in queue
  useEffect(() => {
    if (loading || items.length === 0) return;
    const remaining = items.length - 1 - idx;
    if (remaining <= 5) {
      refreshUpcomingQueue();
    }
  }, [idx, items.length, loading, refreshUpcomingQueue]);

  const cur = items[idx] || null;
  const curId = cur?.video_id || "";

  // Top 10 is ALWAYS derived directly from items[idx+1 .. idx+10] -> #1 is guaranteed to be next video!
  const top10 = useMemo(() => {
    return items.slice(idx + 1, idx + 11).map((v, i) => ({
      ...v,
      rank: i + 1,
      delta: rankDiffMap[v.video_id] ?? null,
    }));
  }, [items, idx, rankDiffMap]);

  useEffect(() => {
    const v = videoRef.current;
    if (!v || !curId || !cur) return;
    v.muted = muted;
    const play = () => v.play().catch(() => {});
    if (lastViewed.current !== curId) {
      watchStart.current = Date.now();
      lastViewed.current = curId;
      v.currentTime = 0;
      setProgress(0);
      setTimeLabel("0.0s");
      setBloomSeenCount((n) => n + 1);
      postEvent({ user_id: userId, video_id: curId, event_type: "view", watch_time: 0 }).catch(() => {});
      pushSessionEvent("view", cur, "0.0s");
    }
    play();
  }, [curId, cur, muted, userId, pushSessionEvent]);

  const sendEvent = async (event_type) => {
    if (!cur || busy) return false;
    const watch_time = Number(((Date.now() - watchStart.current) / 1000).toFixed(1));
    setBusy(true);
    try {
      await postEvent({ user_id: userId, video_id: cur.video_id, event_type, watch_time });
      pushSessionEvent(event_type, cur, `${watch_time}s`);
      scheduleQueueRerank();
      return true;
    } catch {
      return false;
    } finally {
      setBusy(false);
    }
  };

  const goNext = useCallback(() => {
    const item = itemsRef.current[idxRef.current];
    if (item) {
      const watch_time = Number(((Date.now() - watchStart.current) / 1000).toFixed(1));
      const et = watch_time < 2.0 ? "skip" : "watch";
      postEvent({
        user_id: userId,
        video_id: item.video_id,
        event_type: et,
        watch_time,
      }).catch(() => {});
      pushSessionEvent(et, item, `${watch_time}s`);
      scheduleQueueRerank();
    }
    setSharedFlash(false);
    setIdx((i) => Math.min(i + 1, Math.max(itemsRef.current.length - 1, 0)));
  }, [userId, pushSessionEvent, scheduleQueueRerank]);

  const goPrev = useCallback(() => {
    setSharedFlash(false);
    setIdx((i) => Math.max(i - 1, 0));
  }, []);

  useEffect(() => {
    const onKey = (e) => {
      if (e.target.tagName === "INPUT" || e.target.tagName === "SELECT") return;
      if (e.key === "ArrowDown" || e.key === "j") goNext();
      if (e.key === "ArrowUp" || e.key === "k") goPrev();
      if (e.key === "m") setMuted((m) => !m);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [goNext, goPrev]);

  const onWheel = (e) => {
    if (wheelLock.current) return;
    if (Math.abs(e.deltaY) < 40) return;
    wheelLock.current = true;
    if (e.deltaY > 0) goNext();
    else goPrev();
    setTimeout(() => {
      wheelLock.current = false;
    }, 450);
  };

  const onTouchStart = (e) => {
    touchY.current = e.touches[0].clientY;
  };
  const onTouchEnd = (e) => {
    if (touchY.current == null) return;
    const dy = e.changedTouches[0].clientY - touchY.current;
    touchY.current = null;
    if (Math.abs(dy) < 50) return;
    if (dy < 0) goNext();
    else goPrev();
  };

  const toggleLike = async () => {
    if (!cur || busy) return;
    const next = !liked[cur.video_id];
    setLiked((s) => ({ ...s, [cur.video_id]: next }));
    const ok = await sendEvent("like");
    if (!ok) setLiked((s) => ({ ...s, [cur.video_id]: !next }));
  };

  const toggleFollow = async () => {
    if (!cur?.creator_id || busy) return;
    const cId = cur.creator_id;
    const currently = Boolean(followedMap[cId]);
    setFollowedMap((m) => ({ ...m, [cId]: !currently }));
    try {
      if (currently) await unfollowCreator(userId, cId);
      else await followCreator(userId, cId);
    } catch {
      setFollowedMap((m) => ({ ...m, [cId]: currently }));
    }
  };

  const onShare = async () => {
    if (!cur || busy) return;
    const ok = await sendEvent("share");
    if (!ok) return;
    setSharedFlash(true);
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(`${window.location.origin}/?v=${cur.video_id}`);
      }
    } catch {
      /* ignore */
    }
    setTimeout(() => setSharedFlash(false), 1200);
  };

  const onSkip = async () => {
    if (!cur || busy) return;
    await sendEvent("skip");
    setSharedFlash(false);
    setIdx((i) => Math.min(i + 1, Math.max(itemsRef.current.length - 1, 0)));
  };

  const applyWeightsAndRerank = async () => {
    setSavingWeights(true);
    try {
      const saved = await putWeights(weights);
      setWeights(saved);
      await refreshUpcomingQueue();
    } finally {
      setSavingWeights(false);
    }
  };

  const onTimeUpdate = (e) => {
    const v = e.currentTarget;
    if (v.duration && Number.isFinite(v.duration)) {
      setProgress(Math.min(100, (v.currentTime / v.duration) * 100));
      setTimeLabel(`${v.currentTime.toFixed(1)}s / ${v.duration.toFixed(0)}s`);
    }
  };

  const onVideoError = () => {
    setItems((prev) => {
      if (!cur) return prev;
      const next = prev.filter((v) => v.video_id !== cur.video_id);
      setIdx((i) => Math.min(i, Math.max(next.length - 1, 0)));
      return next;
    });
  };

  const tags = cur
    ? [...(cur.tags || []), ...(cur.enriched_tags || [])]
        .map((t) => String(t || "").trim())
        .filter((t) => t && !TAG_NOISE.has(t.toLowerCase().replace(/\s+/g, "_")))
        .slice(0, 6)
    : [];

  const mediaSrc = toSameOriginMedia(cur?.media_url);
  const thumbSrc = toSameOriginMedia(cur?.thumb_url);
  const isFollowed = cur?.creator_id ? Boolean(followedMap[cur.creator_id]) : false;

  const affinityEntries = Object.entries(sessionAffinity)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5);
  const maxAff = Math.max(1, ...affinityEntries.map((x) => x[1]));

  return (
    <div className="tk-split-layout">
      <section className="tk-feed-stage" onWheel={onWheel} onTouchStart={onTouchStart} onTouchEnd={onTouchEnd}>
        {loading && !cur && <div className="tk-empty-msg">Loading For You…</div>}
        {error && <div className="tk-empty-msg err">{error}</div>}
        {!loading && !cur && <div className="tk-empty-msg">No videos in catalog yet.</div>}

        {cur && (
          <div className="tk-player-row">
            <div className="tk-phone-frame">
              <video
                key={`${userId}-${cur.video_id}`}
                ref={videoRef}
                className="tk-video"
                src={mediaSrc}
                poster={thumbSrc || undefined}
                playsInline
                autoPlay
                loop
                muted={muted}
                preload="auto"
                onTimeUpdate={onTimeUpdate}
                onError={onVideoError}
                onClick={() => setMuted((m) => !m)}
              />

              <div className="tk-video-vignette" />

              <div className="tk-video-top">
                <button type="button" className="tk-pill-btn" onClick={() => setMuted((m) => !m)}>
                  {muted ? "🔇 Muted" : "🔊 Sound"}
                </button>
                <span className="tk-time-pill">{timeLabel}</span>
              </div>

              <div className="tk-video-bottom">
                <div className="tk-author-row">
                  <strong className="tk-author">@{cur.creator_id || "creator"}</strong>
                  {cur.category && <span className="tk-cat-chip">{cur.category}</span>}
                </div>
                <p className="tk-caption">{cur.title}</p>
                {tags.length > 0 && (
                  <div className="tk-hashtags">
                    {tags.map((t) => (
                      <span key={`${cur.video_id}-${t}`}>#{t}</span>
                    ))}
                  </div>
                )}
                <div className="tk-scrub-track">
                  <div className="tk-scrub-fill" style={{ width: `${progress}%` }} />
                </div>
              </div>
            </div>

            <div className="tk-action-rail">
              <div className="tk-avatar-wrap">
                <div className="tk-avatar">{(cur.creator_id || "C").slice(0, 1).toUpperCase()}</div>
                <button
                  type="button"
                  className={`tk-follow-badge ${isFollowed ? "on" : ""}`}
                  onClick={toggleFollow}
                  title={isFollowed ? "Unfollow" : "Follow creator"}
                >
                  {isFollowed ? "✓" : "+"}
                </button>
              </div>

              <button
                type="button"
                className={`tk-rail-btn ${liked[cur.video_id] ? "liked" : ""}`}
                onClick={toggleLike}
                disabled={busy}
              >
                <span className="tk-rail-circle">♥</span>
                <span className="tk-rail-txt">{liked[cur.video_id] ? "Liked" : "Like"}</span>
              </button>

              <button
                type="button"
                className={`tk-rail-btn ${sharedFlash ? "active" : ""}`}
                onClick={onShare}
                disabled={busy}
              >
                <span className="tk-rail-circle">↗</span>
                <span className="tk-rail-txt">{sharedFlash ? "Copied" : "Share"}</span>
              </button>

              <button type="button" className="tk-rail-btn" onClick={onSkip} disabled={busy}>
                <span className="tk-rail-circle">✕</span>
                <span className="tk-rail-txt">Skip</span>
              </button>

              <div className="tk-rail-divider" />

              <button type="button" className="tk-nav-circle" onClick={goPrev} title="Previous video (↑)">
                ↑
              </button>
              <button type="button" className="tk-nav-circle" onClick={goNext} title="Next video (↓)">
                ↓
              </button>
            </div>
          </div>
        )}
      </section>

      {inspectorOpen && (
        <aside className="tk-inspector">
          <div className="tk-ins-head">
            <div>
              <h3>For You · Recommendation Inspector</h3>
              <p>Giải thích trực tiếp điểm đề xuất của video đang xem</p>
            </div>
            <button type="button" className="tk-ins-close" onClick={onCloseInspector}>
              ✕
            </button>
          </div>

          <div className="tk-ins-body">
            {cur?.explain && (
              <div className="tk-ins-card">
                <div className="tk-ins-card-head">
                  <strong>Tại sao đề xuất video này?</strong>
                  <code>{cur.video_id}</code>
                </div>

                <div className="tk-score-bars">
                  <ScoreBar
                    label={`Content-based (α=${cur.explain.weights?.alpha?.toFixed?.(2) ?? weights.alpha.toFixed(2)})`}
                    value={cur.explain.content}
                    tone="c1"
                  />
                  <ScoreBar
                    label={`Batch CF (β=${cur.explain.weights?.beta?.toFixed?.(2) ?? weights.beta.toFixed(2)})`}
                    value={cur.explain.cf_batch}
                    tone="c2"
                  />
                  <ScoreBar
                    label={`Realtime Speed (γ=${cur.explain.weights?.gamma?.toFixed?.(2) ?? weights.gamma.toFixed(2)})`}
                    value={cur.explain.realtime}
                    tone="c3"
                  />
                  {Number(cur.explain.cold_boost || 0) > 0 && (
                    <div className="tk-score-meta" style={{ color: "var(--warn)" }}>
                      <span>Cold-start Traffic Pool Boost</span>
                      <code>+{Number(cur.explain.cold_boost).toFixed(3)}</code>
                    </div>
                  )}
                </div>

                <div className="tk-final-score">
                  <span>Final Hybrid Score</span>
                  <strong>{cur.score}</strong>
                </div>

                <div className="tk-weight-mini">
                  <div className="tk-weight-mini-head">
                    <span>Chỉnh trọng số Serving:</span>
                    <button
                      type="button"
                      className="tk-link-btn"
                      onClick={applyWeightsAndRerank}
                      disabled={savingWeights}
                    >
                      {savingWeights ? "Đang xếp lại…" : "↻ Apply & Re-rank"}
                    </button>
                  </div>
                  <div className="tk-weight-sliders">
                    <label>
                      <span>α {weights.alpha.toFixed(2)}</span>
                      <input
                        type="range"
                        min="0"
                        max="1"
                        step="0.05"
                        value={weights.alpha}
                        onChange={(e) => setWeights((w) => ({ ...w, alpha: +e.target.value }))}
                      />
                    </label>
                    <label>
                      <span>β {weights.beta.toFixed(2)}</span>
                      <input
                        type="range"
                        min="0"
                        max="1"
                        step="0.05"
                        value={weights.beta}
                        onChange={(e) => setWeights((w) => ({ ...w, beta: +e.target.value }))}
                      />
                    </label>
                    <label>
                      <span>γ {weights.gamma.toFixed(2)}</span>
                      <input
                        type="range"
                        min="0"
                        max="1"
                        step="0.05"
                        value={weights.gamma}
                        onChange={(e) => setWeights((w) => ({ ...w, gamma: +e.target.value }))}
                      />
                    </label>
                  </div>
                </div>
              </div>
            )}

            <div className="tk-ins-card">
              <div className="tk-ins-card-head">
                <strong>Top 10 đề xuất tiếp theo (Live)</strong>
                <button
                  type="button"
                  className="tk-link-btn"
                  onClick={refreshUpcomingQueue}
                  disabled={top10Loading}
                >
                  {top10Loading ? "Đang xếp…" : `↻ ${top10UpdatedAt || "Re-rank"}`}
                </button>
              </div>
              <p className="tk-ins-muted">
                #1 luôn là video kế tiếp khi bấm ↓ · Tự nạp lô 20 video mới (Infinite Feed).
              </p>
              <div className="tk-top10-list">
                {top10.map((v) => (
                  <button
                    key={v.video_id}
                    type="button"
                    className="tk-top10-row"
                    style={{ cursor: "pointer", textAlign: "left", width: "100%" }}
                    onClick={() => {
                      const targetIdx = items.findIndex((x, i) => i > idx && x.video_id === v.video_id);
                      if (targetIdx !== -1) setIdx(targetIdx);
                    }}
                    title="Nhấn để phát video này ngay"
                  >
                    <span className="tk-top10-rank">#{v.rank}</span>
                    <div className="tk-top10-meta">
                      <strong>{v.title?.slice(0, 32) || v.video_id}</strong>
                      <span>
                        {v.category || "general"} · score {v.score}
                      </span>
                    </div>
                    {v.delta === "NEW" ? (
                      <span className="tk-diff new">NEW</span>
                    ) : typeof v.delta === "number" && v.delta > 0 ? (
                      <span className="tk-diff up">▲ +{v.delta}</span>
                    ) : typeof v.delta === "number" && v.delta < 0 ? (
                      <span className="tk-diff down">▼ {v.delta}</span>
                    ) : (
                      <span className="tk-diff same">•</span>
                    )}
                  </button>
                ))}
              </div>
            </div>

            <div className="tk-ins-card">
              <div className="tk-ins-card-head">
                <strong>Hồ sơ Realtime (@{userId})</strong>
                <code>Bloom: {bloomSeenCount} seen</code>
              </div>
              {affinityEntries.length === 0 ? (
                <p className="tk-ins-muted">Like hoặc xem video để thấy sở thích realtime cập nhật.</p>
              ) : (
                <div className="tk-aff-list">
                  {affinityEntries.map(([tag, val]) => (
                    <div key={tag} className="tk-aff-row">
                      <div className="tk-aff-meta">
                        <span>#{tag}</span>
                        <code>{val}</code>
                      </div>
                      <div className="tk-aff-track">
                        <div className="tk-aff-fill" style={{ width: `${(val / maxAff) * 100}%` }} />
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div className="tk-ins-card">
              <div className="tk-ins-card-head">
                <strong>Sự kiện bạn vừa gửi vào Kafka</strong>
                <code>events.user</code>
              </div>
              <div className="tk-event-list">
                {sessionEvents.map((ev, i) => (
                  <div key={`${ev.ts}-${ev.video_id}-${i}`} className="tk-event-row">
                    <span>{ev.ts}</span>
                    <strong className={`ev-${ev.event_type}`}>{ev.event_type}</strong>
                    <code>{ev.video_id.slice(0, 10)}</code>
                    <span>{ev.detail}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </aside>
      )}
    </div>
  );
}

function ScoreBar({ label, value, tone }) {
  const pct = Math.max(0, Math.min(100, (Number(value) || 0) * 100));
  return (
    <div className="tk-score-row">
      <div className="tk-score-meta">
        <span>{label}</span>
        <code>{Number(value || 0).toFixed(3)}</code>
      </div>
      <div className="tk-score-track">
        <div className={`tk-score-fill ${tone}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}
