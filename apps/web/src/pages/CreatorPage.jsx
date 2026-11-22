import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { followCreator, getCreator, toSameOriginMedia, unfollowCreator } from "../api";

export default function CreatorPage({ userId }) {
  const { creatorId } = useParams();
  const navigate = useNavigate();
  const [data, setData] = useState(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const load = async () => {
    setError("");
    try {
      const res = await getCreator(creatorId, userId);
      setData(res);
    } catch (e) {
      setError(String(e.message || e));
    }
  };

  useEffect(() => {
    load().catch(() => {});
  }, [creatorId, userId]);

  const toggleFollow = async () => {
    if (!data || busy) return;
    const next = !data.followed;
    setBusy(true);
    setData((d) =>
      d
        ? {
            ...d,
            followed: next,
            counts: {
              ...d.counts,
              followers: Math.max(0, (d.counts?.followers || 0) + (next ? 1 : -1)),
            },
          }
        : d
    );
    try {
      if (next) await followCreator(userId, creatorId);
      else await unfollowCreator(userId, creatorId);
    } catch {
      await load();
    } finally {
      setBusy(false);
    }
  };

  if (error) {
    return (
      <section className="tt-creator">
        <p className="tt-overlay-msg error">{error}</p>
      </section>
    );
  }

  if (!data) {
    return (
      <section className="tt-creator">
        <p className="tt-muted">Loading channel…</p>
      </section>
    );
  }

  return (
    <section className="tt-creator">
      <header className="tt-creator-top">
        <button type="button" className="tt-back" onClick={() => navigate(-1)}>
          ←
        </button>
        <h1>@{data.creator_id}</h1>
        <span />
      </header>

      <div className="tt-creator-hero">
        <div className="tt-creator-avatar">{(data.creator_id || "?").slice(0, 1).toUpperCase()}</div>
        <p className="tt-creator-bio">{data.bio}</p>
        <div className="tt-creator-stats">
          <div>
            <strong>{data.counts?.videos ?? 0}</strong>
            <span>Videos</span>
          </div>
          <div>
            <strong>{data.counts?.followers ?? 0}</strong>
            <span>Followers</span>
          </div>
          <div>
            <strong>{data.counts?.likes ?? 0}</strong>
            <span>Likes</span>
          </div>
          <div>
            <strong>{data.counts?.views ?? 0}</strong>
            <span>Views</span>
          </div>
        </div>
        <button
          type="button"
          className={`tt-btn ${data.followed ? "ghost" : ""}`}
          onClick={toggleFollow}
          disabled={busy}
        >
          {data.followed ? "Subscribed" : "Subscribe"}
        </button>
        <Link className="tt-link-btn" to={`/studio/${encodeURIComponent(data.creator_id)}`}>
          Open as Provider Studio
        </Link>
      </div>

      <div className="tt-creator-grid">
        {(data.videos || []).map((v) => (
          <button
            key={v.video_id}
            type="button"
            className="tt-creator-cell"
            onClick={() => navigate("/")}
            title={v.title}
          >
            {v.thumb_url ? (
              <img src={toSameOriginMedia(v.thumb_url)} alt={v.title} />
            ) : (
              <div className="tt-creator-cell-empty">▶</div>
            )}
            <span className={`status-pill ${v.status}`}>{v.status}</span>
          </button>
        ))}
      </div>
      {!data.videos?.length && <p className="tt-muted">No playable videos on this channel yet.</p>}
    </section>
  );
}
