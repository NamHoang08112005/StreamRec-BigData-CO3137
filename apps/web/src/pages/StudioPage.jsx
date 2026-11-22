import { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { getMetrics, getStudio, listVideos, toSameOriginMedia, uploadVideo } from "../api";

const DEFAULT_CREATOR = "creator_demo";
const CATEGORIES = ["general", "food", "travel", "sports", "music", "tech", "pets", "comedy", "fashion", "nature"];

export default function StudioPage({ inspectorOpen, onCloseInspector, onOpenInspector }) {
  const { creatorId: paramId } = useParams();
  const creatorId = paramId || DEFAULT_CREATOR;
  const [title, setTitle] = useState("");
  const [tags, setTags] = useState("");
  const [category, setCategory] = useState("food");
  const [file, setFile] = useState(null);
  const [preview, setPreview] = useState(null);
  const [result, setResult] = useState(null);
  const [busy, setBusy] = useState(false);
  const [studio, setStudio] = useState(null);

  const [metrics, setMetrics] = useState(null);
  const [recentUploads, setRecentUploads] = useState([]);

  const refreshStudio = useCallback(async () => {
    try {
      const data = await getStudio(creatorId);
      setStudio(data);
    } catch {
      /* ignore */
    }
  }, [creatorId]);

  const refreshPipeline = useCallback(async () => {
    try {
      const [m, vids] = await Promise.all([getMetrics(), listVideos()]);
      setMetrics(m);
      setRecentUploads((vids.items || []).slice(0, 8));
    } catch {
      /* ignore */
    }
  }, []);

  useEffect(() => {
    refreshStudio();
    const t = setInterval(refreshStudio, 4000);
    return () => clearInterval(t);
  }, [refreshStudio]);

  useEffect(() => {
    if (!inspectorOpen) return undefined;
    refreshPipeline();
    const t = setInterval(refreshPipeline, 3000);
    return () => clearInterval(t);
  }, [inspectorOpen, refreshPipeline]);

  useEffect(() => {
    if (!file) {
      setPreview(null);
      return undefined;
    }
    const url = URL.createObjectURL(file);
    setPreview(url);
    return () => URL.revokeObjectURL(url);
  }, [file]);

  const onSubmit = async (e) => {
    e.preventDefault();
    if (!file) return;
    setBusy(true);
    setResult(null);
    try {
      const fd = new FormData();
      fd.append("file", file);
      fd.append("title", title.trim() || file.name.replace(/\.[^.]+$/, ""));
      fd.append("tags", tags);
      fd.append("category", category);
      fd.append("creator_id", creatorId);
      const res = await uploadVideo(fd);
      setResult(res);
      setTitle("");
      setTags("");
      setFile(null);
      onOpenInspector?.();
      await Promise.all([refreshStudio(), refreshPipeline()]);
    } catch (err) {
      setResult({ error: String(err.message || err) });
    } finally {
      setBusy(false);
    }
  };

  const previewTags = tags
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean)
    .map((x) => `#${x}`)
    .join(" ");

  const db = metrics?.db || {};

  return (
    <div className="tk-split-layout">
      <section className="tk-studio">
        <header className="tk-studio-head">
          <div>
            <h1>TikTok Studio · Upload video</h1>
            <p>Đăng video với tư cách Provider (@{creatorId}) — hệ thống tự trích xuất metadata và phân phối lên For You</p>
          </div>
          <span className="tk-studio-pill">
            @{creatorId} · <strong>{studio?.followers ?? 0}</strong> followers
          </span>
        </header>

        <div className="tk-studio-grid">
          <form className="tk-studio-form" onSubmit={onSubmit}>
            <label className="tk-dropzone">
              {file ? (
                <div className="tk-drop-chosen">
                  <strong>{file.name}</strong>
                  <span>{(file.size / (1024 * 1024)).toFixed(2)} MB · Nhấn để đổi file khác</span>
                </div>
              ) : (
                <div className="tk-drop-empty">
                  <span className="tk-drop-ico">☁</span>
                  <strong>Select video to upload</strong>
                  <span>MP4 / WebM / MOV</span>
                </div>
              )}
              <input type="file" accept="video/*" onChange={(e) => setFile(e.target.files?.[0] || null)} hidden />
            </label>

            <label className="tk-field">
              <span>Caption</span>
              <input
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                placeholder="Viết mô tả ngắn cho video…"
                maxLength={120}
              />
            </label>

            <div className="tk-field-row">
              <label className="tk-field">
                <span>Hashtags (cách nhau bởi dấu phẩy)</span>
                <input value={tags} onChange={(e) => setTags(e.target.value)} placeholder="food, streetfood, hanoi" />
              </label>

              <label className="tk-field">
                <span>Category</span>
                <select value={category} onChange={(e) => setCategory(e.target.value)}>
                  {CATEGORIES.map((c) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </select>
              </label>
            </div>

            <div className="tk-studio-actions">
              <button type="submit" className="tk-post-btn" disabled={busy || !file}>
                {busy ? "Uploading…" : "Post"}
              </button>
              {result && !result.error && (
                <span className="tk-toast-inline ok">
                  Posted <code>{result.video_id}</code> · status <b>{result.status}</b>
                </span>
              )}
              {result?.error && <span className="tk-toast-inline err">{result.error}</span>}
            </div>
          </form>

          <div className="tk-preview-col">
            <div className="tk-preview-phone">
              {preview ? (
                <video src={preview} className="tk-preview-video" muted loop autoPlay playsInline />
              ) : (
                <div className="tk-preview-placeholder">
                  <span>▶</span>
                  <p>For You Preview</p>
                </div>
              )}
              <div className="tk-preview-overlay">
                <strong>@{creatorId}</strong>
                <p>{title || "Caption hiển thị tại đây…"}</p>
                <span>{previewTags || `#${category}`}</span>
              </div>
            </div>
          </div>
        </div>

        <div className="tk-studio-list-card">
          <h2>Video đã đăng của @{creatorId}</h2>
          {!(studio?.videos || []).length ? (
            <p className="tk-ins-muted">Chưa có video nào do @{creatorId} đăng tay.</p>
          ) : (
            <div className="tk-studio-videos">
              {(studio.videos || []).map((v) => (
                <div key={v.video_id} className="tk-studio-vid-row">
                  {v.thumb_url ? (
                    <img src={toSameOriginMedia(v.thumb_url)} alt="" className="tk-studio-thumb" />
                  ) : (
                    <div className="tk-studio-thumb placeholder">▶</div>
                  )}
                  <div className="tk-studio-vid-meta">
                    <strong>{v.title}</strong>
                    <span>
                      <code>{v.video_id}</code> · {v.category || "general"}
                      {v.duration_sec ? ` · ${Number(v.duration_sec).toFixed(1)}s` : ""}
                    </span>
                  </div>
                  <span className={`tk-status-chip st-${v.status}`}>{v.status}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </section>

      {inspectorOpen && (
        <aside className="tk-inspector">
          <div className="tk-ins-head">
            <div>
              <h3>Studio · Extract Pipeline Live</h3>
              <p>Số liệu thật của luồng Upload → Kafka → FFprobe Worker</p>
            </div>
            <button type="button" className="tk-ins-close" onClick={onCloseInspector}>
              ✕
            </button>
          </div>

          <div className="tk-ins-body">
            <div className="tk-ins-card">
              <div className="tk-ins-card-head">
                <strong>Extract Worker Live (Kafka)</strong>
                <code>events.extract</code>
              </div>
              <div className="tk-pipe-counts">
                <div>
                  <span>Extracted</span>
                  <strong className="ok">{(metrics?.extract_stats?.completed_total ?? 0).toLocaleString()}</strong>
                </div>
                <div>
                  <span>Catalog</span>
                  <strong>{db.enriched ?? 0}</strong>
                </div>
                <div>
                  <span>Speed</span>
                  <strong>{metrics?.extract_stats?.last_ms ? `${metrics.extract_stats.last_ms}ms` : "—"}</strong>
                </div>
              </div>
              <p className="tk-ins-muted">
                Catalog tự dọn giữ tối đa 60 video auto-upload gần nhất; <b>Extracted</b> tăng +5 mỗi 15s qua Kafka.
              </p>
            </div>

            <div className="tk-ins-card">
              <div className="tk-ins-card-head">
                <strong>Upload Simulator (Cold-start)</strong>
                <code>5 vid / {metrics?.auto_upload?.interval_sec ?? 15}s</code>
              </div>
              {metrics?.auto_upload?.last ? (
                <div className="tk-sim-last">
                  <div>
                    Latest: <code>{metrics.auto_upload.last.video_id}</code>
                  </div>
                  <span className={`tk-status-chip st-${metrics.auto_upload.last.status}`}>
                    {metrics.auto_upload.last.status}
                  </span>
                </div>
              ) : (
                <p className="tk-ins-muted">Đang chờ đợt upload tự động từ upload_pool…</p>
              )}
            </div>

            <div className="tk-ins-card">
              <div className="tk-ins-card-head">
                <strong>Video mới nhất qua Extract Worker</strong>
                <code>ffprobe</code>
              </div>
              <div className="tk-extract-feed">
                {recentUploads.map((v) => (
                  <div key={v.video_id} className="tk-extract-row">
                    <div>
                      <strong>{v.title?.slice(0, 34) || v.video_id}</strong>
                      <span>
                        @{v.creator_id}
                        {v.duration_sec ? ` · ${Number(v.duration_sec).toFixed(1)}s` : ""}
                        {(v.enriched_tags || []).length ? ` · ${v.enriched_tags.slice(0, 3).join(", ")}` : ""}
                      </span>
                    </div>
                    <span className={`tk-status-chip st-${v.status}`}>{v.status}</span>
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
