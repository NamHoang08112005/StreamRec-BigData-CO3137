import { useEffect, useState } from "react";
import { getMetrics, metricsWsUrl, putWeights, runBatchCf } from "../api";

function shortId(value) {
  if (!value) return "—";
  const s = String(value);
  return s.length > 14 ? `${s.slice(0, 10)}…` : s;
}

function formatTime(ts) {
  if (!ts) return "--:--:--";
  try {
    return new Date(ts).toLocaleTimeString("en-GB", { hour12: false });
  } catch {
    return "--:--:--";
  }
}

function normalizeTailItem(item) {
  if (!item) return null;
  if (typeof item === "string") {
    return { ts: null, topic: "events.user", event_type: item, user_id: null, video_id: null };
  }
  return {
    ts: item.ts || null,
    topic: item.topic || "events.user",
    event_type: item.event_type || "?",
    user_id: item.user_id || null,
    video_id: item.video_id || null,
  };
}

export default function DashboardPage() {
  const [metrics, setMetrics] = useState(null);
  const [epsSeries, setEpsSeries] = useState([0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
  const [alpha, setAlpha] = useState(0.35);
  const [beta, setBeta] = useState(0.35);
  const [gamma, setGamma] = useState(0.3);
  const [batchResult, setBatchResult] = useState(null);
  const [runningBatch, setRunningBatch] = useState(false);

  useEffect(() => {
    getMetrics()
      .then((m) => {
        setMetrics(m);
        if (m.weights) {
          setAlpha(m.weights.alpha);
          setBeta(m.weights.beta);
          setGamma(m.weights.gamma);
        }
        if (typeof m.events_per_sec === "number") {
          setEpsSeries((prev) => [...prev.slice(-23), m.events_per_sec]);
        }
      })
      .catch(() => { });

    const ws = new WebSocket(metricsWsUrl());
    ws.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data);
        setMetrics((prev) => ({
          ...(prev || {}),
          ...data,
          db: data.db || prev?.db,
        }));
        if (typeof data.events_per_sec === "number") {
          setEpsSeries((prev) => [...prev.slice(-23), data.events_per_sec]);
        }
      } catch {
        /* ignore */
      }
    };

    const poll = setInterval(() => {
      getMetrics()
        .then((m) => {
          setMetrics(m);
          if (typeof m.events_per_sec === "number") {
            setEpsSeries((prev) => [...prev.slice(-23), m.events_per_sec]);
          }
        })
        .catch(() => { });
    }, 4000);

    return () => {
      ws.close();
      clearInterval(poll);
    };
  }, []);

  const saveWeights = async () => {
    const w = await putWeights({ alpha, beta, gamma });
    setAlpha(w.alpha);
    setBeta(w.beta);
    setGamma(w.gamma);
  };

  const triggerBatch = async () => {
    setRunningBatch(true);
    try {
      const r = await runBatchCf();
      setBatchResult(r);
      const m = await getMetrics();
      setMetrics(m);
    } finally {
      setRunningBatch(false);
    }
  };

  const db = metrics?.db || {};
  const eventsTotal = metrics?.events_total ?? 0;
  const events1m = metrics?.events_1m ?? 0;
  const eventsPerSec = metrics?.events_per_sec ?? 0;
  const tail = (metrics?.event_tail || []).map(normalizeTailItem).filter(Boolean);
  const hllActive1m = metrics?.hll_active_1m ?? 0;
  const hllTotal = metrics?.hll_unique_users ?? db.users ?? 0;

  const chartW = 560;
  const chartH = 140;
  const maxEps = Math.max(10, ...epsSeries) * 1.15;
  const points = epsSeries
    .map((val, i) => {
      const x = (i / Math.max(1, epsSeries.length - 1)) * chartW;
      const y = chartH - (val / maxEps) * (chartH - 16) - 8;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <section className="ops-shell">
      <header className="ops-header">
        <div>
          <h1>System Ops · Big Data Telemetry</h1>
          <p>Giám sát toàn hệ thống Lambda Architecture (Kafka · Speed Layer · Batch CF / Spark ALS · Redis)</p>
        </div>
        <button type="button" className="ops-primary-btn" onClick={triggerBatch} disabled={runningBatch}>
          {runningBatch ? "Running Batch CF…" : "▶ Run Batch CF Now"}
        </button>
      </header>

      <div className="ops-kpi-grid">
        <div className="ops-kpi">
          <span>Velocity · Kafka Throughput</span>
          <strong className="accent">{eventsPerSec} <small>ev/s</small></strong>
          <em>Sliding {metrics?.velocity_window_sec ?? 30}s window</em>
        </div>
        <div className="ops-kpi">
          <span>Volume · Interactions (1m)</span>
          <strong>{Number(events1m).toLocaleString()}</strong>
          <em>{db.videos ?? 0} videos · tổng {(db.interactions ?? 0).toLocaleString()} dòng</em>
        </div>
        <div className="ops-kpi">
          <span>HyperLogLog · Active Users (1m)</span>
          <strong className="ok">{Number(hllActive1m).toLocaleString()}</strong>
          <em>Sliding 6×10s HLL · tổng phiên {Number(hllTotal).toLocaleString()}</em>
        </div>
        <div className="ops-kpi">
          <span>Batch CF · Collaborative Pairs</span>
          <strong>{(db.cf_pairs ?? 0).toLocaleString()}</strong>
          <em>
            {metrics?.batch_cf?.last?.finished_at
              ? `Last run ${formatTime(metrics.batch_cf.last.finished_at)}`
              : `Schedule every ${metrics?.batch_cf?.interval_sec ?? 90}s`}
          </em>
        </div>
      </div>

      <div className="ops-card">
        <div className="ops-card-head">
          <h2>Lambda Architecture · Live Dataflow</h2>
          <span>Số liệu trực tiếp qua từng tầng xử lý</span>
        </div>
        <div className="ops-flow">
          <div className="ops-flow-node">
            <strong>1. Ingestion</strong>
            <p>Web + Traffic Replay + Upload Sim</p>
            <code>{eventsPerSec} ev/s</code>
          </div>
          <div className="ops-flow-node">
            <strong>2. Kafka Bus</strong>
            <p>events.user · content · extract</p>
            <code>{eventsTotal.toLocaleString()} msgs</code>
          </div>
          <div className="ops-flow-node">
            <strong>3a. Speed Layer</strong>
            <p>Tumbling Window 60s → Redis</p>
            <code>{(metrics?.hot_tags || []).length} hot tags</code>
          </div>
          <div className="ops-flow-node">
            <strong>3b. Batch Layer</strong>
            <p>ALS / SVD → cf_scores</p>
            <code>{(db.cf_pairs ?? 0).toLocaleString()} pairs</code>
          </div>
          <div className="ops-flow-node highlight">
            <strong>4. Hybrid Serving</strong>
            <p>α·content + β·cf + γ·realtime</p>
            <code>α{alpha.toFixed(2)} β{beta.toFixed(2)} γ{gamma.toFixed(2)}</code>
          </div>
        </div>
      </div>

      <div className="ops-two-col">
        <div className="ops-card">
          <div className="ops-card-head">
            <h2>Live Throughput (events / sec)</h2>
            <code>WS /ws/metrics</code>
          </div>
          <svg viewBox={`0 0 ${chartW} ${chartH}`} className="ops-chart">
            <polyline fill="none" stroke="#3dd68c" strokeWidth="2.5" points={points} />
          </svg>
          <div className="ops-event-mix">
            {Object.entries(metrics?.events_by_type || {}).map(([k, v]) => (
              <span key={k} className="ops-mix-pill">
                <b>{k}</b> {Number(v).toLocaleString()}
              </span>
            ))}
          </div>
        </div>

        <div className="ops-card">
          <div className="ops-card-head">
            <h2>Hybrid Serving Weights</h2>
            <code>α + β + γ = 1</code>
          </div>
          <div className="ops-sliders">
            <label>
              <div>
                <span>α · Content-based (Metadata)</span>
                <code>{alpha.toFixed(2)}</code>
              </div>
              <input type="range" min="0" max="1" step="0.01" value={alpha} onChange={(e) => setAlpha(+e.target.value)} />
            </label>
            <label>
              <div>
                <span>β · Batch CF (ALS / SVD)</span>
                <code>{beta.toFixed(2)}</code>
              </div>
              <input type="range" min="0" max="1" step="0.01" value={beta} onChange={(e) => setBeta(+e.target.value)} />
            </label>
            <label>
              <div>
                <span>γ · Realtime Speed (Redis)</span>
                <code>{gamma.toFixed(2)}</code>
              </div>
              <input type="range" min="0" max="1" step="0.01" value={gamma} onChange={(e) => setGamma(+e.target.value)} />
            </label>
          </div>
          <button type="button" className="ops-primary-btn full" onClick={saveWeights}>
            Apply Weights
          </button>
          {batchResult && <pre className="ops-result">{JSON.stringify(batchResult, null, 2)}</pre>}
        </div>
      </div>

      <div className="ops-card">
        <div className="ops-card-head">
          <h2>Live Kafka Event Tail</h2>
          <span>{db.enriched ?? 0} enriched · {db.extracting ?? 0} extracting · {db.draft ?? 0} draft</span>
        </div>
        <div className="ops-tail">
          {tail.length === 0 ? (
            <p className="tk-ins-muted">Chưa có event mới — traffic-simulator hoặc tương tác trên Feed sẽ hiện ở đây.</p>
          ) : (
            tail.slice(0, 15).map((e, i) => (
              <div key={`${e.ts || e.event_type}-${i}`} className="ops-tail-row">
                <code>{formatTime(e.ts)}</code>
                <code>{e.topic}</code>
                <strong className={`ev-${e.event_type}`}>{e.event_type}</strong>
                <span>{shortId(e.user_id)}</span>
                <code>{shortId(e.video_id)}</code>
              </div>
            ))
          )}
        </div>
      </div>
    </section>
  );
}
