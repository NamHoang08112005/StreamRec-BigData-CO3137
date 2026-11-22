const API_BASE = import.meta.env.VITE_API_BASE || "";

export async function getFeed(userId, limit = 20) {
  const r = await fetch(`${API_BASE}/api/feed/${encodeURIComponent(userId)}?limit=${limit}`);
  if (!r.ok) throw new Error("feed failed");
  return r.json();
}

export async function getDemoUsers(limit = 40) {
  const r = await fetch(`${API_BASE}/api/demo-users?limit=${limit}`);
  if (!r.ok) throw new Error("demo users failed");
  return r.json();
}

export async function getTrending(limit = 24, window = "1m") {
  const r = await fetch(`${API_BASE}/api/trending?limit=${limit}&window=${encodeURIComponent(window)}`);
  if (!r.ok) throw new Error("trending failed");
  return r.json();
}

export async function getFollowingFeed(userId, limit = 40) {
  const r = await fetch(
    `${API_BASE}/api/feed/${encodeURIComponent(userId)}/following?limit=${limit}`
  );
  if (!r.ok) throw new Error("following feed failed");
  return r.json();
}

export async function postEvent(payload) {
  const r = await fetch(`${API_BASE}/api/events`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!r.ok) throw new Error("event failed");
  return r.json();
}

export async function followCreator(userId, creatorId) {
  const r = await fetch(`${API_BASE}/api/follow`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id: userId, creator_id: creatorId }),
  });
  if (!r.ok) throw new Error("follow failed");
  return r.json();
}

export async function unfollowCreator(userId, creatorId) {
  const r = await fetch(`${API_BASE}/api/unfollow`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id: userId, creator_id: creatorId }),
  });
  if (!r.ok) throw new Error("unfollow failed");
  return r.json();
}

export async function getCreator(creatorId, viewerId) {
  const q = viewerId ? `?viewer_id=${encodeURIComponent(viewerId)}` : "";
  const r = await fetch(`${API_BASE}/api/creators/${encodeURIComponent(creatorId)}${q}`);
  if (!r.ok) throw new Error("creator failed");
  return r.json();
}

export async function getStudio(creatorId) {
  const r = await fetch(`${API_BASE}/api/studio/${encodeURIComponent(creatorId)}`);
  if (!r.ok) throw new Error("studio failed");
  return r.json();
}

export async function uploadVideo(formData) {
  const r = await fetch(`${API_BASE}/api/upload`, { method: "POST", body: formData });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function getMetrics() {
  const r = await fetch(`${API_BASE}/api/metrics`);
  if (!r.ok) throw new Error("metrics failed");
  return r.json();
}

export async function getSimulationLive() {
  const r = await fetch(`${API_BASE}/api/simulation/live`);
  if (!r.ok) throw new Error("simulation live failed");
  return r.json();
}

export async function getWeights() {
  const r = await fetch(`${API_BASE}/api/weights`);
  return r.json();
}

export async function putWeights(weights) {
  const r = await fetch(`${API_BASE}/api/weights`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(weights),
  });
  return r.json();
}

export async function runBatchCf() {
  const r = await fetch(`${API_BASE}/api/admin/run-batch-cf`, { method: "POST" });
  return r.json();
}

export async function listVideos(creatorId) {
  const q = creatorId ? `?creator_id=${encodeURIComponent(creatorId)}` : "";
  const r = await fetch(`${API_BASE}/api/videos${q}`);
  return r.json();
}

export function metricsWsUrl() {
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  const host = import.meta.env.VITE_WS_HOST || window.location.host;
  if (import.meta.env.DEV && !import.meta.env.VITE_WS_HOST) {
    return `ws://localhost:8000/ws/metrics`;
  }
  return `${proto}://${host}/ws/metrics`;
}

export function toSameOriginMedia(url) {
  if (!url) return url;
  try {
    const u = new URL(url, window.location.origin);
    if (u.pathname.startsWith("/media/")) {
      return `${u.pathname}${u.search}`;
    }
  } catch {
    /* keep original */
  }
  return url;
}
