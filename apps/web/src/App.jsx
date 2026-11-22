import { useCallback, useEffect, useState } from "react";
import { NavLink, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { getDemoUsers } from "./api";
import FeedPage from "./pages/FeedPage";
import TrendingPage from "./pages/TrendingPage";
import StudioPage from "./pages/StudioPage";
import SimulationPage from "./pages/SimulationPage";
import DashboardPage from "./pages/DashboardPage";

const USER_KEY = "streamrec_user";

function getStoredUserId() {
  let id = localStorage.getItem(USER_KEY);
  if (!id) {
    id = `viewer_${Math.random().toString(36).slice(2, 8)}`;
    localStorage.setItem(USER_KEY, id);
  }
  return id;
}

export default function App() {
  const [userId, setUserIdState] = useState(getStoredUserId);
  const [demoUsers, setDemoUsers] = useState([]);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const loc = useLocation();
  const navigate = useNavigate();

  const isOps = loc.pathname.startsWith("/dashboard") || loc.pathname.startsWith("/simulation");

  const setUserId = useCallback((id) => {
    const next = String(id || "").trim();
    if (!next) return;
    localStorage.setItem(USER_KEY, next);
    setUserIdState(next);
  }, []);

  useEffect(() => {
    getDemoUsers(50)
      .then((d) => setDemoUsers(d.items || []))
      .catch(() => setDemoUsers([]));
  }, []);

  const pickerOptions = demoUsers.some((u) => u.user_id === userId)
    ? demoUsers
    : [{ user_id: userId, interactions: 0 }, ...demoUsers];

  const onSearchSubmit = (e) => {
    e.preventDefault();
    if (!loc.pathname.startsWith("/trending")) {
      navigate("/trending");
    }
  };

  return (
    <div className="tk-app">
      <header className="tk-topbar">
        <div className="tk-brand" onClick={() => navigate("/")} role="button" tabIndex={0}>
          <span className="tk-logo-mark" aria-hidden="true">♪</span>
          <span className="tk-logo-text">StreamRec</span>
        </div>

        <form className="tk-search" onSubmit={onSearchSubmit}>
          <span className="tk-search-icon" aria-hidden="true">⌕</span>
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search videos, #hashtags, @creators…"
            aria-label="Search videos"
          />
          {searchQuery && (
            <button type="button" className="tk-search-clear" onClick={() => setSearchQuery("")}>
              ✕
            </button>
          )}
        </form>

        <div className="tk-top-actions">
          <button type="button" className="tk-upload-btn" onClick={() => navigate("/studio")}>
            <span>＋</span>
            <span>Upload</span>
          </button>

          <select
            className="tk-user-select"
            value={userId}
            onChange={(e) => setUserId(e.target.value)}
            title="Switch Customer (Demo User)"
            aria-label="Switch Customer"
          >
            {pickerOptions.map((u) => (
              <option key={u.user_id} value={u.user_id}>
                {u.cold_start ? "[cold-start] " : "@"}
                {u.user_id}
                {u.cold_start ? " (0)" : u.interactions ? ` (${u.interactions})` : ""}
              </option>
            ))}
          </select>

          {!isOps && (
            <button
              type="button"
              className={`tk-bigdata-toggle ${inspectorOpen ? "active" : ""}`}
              onClick={() => setInspectorOpen((o) => !o)}
            >
              <span className="tk-live-dot" aria-hidden="true" />
              <span>{inspectorOpen ? "Hide Big Data" : "Show Big Data"}</span>
            </button>
          )}
        </div>
      </header>

      <div className="tk-body">
        <aside className="tk-sidebar">
          <nav className="tk-nav" aria-label="Main navigation">
            <NavLink to="/" end className={({ isActive }) => `tk-nav-item ${isActive ? "active" : ""}`}>
              <span className="tk-nav-ico">⌂</span>
              <span className="tk-nav-label">For You</span>
            </NavLink>
            <NavLink to="/trending" className={({ isActive }) => `tk-nav-item ${isActive ? "active" : ""}`}>
              <span className="tk-nav-ico">◈</span>
              <span className="tk-nav-label">Explore</span>
            </NavLink>
            <NavLink to="/studio" className={({ isActive }) => `tk-nav-item ${isActive ? "active" : ""}`}>
              <span className="tk-nav-ico">↥</span>
              <span className="tk-nav-label">TikTok Studio</span>
            </NavLink>
            <NavLink to="/simulation" className={({ isActive }) => `tk-nav-item ${isActive ? "active" : ""}`}>
              <span className="tk-nav-ico">📱</span>
              <span className="tk-nav-label">Live Simulation</span>
            </NavLink>
          </nav>

          <div className="tk-sidebar-foot">
            <NavLink to="/dashboard" className={({ isActive }) => `tk-nav-item ops ${isActive ? "active" : ""}`}>
              <span className="tk-nav-ico">⚡</span>
              <span className="tk-nav-label">System Ops</span>
            </NavLink>
          </div>
        </aside>

        <main className={`tk-main ${isOps ? "tk-main-ops" : ""}`}>
          <Routes>
            <Route
              path="/"
              element={
                <FeedPage
                  userId={userId}
                  inspectorOpen={inspectorOpen}
                  onCloseInspector={() => setInspectorOpen(false)}
                />
              }
            />
            <Route
              path="/trending"
              element={
                <TrendingPage
                  userId={userId}
                  searchQuery={searchQuery}
                  inspectorOpen={inspectorOpen}
                  onCloseInspector={() => setInspectorOpen(false)}
                />
              }
            />
            <Route
              path="/studio"
              element={
                <StudioPage
                  inspectorOpen={inspectorOpen}
                  onCloseInspector={() => setInspectorOpen(false)}
                  onOpenInspector={() => setInspectorOpen(true)}
                />
              }
            />
            <Route
              path="/studio/:creatorId"
              element={
                <StudioPage
                  inspectorOpen={inspectorOpen}
                  onCloseInspector={() => setInspectorOpen(false)}
                  onOpenInspector={() => setInspectorOpen(true)}
                />
              }
            />
            <Route path="/simulation" element={<SimulationPage />} />
            <Route path="/dashboard" element={<DashboardPage />} />
            <Route
              path="*"
              element={
                <FeedPage
                  userId={userId}
                  inspectorOpen={inspectorOpen}
                  onCloseInspector={() => setInspectorOpen(false)}
                />
              }
            />
          </Routes>
        </main>
      </div>
    </div>
  );
}
