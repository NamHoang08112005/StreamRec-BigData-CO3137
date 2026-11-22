import { Link } from "react-router-dom";

const PROVIDER_ID = "creator_demo";

export default function ProfilePage({ userId }) {
  return (
    <section className="tt-profile">
      <div className="tt-profile-hero">
        <div className="tt-profile-avatar">{userId.slice(0, 1).toUpperCase()}</div>
        <h1>@{userId}</h1>
        <p className="tt-profile-bio">Customer · hybrid recommend viewer</p>
      </div>

      <div className="tt-profile-block">
        <h2>Customer</h2>
        <p className="tt-profile-note">Watch For You, Subscribe channels, send Like/Skip events into the stream.</p>
        <div className="tt-profile-actions">
          <Link className="tt-btn" to="/">
            Open Feed
          </Link>
        </div>
      </div>

      <div className="tt-profile-block">
        <h2>Provider Studio</h2>
        <p className="tt-profile-note">
          Upload with draft metadata → async deep extract. Demo provider: @{PROVIDER_ID}
        </p>
        <div className="tt-profile-actions">
          <Link className="tt-btn" to={`/studio/${PROVIDER_ID}`}>
            Open Studio
          </Link>
          <Link className="tt-btn ghost" to={`/creator/${PROVIDER_ID}`}>
            View channel
          </Link>
        </div>
      </div>

      <div className="tt-profile-block">
        <h2>Ops · Big Data</h2>
        <p className="tt-profile-note">
          Lambda Speed/Batch/Serving, 4V metrics, Kafka event tail, α/β/γ weights — not part of consumer UI.
        </p>
        <div className="tt-profile-actions">
          <Link className="tt-btn ghost" to="/dashboard">
            Ops Dashboard
          </Link>
        </div>
      </div>
    </section>
  );
}
