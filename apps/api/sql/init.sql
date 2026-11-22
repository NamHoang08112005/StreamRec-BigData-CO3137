CREATE TABLE IF NOT EXISTS users (
    user_id   TEXT PRIMARY KEY,
    display_name TEXT,
    interest_tags TEXT[] DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS videos (
    video_id      TEXT PRIMARY KEY,
    creator_id    TEXT,
    title         TEXT NOT NULL,
    tags          TEXT[] DEFAULT '{}',
    category      TEXT,
    media_key     TEXT NOT NULL,
    thumb_key     TEXT,
    status        TEXT NOT NULL DEFAULT 'draft',
    duration_sec  DOUBLE PRECISION,
    width         INT,
    height        INT,
    fps           DOUBLE PRECISION,
    enriched_tags TEXT[] DEFAULT '{}',
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS interactions (
    id          BIGSERIAL PRIMARY KEY,
    user_id     TEXT NOT NULL,
    video_id    TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    watch_time  DOUBLE PRECISION DEFAULT 0,
    weight      DOUBLE PRECISION DEFAULT 1,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_interactions_user ON interactions(user_id);
CREATE INDEX IF NOT EXISTS idx_interactions_video ON interactions(video_id);
CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);
CREATE INDEX IF NOT EXISTS idx_videos_category ON videos(category);

CREATE TABLE IF NOT EXISTS cf_scores (
    user_id   TEXT NOT NULL,
    video_id  TEXT NOT NULL,
    score     DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, video_id)
);

CREATE TABLE IF NOT EXISTS serving_weights (
    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    alpha DOUBLE PRECISION NOT NULL DEFAULT 0.35,
    beta  DOUBLE PRECISION NOT NULL DEFAULT 0.35,
    gamma DOUBLE PRECISION NOT NULL DEFAULT 0.30,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO serving_weights (id, alpha, beta, gamma)
VALUES (1, 0.35, 0.35, 0.30)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS pipeline_stats (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS follows (
    user_id    TEXT NOT NULL,
    creator_id TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, creator_id)
);

CREATE INDEX IF NOT EXISTS idx_follows_creator ON follows(creator_id);
CREATE INDEX IF NOT EXISTS idx_videos_creator ON videos(creator_id);
