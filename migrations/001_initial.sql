-- ============================================================
--  GoRelay — Initial Schema
--  Run: psql $DATABASE_URL -f migrations/001_initial.sql
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Applications (API key tenants) ────────────────────────
CREATE TABLE IF NOT EXISTS applications (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name         VARCHAR(255) NOT NULL,
    api_key_hash VARCHAR(64)  UNIQUE NOT NULL,   -- SHA-256 hex
    is_active    BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_apps_key_hash ON applications(api_key_hash);

-- ── Endpoints (webhook target URLs) ───────────────────────
CREATE TABLE IF NOT EXISTS endpoints (
    id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    app_id         UUID         NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    url            TEXT         NOT NULL,
    description    TEXT         NOT NULL DEFAULT '',
    secret_hash    VARCHAR(255) NOT NULL DEFAULT '',   -- signing secret (stored plaintext for HMAC)
    enabled_events JSONB        NOT NULL DEFAULT '["*"]',
    is_active      BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_endpoints_app_id    ON endpoints(app_id);
CREATE INDEX IF NOT EXISTS idx_endpoints_is_active ON endpoints(app_id, is_active);

-- ── Events (inbound from API clients) ─────────────────────
CREATE TABLE IF NOT EXISTS events (
    id         UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    app_id     UUID         NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    event_type VARCHAR(255) NOT NULL,
    payload    JSONB        NOT NULL,
    status     VARCHAR(50)  NOT NULL DEFAULT 'pending',  -- pending|delivered|failed|no_endpoints
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_app_id    ON events(app_id);
CREATE INDEX IF NOT EXISTS idx_events_type      ON events(app_id, event_type);
CREATE INDEX IF NOT EXISTS idx_events_status    ON events(status);
CREATE INDEX IF NOT EXISTS idx_events_created   ON events(created_at DESC);

-- ── Deliveries (one attempt per endpoint per event) ───────
CREATE TABLE IF NOT EXISTS deliveries (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         UUID         NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    endpoint_id      UUID         NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    app_id           UUID         NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    attempt_number   INTEGER      NOT NULL DEFAULT 1,
    status           VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending|success|failed|dlq
    http_status_code INTEGER,
    response_body    TEXT         NOT NULL DEFAULT '',
    error_message    TEXT         NOT NULL DEFAULT '',
    next_retry_at    TIMESTAMPTZ,
    delivered_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deliveries_event_id   ON deliveries(event_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_endpoint   ON deliveries(endpoint_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_status     ON deliveries(status);
CREATE INDEX IF NOT EXISTS idx_deliveries_retry_at   ON deliveries(next_retry_at) WHERE status = 'failed';
