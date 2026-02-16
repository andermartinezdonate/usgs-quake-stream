-- Multi-source earthquake ingestion schema
-- Run via: quake init-db-v2

-- 1. Immutable append-only log of raw API responses
CREATE TABLE IF NOT EXISTS raw_events (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    raw_payload     JSONB NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_raw_events_source
    ON raw_events (source, source_event_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_fetched
    ON raw_events (fetched_at DESC);

-- 2. Per-source normalized events (canonical schema)
CREATE TABLE IF NOT EXISTS normalized_events (
    event_uid       TEXT PRIMARY KEY,   -- "{source}:{source_event_id}"
    source          TEXT NOT NULL,
    source_event_id TEXT NOT NULL,

    origin_time_utc TIMESTAMPTZ NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    depth_km        DOUBLE PRECISION NOT NULL,

    magnitude_value DOUBLE PRECISION NOT NULL,
    magnitude_type  TEXT NOT NULL,

    place           TEXT,
    region          TEXT,

    lat_error_km    DOUBLE PRECISION,
    lon_error_km    DOUBLE PRECISION,
    depth_error_km  DOUBLE PRECISION,
    mag_error       DOUBLE PRECISION,
    time_error_sec  DOUBLE PRECISION,

    status          TEXT NOT NULL DEFAULT 'automatic',
    num_phases      INTEGER,
    azimuthal_gap   DOUBLE PRECISION,

    author          TEXT,
    url             TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ,
    raw_payload     TEXT
);
CREATE INDEX IF NOT EXISTS idx_normalized_source
    ON normalized_events (source);
CREATE INDEX IF NOT EXISTS idx_normalized_time
    ON normalized_events (origin_time_utc DESC);
CREATE INDEX IF NOT EXISTS idx_normalized_mag
    ON normalized_events (magnitude_value);

-- 3. Deduplicated best-estimate events
CREATE TABLE IF NOT EXISTS unified_events (
    unified_event_id    TEXT PRIMARY KEY,
    origin_time_utc     TIMESTAMPTZ NOT NULL,
    latitude            DOUBLE PRECISION NOT NULL,
    longitude           DOUBLE PRECISION NOT NULL,
    depth_km            DOUBLE PRECISION NOT NULL,

    magnitude_value     DOUBLE PRECISION NOT NULL,
    magnitude_type      TEXT NOT NULL,

    place               TEXT,
    region              TEXT,
    status              TEXT NOT NULL DEFAULT 'automatic',

    num_sources         INTEGER NOT NULL DEFAULT 1,
    preferred_source    TEXT NOT NULL,
    preferred_event_uid TEXT NOT NULL,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_unified_time
    ON unified_events (origin_time_utc DESC);
CREATE INDEX IF NOT EXISTS idx_unified_mag
    ON unified_events (magnitude_value);

-- 4. Mapping: normalized event → unified event
CREATE TABLE IF NOT EXISTS event_crosswalk (
    event_uid        TEXT NOT NULL REFERENCES normalized_events(event_uid),
    unified_event_id TEXT NOT NULL REFERENCES unified_events(unified_event_id),
    match_score      DOUBLE PRECISION NOT NULL,
    is_preferred     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_uid, unified_event_id)
);
CREATE INDEX IF NOT EXISTS idx_crosswalk_unified
    ON event_crosswalk (unified_event_id);

-- 5. Dead-letter queue for events that fail validation
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT NOT NULL,
    source_event_id TEXT,
    raw_payload     TEXT NOT NULL,
    error_messages  TEXT[] NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dead_letter_created
    ON dead_letter_events (created_at DESC);
