-- BigQuery schema for serverless earthquake pipeline
-- Run: bq query --use_legacy_sql=false < gcp/bigquery/schema.sql

-- 1. Append-only log of normalized events from all sources
CREATE TABLE IF NOT EXISTS `quake_stream.raw_events` (
    event_uid           STRING NOT NULL,
    source              STRING NOT NULL,
    source_event_id     STRING NOT NULL,

    origin_time_utc     TIMESTAMP NOT NULL,
    latitude            FLOAT64 NOT NULL,
    longitude           FLOAT64 NOT NULL,
    depth_km            FLOAT64 NOT NULL,

    magnitude_value     FLOAT64 NOT NULL,
    magnitude_type      STRING NOT NULL,

    place               STRING,
    region              STRING,
    status              STRING NOT NULL,

    lat_error_km        FLOAT64,
    lon_error_km        FLOAT64,
    depth_error_km      FLOAT64,
    mag_error           FLOAT64,
    time_error_sec      FLOAT64,

    num_phases          INT64,
    azimuthal_gap       FLOAT64,

    author              STRING,
    url                 STRING,
    fetched_at          TIMESTAMP NOT NULL,
    ingested_at         TIMESTAMP NOT NULL,

    raw_payload         STRING
)
PARTITION BY DATE(origin_time_utc)
CLUSTER BY source, magnitude_value
OPTIONS (
    partition_expiration_days = 90,
    description = "Append-only log of normalized earthquake events from all sources"
);

-- 2. Deduplicated best-estimate events
CREATE TABLE IF NOT EXISTS `quake_stream.unified_events` (
    unified_event_id    STRING NOT NULL,
    origin_time_utc     TIMESTAMP NOT NULL,
    latitude            FLOAT64 NOT NULL,
    longitude           FLOAT64 NOT NULL,
    depth_km            FLOAT64 NOT NULL,

    magnitude_value     FLOAT64 NOT NULL,
    magnitude_type      STRING NOT NULL,

    place               STRING,
    region              STRING,
    status              STRING NOT NULL,

    num_sources         INT64 NOT NULL,
    preferred_source    STRING NOT NULL,
    source_event_uids   ARRAY<STRING>,

    created_at          TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP NOT NULL
)
PARTITION BY DATE(origin_time_utc)
CLUSTER BY magnitude_value
OPTIONS (
    partition_expiration_days = 365,
    description = "Deduplicated best-estimate earthquake events"
);

-- 3. Dead-letter queue for events that fail validation
CREATE TABLE IF NOT EXISTS `quake_stream.dead_letter_events` (
    source              STRING NOT NULL,
    source_event_id     STRING,
    raw_payload         STRING NOT NULL,
    error_messages      ARRAY<STRING> NOT NULL,
    created_at          TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
OPTIONS (
    partition_expiration_days = 30,
    description = "Events that failed validation"
);

-- 4. Pipeline run log for monitoring
CREATE TABLE IF NOT EXISTS `quake_stream.pipeline_runs` (
    run_id              STRING NOT NULL,
    started_at          TIMESTAMP NOT NULL,
    finished_at         TIMESTAMP NOT NULL,
    status              STRING NOT NULL,
    sources_fetched     ARRAY<STRING>,
    raw_events_count    INT64,
    unified_events_count INT64,
    dead_letter_count   INT64,
    error_message       STRING,
    duration_seconds    FLOAT64
)
PARTITION BY DATE(started_at)
OPTIONS (
    partition_expiration_days = 30,
    description = "Pipeline execution log for monitoring"
);
