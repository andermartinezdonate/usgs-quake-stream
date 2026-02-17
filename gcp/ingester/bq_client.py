"""BigQuery read/write operations for the ingestion pipeline."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT", ""))
DATASET = os.environ.get("BQ_DATASET", "quake_stream")

_client: bigquery.Client | None = None


def _get_client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT_ID or None)
    return _client


def _table(name: str) -> str:
    project = PROJECT_ID or _get_client().project
    return f"`{project}.{DATASET}.{name}`"


# ── Raw events (append-only) ────────────────────────────────────────────


def insert_raw_events(events) -> int:
    """Stream-insert normalized events into raw_events.

    Append-only. Duplicates are handled at query time via DISTINCT on event_uid.
    """
    if not events:
        return 0

    client = _get_client()
    table_ref = f"{client.project}.{DATASET}.raw_events"
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for e in events:
        rows.append({
            "event_uid": e.event_uid,
            "source": e.source,
            "source_event_id": e.source_event_id,
            "origin_time_utc": e.origin_time_utc.isoformat(),
            "latitude": e.latitude,
            "longitude": e.longitude,
            "depth_km": e.depth_km,
            "magnitude_value": e.magnitude_value,
            "magnitude_type": e.magnitude_type,
            "place": e.place,
            "region": e.region,
            "status": e.status,
            "lat_error_km": e.lat_error_km,
            "lon_error_km": e.lon_error_km,
            "depth_error_km": e.depth_error_km,
            "mag_error": e.mag_error,
            "time_error_sec": e.time_error_sec,
            "num_phases": e.num_phases,
            "azimuthal_gap": e.azimuthal_gap,
            "author": e.author,
            "url": e.url,
            "fetched_at": e.fetched_at.isoformat(),
            "ingested_at": now,
            "raw_payload": (e.raw_payload or "")[:10000],
            "evaluation_mode": getattr(e, "status", None),
        })

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        logger.error("BigQuery raw_events insert errors: %s", errors[:3])
        raise RuntimeError(f"BQ insert failed: {errors[:3]}")

    return len(rows)


# ── Unified events (MERGE / upsert) ─────────────────────────────────────


def merge_unified_events(unified_rows: list[dict]) -> int:
    """Upsert unified events using a BigQuery MERGE statement.

    This ensures idempotency — re-running with the same data is a no-op.
    """
    if not unified_rows:
        return 0

    client = _get_client()
    project = client.project
    table = f"`{project}.{DATASET}.unified_events`"

    # Build SELECT UNION ALL for the staging data
    select_clauses = []
    for r in unified_rows:
        uids_array = ", ".join(f"'{uid}'" for uid in r["source_event_uids"])
        select_clauses.append(f"""SELECT
            '{r["unified_event_id"]}' AS unified_event_id,
            TIMESTAMP('{r["origin_time_utc"]}') AS origin_time_utc,
            {r["latitude"]} AS latitude, {r["longitude"]} AS longitude, {r["depth_km"]} AS depth_km,
            {r["magnitude_value"]} AS magnitude_value, '{r["magnitude_type"]}' AS magnitude_type,
            {_sql_str(r.get("place"))} AS place, {_sql_str(r.get("region"))} AS region,
            '{r["status"]}' AS status,
            {r["num_sources"]} AS num_sources, '{r["preferred_source"]}' AS preferred_source,
            [{uids_array}] AS source_event_uids,
            {r.get("magnitude_std", 0.0)} AS magnitude_std,
            {r.get("location_spread_km", 0.0)} AS location_spread_km,
            {r.get("source_agreement_score", 0.0)} AS source_agreement_score,
            TIMESTAMP('{r["created_at"]}') AS created_at,
            TIMESTAMP('{r["updated_at"]}') AS updated_at""")

    staging_sql = "\n        UNION ALL\n        ".join(select_clauses)

    query = f"""
    MERGE {table} T
    USING (
        {staging_sql}
    ) S
    ON T.unified_event_id = S.unified_event_id
    WHEN MATCHED THEN UPDATE SET
        origin_time_utc = S.origin_time_utc,
        latitude = S.latitude,
        longitude = S.longitude,
        depth_km = S.depth_km,
        magnitude_value = S.magnitude_value,
        magnitude_type = S.magnitude_type,
        place = S.place,
        region = S.region,
        status = S.status,
        num_sources = S.num_sources,
        preferred_source = S.preferred_source,
        source_event_uids = S.source_event_uids,
        magnitude_std = S.magnitude_std,
        location_spread_km = S.location_spread_km,
        source_agreement_score = S.source_agreement_score,
        updated_at = S.updated_at
    WHEN NOT MATCHED THEN INSERT ROW
    """

    job = client.query(query)
    job.result()  # Wait for completion

    return len(unified_rows)


# ── Query recent events (for dedup context) ──────────────────────────────


def query_recent_raw_events(hours: int = 6):
    """Query recent distinct events from raw_events for dedup context."""
    client = _get_client()
    project = client.project

    query = f"""
        SELECT DISTINCT
            event_uid, source, source_event_id,
            origin_time_utc, latitude, longitude, depth_km,
            magnitude_value, magnitude_type, place, region, status,
            lat_error_km, lon_error_km, depth_error_km, mag_error, time_error_sec,
            num_phases, azimuthal_gap, author, url, fetched_at
        FROM `{project}.{DATASET}.raw_events`
        WHERE origin_time_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY event_uid ORDER BY fetched_at DESC) = 1
    """

    from quake_stream.models_v2 import NormalizedEvent

    rows = client.query(query).result()
    events = []
    for row in rows:
        ot = row.origin_time_utc
        if ot.tzinfo is None:
            ot = ot.replace(tzinfo=timezone.utc)
        fa = row.fetched_at
        if fa.tzinfo is None:
            fa = fa.replace(tzinfo=timezone.utc)

        events.append(NormalizedEvent(
            event_uid=row.event_uid,
            source=row.source,
            source_event_id=row.source_event_id,
            origin_time_utc=ot,
            latitude=row.latitude,
            longitude=row.longitude,
            depth_km=row.depth_km,
            magnitude_value=row.magnitude_value,
            magnitude_type=row.magnitude_type,
            place=row.place,
            region=row.region,
            status=row.status,
            lat_error_km=row.lat_error_km,
            lon_error_km=row.lon_error_km,
            depth_error_km=row.depth_error_km,
            mag_error=row.mag_error,
            time_error_sec=row.time_error_sec,
            num_phases=row.num_phases,
            azimuthal_gap=row.azimuthal_gap,
            author=row.author,
            url=row.url,
            fetched_at=fa,
        ))
    return events


# ── Dead letter ──────────────────────────────────────────────────────────


def insert_dead_letter(dead_letters: list[dict]) -> None:
    """Insert failed events into dead_letter_events."""
    if not dead_letters:
        return

    client = _get_client()
    table_ref = f"{client.project}.{DATASET}.dead_letter_events"
    now = datetime.now(timezone.utc).isoformat()

    rows = [
        {
            "source": dl["source"],
            "source_event_id": dl.get("source_event_id"),
            "raw_payload": (dl.get("raw_payload") or "")[:10000],
            "error_messages": dl["errors"],
            "created_at": now,
        }
        for dl in dead_letters
    ]

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        logger.error("Dead letter insert errors: %s", errors[:3])


# ── Pipeline run log ─────────────────────────────────────────────────────


def log_pipeline_run(
    run_id: str, started_at: datetime, status: str,
    sources: list[str], raw_count: int, unified_count: int,
    dead_count: int, error_msg: str | None, duration: float,
    source_name: str | None = None,
) -> None:
    """Log pipeline execution metadata."""
    client = _get_client()
    table_ref = f"{client.project}.{DATASET}.pipeline_runs"

    row = {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "sources_fetched": sources,
        "raw_events_count": raw_count,
        "unified_events_count": unified_count,
        "dead_letter_count": dead_count,
        "error_message": error_msg,
        "duration_seconds": round(duration, 3),
        "source_name": source_name,
    }

    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        logger.error("Pipeline run log error: %s", errors)


# ── Source health ────────────────────────────────────────────────────


def check_source_health(hours: int = 1) -> dict:
    """Query pipeline_runs to get per-source health status.

    Returns dict keyed by source_name with:
        runs, ok_count, failed_count, last_run, avg_duration
    """
    client = _get_client()
    project = client.project

    query = f"""
        SELECT
            source_name,
            COUNT(*) AS runs,
            COUNTIF(status = 'ok') AS ok_count,
            COUNTIF(status = 'failed') AS failed_count,
            MAX(started_at) AS last_run,
            AVG(duration_seconds) AS avg_duration
        FROM `{project}.{DATASET}.pipeline_runs`
        WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
          AND source_name IS NOT NULL
        GROUP BY source_name
        ORDER BY source_name
    """

    rows = list(client.query(query).result())
    result = {}
    for row in rows:
        result[row.source_name] = {
            "runs": row.runs,
            "ok_count": row.ok_count,
            "failed_count": row.failed_count,
            "last_run": row.last_run.isoformat() if row.last_run else None,
            "avg_duration": round(row.avg_duration, 2) if row.avg_duration else 0,
        }
    return result


# ── Helpers ──────────────────────────────────────────────────────────────


def _sql_str(val) -> str:
    """Escape a string for SQL, or return NULL."""
    if val is None:
        return "NULL"
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"
