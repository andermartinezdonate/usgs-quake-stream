"""Deduplication pipeline for BigQuery raw events.

Queries raw_events (6h window), runs DBSCAN clustering, computes quality
metrics, and MERGEs results into unified_events.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery

from quake_stream.models_v2 import NormalizedEvent
from quake_stream.deduplicator import (
    cluster_events,
    EventRecord,
    _select_preferred,
    _compute_unified_id,
    _weighted_mean,
    _compute_quality_metrics,
)

logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", os.environ.get("GOOGLE_CLOUD_PROJECT", ""))
DATASET = os.environ.get("BQ_DATASET", "quake_stream")
DEDUP_LOOKBACK_HOURS = 6

_client: bigquery.Client | None = None


def _get_client() -> bigquery.Client:
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT_ID or None)
    return _client


def _table(name: str) -> str:
    project = PROJECT_ID or _get_client().project
    return f"`{project}.{DATASET}.{name}`"


def _sql_str(val) -> str:
    if val is None:
        return "NULL"
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"


def run_dedup_pipeline() -> dict:
    """Execute one deduplication cycle."""
    run_id = str(uuid.uuid4())[:8]
    t0 = time.monotonic()
    now = datetime.now(timezone.utc)
    client = _get_client()
    project = client.project

    logger.info("[%s] Dedup pipeline starting — %dh lookback", run_id, DEDUP_LOOKBACK_HOURS)

    # Query recent distinct events from raw_events
    query = f"""
        SELECT DISTINCT
            event_uid, source, source_event_id,
            origin_time_utc, latitude, longitude, depth_km,
            magnitude_value, magnitude_type, place, region, status
        FROM `{project}.{DATASET}.raw_events`
        WHERE origin_time_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {DEDUP_LOOKBACK_HOURS} HOUR)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY event_uid ORDER BY fetched_at DESC) = 1
    """
    rows = list(client.query(query).result())

    if not rows:
        logger.info("[%s] No events to deduplicate", run_id)
        return {"run_id": run_id, "events": 0, "clusters": 0, "duration_s": round(time.monotonic() - t0, 2)}

    records = []
    for row in rows:
        ot = row.origin_time_utc
        if ot.tzinfo is None:
            ot = ot.replace(tzinfo=timezone.utc)
        records.append(EventRecord(
            event_uid=row.event_uid,
            source=row.source,
            origin_time_utc=ot,
            latitude=row.latitude,
            longitude=row.longitude,
            depth_km=row.depth_km,
            magnitude_value=row.magnitude_value,
            magnitude_type=row.magnitude_type,
            place=row.place,
            region=row.region,
            status=row.status,
        ))

    logger.info("[%s] Loaded %d events for clustering", run_id, len(records))

    # Cluster
    clusters = cluster_events(records)

    # Build unified rows
    unified_rows = []
    for cluster in clusters:
        preferred = _select_preferred(cluster)
        unified_id = _compute_unified_id(cluster)
        lat, lon, depth = _weighted_mean(cluster)
        metrics = _compute_quality_metrics(cluster)

        unified_rows.append({
            "unified_event_id": unified_id,
            "origin_time_utc": preferred.origin_time_utc.isoformat(),
            "latitude": lat,
            "longitude": lon,
            "depth_km": depth,
            "magnitude_value": preferred.magnitude_value,
            "magnitude_type": preferred.magnitude_type,
            "place": preferred.place,
            "region": preferred.region,
            "status": preferred.status,
            "num_sources": len(set(m.source for m in cluster.members)),
            "preferred_source": preferred.source,
            "source_event_uids": [m.event_uid for m in cluster.members],
            "magnitude_std": metrics["magnitude_std"],
            "location_spread_km": metrics["location_spread_km"],
            "source_agreement_score": metrics["source_agreement_score"],
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        })

    # MERGE into unified_events
    unified_count = 0
    if unified_rows:
        unified_count = _merge_unified_events(unified_rows)

    duration = time.monotonic() - t0
    multi_source = sum(1 for r in unified_rows if r["num_sources"] > 1)
    result = {
        "run_id": run_id,
        "events": len(records),
        "clusters": len(clusters),
        "unified_events": unified_count,
        "multi_source": multi_source,
        "duration_s": round(duration, 2),
    }

    logger.info("[%s] Dedup complete in %.1fs: %s", run_id, duration, result)
    return result


def _merge_unified_events(unified_rows: list[dict]) -> int:
    """Upsert unified events using BigQuery MERGE."""
    if not unified_rows:
        return 0

    client = _get_client()
    project = client.project
    table = f"`{project}.{DATASET}.unified_events`"

    select_clauses = []
    for r in unified_rows:
        uids_array = ", ".join(f"'{uid}'" for uid in r["source_event_uids"])
        select_clauses.append(f"""SELECT
            '{r["unified_event_id"]}' AS unified_event_id,
            TIMESTAMP('{r["origin_time_utc"]}') AS origin_time_utc,
            CAST({r["latitude"]} AS FLOAT64) AS latitude,
            CAST({r["longitude"]} AS FLOAT64) AS longitude,
            CAST({r["depth_km"]} AS FLOAT64) AS depth_km,
            CAST({r["magnitude_value"]} AS FLOAT64) AS magnitude_value,
            '{r["magnitude_type"]}' AS magnitude_type,
            {_sql_str(r.get("place"))} AS place,
            {_sql_str(r.get("region"))} AS region,
            '{r["status"]}' AS status,
            CAST({r["num_sources"]} AS INT64) AS num_sources,
            '{r["preferred_source"]}' AS preferred_source,
            [{uids_array}] AS source_event_uids,
            CAST({r.get("magnitude_std", 0.0)} AS FLOAT64) AS magnitude_std,
            CAST({r.get("location_spread_km", 0.0)} AS FLOAT64) AS location_spread_km,
            CAST({r.get("source_agreement_score", 0.0)} AS FLOAT64) AS source_agreement_score,
            CURRENT_TIMESTAMP() AS created_at,
            CURRENT_TIMESTAMP() AS updated_at""")

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
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        unified_event_id, origin_time_utc, latitude, longitude, depth_km,
        magnitude_value, magnitude_type, place, region, status,
        num_sources, preferred_source, source_event_uids,
        magnitude_std, location_spread_km, source_agreement_score,
        created_at, updated_at
    ) VALUES (
        S.unified_event_id, S.origin_time_utc, S.latitude, S.longitude, S.depth_km,
        S.magnitude_value, S.magnitude_type, S.place, S.region, S.status,
        S.num_sources, S.preferred_source, S.source_event_uids,
        S.magnitude_std, S.location_spread_km, S.source_agreement_score,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    )
    """

    job = client.query(query)
    job.result()
    return len(unified_rows)
