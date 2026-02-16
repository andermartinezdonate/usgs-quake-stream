"""Core microbatch pipeline: fetch → normalize → deduplicate → store.

Each invocation (triggered every minute by Cloud Scheduler) fetches recent
events from all configured FDSN sources, normalizes them, deduplicates against
the last 6 hours of BigQuery data, and writes results to BigQuery.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone, timedelta

import httpx

from quake_stream.models_v2 import NormalizedEvent
from quake_stream.parsers import PARSER_MAP
from quake_stream.parsers.base import EventParser
from quake_stream.deduplicator import (
    cluster_events,
    EventRecord,
    _select_preferred,
    _compute_unified_id,
    _weighted_mean,
)
from quake_stream.sources import SOURCES

from bq_client import (
    insert_raw_events,
    merge_unified_events,
    insert_dead_letter,
    query_recent_raw_events,
    log_pipeline_run,
)

logger = logging.getLogger(__name__)

# FDSN format parameter per source
FORMAT_MAP = {"usgs": "geojson", "emsc": "json", "gfz": "text"}

# Lookback window — 10 min overlap ensures no missed events between 1-min triggers
LOOKBACK_MINUTES = 10

# Dedup context window
DEDUP_LOOKBACK_HOURS = 6


async def _fetch_source(
    client: httpx.AsyncClient,
    name: str,
    start: datetime,
    end: datetime,
) -> str:
    """Fetch events from one FDSN source with retry."""
    config = SOURCES[name]
    params = {
        "format": FORMAT_MAP[name],
        "starttime": start.strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime": end.strftime("%Y-%m-%dT%H:%M:%S"),
        "minmagnitude": "0.0",
        "orderby": "time",
    }

    last_exc: Exception | None = None
    for attempt in range(config.max_retries + 1):
        try:
            resp = await client.get(
                config.base_url,
                params=params,
                timeout=config.timeout_seconds,
            )
            if resp.status_code == 204:
                return "" if config.format == "fdsn_text" else '{"features":[]}'
            resp.raise_for_status()
            return resp.text
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            last_exc = exc
            if attempt < config.max_retries:
                backoff = config.retry_backoff_base ** attempt
                logger.warning(
                    "[%s] attempt %d/%d failed: %s — retrying in %.1fs",
                    name, attempt + 1, config.max_retries + 1, exc, backoff,
                )
                await asyncio.sleep(backoff)

    raise RuntimeError(f"[{name}] all attempts failed") from last_exc


async def run_pipeline() -> dict:
    """Execute one microbatch cycle. Returns summary dict."""
    run_id = str(uuid.uuid4())[:8]
    t0 = time.monotonic()
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=LOOKBACK_MINUTES)
    fetched_at = now

    logger.info("[%s] Pipeline starting — window %s to %s", run_id, start, now)

    # ── 1. Fetch all sources concurrently ────────────────────────────────
    raw_data: dict[str, str] = {}
    fetch_errors: list[str] = []

    async with httpx.AsyncClient() as client:
        enabled = {n: c for n, c in SOURCES.items() if c.enabled}
        coros = {
            name: _fetch_source(client, name, start, now)
            for name in enabled
        }
        results = await asyncio.gather(*coros.values(), return_exceptions=True)

        for name, result in zip(coros.keys(), results):
            if isinstance(result, Exception):
                logger.error("[%s] fetch failed: %s", name, result)
                fetch_errors.append(f"{name}: {result}")
            else:
                raw_data[name] = result

    if not raw_data:
        error_msg = f"All sources failed: {fetch_errors}"
        log_pipeline_run(run_id, now, "failed", [], 0, 0, 0, error_msg, time.monotonic() - t0)
        raise RuntimeError(error_msg)

    # ── 2. Parse + normalize + validate ──────────────────────────────────
    all_events: list[NormalizedEvent] = []
    dead_letters: list[dict] = []

    for name, raw_text in raw_data.items():
        if not raw_text.strip():
            continue

        parser = PARSER_MAP.get(name)
        if parser is None:
            logger.error("[%s] no parser registered", name)
            continue

        try:
            events = parser.parse(raw_text, fetched_at)
        except Exception as exc:
            logger.error("[%s] parse error: %s", name, exc)
            dead_letters.append({
                "source": name,
                "source_event_id": None,
                "raw_payload": raw_text[:10000],
                "errors": [f"Parse error: {exc}"],
            })
            continue

        for event in events:
            errors = EventParser.validate(event)
            if errors:
                dead_letters.append({
                    "source": name,
                    "source_event_id": event.source_event_id,
                    "raw_payload": event.raw_payload[:5000] if event.raw_payload else "",
                    "errors": errors,
                })
            else:
                all_events.append(event)

    logger.info("[%s] Parsed %d events (%d dead-lettered)", run_id, len(all_events), len(dead_letters))

    # ── 3. Write raw events to BigQuery (append-only, dedup at query) ────
    raw_count = 0
    if all_events:
        raw_count = insert_raw_events(all_events)

    # ── 4. Deduplicate against recent history ────────────────────────────
    recent_events = query_recent_raw_events(hours=DEDUP_LOOKBACK_HOURS)

    # Merge new events with history, dedup by event_uid
    seen_uids = {e.event_uid for e in recent_events}
    for event in all_events:
        if event.event_uid not in seen_uids:
            recent_events.append(event)
            seen_uids.add(event.event_uid)

    records = [
        EventRecord(
            event_uid=e.event_uid,
            source=e.source,
            origin_time_utc=e.origin_time_utc,
            latitude=e.latitude,
            longitude=e.longitude,
            depth_km=e.depth_km,
            magnitude_value=e.magnitude_value,
            magnitude_type=e.magnitude_type,
            place=e.place,
            region=e.region,
            status=e.status,
        )
        for e in recent_events
    ]

    clusters = cluster_events(records)

    # Build unified rows
    unified_rows = []
    for cluster in clusters:
        preferred = _select_preferred(cluster)
        unified_id = _compute_unified_id(cluster)
        lat, lon, depth = _weighted_mean(cluster)

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
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        })

    # ── 5. Merge unified events into BigQuery ────────────────────────────
    unified_count = 0
    if unified_rows:
        unified_count = merge_unified_events(unified_rows)

    # ── 6. Write dead letters ────────────────────────────────────────────
    if dead_letters:
        insert_dead_letter(dead_letters)

    duration = time.monotonic() - t0
    result = {
        "run_id": run_id,
        "sources": list(raw_data.keys()),
        "raw_events": raw_count,
        "unified_events": unified_count,
        "dead_letters": len(dead_letters),
        "duration_s": round(duration, 2),
    }

    log_pipeline_run(
        run_id, now, "ok", list(raw_data.keys()),
        raw_count, unified_count, len(dead_letters), None, duration,
    )

    logger.info("[%s] Pipeline complete in %.1fs: %s", run_id, duration, result)
    return result
