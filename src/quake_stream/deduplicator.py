"""Periodic batch deduplicator for earthquake events.

Runs every N minutes, queries normalized_events, clusters events that represent
the same physical earthquake, and writes unified_events + event_crosswalk.
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from quake_stream.geo import haversine_km
from quake_stream.sources import SOURCE_PRIORITY

logger = logging.getLogger(__name__)

# Matching thresholds
MAX_TIME_DIFF_SEC = 30.0
MAX_DISTANCE_KM = 100.0
MAX_MAG_DIFF = 0.5
MATCH_SCORE_THRESHOLD = 0.6


@dataclass
class EventRecord:
    """Lightweight record for clustering (loaded from normalized_events)."""
    event_uid: str
    source: str
    origin_time_utc: datetime
    latitude: float
    longitude: float
    depth_km: float
    magnitude_value: float
    magnitude_type: str
    place: str | None
    region: str | None
    status: str


@dataclass
class Cluster:
    """Group of events representing the same physical earthquake."""
    members: list[EventRecord]
    best_score: float = 0.0

    @property
    def anchor(self) -> EventRecord:
        return self.members[0]


def compute_match_score(a: EventRecord, b: EventRecord) -> float:
    """Compute similarity score between two events (0 → 1)."""
    dt = abs((a.origin_time_utc - b.origin_time_utc).total_seconds())
    if dt > MAX_TIME_DIFF_SEC:
        return 0.0

    dist = haversine_km(a.latitude, a.longitude, b.latitude, b.longitude)
    if dist > MAX_DISTANCE_KM:
        return 0.0

    dmag = abs(a.magnitude_value - b.magnitude_value)
    if dmag > MAX_MAG_DIFF:
        return 0.0

    score = (
        0.4 * max(0.0, 1.0 - dt / MAX_TIME_DIFF_SEC)
        + 0.4 * max(0.0, 1.0 - dist / MAX_DISTANCE_KM)
        + 0.2 * max(0.0, 1.0 - dmag / MAX_MAG_DIFF)
    )
    return score


def cluster_events(events: list[EventRecord]) -> list[Cluster]:
    """Greedy chronological clustering.

    Each event either joins the best-scoring existing cluster or starts a new one.
    """
    # Sort by time
    events_sorted = sorted(events, key=lambda e: e.origin_time_utc)
    clusters: list[Cluster] = []

    for event in events_sorted:
        best_cluster: Cluster | None = None
        best_score = 0.0

        for cluster in clusters:
            score = compute_match_score(event, cluster.anchor)
            if score >= MATCH_SCORE_THRESHOLD and score > best_score:
                best_cluster = cluster
                best_score = score

        if best_cluster is not None:
            best_cluster.members.append(event)
            best_cluster.best_score = max(best_cluster.best_score, best_score)
        else:
            clusters.append(Cluster(members=[event]))

    return clusters


def _select_preferred(cluster: Cluster) -> EventRecord:
    """Select the preferred event from a cluster.

    Priority: reviewed > automatic. Among same status, use source priority.
    """
    reviewed = [m for m in cluster.members if m.status == "reviewed"]
    candidates = reviewed if reviewed else cluster.members

    def source_rank(e: EventRecord) -> int:
        try:
            return SOURCE_PRIORITY.index(e.source)
        except ValueError:
            return len(SOURCE_PRIORITY)

    return min(candidates, key=source_rank)


def _compute_unified_id(cluster: Cluster) -> str:
    """Generate a stable unified event ID from cluster members."""
    uids = sorted(m.event_uid for m in cluster.members)
    content = "|".join(uids)
    return "UE-" + hashlib.sha256(content.encode()).hexdigest()[:16]


def _weighted_mean(cluster: Cluster) -> tuple[float, float, float]:
    """Compute weighted mean lat/lon/depth. Source priority = weight."""
    total_weight = 0.0
    lat_sum = lon_sum = depth_sum = 0.0

    for member in cluster.members:
        try:
            rank = SOURCE_PRIORITY.index(member.source)
        except ValueError:
            rank = len(SOURCE_PRIORITY)
        weight = max(1.0, len(SOURCE_PRIORITY) - rank)

        lat_sum += member.latitude * weight
        lon_sum += member.longitude * weight
        depth_sum += member.depth_km * weight
        total_weight += weight

    if total_weight == 0:
        m = cluster.anchor
        return m.latitude, m.longitude, m.depth_km

    return lat_sum / total_weight, lon_sum / total_weight, depth_sum / total_weight


def run_deduplicator(
    interval_seconds: int = 300,
    lookback_hours: int = 6,
) -> None:
    """Periodically cluster normalized events and write unified events."""
    import click
    click.echo(
        f"Deduplicator started — running every {interval_seconds}s, "
        f"looking back {lookback_hours}h"
    )

    while True:
        try:
            _run_dedup_cycle(lookback_hours)
        except Exception as exc:
            logger.error("Dedup cycle error: %s", exc)

        time.sleep(interval_seconds)


def _run_dedup_cycle(lookback_hours: int) -> None:
    """Single deduplication cycle."""
    import click
    from quake_stream.db import get_connection
    conn = get_connection()

    # Load normalized events from the lookback window
    with conn.cursor() as cur:
        cur.execute("""
            SELECT event_uid, source, origin_time_utc, latitude, longitude,
                   depth_km, magnitude_value, magnitude_type, place, region, status
            FROM normalized_events
            WHERE origin_time_utc >= NOW() - INTERVAL '%s hours'
            ORDER BY origin_time_utc
        """, (lookback_hours,))
        rows = cur.fetchall()

    if not rows:
        return

    events = [
        EventRecord(
            event_uid=r[0], source=r[1],
            origin_time_utc=r[2] if r[2].tzinfo else r[2].replace(tzinfo=timezone.utc),
            latitude=r[3], longitude=r[4], depth_km=r[5],
            magnitude_value=r[6], magnitude_type=r[7],
            place=r[8], region=r[9], status=r[10],
        )
        for r in rows
    ]

    clusters = cluster_events(events)

    # Write unified events and crosswalk
    with conn.cursor() as cur:
        for cluster in clusters:
            preferred = _select_preferred(cluster)
            unified_id = _compute_unified_id(cluster)
            lat, lon, depth = _weighted_mean(cluster)

            # Upsert unified event
            cur.execute("""
                INSERT INTO unified_events (
                    unified_event_id, origin_time_utc, latitude, longitude, depth_km,
                    magnitude_value, magnitude_type, place, region, status,
                    num_sources, preferred_source, preferred_event_uid, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (unified_event_id) DO UPDATE SET
                    origin_time_utc = EXCLUDED.origin_time_utc,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    depth_km = EXCLUDED.depth_km,
                    magnitude_value = EXCLUDED.magnitude_value,
                    magnitude_type = EXCLUDED.magnitude_type,
                    place = EXCLUDED.place,
                    region = EXCLUDED.region,
                    status = EXCLUDED.status,
                    num_sources = EXCLUDED.num_sources,
                    preferred_source = EXCLUDED.preferred_source,
                    preferred_event_uid = EXCLUDED.preferred_event_uid,
                    updated_at = NOW()
            """, (
                unified_id, preferred.origin_time_utc, lat, lon, depth,
                preferred.magnitude_value, preferred.magnitude_type,
                preferred.place, preferred.region, preferred.status,
                len(set(m.source for m in cluster.members)),
                preferred.source, preferred.event_uid,
            ))

            # Upsert crosswalk entries
            for member in cluster.members:
                score = compute_match_score(member, preferred) if member != preferred else 1.0
                is_preferred = member.event_uid == preferred.event_uid
                cur.execute("""
                    INSERT INTO event_crosswalk (event_uid, unified_event_id, match_score, is_preferred)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (event_uid, unified_event_id) DO UPDATE SET
                        match_score = EXCLUDED.match_score,
                        is_preferred = EXCLUDED.is_preferred
                """, (member.event_uid, unified_id, score, is_preferred))

    conn.commit()
    conn.close()

    multi_source = sum(1 for c in clusters if len(set(m.source for m in c.members)) > 1)
    click.echo(
        f"Dedup cycle: {len(events)} events → {len(clusters)} clusters "
        f"({multi_source} multi-source)"
    )
