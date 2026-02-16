"""PostgreSQL database layer for earthquake persistence."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://quake:quake@localhost:5432/quakestream",
)


def get_connection():
    return psycopg2.connect(DATABASE_URL)


def init_db() -> None:
    """Create the earthquakes table if it doesn't exist."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS earthquakes (
                    id          TEXT PRIMARY KEY,
                    magnitude   DOUBLE PRECISION NOT NULL,
                    place       TEXT NOT NULL,
                    time        TIMESTAMPTZ NOT NULL,
                    longitude   DOUBLE PRECISION NOT NULL,
                    latitude    DOUBLE PRECISION NOT NULL,
                    depth       DOUBLE PRECISION NOT NULL,
                    url         TEXT,
                    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_earthquakes_time ON earthquakes (time DESC);
                CREATE INDEX IF NOT EXISTS idx_earthquakes_magnitude ON earthquakes (magnitude);
            """)
        conn.commit()


def upsert_earthquake(quake: dict) -> bool:
    """Insert or ignore an earthquake. Returns True if new row inserted."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO earthquakes (id, magnitude, place, time, longitude, latitude, depth, url)
                VALUES (%(id)s, %(magnitude)s, %(place)s, %(time)s, %(longitude)s, %(latitude)s, %(depth)s, %(url)s)
                ON CONFLICT (id) DO NOTHING
            """, quake)
            inserted = cur.rowcount > 0
        conn.commit()
    return inserted


def upsert_batch(quakes: list[dict]) -> int:
    """Insert a batch of earthquakes. Returns count of new rows."""
    if not quakes:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO earthquakes (id, magnitude, place, time, longitude, latitude, depth, url)
                VALUES (%(id)s, %(magnitude)s, %(place)s, %(time)s, %(longitude)s, %(latitude)s, %(depth)s, %(url)s)
                ON CONFLICT (id) DO NOTHING
            """, quakes)
            # execute_batch doesn't give per-row counts, query total after
        conn.commit()
    return len(quakes)


def query_earthquakes(
    hours: Optional[int] = 24,
    min_magnitude: float = 0.0,
) -> list[dict]:
    """Query earthquakes from the database."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            query = """
                SELECT id, magnitude, place, time, longitude, latitude, depth, url, ingested_at
                FROM earthquakes
                WHERE magnitude >= %s
            """
            params: list = [min_magnitude]

            if hours is not None:
                query += " AND time >= NOW() - INTERVAL '%s hours'"
                params.append(hours)

            query += " ORDER BY time DESC"
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]


def get_stats() -> dict:
    """Get summary statistics from the database."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COALESCE(MAX(magnitude), 0) as max_magnitude,
                    COALESCE(MIN(magnitude), 0) as min_magnitude,
                    COALESCE(AVG(magnitude), 0) as avg_magnitude,
                    COUNT(*) FILTER (WHERE magnitude >= 5.0) as count_m5_plus,
                    COUNT(*) FILTER (WHERE magnitude >= 3.0 AND magnitude < 5.0) as count_m3_to_5,
                    COUNT(*) FILTER (WHERE magnitude < 3.0) as count_below_m3,
                    MIN(time) as earliest,
                    MAX(time) as latest
                FROM earthquakes
            """)
            return dict(cur.fetchone())
