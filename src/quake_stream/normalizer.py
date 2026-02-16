"""Kafka consumer that normalizes raw earthquake events.

Consumes from `raw_earthquakes`, dispatches to the appropriate parser,
validates, and writes to PostgreSQL (raw_events + normalized_events).
Invalid events go to dead_letter_events.
"""

from __future__ import annotations

import json
import logging

import click
from confluent_kafka import Consumer, KafkaError

from quake_stream.db import get_connection
from quake_stream.models_v2 import RawEventEnvelope, NormalizedEvent
from quake_stream.parsers import PARSER_MAP
from quake_stream.parsers.base import EventParser

logger = logging.getLogger(__name__)

RAW_TOPIC = "raw_earthquakes"


def _insert_raw_event(cur, source: str, source_event_id: str, raw_payload: str, fetched_at):
    cur.execute(
        """INSERT INTO raw_events (source, source_event_id, raw_payload, fetched_at)
           VALUES (%s, %s, %s::jsonb, %s)""",
        (source, source_event_id, raw_payload, fetched_at),
    )


def _upsert_normalized_event(cur, event: NormalizedEvent):
    cur.execute("""
        INSERT INTO normalized_events (
            event_uid, source, source_event_id,
            origin_time_utc, latitude, longitude, depth_km,
            magnitude_value, magnitude_type,
            place, region,
            lat_error_km, lon_error_km, depth_error_km, mag_error, time_error_sec,
            status, num_phases, azimuthal_gap,
            author, url, fetched_at, updated_at, raw_payload
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (event_uid) DO UPDATE SET
            origin_time_utc = EXCLUDED.origin_time_utc,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            depth_km = EXCLUDED.depth_km,
            magnitude_value = EXCLUDED.magnitude_value,
            magnitude_type = EXCLUDED.magnitude_type,
            place = EXCLUDED.place,
            region = EXCLUDED.region,
            lat_error_km = EXCLUDED.lat_error_km,
            lon_error_km = EXCLUDED.lon_error_km,
            depth_error_km = EXCLUDED.depth_error_km,
            mag_error = EXCLUDED.mag_error,
            time_error_sec = EXCLUDED.time_error_sec,
            status = EXCLUDED.status,
            num_phases = EXCLUDED.num_phases,
            azimuthal_gap = EXCLUDED.azimuthal_gap,
            author = EXCLUDED.author,
            url = EXCLUDED.url,
            fetched_at = EXCLUDED.fetched_at,
            updated_at = EXCLUDED.updated_at,
            raw_payload = EXCLUDED.raw_payload
    """, (
        event.event_uid, event.source, event.source_event_id,
        event.origin_time_utc, event.latitude, event.longitude, event.depth_km,
        event.magnitude_value, event.magnitude_type,
        event.place, event.region,
        event.lat_error_km, event.lon_error_km, event.depth_error_km,
        event.mag_error, event.time_error_sec,
        event.status, event.num_phases, event.azimuthal_gap,
        event.author, event.url, event.fetched_at, event.updated_at,
        event.raw_payload,
    ))


def _insert_dead_letter(cur, source: str, source_event_id: str | None,
                        raw_payload: str, errors: list[str]):
    cur.execute(
        """INSERT INTO dead_letter_events (source, source_event_id, raw_payload, error_messages)
           VALUES (%s, %s, %s, %s)""",
        (source, source_event_id, raw_payload, errors),
    )


def run_normalizer(
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "quake-normalizer",
) -> None:
    """Consume raw events, normalize, validate, and persist to PostgreSQL."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([RAW_TOPIC])

    click.echo(f"Normalizer started — consuming from '{RAW_TOPIC}' (group={group_id})")

    total_processed = 0
    total_normalized = 0
    total_dead = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue

            try:
                envelope = RawEventEnvelope.from_json(msg.value().decode("utf-8"))
            except Exception as exc:
                logger.error("Failed to deserialize envelope: %s", exc)
                continue

            total_processed += 1
            source = envelope.source

            parser = PARSER_MAP.get(source)
            if parser is None:
                logger.error("No parser for source '%s'", source)
                continue

            # The envelope's raw_payload contains the event JSON from the producer
            try:
                event = NormalizedEvent.from_json(envelope.raw_payload)
            except Exception:
                # Fallback: try parsing as raw API response
                try:
                    events = parser.parse(envelope.raw_payload, envelope.fetched_at)
                    if not events:
                        continue
                    event = events[0]
                except Exception as exc:
                    logger.error("[%s] parse error: %s", source, exc)
                    _dead_letter(source, envelope, [str(exc)])
                    total_dead += 1
                    continue

            # Validate
            errors = EventParser.validate(event)
            if errors:
                logger.warning("[%s] validation failed for %s: %s",
                               source, event.event_uid, errors)
                _dead_letter(source, envelope, errors)
                total_dead += 1
                continue

            # Persist
            try:
                conn = get_connection()
                with conn.cursor() as cur:
                    _insert_raw_event(
                        cur, source, envelope.source_event_id,
                        json.dumps({"envelope": envelope.to_json()}),
                        envelope.fetched_at,
                    )
                    _upsert_normalized_event(cur, event)
                conn.commit()
                conn.close()
                total_normalized += 1
            except Exception as exc:
                logger.error("[%s] DB error: %s", source, exc)
                continue

            if total_normalized % 50 == 0 and total_normalized > 0:
                click.echo(
                    f"Progress: {total_processed} processed, "
                    f"{total_normalized} normalized, {total_dead} dead-lettered"
                )

    except KeyboardInterrupt:
        click.echo(
            f"\nNormalizer stopped — {total_processed} processed, "
            f"{total_normalized} normalized, {total_dead} dead-lettered"
        )
    finally:
        consumer.close()


def _dead_letter(source: str, envelope: RawEventEnvelope, errors: list[str]) -> None:
    """Write a failed event to the dead_letter_events table."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            _insert_dead_letter(cur, source, envelope.source_event_id,
                                envelope.raw_payload, errors)
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.error("Failed to write dead letter: %s", exc)
