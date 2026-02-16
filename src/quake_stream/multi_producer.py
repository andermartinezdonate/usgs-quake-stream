"""Async multi-source Kafka producer.

Polls all enabled earthquake sources concurrently and publishes raw events to
the `raw_earthquakes` Kafka topic. Also produces USGS events to the legacy
`earthquakes` topic for backward compatibility.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta

import click
from confluent_kafka import Producer

from quake_stream.clients.fdsn_client import FDSNClient
from quake_stream.models_v2 import RawEventEnvelope
from quake_stream.parsers import PARSER_MAP
from quake_stream.sources import SOURCES, SourceConfig

logger = logging.getLogger(__name__)

RAW_TOPIC = "raw_earthquakes"
LEGACY_TOPIC = "earthquakes"


def _delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed: %s", err)


class MultiSourceProducer:
    """Coordinates async polling of multiple earthquake sources."""

    def __init__(self, bootstrap_servers: str = "localhost:9092", min_magnitude: float = 0.0):
        self.bootstrap_servers = bootstrap_servers
        self.min_magnitude = min_magnitude
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})
        self._seen: dict[str, set[str]] = {}  # source → set of source_event_ids

    async def run(self) -> None:
        """Start polling all enabled sources concurrently."""
        tasks = []
        for name, config in SOURCES.items():
            if not config.enabled:
                continue
            self._seen[name] = set()
            tasks.append(asyncio.create_task(self._poll_source(config)))

        click.echo(f"Multi-source producer started — {len(tasks)} source(s) enabled")
        await asyncio.gather(*tasks)

    async def _poll_source(self, config: SourceConfig) -> None:
        """Poll a single source in a loop."""
        client = FDSNClient(config)
        click.echo(f"  [{config.name}] polling every {config.poll_interval_seconds}s")

        try:
            while True:
                try:
                    await self._fetch_and_produce(client, config)
                except Exception as exc:
                    logger.error("[%s] error: %s", config.name, exc)

                await asyncio.sleep(config.poll_interval_seconds)
        finally:
            await client.close()

    async def _fetch_and_produce(self, client: FDSNClient, config: SourceConfig) -> None:
        """Fetch events from source and produce to Kafka."""
        now = datetime.now(timezone.utc)
        # Look back further than poll interval to catch late-arriving events
        lookback = max(config.poll_interval_seconds * 3, 600)
        start_time = now - timedelta(seconds=lookback)

        raw_text = await client.fetch_events(
            start_time=start_time,
            end_time=now,
            min_magnitude=self.min_magnitude,
        )

        if not raw_text.strip():
            return

        fetched_at = datetime.now(timezone.utc)
        parser = PARSER_MAP.get(config.name)
        if parser is None:
            logger.error("[%s] no parser registered", config.name)
            return

        events = parser.parse(raw_text, fetched_at)
        seen = self._seen[config.name]
        new_events = [e for e in events if e.source_event_id not in seen]

        for event in new_events:
            envelope = RawEventEnvelope(
                source=config.name,
                source_event_id=event.source_event_id,
                format=config.format,
                raw_payload=event.to_json(),
                fetched_at=fetched_at,
            )

            # Produce to raw_earthquakes topic
            self._producer.produce(
                RAW_TOPIC,
                key=envelope.key,
                value=envelope.to_json(),
                callback=_delivery_report,
            )

            # Backward compatibility: also produce USGS events to legacy topic
            if config.name == "usgs":
                legacy_payload = _to_legacy_json(event)
                self._producer.produce(
                    LEGACY_TOPIC,
                    key=event.source_event_id,
                    value=legacy_payload,
                    callback=_delivery_report,
                )

            seen.add(event.source_event_id)

        self._producer.flush()

        if new_events:
            click.echo(f"  [{config.name}] published {len(new_events)} new event(s)")


def _to_legacy_json(event) -> str:
    """Convert NormalizedEvent to legacy Earthquake JSON format."""
    d = {
        "id": event.source_event_id,
        "magnitude": event.magnitude_value,
        "place": event.place or "Unknown",
        "time": event.origin_time_utc.isoformat(),
        "longitude": event.longitude,
        "latitude": event.latitude,
        "depth": event.depth_km,
        "url": event.url or "",
    }
    return json.dumps(d)


def run_multi_producer(
    bootstrap_servers: str = "localhost:9092",
    min_magnitude: float = 0.0,
) -> None:
    """Entry point: start the async multi-source producer."""
    producer = MultiSourceProducer(bootstrap_servers, min_magnitude)
    asyncio.run(producer.run())
