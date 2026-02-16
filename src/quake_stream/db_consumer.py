"""Kafka consumer that persists earthquake events to PostgreSQL."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from threading import Lock

import click
from confluent_kafka import Consumer, KafkaError, TopicPartition

from quake_stream.db import init_db, upsert_earthquake

TOPIC = "earthquakes"


class PipelineMetrics:
    """Thread-safe metrics for the Kafka pipeline."""

    def __init__(self):
        self._lock = Lock()
        self.total_consumed = 0
        self.total_inserted = 0
        self.total_duplicates = 0
        self.errors = 0
        self.last_message_time = None
        self.messages_per_interval = []
        self.consumer_lag = 0
        self.connected = False
        self.start_time = datetime.now(timezone.utc)

    def record_message(self, inserted: bool):
        with self._lock:
            self.total_consumed += 1
            if inserted:
                self.total_inserted += 1
            else:
                self.total_duplicates += 1
            self.last_message_time = datetime.now(timezone.utc)

    def record_error(self):
        with self._lock:
            self.errors += 1

    def snapshot(self) -> dict:
        with self._lock:
            uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()
            return {
                "total_consumed": self.total_consumed,
                "total_inserted": self.total_inserted,
                "total_duplicates": self.total_duplicates,
                "errors": self.errors,
                "last_message_time": self.last_message_time.isoformat() if self.last_message_time else None,
                "consumer_lag": self.consumer_lag,
                "connected": self.connected,
                "uptime_seconds": uptime,
                "msgs_per_sec": self.total_consumed / max(uptime, 1),
            }


# Global metrics instance so the dashboard can read it
metrics = PipelineMetrics()


def run_db_consumer(
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "quake-db-writer",
) -> None:
    """Consume earthquake events from Kafka and write to PostgreSQL."""
    init_db()

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "statistics.interval.ms": 5000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    metrics.connected = True

    click.echo(f"DB Consumer started — writing to PostgreSQL from topic '{TOPIC}'")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                click.echo(f"Consumer error: {msg.error()}", err=True)
                metrics.record_error()
                continue

            try:
                raw = msg.value().decode("utf-8")
                quake_data = json.loads(raw)
                # Ensure time is a string for psycopg2
                inserted = upsert_earthquake(quake_data)
                metrics.record_message(inserted)

                if inserted:
                    click.echo(
                        f"  + M{quake_data['magnitude']:.1f} {quake_data['place']}"
                    )
            except Exception as exc:
                click.echo(f"Error processing message: {exc}", err=True)
                metrics.record_error()

            # Update lag estimate
            try:
                partitions = consumer.assignment()
                if partitions:
                    committed = consumer.committed(partitions, timeout=1.0)
                    total_lag = 0
                    for tp in committed:
                        low, high = consumer.get_watermark_offsets(tp, timeout=1.0)
                        if tp.offset >= 0:
                            total_lag += high - tp.offset
                    metrics.consumer_lag = total_lag
            except Exception:
                pass

    except KeyboardInterrupt:
        click.echo("\nShutting down DB consumer...")
    finally:
        metrics.connected = False
        consumer.close()
