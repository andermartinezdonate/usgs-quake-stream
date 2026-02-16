# SeisMonitor-Platform

## Project overview
Multi-source earthquake monitoring platform: USGS + EMSC + GFZ ingestion, normalization, and deduplication. Supports two deployment modes: local (Kafka + PostgreSQL) and GCP serverless (Cloud Run + BigQuery + Cloud Scheduler).

## Tech stack
- Python 3.10+, confluent-kafka, httpx, click, rich
- Apache Kafka (KRaft mode, no Zookeeper) via Docker

## Key commands
- `pip install -e ".[dev]"` — install with dev deps
- `docker compose up -d` — start Kafka + PostgreSQL
- `pytest -v` — run tests
- `quake recent` — fetch earthquakes directly (no Kafka)
- `quake produce` — start legacy single-source Kafka producer
- `quake consume` — start Kafka consumer

### Multi-source pipeline (v2)
- `quake init-db-v2` — create multi-source database tables
- `quake multi-produce` — start multi-source Kafka producer (USGS, EMSC, GFZ)
- `quake normalize` — start normalizer (raw_earthquakes → normalized_events)
- `quake deduplicate` — start deduplicator (normalized → unified_events)

## Project structure
- `src/quake_stream/` — main package
  - `models.py` — Legacy Earthquake dataclass
  - `models_v2.py` — NormalizedEvent, UnifiedEvent, RawEventEnvelope dataclasses
  - `usgs_client.py` — Legacy HTTP client for USGS API
  - `producer.py` — Legacy single-source Kafka producer
  - `multi_producer.py` — Async multi-source Kafka producer
  - `normalizer.py` — Kafka consumer: raw → normalized + validation
  - `deduplicator.py` — Periodic batch: cluster + unify + crosswalk
  - `consumer.py` — Kafka consumer (display)
  - `db.py` — PostgreSQL database layer (legacy + v2 functions)
  - `db_consumer.py` — Kafka → PostgreSQL consumer
  - `dashboard_web.py` — Streamlit web dashboard (legacy + unified view toggle)
  - `map_layers.py` — Map rendering (globe + interactive mapbox views)
  - `tectonic.py` — PB2002 tectonic plate data loading/caching
  - `dashboard.py` — Rich terminal dashboard
  - `cli.py` — Click CLI entrypoint
  - `geo.py` — Haversine distance (pure Python)
  - `sources/` — SourceConfig dataclass + SOURCES registry (USGS, EMSC, GFZ)
  - `clients/fdsn_client.py` — Generic async FDSN HTTP client with retry + rate limiting
  - `parsers/` — Event parsers (USGS GeoJSON, EMSC GeoJSON, FDSN text)
  - `migrations/001_multi_source.sql` — DDL for 5 new tables
- `tests/` — pytest tests (use pytest-httpx for mocking)
- `docker-compose.yml` — Kafka KRaft single-node + PostgreSQL

## Kafka topics
- `earthquakes` — legacy single-source USGS events
- `raw_earthquakes` — multi-source raw event envelopes

## Database tables
- `earthquakes` — legacy USGS-only events
- `raw_events` — immutable append-only log of raw API responses
- `normalized_events` — per-source cleaned events (canonical schema)
- `unified_events` — deduplicated best-estimate events
- `event_crosswalk` — mapping: normalized → unified with match scores
- `dead_letter_events` — events that failed validation

## GCP Serverless Pipeline
- `gcp/` — Cloud Run + BigQuery + Cloud Scheduler deployment
  - `gcp/ingester/` — Flask app: fetch → normalize → dedupe → BigQuery (triggered every 1 min)
  - `gcp/dashboard/` — Streamlit app reading from BigQuery unified_events
  - `gcp/bigquery/schema.sql` — BigQuery table DDL (raw_events, unified_events, dead_letter, pipeline_runs)
  - `gcp/deploy.sh` — One-command deployment script
- Deploy: `export GCP_PROJECT_ID=your-project && bash gcp/deploy.sh`
- Reuses: parsers/, models_v2.py, deduplicator.py, geo.py, sources/, map_layers.py, tectonic.py

## Conventions
- Use `httpx` for HTTP (not requests)
- Use `rich` for terminal output
- Use `click` for CLI
- Kafka topics: `earthquakes` (legacy), `raw_earthquakes` (v2)
- Sources: USGS, EMSC (SeismicPortal), GFZ (GEOFON) — all FDSN-compliant
