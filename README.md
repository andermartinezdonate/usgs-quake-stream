# SeisMonitor-Platform

**Real-time multi-source earthquake monitoring platform** that ingests seismic data from 6 global agencies, normalizes heterogeneous formats into a canonical schema, deduplicates overlapping reports using DBSCAN clustering with region-aware source priority, computes quality metrics, and serves everything through an interactive Streamlit dashboard.

Built as a complete data engineering pipeline with two deployment modes: **local** (Kafka + PostgreSQL) for development and **GCP serverless** (per-source Cloud Run + BigQuery + Cloud Scheduler) for production.

---

## Architecture

### GCP Serverless (Production)

Each seismic source runs as an independent Cloud Run service, triggered on its own schedule. A dedicated dedup service clusters and merges events every 5 minutes.

```
Cloud Scheduler
  ├── ingest-usgs    (every 1 min)  ─┐
  ├── ingest-emsc    (every 2 min)   │
  ├── ingest-gfz     (every 3 min)   ├──► BigQuery raw_events (append-only)
  ├── ingest-isc     (every 5 min)   │
  └── ingest-ipgp    (every 3 min)  ─┘
                                      │
  └── quake-dedup    (every 5 min) ───┼──► BigQuery unified_events (MERGE upsert)
                                      │
  └── quake-dashboard ────────────────┘──► Streamlit UI (public)
```

### Local Development (Kafka + PostgreSQL)

```
USGS / EMSC / GFZ / ISC / IPGP / GeoNet
  │
  ▼
Kafka (per-source topics: raw_usgs, raw_emsc, raw_gfz, ...)
  │
  ▼
Normalizer ──► Deduplicator (DBSCAN) ──► PostgreSQL
  │
  ▼
Streamlit Dashboard (:8501)
```

---

## Data Sources

The platform ingests from 6 seismic agencies, each serving different formats and geographic strengths:

| Source | Agency | Coverage | Format | Poll Interval |
|--------|--------|----------|--------|:-------------:|
| **USGS** | U.S. Geological Survey | Global (Americas focus) | GeoJSON | 1 min |
| **EMSC** | Euro-Med Seismological Centre | Euro-Mediterranean | GeoJSON | 2 min |
| **GFZ** | GFZ GEOFON (Potsdam) | Global (Europe focus) | FDSN Text | 3 min |
| **ISC** | International Seismological Centre | Global (Africa/Asia) | QuakeML | 5 min |
| **IPGP** | Institut de Physique du Globe de Paris | French territories | QuakeML | 3 min |
| **GeoNet** | GeoNet New Zealand | New Zealand / Pacific | QuakeML | 3 min |

All sources implement the [FDSN Web Services](https://www.fdsn.org/webservices/) specification. Parsers handle three response formats:

- **GeoJSON** — USGS and EMSC custom GeoJSON with `features[].properties`
- **FDSN Text** — GFZ pipe-delimited text (`EventID|Time|Latitude|...`)
- **QuakeML 1.2** — ISC, IPGP, GeoNet XML with namespace handling, preferred origin/magnitude resolution, and ISC-specific quirks (no `preferredMagnitudeID`, depth in meters)

---

## Deduplication

When a M 5.0 earthquake hits the Mediterranean, USGS, EMSC, GFZ, and ISC may all report it — each with slightly different coordinates, magnitudes, and timestamps. The deduplicator resolves these into a single best-estimate event.

### DBSCAN Clustering

1. **Spatial clustering** — `DBSCAN(eps=100km, metric=haversine, min_samples=1)` groups events within 100 km
2. **Sub-clustering** — Within each spatial cluster, events are separated by time (30s) and magnitude (0.5) to distinguish aftershocks at the same location
3. **Match scoring** — `0.4 * time_similarity + 0.4 * distance_similarity + 0.2 * magnitude_similarity` (threshold: 0.6)

### Region-Aware Source Priority

The preferred source for each unified event depends on where the earthquake occurred:

| Region | Preferred Source Order |
|--------|----------------------|
| **Americas** (lon -170 to -30) | USGS > EMSC > GFZ > ISC > IPGP > GeoNet |
| **Europe** (lon -30 to 45, lat >= 30) | EMSC > GFZ > USGS > ISC > IPGP > GeoNet |
| **Africa** (lon -20 to 55, lat < 30) | ISC > EMSC > IPGP > USGS > GFZ > GeoNet |
| **Asia / Pacific** (lon > 45) | ISC > USGS > GeoNet > EMSC > GFZ > IPGP |

Reviewed events are always preferred over automatic, regardless of source priority.

### Quality Metrics

Each unified event includes three quality indicators computed from its contributing sources:

| Metric | Description | Ideal Value |
|--------|-------------|:-----------:|
| **magnitude_std** | Std dev of magnitudes across sources | 0.0 (perfect agreement) |
| **location_spread_km** | Max pairwise distance between source locations | 0.0 km |
| **source_agreement_score** | Unique sources / cluster members | 1.0 (no duplicates) |

---

## Quick Start

### Local (Kafka + PostgreSQL)

```bash
# Install
pip install -e ".[dev]"

# Start infrastructure
docker compose up -d          # Kafka (KRaft) + PostgreSQL + Kafka UI (:8080)

# Run the pipeline
quake init-db-v2              # Create multi-source database tables
quake multi-produce           # Poll all 6 sources → per-source Kafka topics
quake normalize               # raw_{source} topics → normalized_events
quake deduplicate             # DBSCAN clustering → unified_events

# View results
quake web                     # Streamlit dashboard on :8501
quake recent --period day     # Quick CLI table (no Kafka required)
quake dashboard               # Rich terminal dashboard (auto-refresh)
```

### GCP Serverless

```bash
export GCP_PROJECT_ID=your-project-id
bash gcp/deploy.sh
```

This deploys:
- 5 per-source ingester Cloud Run services (shared Docker image, `SOURCE_NAME` env var)
- 1 dedup Cloud Run service (with scikit-learn for DBSCAN)
- 1 Streamlit dashboard (publicly accessible)
- 6 Cloud Scheduler jobs (per-source + dedup)
- BigQuery dataset with 4 tables + migrations

---

## BigQuery Schema

| Table | Purpose | Partitioned By | Retention |
|-------|---------|:-------------:|:---------:|
| **raw_events** | Append-only log of normalized events from all sources | `origin_time_utc` | 90 days |
| **unified_events** | Deduplicated best-estimate events with quality metrics | `origin_time_utc` | 365 days |
| **dead_letter_events** | Events that failed validation | `created_at` | 30 days |
| **pipeline_runs** | Execution log for observability | `started_at` | 30 days |

---

## Dashboard

The Streamlit dashboard provides real-time monitoring across 7 analytics tabs:

| Tab | What It Shows |
|-----|---------------|
| **Frequency** | Time-series event counts + cumulative trend |
| **Magnitude** | Distribution histogram + magnitude over time |
| **Depth** | Depth histogram + depth vs magnitude scatter |
| **Regions** | Most active regions + magnitude by region box plots |
| **Source Coverage** | Geographic scatter colored by source + per-source event counts |
| **Source Comparison** | Quality metric histograms (magnitude_std, location_spread, agreement) + multi-source delta table |
| **Pipeline Health** | Per-source status (green/yellow/red), success rate, avg duration, dead letter counts |

Additional features:
- Source filtering sidebar (multiselect by agency)
- Per-source KPI cards with color coding
- Globe view (orthographic/natural earth) and interactive Mapbox map
- Tectonic plate boundary overlay (PB2002 dataset)
- M 5.0+ alert banner
- CSV/JSON export
- 60-second auto-refresh

---

## Project Structure

```
src/quake_stream/
├── cli.py                    # Click CLI (15+ commands)
├── models_v2.py              # NormalizedEvent, UnifiedEvent, RawEventEnvelope
├── sources/__init__.py       # SourceConfig registry (6 sources)
├── clients/fdsn_client.py    # Async HTTP client with retry + rate limiting
├── parsers/
│   ├── base.py               # Abstract EventParser + validation
│   ├── usgs_geojson.py       # USGS GeoJSON parser
│   ├── emsc_geojson.py       # EMSC GeoJSON parser
│   ├── fdsn_text.py          # GFZ FDSN text parser
│   └── quakeml.py            # QuakeML 1.2 parser (ISC/IPGP/GeoNet)
├── deduplicator.py           # DBSCAN clustering + quality metrics
├── region_priority.py        # Continent classifier + source priority
├── multi_producer.py         # Async multi-source Kafka producer
├── normalizer.py             # Kafka consumer: raw → normalized
├── logging_config.py         # Structured JSON logging (Cloud Run)
├── db.py                     # PostgreSQL layer (legacy + v2)
├── dashboard_web.py          # Streamlit dashboard (local mode)
└── geo.py                    # Haversine distance

gcp/
├── ingester/
│   ├── main.py               # Flask entrypoint (per-source routing)
│   ├── source_pipeline.py    # Single-source fetch → normalize → BigQuery
│   ├── bq_client.py          # BigQuery insert/merge/health queries
│   └── Dockerfile
├── dedup/
│   ├── main.py               # Flask entrypoint
│   ├── dedup_pipeline.py     # DBSCAN clustering on BigQuery raw_events
│   └── Dockerfile
├── dashboard/
│   ├── app.py                # Streamlit dashboard (BigQuery backend)
│   └── Dockerfile
├── bigquery/
│   ├── schema.sql            # Table DDL (4 tables)
│   └── migrations/           # Additive schema migrations
└── deploy.sh                 # One-command GCP deployment

tests/
├── test_usgs_client.py       # Legacy USGS client
├── test_multi_source.py      # Parsers, validation, clustering
├── test_quakeml_parser.py    # QuakeML (ISC/IPGP/GeoNet formats)
└── test_region_priority.py   # Region classification, quality metrics, DBSCAN
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Ingestion** | `httpx` (async HTTP), FDSN Web Services API |
| **Streaming** | Apache Kafka (KRaft mode, per-source topics) |
| **Processing** | `scikit-learn` (DBSCAN), `scipy`, `numpy` |
| **Storage** | PostgreSQL (local), BigQuery (GCP) |
| **Compute** | Cloud Run (per-source services), Cloud Scheduler |
| **Visualization** | Streamlit, Plotly, pydeck |
| **CLI** | Click, Rich |
| **Testing** | pytest, pytest-httpx |

---

## Tests

```bash
pytest -v                  # Run all 61 tests
pytest tests/test_quakeml_parser.py -v   # QuakeML parser only
pytest tests/test_region_priority.py -v  # Region + quality metrics only
```

Test coverage includes:
- All 3 parser formats (GeoJSON, FDSN Text, QuakeML)
- DBSCAN clustering with spatial/temporal/magnitude sub-clustering
- Region classification for all continents
- Quality metric computation (magnitude_std, location_spread, agreement)
- Event validation (lat/lon/depth/magnitude bounds)
- Match scoring and preferred source selection
- ISC-specific QuakeML quirks (event ID formats, magnitude selection, depth in meters)

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Per-source Cloud Run services** | Independent scaling, isolated failures, per-source observability |
| **DBSCAN over greedy clustering** | Handles arbitrary cluster shapes; O(n log n) with BallTree for haversine |
| **Region-aware priority** | Local agencies are more authoritative for their region |
| **Lazy sklearn import** | Ingester images don't need sklearn — only the dedup service does |
| **Append-only raw_events** | Full audit trail; dedup runs on sliding window, raw data is immutable |
| **Dead letter queue** | Events failing validation are preserved for debugging, not silently dropped |
| **Quality metrics on unified events** | Quantifies confidence: low magnitude_std + low location_spread = high-confidence event |

---

## License

MIT
