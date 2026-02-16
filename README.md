# SeisMonitor-Platform

Multi-source earthquake monitoring platform with real-time ingestion from **USGS**, **EMSC (SeismicPortal)**, and **GFZ GEOFON**. Events are normalized to a canonical schema, deduplicated across agencies, and visualized in a Streamlit dashboard.

## Architecture

Two deployment modes:

### Local (Kafka + PostgreSQL)
```
USGS / EMSC / GFZ  →  Kafka  →  Normalizer  →  Deduplicator  →  PostgreSQL  →  Streamlit
```

### GCP Serverless (Cloud Run + BigQuery)
```
Cloud Scheduler (1 min)  →  Cloud Run (ingester)  →  BigQuery  →  Cloud Run (dashboard)
```

## Quick start — Local

```bash
pip install -e ".[dev]"
docker compose up -d          # Kafka + PostgreSQL

quake init-db-v2              # create multi-source tables
quake multi-produce           # poll USGS, EMSC, GFZ → Kafka
quake normalize               # raw → normalized_events
quake deduplicate             # normalized → unified_events
quake web                     # Streamlit dashboard on :8501
```

## Quick start — GCP

```bash
export GCP_PROJECT_ID=your-project-id
bash gcp/deploy.sh            # deploys everything to GCP
```

## Tests

```bash
pytest -v
```
