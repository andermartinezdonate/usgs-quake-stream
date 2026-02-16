#!/usr/bin/env bash
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────
# Set these before running, or export them as environment variables.
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
REGION="${GCP_REGION:-us-central1}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== Deploying Quake Stream to GCP ==="
echo "Project: $PROJECT_ID"
echo "Region:  $REGION"
echo ""

# ── 1. Enable APIs ───────────────────────────────────────────────────────
echo "--- Enabling GCP APIs ---"
gcloud services enable \
    run.googleapis.com \
    cloudscheduler.googleapis.com \
    bigquery.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    --project="$PROJECT_ID"

# ── 2. Create BigQuery dataset + tables ──────────────────────────────────
echo "--- Creating BigQuery dataset ---"
bq mk --dataset --location=US --project_id="$PROJECT_ID" \
    "${PROJECT_ID}:quake_stream" 2>/dev/null || echo "Dataset already exists"

echo "--- Creating BigQuery tables ---"
bq query --use_legacy_sql=false --project_id="$PROJECT_ID" \
    < "$REPO_ROOT/gcp/bigquery/schema.sql"

# ── 3. Build and deploy the ingester ─────────────────────────────────────
echo "--- Deploying ingester to Cloud Run ---"

# Build from repo root so Docker can access src/quake_stream/
cd "$REPO_ROOT"
gcloud run deploy ingest-quakes \
    --source=. \
    --dockerfile=gcp/ingester/Dockerfile \
    --region="$REGION" \
    --memory=512Mi \
    --cpu=1 \
    --timeout=60 \
    --max-instances=1 \
    --min-instances=0 \
    --set-env-vars="GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=quake_stream" \
    --no-allow-unauthenticated \
    --project="$PROJECT_ID"

INGESTER_URL=$(gcloud run services describe ingest-quakes \
    --region="$REGION" --project="$PROJECT_ID" \
    --format='value(status.url)')
echo "Ingester URL: $INGESTER_URL"

# ── 4. Create scheduler service account ──────────────────────────────────
echo "--- Setting up Cloud Scheduler ---"

SA_EMAIL="quake-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"

# Create service account (ignore if exists)
gcloud iam service-accounts create quake-scheduler \
    --display-name="Quake Pipeline Scheduler" \
    --project="$PROJECT_ID" 2>/dev/null || echo "Service account already exists"

# Grant invoker role
gcloud run services add-iam-policy-binding ingest-quakes \
    --region="$REGION" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/run.invoker" \
    --project="$PROJECT_ID"

# ── 5. Create Cloud Scheduler job ────────────────────────────────────────
echo "--- Creating Cloud Scheduler job (every 1 minute) ---"

# Delete existing job if it exists
gcloud scheduler jobs delete quake-ingest-every-minute \
    --location="$REGION" --project="$PROJECT_ID" --quiet 2>/dev/null || true

gcloud scheduler jobs create http quake-ingest-every-minute \
    --location="$REGION" \
    --schedule="* * * * *" \
    --uri="${INGESTER_URL}/ingest" \
    --http-method=POST \
    --oidc-service-account-email="$SA_EMAIL" \
    --attempt-deadline=60s \
    --max-retry-attempts=3 \
    --min-backoff-duration=10s \
    --max-backoff-duration=30s \
    --project="$PROJECT_ID"

# ── 6. Deploy the Streamlit dashboard ────────────────────────────────────
echo "--- Deploying dashboard to Cloud Run ---"

cd "$REPO_ROOT"
gcloud run deploy quake-dashboard \
    --source=. \
    --dockerfile=gcp/dashboard/Dockerfile \
    --region="$REGION" \
    --memory=512Mi \
    --cpu=1 \
    --timeout=300 \
    --max-instances=2 \
    --min-instances=0 \
    --set-env-vars="GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=quake_stream" \
    --allow-unauthenticated \
    --session-affinity \
    --project="$PROJECT_ID"

DASHBOARD_URL=$(gcloud run services describe quake-dashboard \
    --region="$REGION" --project="$PROJECT_ID" \
    --format='value(status.url)')

# ── 7. Test the pipeline ─────────────────────────────────────────────────
echo ""
echo "--- Testing pipeline (manual trigger) ---"

# Get an ID token for the scheduler service account
gcloud scheduler jobs run quake-ingest-every-minute \
    --location="$REGION" --project="$PROJECT_ID" || \
    echo "Manual trigger sent (check Cloud Run logs for result)"

# ── Done ─────────────────────────────────────────────────────────────────
echo ""
echo "=========================================="
echo "  Deployment complete!"
echo "=========================================="
echo ""
echo "  Dashboard:  $DASHBOARD_URL"
echo "  Ingester:   $INGESTER_URL"
echo ""
echo "  Pipeline runs every 1 minute via Cloud Scheduler."
echo "  Monitor: https://console.cloud.google.com/run?project=$PROJECT_ID"
echo "  Logs:    https://console.cloud.google.com/logs?project=$PROJECT_ID"
echo ""
