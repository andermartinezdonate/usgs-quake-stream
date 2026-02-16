"""Cloud Run entrypoint — HTTP handler for scheduled earthquake ingestion."""

from __future__ import annotations

import asyncio
import logging
import os

from flask import Flask, jsonify, request

from pipeline import run_pipeline

app = Flask(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


@app.route("/ingest", methods=["POST"])
def ingest():
    """Triggered by Cloud Scheduler every minute."""
    try:
        result = asyncio.run(run_pipeline())
        logger.info("Pipeline OK: %s", result)
        return jsonify(result), 200
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc, exc_info=True)
        return jsonify({"error": str(exc)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/", methods=["GET"])
def root():
    return jsonify({"service": "quake-ingester", "status": "running"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
