"""Source registry for earthquake data providers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SourceConfig:
    """Configuration for a single earthquake data source."""

    name: str
    base_url: str
    poll_interval_seconds: int
    max_retries: int
    retry_backoff_base: float
    rate_limit_rpm: int
    timeout_seconds: int
    format: str                 # "geojson" or "fdsn_text"
    enabled: bool


SOURCES: dict[str, SourceConfig] = {
    "usgs": SourceConfig(
        name="usgs",
        base_url="https://earthquake.usgs.gov/fdsnws/event/1/query",
        poll_interval_seconds=60,
        max_retries=3,
        retry_backoff_base=2.0,
        rate_limit_rpm=30,
        timeout_seconds=15,
        format="geojson",
        enabled=True,
    ),
    "emsc": SourceConfig(
        name="emsc",
        base_url="https://seismicportal.eu/fdsnws/event/1/query",
        poll_interval_seconds=120,
        max_retries=3,
        retry_backoff_base=2.0,
        rate_limit_rpm=20,
        timeout_seconds=20,
        format="geojson",
        enabled=True,
    ),
    "gfz": SourceConfig(
        name="gfz",
        base_url="https://geofon.gfz.de/fdsnws/event/1/query",
        poll_interval_seconds=180,
        max_retries=3,
        retry_backoff_base=2.0,
        rate_limit_rpm=10,
        timeout_seconds=20,
        format="fdsn_text",
        enabled=True,
    ),
}

# Source priority for unified event selection (lower index = higher priority)
SOURCE_PRIORITY = ["usgs", "emsc", "gfz"]
