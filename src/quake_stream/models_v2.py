"""V2 data models for multi-source earthquake ingestion."""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class NormalizedEvent:
    """Canonical earthquake event from any source, fully normalized."""

    event_uid: str              # "{source}:{source_event_id}"
    source: str                 # "usgs", "emsc", "gfz"
    source_event_id: str

    origin_time_utc: datetime   # Always UTC
    latitude: float             # WGS84, [-90, 90]
    longitude: float            # WGS84, [-180, 180]
    depth_km: float             # Kilometers

    magnitude_value: float
    magnitude_type: str         # Lowercase: "mw", "ml", "mb", "ms", "md"

    place: Optional[str] = None
    region: Optional[str] = None

    # Uncertainty (all nullable)
    lat_error_km: Optional[float] = None
    lon_error_km: Optional[float] = None
    depth_error_km: Optional[float] = None
    mag_error: Optional[float] = None
    time_error_sec: Optional[float] = None

    # Quality
    status: str = "automatic"   # "automatic", "reviewed", "deleted"
    num_phases: Optional[int] = None
    azimuthal_gap: Optional[float] = None

    # Provenance
    author: Optional[str] = None
    url: Optional[str] = None
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = None
    raw_payload: str = ""

    def to_json(self) -> str:
        d = asdict(self)
        for key in ("origin_time_utc", "fetched_at", "updated_at"):
            if d[key] is not None:
                d[key] = d[key].isoformat()
        return json.dumps(d)

    @classmethod
    def from_json(cls, raw: str) -> NormalizedEvent:
        d = json.loads(raw)
        for key in ("origin_time_utc", "fetched_at", "updated_at"):
            if d.get(key) is not None:
                d[key] = datetime.fromisoformat(d[key])
        return cls(**d)


@dataclass
class UnifiedEvent:
    """Deduplicated best-estimate earthquake event."""

    unified_event_id: str
    origin_time_utc: datetime
    latitude: float
    longitude: float
    depth_km: float

    magnitude_value: float
    magnitude_type: str

    place: Optional[str] = None
    region: Optional[str] = None
    status: str = "automatic"

    num_sources: int = 1
    preferred_source: str = ""
    preferred_event_uid: str = ""

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = None

    def to_json(self) -> str:
        d = asdict(self)
        for key in ("origin_time_utc", "created_at", "updated_at"):
            if d[key] is not None:
                d[key] = d[key].isoformat()
        return json.dumps(d)

    @classmethod
    def from_json(cls, raw: str) -> UnifiedEvent:
        d = json.loads(raw)
        for key in ("origin_time_utc", "created_at", "updated_at"):
            if d.get(key) is not None:
                d[key] = datetime.fromisoformat(d[key])
        return cls(**d)


@dataclass
class RawEventEnvelope:
    """Wrapper for raw data from any source, sent to Kafka."""

    source: str
    source_event_id: str
    format: str                 # "geojson" or "fdsn_text"
    raw_payload: str            # Original response body (text)
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def key(self) -> str:
        return f"{self.source}:{self.source_event_id}"

    def to_json(self) -> str:
        d = asdict(self)
        d["fetched_at"] = d["fetched_at"].isoformat()
        return json.dumps(d)

    @classmethod
    def from_json(cls, raw: str) -> RawEventEnvelope:
        d = json.loads(raw)
        d["fetched_at"] = datetime.fromisoformat(d["fetched_at"])
        return cls(**d)
