"""Parser for USGS GeoJSON earthquake feed."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from quake_stream.models_v2 import NormalizedEvent
from quake_stream.parsers.base import EventParser

# USGS status mapping
_STATUS_MAP = {
    "automatic": "automatic",
    "reviewed": "reviewed",
    "deleted": "deleted",
}


class USGSGeoJSONParser(EventParser):
    """Parse USGS GeoJSON response → list of NormalizedEvent."""

    def parse(self, raw_payload: str, fetched_at: datetime) -> list[NormalizedEvent]:
        data = json.loads(raw_payload)
        features = data.get("features", [])
        events: list[NormalizedEvent] = []

        for feature in features:
            try:
                events.append(self._parse_feature(feature, fetched_at, raw_payload=""))
            except (KeyError, TypeError, ValueError):
                continue

        return events

    def parse_single_feature(
        self, feature: dict, fetched_at: datetime, raw_payload: str = "",
    ) -> NormalizedEvent:
        """Parse a single GeoJSON feature dict."""
        return self._parse_feature(feature, fetched_at, raw_payload)

    @staticmethod
    def _parse_feature(
        feature: dict, fetched_at: datetime, raw_payload: str,
    ) -> NormalizedEvent:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]

        source_event_id = feature["id"]
        mag = props.get("mag") or 0.0
        mag_type = (props.get("magType") or "ml").lower()
        place = props.get("place")
        time_ms = props["time"]
        origin_time = datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc)

        updated_ms = props.get("updated")
        updated_at = (
            datetime.fromtimestamp(updated_ms / 1000, tz=timezone.utc)
            if updated_ms else None
        )

        status_raw = (props.get("status") or "automatic").lower()
        status = _STATUS_MAP.get(status_raw, "automatic")

        # Longitude normalization to [-180, 180]
        longitude = coords[0]
        if longitude > 180:
            longitude -= 360
        elif longitude < -180:
            longitude += 360

        return NormalizedEvent(
            event_uid=f"usgs:{source_event_id}",
            source="usgs",
            source_event_id=source_event_id,
            origin_time_utc=origin_time,
            latitude=coords[1],
            longitude=longitude,
            depth_km=coords[2],
            magnitude_value=float(mag),
            magnitude_type=mag_type,
            place=place,
            region=_extract_region(place),
            lat_error_km=_safe_float(props.get("horizontalError")),
            lon_error_km=_safe_float(props.get("horizontalError")),
            depth_error_km=_safe_float(props.get("depthError")),
            mag_error=_safe_float(props.get("magError")),
            time_error_sec=_safe_float(props.get("timeError")),
            status=status,
            num_phases=_safe_int(props.get("nph")),
            azimuthal_gap=_safe_float(props.get("gap")),
            author=props.get("net"),
            url=props.get("url"),
            fetched_at=fetched_at,
            updated_at=updated_at,
            raw_payload=raw_payload,
        )


def _extract_region(place: str | None) -> str | None:
    if not place:
        return None
    parts = place.split(", ")
    return parts[-1] if len(parts) > 1 else place


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None
