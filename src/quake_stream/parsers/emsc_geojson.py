"""Parser for EMSC (SeismicPortal) GeoJSON earthquake feed."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from quake_stream.models_v2 import NormalizedEvent
from quake_stream.parsers.base import EventParser


class EMSCGeoJSONParser(EventParser):
    """Parse EMSC/SeismicPortal GeoJSON response → list of NormalizedEvent."""

    def parse(self, raw_payload: str, fetched_at: datetime) -> list[NormalizedEvent]:
        data = json.loads(raw_payload)
        features = data.get("features", [])
        events: list[NormalizedEvent] = []

        for feature in features:
            try:
                events.append(self._parse_feature(feature, fetched_at))
            except (KeyError, TypeError, ValueError):
                continue

        return events

    @staticmethod
    def _parse_feature(feature: dict, fetched_at: datetime) -> NormalizedEvent:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]

        # EMSC uses "unid" as the event identifier
        source_event_id = props.get("unid") or props.get("source_id") or feature.get("id", "")

        # Time: EMSC FDSN returns ISO 8601 string in the "time" property
        time_raw = props["time"]
        if isinstance(time_raw, str):
            origin_time = datetime.fromisoformat(time_raw.replace("Z", "+00:00"))
        else:
            origin_time = datetime.fromtimestamp(time_raw / 1000, tz=timezone.utc)

        if origin_time.tzinfo is None:
            origin_time = origin_time.replace(tzinfo=timezone.utc)

        mag = float(props.get("mag", 0.0))
        mag_type = (props.get("magtype") or props.get("magType") or "ml").lower()

        # EMSC uses "flynn_region" for the region name
        flynn_region = props.get("flynn_region")
        place = flynn_region or props.get("place")

        updated_raw = props.get("lastupdate") or props.get("updated")
        updated_at = None
        if updated_raw:
            if isinstance(updated_raw, str):
                updated_at = datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
            else:
                updated_at = datetime.fromtimestamp(updated_raw / 1000, tz=timezone.utc)

        status_raw = (props.get("status") or "automatic").lower()
        if status_raw not in ("automatic", "reviewed", "deleted"):
            status_raw = "automatic"

        # Longitude normalization
        longitude = coords[0]
        if longitude > 180:
            longitude -= 360
        elif longitude < -180:
            longitude += 360

        return NormalizedEvent(
            event_uid=f"emsc:{source_event_id}",
            source="emsc",
            source_event_id=str(source_event_id),
            origin_time_utc=origin_time,
            latitude=coords[1],
            longitude=longitude,
            depth_km=coords[2],
            magnitude_value=mag,
            magnitude_type=mag_type,
            place=place,
            region=flynn_region,
            lat_error_km=_safe_float(props.get("horizontalError")),
            lon_error_km=_safe_float(props.get("horizontalError")),
            depth_error_km=_safe_float(props.get("depthError")),
            mag_error=_safe_float(props.get("magError")),
            time_error_sec=_safe_float(props.get("timeError")),
            status=status_raw,
            num_phases=_safe_int(props.get("nph")),
            azimuthal_gap=_safe_float(props.get("gap")),
            author=props.get("auth") or props.get("net"),
            url=props.get("url"),
            fetched_at=fetched_at,
            updated_at=updated_at,
            raw_payload="",
        )


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
