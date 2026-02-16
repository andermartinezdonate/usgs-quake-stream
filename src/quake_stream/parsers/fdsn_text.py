"""Parser for FDSN pipe-delimited text format (GFZ GEOFON, ISC, GeoNet, etc.)."""

from __future__ import annotations

from datetime import datetime, timezone

from quake_stream.models_v2 import NormalizedEvent
from quake_stream.parsers.base import EventParser

# FDSN text columns (pipe-delimited):
# EventID|Time|Latitude|Longitude|Depth/km|Author|Catalog|Contributor|ContributorID|
# MagType|Magnitude|MagAuthor|EventLocationName
_COL_EVENT_ID = 0
_COL_TIME = 1
_COL_LAT = 2
_COL_LON = 3
_COL_DEPTH = 4
_COL_AUTHOR = 5
# _COL_CATALOG = 6
# _COL_CONTRIBUTOR = 7
# _COL_CONTRIBUTOR_ID = 8
_COL_MAG_TYPE = 9
_COL_MAG = 10
# _COL_MAG_AUTHOR = 11
_COL_LOCATION = 12


class FDSNTextParser(EventParser):
    """Parse FDSN pipe-delimited text response → list of NormalizedEvent.

    Reusable for any FDSN-compliant service that supports format=text
    (GFZ GEOFON, ISC, GeoNet NZ, etc.).
    """

    def __init__(self, default_source: str = "gfz"):
        self.default_source = default_source

    def parse(self, raw_payload: str, fetched_at: datetime) -> list[NormalizedEvent]:
        lines = raw_payload.strip().splitlines()
        events: list[NormalizedEvent] = []

        for line in lines:
            # Skip header line and empty lines
            if not line.strip() or line.startswith("EventID") or line.startswith("#"):
                continue
            try:
                events.append(self._parse_line(line, fetched_at))
            except (IndexError, ValueError):
                continue

        return events

    def _parse_line(self, line: str, fetched_at: datetime) -> NormalizedEvent:
        cols = [c.strip() for c in line.split("|")]

        source_event_id = cols[_COL_EVENT_ID]

        # Time: ISO 8601 format from FDSN text
        time_str = cols[_COL_TIME].replace("Z", "+00:00")
        # Normalize fractional seconds to 6 digits for Python 3.9 compatibility
        if "." in time_str:
            base, rest = time_str.split(".", 1)
            # Separate fractional seconds from timezone suffix
            frac = ""
            tz_suffix = ""
            for i, ch in enumerate(rest):
                if ch in "+-Z" or rest[i:] == "+00:00":
                    frac = rest[:i]
                    tz_suffix = rest[i:]
                    break
            else:
                frac = rest
            frac = frac.ljust(6, "0")[:6]
            time_str = f"{base}.{frac}{tz_suffix}"
        origin_time = datetime.fromisoformat(time_str)
        if origin_time.tzinfo is None:
            origin_time = origin_time.replace(tzinfo=timezone.utc)

        latitude = float(cols[_COL_LAT])

        longitude = float(cols[_COL_LON])
        if longitude > 180:
            longitude -= 360
        elif longitude < -180:
            longitude += 360

        depth_km = float(cols[_COL_DEPTH]) if cols[_COL_DEPTH] else 0.0

        mag_type = (cols[_COL_MAG_TYPE] if len(cols) > _COL_MAG_TYPE and cols[_COL_MAG_TYPE] else "ml").lower()
        mag_value = float(cols[_COL_MAG]) if len(cols) > _COL_MAG and cols[_COL_MAG] else 0.0

        author = cols[_COL_AUTHOR] if len(cols) > _COL_AUTHOR and cols[_COL_AUTHOR] else None

        place = cols[_COL_LOCATION] if len(cols) > _COL_LOCATION and cols[_COL_LOCATION] else None

        return NormalizedEvent(
            event_uid=f"{self.default_source}:{source_event_id}",
            source=self.default_source,
            source_event_id=source_event_id,
            origin_time_utc=origin_time,
            latitude=latitude,
            longitude=longitude,
            depth_km=depth_km,
            magnitude_value=mag_value,
            magnitude_type=mag_type,
            place=place,
            region=place,
            status="automatic",
            author=author,
            fetched_at=fetched_at,
            raw_payload="",
        )
