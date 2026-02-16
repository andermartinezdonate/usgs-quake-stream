"""Abstract base parser with validation logic."""

from __future__ import annotations

import abc
from datetime import datetime, timezone

from quake_stream.models_v2 import NormalizedEvent


class ValidationError(Exception):
    """Raised when an event fails validation."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__(f"Validation failed: {'; '.join(errors)}")


class EventParser(abc.ABC):
    """Abstract parser that converts raw data → list of NormalizedEvent."""

    @abc.abstractmethod
    def parse(self, raw_payload: str, fetched_at: datetime) -> list[NormalizedEvent]:
        """Parse raw API response into normalized events.

        Args:
            raw_payload: The raw response body (text).
            fetched_at: When the data was fetched.

        Returns:
            List of NormalizedEvent instances.
        """

    @staticmethod
    def validate(event: NormalizedEvent) -> list[str]:
        """Validate a NormalizedEvent. Returns list of error messages (empty = valid)."""
        errors: list[str] = []

        # Latitude: [-90, 90]
        if not -90 <= event.latitude <= 90:
            errors.append(f"latitude {event.latitude} out of range [-90, 90]")

        # Longitude: [-180, 180]
        if not -180 <= event.longitude <= 180:
            errors.append(f"longitude {event.longitude} out of range [-180, 180]")

        # Depth: >= 0 (some events can be negative/shallow, but cap at -10)
        if event.depth_km < -10:
            errors.append(f"depth_km {event.depth_km} unreasonably negative")

        if event.depth_km > 800:
            errors.append(f"depth_km {event.depth_km} exceeds 800 km")

        # Magnitude: typically -2 to 10
        if not -2.0 <= event.magnitude_value <= 10.0:
            errors.append(f"magnitude_value {event.magnitude_value} out of range [-2, 10]")

        # Time: not in the future (with 1-hour tolerance)
        now = datetime.now(timezone.utc)
        if event.origin_time_utc.tzinfo is None:
            errors.append("origin_time_utc is not timezone-aware")
        elif event.origin_time_utc > now.replace(hour=now.hour + 1 if now.hour < 23 else 23):
            errors.append(f"origin_time_utc {event.origin_time_utc} is in the future")

        # Status must be one of the allowed values
        if event.status not in ("automatic", "reviewed", "deleted"):
            errors.append(f"status '{event.status}' not in (automatic, reviewed, deleted)")

        # Required string fields
        if not event.event_uid:
            errors.append("event_uid is empty")
        if not event.source:
            errors.append("source is empty")
        if not event.source_event_id:
            errors.append("source_event_id is empty")

        return errors
