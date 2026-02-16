"""Tests for multi-source earthquake ingestion components."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from quake_stream.geo import haversine_km
from quake_stream.models_v2 import NormalizedEvent, UnifiedEvent, RawEventEnvelope
from quake_stream.parsers.base import EventParser
from quake_stream.parsers.usgs_geojson import USGSGeoJSONParser
from quake_stream.parsers.emsc_geojson import EMSCGeoJSONParser
from quake_stream.parsers.fdsn_text import FDSNTextParser
from quake_stream.deduplicator import compute_match_score, cluster_events, EventRecord


# ── Haversine tests ──────────────────────────────────────────────────────


class TestHaversine:
    def test_same_point(self):
        assert haversine_km(0, 0, 0, 0) == 0.0

    def test_known_distance(self):
        # New York to London ≈ 5570 km
        dist = haversine_km(40.7128, -74.0060, 51.5074, -0.1278)
        assert 5550 < dist < 5590

    def test_antipodal(self):
        # North pole to south pole ≈ 20015 km (half circumference)
        dist = haversine_km(90, 0, -90, 0)
        assert 20000 < dist < 20100

    def test_equator_one_degree(self):
        # One degree of longitude at equator ≈ 111.32 km
        dist = haversine_km(0, 0, 0, 1)
        assert 110 < dist < 113


# ── Model tests ──────────────────────────────────────────────────────────


class TestNormalizedEvent:
    def test_json_roundtrip(self):
        event = NormalizedEvent(
            event_uid="usgs:us7000test",
            source="usgs",
            source_event_id="us7000test",
            origin_time_utc=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            latitude=35.5,
            longitude=-120.3,
            depth_km=10.0,
            magnitude_value=4.5,
            magnitude_type="mw",
            place="10 km NW of Somewhere, CA",
            status="reviewed",
            fetched_at=datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc),
        )
        json_str = event.to_json()
        restored = NormalizedEvent.from_json(json_str)
        assert restored.event_uid == event.event_uid
        assert restored.magnitude_value == event.magnitude_value
        assert restored.origin_time_utc == event.origin_time_utc

    def test_event_uid_format(self):
        event = NormalizedEvent(
            event_uid="emsc:20240115_0001",
            source="emsc",
            source_event_id="20240115_0001",
            origin_time_utc=datetime(2024, 1, 15, tzinfo=timezone.utc),
            latitude=0, longitude=0, depth_km=0,
            magnitude_value=3.0, magnitude_type="ml",
        )
        assert event.event_uid == "emsc:20240115_0001"


class TestRawEventEnvelope:
    def test_key_format(self):
        env = RawEventEnvelope(
            source="gfz",
            source_event_id="gfz2024abc",
            format="fdsn_text",
            raw_payload="test",
        )
        assert env.key == "gfz:gfz2024abc"

    def test_json_roundtrip(self):
        env = RawEventEnvelope(
            source="usgs",
            source_event_id="us123",
            format="geojson",
            raw_payload='{"test": true}',
            fetched_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        restored = RawEventEnvelope.from_json(env.to_json())
        assert restored.source == "usgs"
        assert restored.source_event_id == "us123"


# ── Parser tests ─────────────────────────────────────────────────────────


class TestUSGSParser:
    SAMPLE_GEOJSON = json.dumps({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "id": "us7000test",
            "properties": {
                "mag": 5.2,
                "place": "10 km NW of Testville, CA",
                "time": 1705312800000,  # 2024-01-15 12:00:00 UTC
                "updated": 1705316400000,
                "magType": "Mw",
                "status": "reviewed",
                "net": "us",
                "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000test",
                "horizontalError": 1.5,
                "depthError": 2.0,
                "magError": 0.1,
                "timeError": 0.5,
                "nph": 42,
                "gap": 30.0,
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-120.5, 35.8, 12.3],
            },
        }],
    })

    def test_parse(self):
        parser = USGSGeoJSONParser()
        events = parser.parse(self.SAMPLE_GEOJSON, datetime.now(timezone.utc))
        assert len(events) == 1
        e = events[0]
        assert e.source == "usgs"
        assert e.source_event_id == "us7000test"
        assert e.magnitude_value == 5.2
        assert e.magnitude_type == "mw"  # lowercase
        assert e.status == "reviewed"
        assert e.latitude == 35.8
        assert e.longitude == -120.5
        assert e.depth_km == 12.3

    def test_empty_features(self):
        parser = USGSGeoJSONParser()
        events = parser.parse('{"features": []}', datetime.now(timezone.utc))
        assert events == []


class TestEMSCParser:
    SAMPLE_GEOJSON = json.dumps({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "id": "20240115_0001",
            "properties": {
                "unid": "20240115_0001",
                "time": "2024-01-15T12:00:00Z",
                "mag": 5.1,
                "magtype": "mw",
                "flynn_region": "Central Italy",
                "status": "reviewed",
                "auth": "EMSC",
                "lastupdate": "2024-01-15T13:00:00Z",
            },
            "geometry": {
                "type": "Point",
                "coordinates": [13.5, 42.3, 8.5],
            },
        }],
    })

    def test_parse(self):
        parser = EMSCGeoJSONParser()
        events = parser.parse(self.SAMPLE_GEOJSON, datetime.now(timezone.utc))
        assert len(events) == 1
        e = events[0]
        assert e.source == "emsc"
        assert e.source_event_id == "20240115_0001"
        assert e.magnitude_value == 5.1
        assert e.region == "Central Italy"


class TestFDSNTextParser:
    SAMPLE_TEXT = (
        "EventID|Time|Latitude|Longitude|Depth/km|Author|Catalog|Contributor|ContributorID|"
        "MagType|Magnitude|MagAuthor|EventLocationName\n"
        "gfz2024abc|2024-01-15T12:00:00+00:00|35.8|-120.5|12.3|GFZ||GFZ||Mw|5.0|GFZ|Central California\n"
        "gfz2024def|2024-01-15T13:00:00+00:00|40.0|25.0|10.0|GFZ||GFZ||ML|3.5|GFZ|Aegean Sea\n"
    )

    def test_parse(self):
        parser = FDSNTextParser(default_source="gfz")
        events = parser.parse(self.SAMPLE_TEXT, datetime.now(timezone.utc))
        assert len(events) == 2
        assert events[0].source == "gfz"
        assert events[0].source_event_id == "gfz2024abc"
        assert events[0].magnitude_value == 5.0
        assert events[0].magnitude_type == "mw"
        assert events[1].place == "Aegean Sea"

    def test_empty_input(self):
        parser = FDSNTextParser()
        events = parser.parse("", datetime.now(timezone.utc))
        assert events == []

    def test_header_only(self):
        parser = FDSNTextParser()
        events = parser.parse(
            "EventID|Time|Latitude|Longitude|Depth/km|Author|Catalog|Contributor|ContributorID|"
            "MagType|Magnitude|MagAuthor|EventLocationName\n",
            datetime.now(timezone.utc),
        )
        assert events == []


# ── Validation tests ─────────────────────────────────────────────────────


class TestValidation:
    def _make_event(self, **overrides):
        defaults = dict(
            event_uid="usgs:test1",
            source="usgs",
            source_event_id="test1",
            origin_time_utc=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            latitude=35.0,
            longitude=-120.0,
            depth_km=10.0,
            magnitude_value=4.0,
            magnitude_type="mw",
        )
        defaults.update(overrides)
        return NormalizedEvent(**defaults)

    def test_valid_event(self):
        event = self._make_event()
        errors = EventParser.validate(event)
        assert errors == []

    def test_invalid_latitude(self):
        event = self._make_event(latitude=95.0)
        errors = EventParser.validate(event)
        assert any("latitude" in e for e in errors)

    def test_invalid_longitude(self):
        event = self._make_event(longitude=200.0)
        errors = EventParser.validate(event)
        assert any("longitude" in e for e in errors)

    def test_invalid_depth(self):
        event = self._make_event(depth_km=900.0)
        errors = EventParser.validate(event)
        assert any("depth_km" in e for e in errors)

    def test_invalid_magnitude(self):
        event = self._make_event(magnitude_value=11.0)
        errors = EventParser.validate(event)
        assert any("magnitude_value" in e for e in errors)

    def test_invalid_status(self):
        event = self._make_event(status="bogus")
        errors = EventParser.validate(event)
        assert any("status" in e for e in errors)


# ── Deduplication tests ──────────────────────────────────────────────────


def _make_record(uid="usgs:a", source="usgs", time_utc=None, lat=35.0, lon=-120.0,
                 depth=10.0, mag=5.0, status="automatic"):
    if time_utc is None:
        time_utc = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    return EventRecord(
        event_uid=uid, source=source, origin_time_utc=time_utc,
        latitude=lat, longitude=lon, depth_km=depth,
        magnitude_value=mag, magnitude_type="mw",
        place=None, region=None, status=status,
    )


class TestMatchScore:
    def test_identical_events(self):
        a = _make_record()
        b = _make_record(uid="emsc:a", source="emsc")
        score = compute_match_score(a, b)
        assert score == 1.0

    def test_time_too_far(self):
        a = _make_record()
        b = _make_record(
            uid="emsc:b", source="emsc",
            time_utc=datetime(2024, 1, 15, 12, 1, 0, tzinfo=timezone.utc),  # 60s apart
        )
        score = compute_match_score(a, b)
        assert score == 0.0

    def test_distance_too_far(self):
        a = _make_record()
        b = _make_record(uid="emsc:b", source="emsc", lat=36.5)  # ~167 km away
        score = compute_match_score(a, b)
        assert score == 0.0

    def test_magnitude_too_far(self):
        a = _make_record()
        b = _make_record(uid="emsc:b", source="emsc", mag=6.0)  # 1.0 diff
        score = compute_match_score(a, b)
        assert score == 0.0

    def test_borderline_match(self):
        a = _make_record()
        # 10s apart, ~11 km away, 0.2 mag diff → should match
        from datetime import timedelta
        b = _make_record(
            uid="emsc:b", source="emsc",
            time_utc=a.origin_time_utc + timedelta(seconds=10),
            lat=35.1, mag=5.2,
        )
        score = compute_match_score(a, b)
        assert score >= MATCH_SCORE_THRESHOLD


class TestClustering:
    def test_two_sources_same_event(self):
        a = _make_record(uid="usgs:eq1", source="usgs")
        b = _make_record(uid="emsc:eq1", source="emsc")
        clusters = cluster_events([a, b])
        assert len(clusters) == 1
        assert len(clusters[0].members) == 2

    def test_two_distinct_events(self):
        a = _make_record(uid="usgs:eq1", source="usgs", lat=35.0, lon=-120.0)
        b = _make_record(
            uid="usgs:eq2", source="usgs", lat=50.0, lon=10.0,
            time_utc=datetime(2024, 1, 15, 14, 0, 0, tzinfo=timezone.utc),
        )
        clusters = cluster_events([a, b])
        assert len(clusters) == 2

    def test_three_sources_one_event(self):
        from datetime import timedelta
        t = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        a = _make_record(uid="usgs:eq1", source="usgs", time_utc=t)
        b = _make_record(uid="emsc:eq1", source="emsc", time_utc=t + timedelta(seconds=5))
        c = _make_record(uid="gfz:eq1", source="gfz", time_utc=t + timedelta(seconds=8))
        clusters = cluster_events([a, b, c])
        assert len(clusters) == 1
        assert len(clusters[0].members) == 3


from quake_stream.deduplicator import MATCH_SCORE_THRESHOLD
