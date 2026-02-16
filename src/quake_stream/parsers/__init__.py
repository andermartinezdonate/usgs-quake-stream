"""Parsers for converting raw earthquake data to NormalizedEvent."""

from quake_stream.parsers.usgs_geojson import USGSGeoJSONParser
from quake_stream.parsers.emsc_geojson import EMSCGeoJSONParser
from quake_stream.parsers.fdsn_text import FDSNTextParser

PARSER_MAP = {
    "usgs": USGSGeoJSONParser(),
    "emsc": EMSCGeoJSONParser(),
    "gfz": FDSNTextParser(default_source="gfz"),
}

__all__ = ["PARSER_MAP", "USGSGeoJSONParser", "EMSCGeoJSONParser", "FDSNTextParser"]
