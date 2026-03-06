"""
Shared geocoding for scrapers: town, postal code, or location text → (lat, lng).
Uses Nominatim (OSM) with cache and 1 req/s rate limit. Optional dependency: geopy.
"""
from __future__ import annotations

import re
import time
from typing import Tuple

# In-memory cache: query -> (lat, lng); avoids repeated Nominatim calls for same city
_geo_cache: dict[str, tuple[float | None, float | None]] = {}
_last_geocode_time = 0.0
_MIN_DELAY = 1.1  # Nominatim policy: 1 request per second


def _normalize_query(text: str) -> str:
    """Normalize location string for cache key and geocoding (e.g. 'Saint-Vérand 38160' → '38160, France')."""
    if not text or not isinstance(text, str):
        return ""
    text = text.strip()
    if not text:
        return ""
    # Prefer postal code + France for French locations (more reliable)
    match = re.search(r"\b(\d{5})\b", text)
    if match:
        return f"{match.group(1)}, France"
    # Else use full text + France
    return f"{text}, France"


def geocode(query: str) -> Tuple[float | None, float | None]:
    """
    Return (latitude, longitude) for a place name or postal code. Returns (None, None) on failure or if geopy missing.
    Rate-limited and cached.
    """
    global _last_geocode_time
    q = _normalize_query(query)
    if not q:
        return (None, None)
    if q in _geo_cache:
        return _geo_cache[q]
    try:
        from geopy.geocoders import Nominatim
        from geopy.exc import GeocoderTimedOut, GeocoderServiceError
    except ImportError:
        return (None, None)
    now = time.monotonic()
    elapsed = now - _last_geocode_time
    if elapsed < _MIN_DELAY:
        time.sleep(_MIN_DELAY - elapsed)
    _last_geocode_time = time.monotonic()
    try:
        geolocator = Nominatim(user_agent="kiabi-second-hand-scraper/1.0")
        location = geolocator.geocode(q, timeout=10, exactly_one=True)
        if location and location.latitude is not None and location.longitude is not None:
            result = (float(location.latitude), float(location.longitude))
            _geo_cache[q] = result
            return result
    except (GeocoderTimedOut, GeocoderServiceError, Exception):
        pass
    _geo_cache[q] = (None, None)
    return (None, None)


def parse_lat_lng_from_leboncoin_location_string(loc_str: str) -> Tuple[float | None, float | None]:
    """Parse lat=... lng=... from Leboncoin Location repr (e.g. 'Location(..., lat=45.17, lng=5.33, ...)')."""
    if not loc_str or not isinstance(loc_str, str):
        return (None, None)
    m_lat = re.search(r"\blat=([0-9]+\.?[0-9]*)", loc_str)
    m_lng = re.search(r"\blng=([0-9]+\.?[0-9]*)", loc_str)
    try:
        lat = float(m_lat.group(1)) if m_lat else None
        lng = float(m_lng.group(1)) if m_lng else None
        return (lat, lng) if (lat is not None and lng is not None) else (None, None)
    except (ValueError, TypeError):
        return (None, None)
