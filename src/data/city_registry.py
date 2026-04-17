"""
City Registry — centralized configuration for all supported weather markets.

Loads city metadata from config/cities.yaml and provides a unified interface
for all modules (fetchers, normalizer, probability engine, series tracker).

Replaces hardcoded CITY_COORDINATES, CITY_TIMEZONES, STATION_MAP, PEAK_TEMP_HOUR
scattered across multiple files.
"""

import os
from functools import lru_cache
from pathlib import Path

import yaml

from src.utils.logger import logger

# Default path relative to project root
_DEFAULT_CONFIG = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "config",
    "cities.yaml",
)


def _load_cities(config_path: str | None = None) -> dict[str, dict]:
    """Load cities from YAML config file."""
    path = config_path or _DEFAULT_CONFIG
    if not os.path.exists(path):
        logger.warning(f"cities.yaml not found at {path}, using built-in defaults")
        return _builtin_defaults()

    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}

    cities = data.get("cities", {})
    if not cities:
        logger.warning("No cities defined in cities.yaml, using built-in defaults")
        return _builtin_defaults()

    logger.info(f"Loaded {len(cities)} cities from {path}: {list(cities.keys())}")
    return cities


def _builtin_defaults() -> dict[str, dict]:
    """Minimal fallback if cities.yaml is missing."""
    return {
        "shanghai": {
            "name": "Shanghai",
            "slug_city": "shanghai",
            "station": "Shanghai Pudong International Airport",
            "icao": "ZSPD",
            "wmo": "58362",
            "wu_geocode": "31.14,121.81",
            "country": "cn",
            "coordinates": [31.1434, 121.8052],
            "timezone": "Asia/Shanghai",
            "peak_temp_hour": 15.0,
            "unit": "celsius",
            "models": "best_match,gfs_seamless,icon_seamless",
            "sea_breeze_dir": [45, 135],
        },
    }


# Module-level singleton — loaded once on first access
_cities: dict[str, dict] | None = None


def _ensure_loaded() -> dict[str, dict]:
    global _cities
    if _cities is None:
        _cities = _load_cities()
    return _cities


def reload(config_path: str | None = None):
    """Force reload cities config (e.g., after editing cities.yaml)."""
    global _cities
    _cities = _load_cities(config_path)


# ── Public API ──────────────────────────────────────────────────────────


def all_city_keys() -> list[str]:
    """Return enabled city keys (enabled: true by default)."""
    cities = _ensure_loaded()
    return [k for k, v in cities.items() if v.get("enabled", True)]


def get_city(key: str) -> dict | None:
    """Get full city config dict by key (e.g., 'shanghai')."""
    return _ensure_loaded().get(key)


def get_coordinates(key: str) -> tuple[float, float]:
    """Get (latitude, longitude) for a city."""
    city = _ensure_loaded().get(key)
    if city and "coordinates" in city:
        coords = city["coordinates"]
        return (coords[0], coords[1])
    raise KeyError(f"City '{key}' not found or has no coordinates")


def get_timezone(key: str) -> str:
    """Get timezone string (e.g., 'Asia/Shanghai')."""
    city = _ensure_loaded().get(key)
    if city:
        return city.get("timezone", "UTC")
    return "UTC"


def get_unit(key: str) -> str:
    """Get settlement unit: 'celsius' or 'fahrenheit'."""
    city = _ensure_loaded().get(key)
    if city:
        return city.get("unit", "celsius")
    return "celsius"


def get_peak_temp_hour(key: str) -> float:
    """Get peak temperature hour in local time."""
    city = _ensure_loaded().get(key)
    if city:
        return city.get("peak_temp_hour", 15.0)
    return 15.0


def get_station(key: str) -> dict:
    """Get WU station info dict with icao, wmo, wu_geocode, country."""
    city = _ensure_loaded().get(key)
    if not city:
        return {}
    return {
        "icao": city.get("icao", ""),
        "wmo": city.get("wmo", ""),
        "wu_geocode": city.get("wu_geocode", ""),
        "country": city.get("country", ""),
    }


def get_models(key: str) -> str:
    """Get Open-Meteo model list string for this city."""
    city = _ensure_loaded().get(key)
    if city:
        return city.get("models", "best_match,gfs_seamless,icon_seamless")
    return "best_match,gfs_seamless,icon_seamless"


def get_slug_city(key: str) -> str:
    """Get the slug city name used in Polymarket URLs (e.g., 'los-angeles')."""
    city = _ensure_loaded().get(key)
    if city:
        return city.get("slug_city", key)
    return key


def get_sea_breeze_dir(key: str) -> tuple[float, float]:
    """Get sea breeze wind direction range (start_deg, end_deg)."""
    city = _ensure_loaded().get(key)
    if city and "sea_breeze_dir" in city:
        d = city["sea_breeze_dir"]
        return (d[0], d[1])
    return (45, 135)  # Default: east wind


def get_resolution_url(key: str) -> str:
    """Get WU resolution URL for this city."""
    city = _ensure_loaded().get(key)
    if city:
        return city.get("resolution_url", "")
    return ""


# ── Temperature conversion utilities ────────────────────────────────────


def f_to_c(temp_f: float) -> float:
    """Convert Fahrenheit to Celsius."""
    return (temp_f - 32) * 5 / 9


def c_to_f(temp_c: float) -> float:
    """Convert Celsius to Fahrenheit."""
    return temp_c * 9 / 5 + 32


def is_fahrenheit(city_key: str) -> bool:
    """Check if a city uses Fahrenheit for settlement."""
    return get_unit(city_key) == "fahrenheit"
