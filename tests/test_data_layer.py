"""
Test L1-L2 data layer:
1. Fetch real data from Open-Meteo API
2. Validate RawWeatherData structure
3. Normalize and validate NormalizedForecast
4. Check SQLite persistence
"""
import json
import sqlite3
from datetime import date, datetime, timedelta, timezone

import pytest

from src.data.fetcher_openmeteo import OpenMeteoFetcher
from src.data.normalizer import Normalizer
from src.data.schemas import NormalizedForecast, RawWeatherData
from src.utils.db import init_db
from tests.helpers import skip_if_openmeteo_unavailable


@pytest.fixture(scope="module")
def db():
    conn = init_db()
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def fetcher():
    config = {"retry_count": 3, "retry_delays": [2, 5, 10]}
    return OpenMeteoFetcher(config)


@pytest.fixture(scope="module")
def raw_data(fetcher, db):
    tomorrow = date.today() + timedelta(days=1)
    coords = (40.7828, -73.9653)
    try:
        return fetcher.fetch_forecast("new_york", tomorrow, coords)
    except Exception as exc:
        skip_if_openmeteo_unavailable(exc)


class TestOpenMeteoFetcher:
    def test_fetch_returns_list(self, raw_data):
        assert isinstance(raw_data, list)
        assert len(raw_data) >= 2, f"Expected at least 2 models, got {len(raw_data)}"

    def test_raw_data_structure(self, raw_data):
        for rd in raw_data:
            assert isinstance(rd, RawWeatherData)
            assert rd.city == "new_york"
            assert rd.source == "open-meteo"
            assert rd.model_name in ("ecmwf", "gfs", "icon", "best_match")

    def test_hourly_temps_length(self, raw_data):
        for rd in raw_data:
            assert len(rd.hourly_temps) >= 24, (
                f"Model {rd.model_name}: expected >=24 hourly temps, "
                f"got {len(rd.hourly_temps)}"
            )

    def test_temps_reasonable_range(self, raw_data):
        for rd in raw_data:
            for temp in rd.hourly_temps:
                assert -50 < temp < 60, (
                    f"Unreasonable temp {temp} from model {rd.model_name}"
                )

    def test_data_saved_to_db(self, raw_data, db):
        cursor = db.execute("SELECT COUNT(*) FROM raw_forecasts")
        count = cursor.fetchone()[0]
        assert count >= 2, f"Expected at least 2 rows in raw_forecasts, got {count}"


class TestNormalizer:
    def test_normalize_output(self, raw_data):
        normalizer = Normalizer()
        tomorrow = date.today() + timedelta(days=1)
        settlement = datetime(
            tomorrow.year, tomorrow.month, tomorrow.day, 5, 0, 0,
            tzinfo=timezone.utc
        ) + timedelta(days=1)

        forecast = normalizer.normalize(raw_data, tomorrow, settlement)

        assert isinstance(forecast, NormalizedForecast)
        assert forecast.city == "new_york"
        assert forecast.event_date == tomorrow

    def test_model_forecasts_keys(self, raw_data):
        normalizer = Normalizer()
        tomorrow = date.today() + timedelta(days=1)
        settlement = datetime(
            tomorrow.year, tomorrow.month, tomorrow.day, 5, 0, 0,
            tzinfo=timezone.utc
        ) + timedelta(days=1)

        forecast = normalizer.normalize(raw_data, tomorrow, settlement)

        assert len(forecast.model_forecasts) >= 1
        for key in forecast.model_forecasts:
            assert key in ("ecmwf", "gfs", "icon"), f"Unexpected model: {key}"

    def test_model_temps_reasonable(self, raw_data):
        normalizer = Normalizer()
        tomorrow = date.today() + timedelta(days=1)
        settlement = datetime(
            tomorrow.year, tomorrow.month, tomorrow.day, 5, 0, 0,
            tzinfo=timezone.utc
        ) + timedelta(days=1)

        forecast = normalizer.normalize(raw_data, tomorrow, settlement)

        for model, temp in forecast.model_forecasts.items():
            assert -50 < temp < 60, f"Unreasonable max temp {temp} from {model}"

    def test_hours_to_settlement_positive(self, raw_data):
        normalizer = Normalizer()
        tomorrow = date.today() + timedelta(days=1)
        settlement = datetime(
            tomorrow.year, tomorrow.month, tomorrow.day, 5, 0, 0,
            tzinfo=timezone.utc
        ) + timedelta(days=1)

        forecast = normalizer.normalize(raw_data, tomorrow, settlement)
        assert forecast.hours_to_settlement > 0

    def test_updated_at_timezone_aware(self, raw_data):
        normalizer = Normalizer()
        tomorrow = date.today() + timedelta(days=1)
        settlement = datetime(
            tomorrow.year, tomorrow.month, tomorrow.day, 5, 0, 0,
            tzinfo=timezone.utc
        ) + timedelta(days=1)

        forecast = normalizer.normalize(raw_data, tomorrow, settlement)
        assert forecast.updated_at.tzinfo is not None
