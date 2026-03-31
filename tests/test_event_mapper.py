import os
import tempfile
from datetime import date, datetime, time, timezone

import pytest
import yaml

from src.data.schemas import EventSpec, NormalizedForecast
from src.engine.event_mapper import ConfigError, EventMapper, MarketNotFoundError


@pytest.fixture
def mapper():
    """Create EventMapper with a test config (not the real markets.yaml which is bucket-based)."""
    test_config = {
        "markets": [{
            "id": "nyc-daily-high-30c",
            "city": "new_york",
            "event_type": "daily_high_temp",
            "settlement_time_utc": "2026-08-15T05:00:00Z",
            "settlement_rules": {
                "station": "Central Park",
                "coordinates": [40.7828, -73.9653],
                "metric": "max_temperature",
                "comparator": ">=",
                "threshold_celsius": 30.0,
                "time_window_local": ["00:00", "23:59"],
            },
        }]
    }
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        yaml.dump(test_config, f)
        f.flush()
        m = EventMapper(f.name)
    os.unlink(f.name)
    return m


@pytest.fixture
def sample_event_spec():
    return EventSpec(
        market_id="test-market",
        city="new_york",
        event_type="daily_high_temp",
        station="Central Park",
        coordinates=(40.7828, -73.9653),
        metric="max_temperature",
        comparator=">",
        threshold=30.0,
        time_window_start=time(0, 0),
        time_window_end=time(23, 59),
        settlement_time_utc=datetime(2026, 3, 26, 5, 0, 0, tzinfo=timezone.utc),
    )


def _make_forecast(model_forecasts, hours=24.0):
    return NormalizedForecast(
        city="new_york",
        event_date=date(2026, 3, 25),
        model_forecasts=model_forecasts,
        hours_to_settlement=hours,
        updated_at=datetime.now(timezone.utc),
    )


class TestEventMapper:
    def test_load_valid_config(self, mapper):
        events = mapper.get_all_events()
        assert len(events) >= 1
        assert isinstance(events[0], EventSpec)
        assert events[0].market_id == "nyc-daily-high-30c"

    def test_missing_field_raises_error(self):
        """Config missing 'comparator' should raise ConfigError on init."""
        bad_config = {
            "markets": [{
                "id": "bad-market",
                "city": "new_york",
                "event_type": "daily_high_temp",
                "settlement_time_utc": "2026-03-26T05:00:00Z",
                "settlement_rules": {
                    "station": "Central Park",
                    "coordinates": [40.7828, -73.9653],
                    "metric": "max_temperature",
                    # "comparator" is missing
                    "threshold_celsius": 30.0,
                    "time_window_local": ["00:00", "23:59"],
                },
            }]
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(bad_config, f)
            f.flush()
            with pytest.raises(ConfigError, match="comparator"):
                EventMapper(f.name)
            os.unlink(f.name)

    def test_evaluate_above_threshold(self, sample_event_spec):
        """Weighted avg = 0.45*32 + 0.3*31 + 0.25*31.5 = 31.575 > 30 → True"""
        forecast = _make_forecast({"ecmwf": 32.0, "gfs": 31.0, "icon": 31.5})
        mapper = EventMapper("config/markets.yaml")
        assert mapper.evaluate(forecast, sample_event_spec) is True

    def test_evaluate_below_threshold(self, sample_event_spec):
        """Weighted avg = 0.45*28 + 0.3*29 + 0.25*28.5 = 28.425 > 30 → False"""
        forecast = _make_forecast({"ecmwf": 28.0, "gfs": 29.0, "icon": 28.5})
        mapper = EventMapper("config/markets.yaml")
        assert mapper.evaluate(forecast, sample_event_spec) is False

    def test_evaluate_exact_threshold_strict_greater(self):
        """Weighted avg == 30.0, comparator '>' → False (strict)"""
        spec = EventSpec(
            market_id="test",
            city="new_york",
            event_type="daily_high_temp",
            station="Central Park",
            coordinates=(40.7828, -73.9653),
            metric="max_temperature",
            comparator=">",
            threshold=30.0,
            time_window_start=time(0, 0),
            time_window_end=time(23, 59),
            settlement_time_utc=datetime(2026, 3, 26, 5, 0, 0, tzinfo=timezone.utc),
        )
        # ecmwf=30, gfs=30, icon=30 → weighted avg = 30.0
        forecast = _make_forecast({"ecmwf": 30.0, "gfs": 30.0, "icon": 30.0})
        mapper = EventMapper("config/markets.yaml")
        assert mapper.evaluate(forecast, spec) is False

    def test_evaluate_exact_threshold_gte(self):
        """Weighted avg == 30.0, comparator '>=' → True"""
        spec = EventSpec(
            market_id="test",
            city="new_york",
            event_type="daily_high_temp",
            station="Central Park",
            coordinates=(40.7828, -73.9653),
            metric="max_temperature",
            comparator=">=",
            threshold=30.0,
            time_window_start=time(0, 0),
            time_window_end=time(23, 59),
            settlement_time_utc=datetime(2026, 3, 26, 5, 0, 0, tzinfo=timezone.utc),
        )
        forecast = _make_forecast({"ecmwf": 30.0, "gfs": 30.0, "icon": 30.0})
        mapper = EventMapper("config/markets.yaml")
        assert mapper.evaluate(forecast, spec) is True

    def test_market_not_found(self, mapper):
        with pytest.raises(MarketNotFoundError):
            mapper.get_event("nonexistent-market-id")

    def test_comparator_less_than(self):
        """Test '<' comparator for future snow/rain markets."""
        spec = EventSpec(
            market_id="test-lt",
            city="new_york",
            event_type="daily_high_temp",
            station="Central Park",
            coordinates=(40.7828, -73.9653),
            metric="max_temperature",
            comparator="<",
            threshold=30.0,
            time_window_start=time(0, 0),
            time_window_end=time(23, 59),
            settlement_time_utc=datetime(2026, 3, 26, 5, 0, 0, tzinfo=timezone.utc),
        )
        forecast = _make_forecast({"ecmwf": 28.0, "gfs": 29.0, "icon": 28.5})
        mapper = EventMapper("config/markets.yaml")
        # 28.425 < 30 → True
        assert mapper.evaluate(forecast, spec) is True

    def test_comparator_less_than_above(self):
        """32.0 < 30 → False"""
        spec = EventSpec(
            market_id="test-lt",
            city="new_york",
            event_type="daily_high_temp",
            station="Central Park",
            coordinates=(40.7828, -73.9653),
            metric="max_temperature",
            comparator="<",
            threshold=30.0,
            time_window_start=time(0, 0),
            time_window_end=time(23, 59),
            settlement_time_utc=datetime(2026, 3, 26, 5, 0, 0, tzinfo=timezone.utc),
        )
        forecast = _make_forecast({"ecmwf": 32.0, "gfs": 31.0, "icon": 31.5})
        mapper = EventMapper("config/markets.yaml")
        assert mapper.evaluate(forecast, spec) is False
