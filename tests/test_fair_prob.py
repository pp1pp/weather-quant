from datetime import date, datetime, time, timezone

import pytest

from src.data.schemas import EventSpec, NormalizedForecast
from src.engine.fair_prob import FairProbEngine

CONFIG = {
    "probability": {
        "uncertainty_std": 1.5,
        "observation_influence_start_hours": 6,
        "max_observation_weight": 0.6,
        "prob_clamp_min": 0.02,
        "prob_clamp_max": 0.98,
    },
    "model_weights": {
        "ecmwf": 0.45,
        "gfs": 0.30,
        "icon": 0.25,
    },
}


def _make_spec(threshold=30.0, comparator=">"):
    return EventSpec(
        market_id="test",
        city="new_york",
        event_type="daily_high_temp",
        station="Central Park",
        coordinates=(40.7828, -73.9653),
        metric="max_temperature",
        comparator=comparator,
        threshold=threshold,
        time_window_start=time(0, 0),
        time_window_end=time(23, 59),
        settlement_time_utc=datetime(2026, 3, 26, 5, 0, 0, tzinfo=timezone.utc),
    )


def _make_forecast(models, hours=24.0, obs=None):
    return NormalizedForecast(
        city="new_york",
        event_date=date(2026, 3, 25),
        model_forecasts=models,
        latest_observation=obs,
        observation_time=datetime.now(timezone.utc) if obs else None,
        hours_to_settlement=hours,
        updated_at=datetime.now(timezone.utc),
    )


class TestFairProb:
    def test_temp_to_prob_above_threshold(self):
        """temp=32, threshold=30, std=1.5 → prob ~0.91"""
        p = FairProbEngine.temp_to_prob(32, 30, ">", 1.5)
        assert 0.88 < p < 0.94

    def test_temp_to_prob_below_threshold(self):
        """temp=28, threshold=30, std=1.5 → prob ~0.09"""
        p = FairProbEngine.temp_to_prob(28, 30, ">", 1.5)
        assert 0.06 < p < 0.12

    def test_temp_to_prob_at_threshold(self):
        """temp=30, threshold=30, std=1.5 → prob = 0.5"""
        p = FairProbEngine.temp_to_prob(30, 30, ">", 1.5)
        assert p == pytest.approx(0.5, abs=0.001)

    def test_temp_to_prob_less_than_comparator(self):
        """comparator='<', temp=28, threshold=30 → prob ~0.91"""
        p = FairProbEngine.temp_to_prob(28, 30, "<", 1.5)
        assert 0.88 < p < 0.94

    def test_multi_model_weighting(self):
        """ecmwf=32, gfs=28, icon=30 → different per-model probs, weighted avg"""
        engine = FairProbEngine(CONFIG)
        forecast = _make_forecast({"ecmwf": 32.0, "gfs": 28.0, "icon": 30.0})
        spec = _make_spec()
        result = engine.estimate(forecast, spec)

        # Each model's prob
        probs = result.breakdown["model_probs"]
        assert probs["ecmwf"] > probs["icon"] > probs["gfs"]
        # Final should be between extremes
        assert 0.3 < result.fair_prob < 0.8

    def test_missing_model_reweighting(self):
        """Only ecmwf and gfs (no icon) → weights renormalized."""
        engine = FairProbEngine(CONFIG)
        forecast = _make_forecast({"ecmwf": 32.0, "gfs": 28.0})
        spec = _make_spec()
        result = engine.estimate(forecast, spec)

        # Should still produce a valid probability
        assert 0.02 <= result.fair_prob <= 0.98
        assert len(result.breakdown["model_probs"]) == 2

    def test_observation_correction_applied(self):
        """hours_to_settlement=3 < 6 → observation correction applied."""
        engine = FairProbEngine(CONFIG)
        forecast = _make_forecast(
            {"ecmwf": 29.0, "gfs": 29.0, "icon": 29.0},
            hours=3.0,
            obs=32.0,  # Hot observation
        )
        spec = _make_spec()
        result = engine.estimate(forecast, spec)

        assert result.breakdown["obs_correction_applied"] is True
        assert result.breakdown["obs_weight"] > 0
        # Observation is hot (32), should pull probability up
        base_without_obs = engine.estimate(
            _make_forecast({"ecmwf": 29.0, "gfs": 29.0, "icon": 29.0}, hours=10.0),
            spec,
        )
        assert result.fair_prob > base_without_obs.fair_prob

    def test_observation_correction_not_applied(self):
        """hours_to_settlement=10 > 6 → no observation correction."""
        engine = FairProbEngine(CONFIG)
        forecast = _make_forecast(
            {"ecmwf": 29.0, "gfs": 29.0, "icon": 29.0},
            hours=10.0,
            obs=35.0,
        )
        spec = _make_spec()
        result = engine.estimate(forecast, spec)
        assert result.breakdown["obs_correction_applied"] is False

    def test_prob_clamping(self):
        """Extreme temps should not produce 0 or 1 probability."""
        engine = FairProbEngine(CONFIG)
        # Very hot → high prob but clamped to 0.98
        forecast_hot = _make_forecast({"ecmwf": 50.0, "gfs": 50.0, "icon": 50.0})
        result_hot = engine.estimate(forecast_hot, _make_spec())
        assert result_hot.fair_prob <= 0.98

        # Very cold → low prob but clamped to 0.02
        forecast_cold = _make_forecast({"ecmwf": 10.0, "gfs": 10.0, "icon": 10.0})
        result_cold = engine.estimate(forecast_cold, _make_spec())
        assert result_cold.fair_prob >= 0.02
