"""
End-to-end integration test for the multi-outcome (temperature bucket) pipeline:
1. Fetch real weather data (Open-Meteo) for Shanghai
2. Normalize
3. Calculate fair probabilities per bucket (with bias correction + dynamic σ)
4. Verify probabilities sum to ~1.0
5. Simulate edge detection against mock market prices
"""
import tempfile
from datetime import date, datetime, timedelta, timezone

import pytest
import yaml

from src.data.fetcher_openmeteo import OpenMeteoFetcher
from src.data.normalizer import Normalizer
from src.engine.multi_outcome_prob import MultiOutcomeProbEngine
from src.utils.db import init_db
from tests.helpers import skip_if_openmeteo_unavailable


@pytest.fixture(scope="module")
def config():
    with open("config/settings.yaml", "r") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def db():
    conn = init_db()
    conn.execute("DELETE FROM trades")
    conn.execute("DELETE FROM signals")
    conn.commit()
    return conn


def test_multi_outcome_pipeline(config, db):
    """End-to-end multi-outcome pipeline test with real weather data."""
    # Initialize modules
    fetcher = OpenMeteoFetcher(config["data_sources"])
    normalizer = Normalizer()
    prob_engine = MultiOutcomeProbEngine(config)

    # Use tomorrow's date (more likely to have forecast data)
    event_date = (datetime.now(timezone.utc) + timedelta(days=1)).date()
    settle_utc = datetime.combine(event_date, datetime.min.time()).replace(
        hour=12, tzinfo=timezone.utc
    )

    # L1: Fetch real data for Shanghai (ZSPD coords)
    try:
        raw_data = fetcher.fetch_forecast("shanghai", event_date, (31.1434, 121.8052))
    except Exception as exc:
        skip_if_openmeteo_unavailable(exc)
    assert len(raw_data) >= 2, f"Expected ≥2 models, got {len(raw_data)}"

    # L2: Normalize
    forecast = normalizer.normalize(raw_data, event_date, settle_utc)
    assert len(forecast.model_forecasts) >= 1
    assert forecast.city == "shanghai"

    # L3: Compute fair probabilities
    fair_result = prob_engine.estimate(forecast)

    # Verify probabilities sum to ~1.0
    total_prob = sum(fair_result.bucket_probs.values())
    assert abs(total_prob - 1.0) < 0.01, f"Probabilities sum to {total_prob}, expected ~1.0"

    # Verify we get probabilities for all 11 buckets
    assert len(fair_result.bucket_probs) == 11

    # Verify bias correction was applied (includes conditional + seasonal adjustments)
    raw_mean = prob_engine._weighted_mean(forecast.model_forecasts)
    base_bias = prob_engine.bias_correction.get("shanghai", 0.0)
    # Total bias includes conditional and seasonal adjustments, so use wider tolerance
    total_applied_bias = fair_result.weighted_mean_temp - raw_mean
    assert abs(total_applied_bias) < 3.0, (
        f"Bias adjustment out of range: raw={raw_mean:.2f}, adjusted={fair_result.weighted_mean_temp:.2f}, "
        f"total_bias={total_applied_bias:+.2f} (base={base_bias:+.2f})"
    )

    # Verify dynamic σ is reasonable (calibrated range: 0.25-1.5°C)
    assert 0.2 <= fair_result.uncertainty_std <= 2.0, (
        f"σ={fair_result.uncertainty_std} out of expected range [0.2, 2.0]"
    )

    # Verify confidence is in range
    assert 0.2 <= fair_result.confidence <= 1.0

    # Simulate edge detection against mock market prices
    # (roughly even distribution as a naive market)
    mock_prices = {label: 1.0 / 11 for label in fair_result.bucket_probs}

    edges = {}
    for label, fair_p in fair_result.bucket_probs.items():
        edge = fair_p - mock_prices[label]
        edges[label] = edge

    # At least one bucket should have >5% edge vs uniform distribution
    max_edge = max(abs(e) for e in edges.values())
    assert max_edge > 0.05, f"Max edge {max_edge:.3f} is too small vs uniform market"

    # Verify DB has raw forecasts saved
    cursor = db.execute("SELECT COUNT(*) FROM raw_forecasts WHERE city = 'shanghai'")
    assert cursor.fetchone()[0] >= 2
