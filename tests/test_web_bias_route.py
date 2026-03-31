from datetime import date

from fastapi.testclient import TestClient

import src.utils.db as db_module
from src.engine.bias_calibrator import BiasCalibrator, ensure_calibration_table
from src.web.app import create_app
from src.web.cache import cache


def test_bias_route_separates_trusted_and_research_samples(monkeypatch, tmp_path):
    db_path = tmp_path / "weather.db"
    monkeypatch.setattr(db_module, "DB_PATH", str(db_path))
    ensure_calibration_table()

    calibrator = BiasCalibrator()
    calibrator.record_settlement(
        "shanghai",
        date(2026, 3, 20),
        15,
        13.2,
        source="seed",
        is_reference=False,
    )
    calibrator.record_settlement(
        "shanghai",
        date(2026, 3, 21),
        14,
        14.8,
        source="verified_settlement_seed_forecast",
        is_reference=False,
    )
    calibrator.record_settlement(
        "shanghai",
        date(2026, 3, 25),
        16,
        15.6,
        source="verified_market",
        is_reference=True,
    )

    cache.clear()
    app = create_app({}, {"multi_outcome": {"bias_correction": {"shanghai": 0.0}}})
    client = TestClient(app)

    response = client.get("/api/bias")
    assert response.status_code == 200

    payload = response.json()
    assert payload["current_bias"] == 0.0
    assert payload["trusted_n_samples"] == 1
    assert payload["research_n_samples"] == 2
    assert payload["n_samples"] == 1
    assert payload["source_counts"]["seed"] == 1
    assert payload["source_counts"]["verified_market"] == 1
    assert payload["history"][-1]["is_reference"] is True
