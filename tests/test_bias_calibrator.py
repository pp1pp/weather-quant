import sqlite3
from datetime import date

import yaml

import src.utils.db as db_module
from src.engine.bias_calibrator import BiasCalibrator, ensure_calibration_table


def _use_temp_db(monkeypatch, tmp_path):
    db_path = tmp_path / "weather.db"
    monkeypatch.setattr(db_module, "DB_PATH", str(db_path))
    return db_path


def _write_config(tmp_path, bias=0.0):
    config_path = tmp_path / "settings.yaml"
    with config_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "multi_outcome": {
                    "bias_correction": {"shanghai": bias},
                    "data_source_bias_std": 0.5,
                }
            },
            handle,
            sort_keys=False,
            allow_unicode=True,
        )
    return config_path


def test_ensure_calibration_table_migrates_legacy_schema(monkeypatch, tmp_path):
    db_path = _use_temp_db(monkeypatch, tmp_path)

    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE bias_calibration (
               city TEXT NOT NULL,
               settle_date TEXT NOT NULL,
               wu_temp REAL NOT NULL,
               forecast_mean REAL NOT NULL,
               residual REAL NOT NULL,
               recorded_at TEXT NOT NULL,
               PRIMARY KEY (city, settle_date)
           )"""
    )
    conn.execute(
        """INSERT INTO bias_calibration
           (city, settle_date, wu_temp, forecast_mean, residual, recorded_at)
           VALUES ('shanghai', '2026-03-25', 16, 14.7, 1.3, '2026-03-26T00:00:00+00:00')"""
    )
    conn.commit()
    conn.close()

    ensure_calibration_table()

    conn = db_module.get_connection()
    try:
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(bias_calibration)")
        }
        assert {"id", "source", "settlement_ref", "notes", "is_reference"} <= columns

        row = conn.execute(
            "SELECT source, is_reference, notes FROM bias_calibration"
        ).fetchone()
        assert row["source"] == "legacy"
        assert row["is_reference"] == 0
        assert "legacy" in row["notes"].lower()
    finally:
        conn.close()


def test_update_config_ignores_seed_samples(monkeypatch, tmp_path):
    _use_temp_db(monkeypatch, tmp_path)
    ensure_calibration_table()
    config_path = _write_config(tmp_path, bias=0.0)
    calibrator = BiasCalibrator(config_path=str(config_path))

    for settle_date, actual, forecast in [
        (date(2026, 3, 20), 15, 13.5),
        (date(2026, 3, 21), 16, 14.8),
        (date(2026, 3, 22), 18, 16.6),
    ]:
        calibrator.record_settlement(
            "shanghai",
            settle_date,
            actual,
            forecast,
            source="seed",
            is_reference=False,
        )

    assert calibrator.compute_bias("shanghai")["n"] == 3
    assert calibrator.compute_bias("shanghai", reference_only=True)["n"] == 0
    assert not calibrator.update_config("shanghai")

    with config_path.open("r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    assert cfg["multi_outcome"]["bias_correction"]["shanghai"] == 0.0


def test_update_config_uses_reference_samples_only(monkeypatch, tmp_path):
    _use_temp_db(monkeypatch, tmp_path)
    ensure_calibration_table()
    config_path = _write_config(tmp_path, bias=0.0)
    calibrator = BiasCalibrator(config_path=str(config_path))

    for settle_date, actual, forecast in [
        (date(2026, 3, 20), 15, 13.0),
        (date(2026, 3, 21), 16, 14.0),
        (date(2026, 3, 22), 17, 15.0),
    ]:
        calibrator.record_settlement(
            "shanghai",
            settle_date,
            actual,
            forecast,
            source="seed",
            is_reference=False,
        )

    for settle_date, actual, forecast in [
        (date(2026, 3, 23), 16, 15.5),
        (date(2026, 3, 24), 15, 14.7),
        (date(2026, 3, 25), 16, 15.6),
    ]:
        calibrator.record_settlement(
            "shanghai",
            settle_date,
            actual,
            forecast,
            source="verified_market",
            settlement_ref="https://example.com",
            is_reference=True,
        )

    trusted = calibrator.compute_bias("shanghai", reference_only=True)
    assert trusted["n"] == 3
    assert trusted["bias"] == 0.4
    assert calibrator.update_config("shanghai")

    with config_path.open("r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)

    assert cfg["multi_outcome"]["bias_correction"]["shanghai"] == 0.4
    assert cfg["multi_outcome"]["data_source_bias_std"] == 0.5
