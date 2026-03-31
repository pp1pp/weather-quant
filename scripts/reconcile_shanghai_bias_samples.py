"""
Reconcile Shanghai bias samples into explicit provenance buckets.

This script keeps research seed rows, upgrades any matching legacy rows,
and imports verified Polymarket settlements into bias_calibration with the
best forecast provenance we currently have:

- source=seed: manual warm-start research samples
- source=verified_settlement_seed_forecast: verified settlement, seed forecast only
- source=verified_market: verified settlement + stored raw_forecasts replay

Usage:
    python3 scripts/reconcile_shanghai_bias_samples.py
"""

from __future__ import annotations

import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.seed_bias_data import SEED_DATA
from src.engine.bias_calibrator import BiasCalibrator, ensure_calibration_table
from src.utils.db import get_connection


@dataclass(frozen=True)
class VerifiedOutcome:
    settle_date: date
    resolved_temp: int
    market_url: str


VERIFIED_OUTCOMES = [
    VerifiedOutcome(
        settle_date=date(2026, 3, 20),
        resolved_temp=14,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-20-2026",
    ),
    VerifiedOutcome(
        settle_date=date(2026, 3, 21),
        resolved_temp=14,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-21-2026",
    ),
    VerifiedOutcome(
        settle_date=date(2026, 3, 22),
        resolved_temp=18,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-22-2026",
    ),
    VerifiedOutcome(
        settle_date=date(2026, 3, 23),
        resolved_temp=16,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-23-2026",
    ),
    VerifiedOutcome(
        settle_date=date(2026, 3, 24),
        resolved_temp=14,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-24-2026",
    ),
    VerifiedOutcome(
        settle_date=date(2026, 3, 25),
        resolved_temp=16,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-25-2026",
    ),
]


def load_raw_forecast_replays(conn: sqlite3.Connection) -> dict[str, tuple[float, list[str]]]:
    rows = conn.execute(
        """SELECT event_date, model_name, hourly_temps, fetched_at
           FROM raw_forecasts
           WHERE city = 'shanghai'
           ORDER BY event_date, fetched_at DESC"""
    ).fetchall()

    weights = {"ecmwf": 0.45, "gfs": 0.30, "icon": 0.25}
    grouped: dict[str, dict[str, float]] = {}
    for row in rows:
        event_date = row["event_date"]
        grouped.setdefault(event_date, {})
        model_name = row["model_name"]
        if model_name in grouped[event_date]:
            continue
        hourly = [
            float(x)
            for x in row["hourly_temps"].strip("[]").split(",")
            if x.strip()
        ]
        if not hourly:
            continue
        grouped[event_date][model_name] = max(hourly)

    means: dict[str, tuple[float, list[str]]] = {}
    for event_date, models in grouped.items():
        total_w = sum(weights.get(name, 0.0) for name in models)
        if total_w <= 0:
            continue
        mean = sum(weights[name] * temp for name, temp in models.items()) / total_w
        means[event_date] = (mean, sorted(models))
    return means


def delete_matching_legacy_seed_rows(conn: sqlite3.Connection):
    for settle_date, wu_temp, forecast_mean in SEED_DATA:
        conn.execute(
            """DELETE FROM bias_calibration
               WHERE city = 'shanghai'
                 AND settle_date = ?
                 AND source = 'legacy'
                 AND ABS(wu_temp - ?) < 0.001
                 AND ABS(forecast_mean - ?) < 0.001""",
            (settle_date.isoformat(), wu_temp, forecast_mean),
        )


def record_seed_samples(calibrator: BiasCalibrator):
    for settle_date, wu_temp, forecast_mean in SEED_DATA:
        calibrator.record_settlement(
            "shanghai",
            settle_date,
            wu_temp,
            forecast_mean,
            source="seed",
            notes="Manual warm-start seed sample; keep for research only.",
            is_reference=False,
        )


def main():
    ensure_calibration_table()
    calibrator = BiasCalibrator()
    conn = get_connection()
    try:
        delete_matching_legacy_seed_rows(conn)
        conn.commit()
    finally:
        conn.close()

    record_seed_samples(calibrator)

    conn = get_connection()
    try:
        raw_replays = load_raw_forecast_replays(conn)
    finally:
        conn.close()

    seed_map = {
        settle_date.isoformat(): forecast_mean
        for settle_date, _wu_temp, forecast_mean in SEED_DATA
    }

    trusted_rows = 0
    partial_rows = 0
    missing_raw: list[str] = []

    for item in VERIFIED_OUTCOMES:
        settle_key = item.settle_date.isoformat()
        replay = raw_replays.get(settle_key)
        if replay is not None:
            raw_mean, models = replay
            calibrator.record_settlement(
                "shanghai",
                item.settle_date,
                item.resolved_temp,
                raw_mean,
                source="verified_market",
                settlement_ref=item.market_url,
                notes=(
                    "Verified Polymarket settlement with stored raw_forecasts replay "
                    f"from models={','.join(models)}."
                ),
                is_reference=True,
            )
            trusted_rows += 1
            continue

        seed_forecast = seed_map.get(settle_key)
        if seed_forecast is None:
            missing_raw.append(settle_key)
            continue

        calibrator.record_settlement(
            "shanghai",
            item.settle_date,
            item.resolved_temp,
            seed_forecast,
            source="verified_settlement_seed_forecast",
            settlement_ref=item.market_url,
            notes=(
                "Resolved market outcome verified, but forecast_mean still comes "
                "from the seed backtest and is not trusted for live calibration."
            ),
            is_reference=False,
        )
        partial_rows += 1
        missing_raw.append(settle_key)

    seed_summary = calibrator.compute_bias("shanghai", sources=("seed",))
    trusted_summary = calibrator.compute_bias("shanghai", reference_only=True)

    print("Shanghai bias samples reconciled")
    print("=" * 72)
    print(f"Seed samples retained:          {seed_summary['n']}")
    print(f"Verified settlement only rows:  {partial_rows}")
    print(f"Trusted replay rows:            {trusted_rows}")
    print(f"Research seed bias:             {seed_summary['bias']:+.2f}°C (n={seed_summary['n']})")
    print(
        f"Trusted replay bias:            {trusted_summary['bias']:+.2f}°C "
        f"(n={trusted_summary['n']})"
    )
    if missing_raw:
        print(
            "Dates still missing stored raw_forecasts for trusted replay: "
            + ", ".join(sorted(missing_raw))
        )
    if trusted_rows:
        print("Trusted replay bucket hits are based on stored raw_forecasts snapshots.")
    print(
        "Current trusted recommendation: keep Shanghai bias neutral until at least "
        "3 trusted replay samples exist."
    )


if __name__ == "__main__":
    main()
