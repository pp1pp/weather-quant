"""
Validate the local Shanghai backtest assumptions against verified Polymarket outcomes.

This script compares three layers of data:
1. Verified resolved outcomes from Polymarket event pages
2. Local bias_calibration entries (used for bias estimation)
3. Raw forecast snapshots stored in SQLite (when available)

It also computes walk-forward metrics so we can separate
in-sample calibration from live-usable performance.
"""

from __future__ import annotations

import math
import os
import sqlite3
import sys
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.engine.bias_calibrator import ensure_calibration_table


DB_PATH = "data/weather.db"
REFERENCE_SOURCES = ("verified_market", "live_replay")
SEEDLIKE_SOURCES = ("seed", "legacy")
SOURCE_PRIORITY = {
    "verified_market": 0,
    "live_replay": 1,
    "verified_settlement_seed_forecast": 2,
    "seed": 3,
    "legacy": 4,
}


@dataclass(frozen=True)
class VerifiedOutcome:
    date: str
    resolved_temp: int
    market_url: str


VERIFIED_OUTCOMES = [
    VerifiedOutcome(
        date="2026-03-20",
        resolved_temp=14,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-20-2026",
    ),
    VerifiedOutcome(
        date="2026-03-21",
        resolved_temp=14,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-21-2026",
    ),
    VerifiedOutcome(
        date="2026-03-22",
        resolved_temp=18,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-22-2026",
    ),
    VerifiedOutcome(
        date="2026-03-23",
        resolved_temp=16,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-23-2026",
    ),
    VerifiedOutcome(
        date="2026-03-24",
        resolved_temp=14,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-24-2026",
    ),
    VerifiedOutcome(
        date="2026-03-25",
        resolved_temp=16,
        market_url="https://polymarket.com/event/highest-temperature-in-shanghai-on-march-25-2026",
    ),
]


def round_half_up(value: float) -> int:
    return math.floor(value + 0.5)


def load_bias_rows(conn: sqlite3.Connection) -> dict[str, list[sqlite3.Row]]:
    rows = conn.execute(
        """SELECT settle_date, wu_temp, forecast_mean, residual, recorded_at,
                  source, settlement_ref, notes, is_reference
           FROM bias_calibration
           WHERE city = 'shanghai'
           ORDER BY settle_date, is_reference DESC, recorded_at DESC"""
    ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(row["settle_date"], []).append(row)
    return grouped


def load_raw_means(conn: sqlite3.Connection) -> dict[str, float]:
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
        if model_name not in weights or model_name in grouped[event_date]:
            continue
        hourly = [float(x) for x in row["hourly_temps"].strip("[]").split(",") if x.strip()]
        if not hourly:
            continue
        grouped[event_date][model_name] = max(hourly)

    means: dict[str, float] = {}
    for event_date, models in grouped.items():
        total_w = sum(weights.get(name, 0.0) for name in models)
        if total_w <= 0:
            continue
        means[event_date] = sum(weights[name] * temp for name, temp in models.items()) / total_w
    return means


def mae(errors: list[float]) -> float:
    return sum(abs(x) for x in errors) / len(errors) if errors else 0.0


def rmse(errors: list[float]) -> float:
    return math.sqrt(sum(x * x for x in errors) / len(errors)) if errors else 0.0


def summarize(name: str, actuals: list[int], preds: list[float]) -> str:
    errors = [a - p for a, p in zip(actuals, preds)]
    hit_rate = sum(round_half_up(p) == a for a, p in zip(actuals, preds)) / len(actuals)
    return (
        f"{name}: bias={sum(errors) / len(errors):+.3f}, "
        f"MAE={mae(errors):.3f}, RMSE={rmse(errors):.3f}, "
        f"bucket_hit={hit_rate:.1%}"
    )


def pick_row(
    rows: list[sqlite3.Row],
    *,
    reference_only: bool = False,
    allowed_sources: tuple[str, ...] | None = None,
) -> sqlite3.Row | None:
    candidates = rows
    if reference_only:
        candidates = [row for row in candidates if row["is_reference"]]
    if allowed_sources is not None:
        candidates = [row for row in candidates if row["source"] in allowed_sources]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda row: (
            SOURCE_PRIORITY.get(row["source"], 99),
            -int(bool(row["is_reference"])),
            row["recorded_at"],
        ),
    )


def main():
    ensure_calibration_table()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    bias_rows = load_bias_rows(conn)
    raw_means = load_raw_means(conn)

    print("Shanghai Backtest Validation")
    print("=" * 80)
    print("Verified market outcomes:")
    for item in VERIFIED_OUTCOMES:
        print(f"  {item.date}: {item.resolved_temp}°C  {item.market_url}")

    print("\nDaily comparison:")
    print("  date        market  seed_wu/source        seed_fcst  ref_fcst  raw_fcst   flags")

    actuals: list[int] = []
    seeded_actuals: list[int] = []
    seeded_preds: list[float] = []
    walk_forward_actuals: list[int] = []
    walk_forward_preds: list[float] = []
    expanding_residuals: list[float] = []
    reference_actuals: list[int] = []
    reference_preds: list[float] = []

    for item in VERIFIED_OUTCOMES:
        rows = bias_rows.get(item.date, [])
        market_temp = item.resolved_temp
        seed_row = pick_row(rows, allowed_sources=SEEDLIKE_SOURCES)
        reference_row = pick_row(rows, reference_only=True)
        actuals.append(market_temp)

        local_wu = seed_row["wu_temp"] if seed_row else None
        seeded_fcst = seed_row["forecast_mean"] if seed_row else None
        ref_fcst = reference_row["forecast_mean"] if reference_row else None
        raw_fcst = raw_means.get(item.date)

        flags = []
        if seed_row is None:
            flags.append("missing_bias_row")
        elif int(round(local_wu)) != market_temp:
            flags.append("WU_MISMATCH")

        if seeded_fcst is None:
            flags.append("missing_seed_forecast")
        else:
            seeded_actuals.append(market_temp)
            seeded_preds.append(seeded_fcst)

        if raw_fcst is not None and seeded_fcst is not None and abs(raw_fcst - seeded_fcst) > 0.5:
            flags.append("RAW_SEED_DRIFT")

        if seeded_fcst is not None:
            prior_bias = sum(expanding_residuals) / len(expanding_residuals) if expanding_residuals else 0.0
            walk_forward_actuals.append(market_temp)
            walk_forward_preds.append(seeded_fcst + prior_bias)
            expanding_residuals.append(market_temp - seeded_fcst)

        if reference_row is not None:
            reference_actuals.append(market_temp)
            reference_preds.append(ref_fcst)

        seed_err = market_temp - seeded_fcst if seeded_fcst is not None else None
        seed_text = f"{seeded_fcst:6.2f}" if seeded_fcst is not None else "   n/a"
        ref_text = f"{ref_fcst:6.2f}" if ref_fcst is not None else "   n/a"
        raw_text = f"{raw_fcst:6.2f}" if raw_fcst is not None else "   n/a"
        if local_wu is None or seed_row is None:
            wu_text = "    n/a"
        else:
            wu_text = f"{local_wu:4.1f}/{seed_row['source']}"
        if reference_row and raw_fcst is not None and abs(ref_fcst - raw_fcst) > 0.5:
            flags.append("REF_RAW_DRIFT")
        if reference_row and seed_row and abs(ref_fcst - seeded_fcst) > 0.5:
            flags.append("REF_SEED_DRIFT")
        print(
            f"  {item.date}  {market_temp:>3}°C   {wu_text:<20} {seed_text}   {ref_text}   "
            f"{raw_text}  {','.join(flags) if flags else '-'}"
        )

    full_sample_bias = (
        sum(a - p for a, p in zip(seeded_actuals, seeded_preds)) / len(seeded_preds)
        if seeded_preds
        else 0.0
    )
    full_sample_preds = [p + full_sample_bias for p in seeded_preds]

    print("\nMetrics against verified Polymarket outcomes:")
    print(" ", summarize("Seeded raw means", seeded_actuals, seeded_preds))
    print(" ", summarize("Walk-forward bias", walk_forward_actuals, walk_forward_preds))
    print(" ", summarize("Full-sample bias (leaky reference)", seeded_actuals, full_sample_preds))
    if reference_preds:
        print(" ", summarize("Reference replay samples", reference_actuals, reference_preds))

    raw_dates = [item.date for item in VERIFIED_OUTCOMES if item.date in raw_means]
    if raw_dates:
        raw_actuals = [item.resolved_temp for item in VERIFIED_OUTCOMES if item.date in raw_means]
        raw_preds = [raw_means[item.date] for item in VERIFIED_OUTCOMES if item.date in raw_means]
        print(" ", summarize("Stored raw_forecasts means", raw_actuals, raw_preds))

    print("\nNotes:")
    print("  - Reference replay samples are trusted settlement + forecast pairs used for live bias updates.")
    print("  - WU_MISMATCH means the local bias_calibration entry disagrees with the resolved market.")
    print("  - RAW_SEED_DRIFT means the seeded forecast_mean disagrees with the stored raw forecast snapshot.")
    print("  - REF_SEED_DRIFT means a trusted replay differs materially from the seed sample for that date.")
    print("  - Full-sample bias is for reference only and is not tradable in live mode.")


if __name__ == "__main__":
    main()
