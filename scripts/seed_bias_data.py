"""
Seed the bias_calibration table with 6 Shanghai research samples (March 20-25, 2026).

These rows are intentionally marked as `source=seed` and `is_reference=0`.
They are useful for analysis, but they should not drive live bias updates
until we have verified settlement + forecast replay pairs.

Usage:
    python3 -m scripts.seed_bias_data
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import date

from src.engine.bias_calibrator import BiasCalibrator, ensure_calibration_table
from src.utils.logger import logger

# 6-day backtest data: (settle_date, wu_settlement_°C, open_meteo_forecast_mean_°C)
# Residual = WU - OpenMeteo = systematic bias we need to correct
SEED_DATA = [
    # date, WU settlement, raw Open-Meteo weighted mean (before bias correction)
    (date(2026, 3, 20), 15, 13.2),   # residual +1.8
    (date(2026, 3, 21), 16, 14.8),   # residual +1.2
    (date(2026, 3, 22), 18, 16.3),   # residual +1.7
    (date(2026, 3, 23), 14, 12.6),   # residual +1.4
    (date(2026, 3, 24), 17, 15.2),   # residual +1.8
    (date(2026, 3, 25), 16, 14.7),   # residual +1.3
]


def main():
    ensure_calibration_table()
    calibrator = BiasCalibrator()

    for settle_date, wu_temp, forecast_mean in SEED_DATA:
        calibrator.record_settlement(
            "shanghai",
            settle_date,
            wu_temp,
            forecast_mean,
            source="seed",
            notes="Manual warm-start seed sample; not trusted for live bias calibration.",
            is_reference=False,
        )
        logger.info(
            f"Seeded: {settle_date} WU={wu_temp}°C forecast={forecast_mean:.1f}°C "
            f"residual={wu_temp - forecast_mean:+.1f}°C"
        )

    # Verify
    result = calibrator.compute_bias("shanghai", sources=("seed",))
    print(f"\nResearch bias result after seeding:")
    print(f"  Bias: {result['bias']:+.2f}°C")
    print(f"  Std:  {result['std']:.2f}°C")
    print(f"  N:    {result['n']} data points")
    print(f"  Samples: {result['samples']}")


if __name__ == "__main__":
    main()
