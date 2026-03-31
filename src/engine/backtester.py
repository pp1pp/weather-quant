"""
Offline Backtester with Brier Score evaluation.

Uses historical forecast + settlement data to evaluate model improvements
without waiting for live market feedback.

Metrics:
- Brier Score: mean((forecast_prob - actual_outcome)^2) per bucket
- Calibration: how well predicted probabilities match actual frequencies
- Resolution: ability to discriminate between outcomes
- Bucket Hit Rate: how often the highest-probability bucket is correct
"""

import json
import statistics
from datetime import date

from src.data.schemas import NormalizedForecast, WeatherFactors
from src.engine.multi_outcome_prob import MultiOutcomeProbEngine, DEFAULT_BUCKETS
from src.utils.db import get_connection
from src.utils.logger import logger


class Backtester:
    """Evaluate probability model quality using historical data."""

    def __init__(self, config: dict):
        self.config = config
        self.prob_engine = MultiOutcomeProbEngine(config)

    def run(self, city: str = "shanghai", max_days: int = 30) -> dict:
        """Run backtest on all available historical data.

        Returns comprehensive metrics including Brier Score,
        calibration, and hit rate.
        """
        conn = get_connection()
        try:
            # Get settlement actuals
            cal_rows = conn.execute(
                """SELECT settle_date, wu_temp, forecast_mean, residual, source, is_reference
                   FROM bias_calibration
                   WHERE city = ? AND is_reference = 1
                   ORDER BY settle_date DESC LIMIT ?""",
                (city, max_days),
            ).fetchall()

            if not cal_rows:
                return {"error": "No settlement data available", "n_days": 0}

            # Get raw forecasts for each date
            forecast_rows = conn.execute(
                """SELECT event_date, model_name, hourly_temps
                   FROM raw_forecasts WHERE city = ?
                   ORDER BY event_date DESC, fetched_at DESC""",
                (city,),
            ).fetchall()
        finally:
            conn.close()

        # Build model forecasts per date
        date_forecasts = {}
        seen = set()
        for row in forecast_rows:
            key = (row["event_date"], row["model_name"])
            if key in seen or row["model_name"] not in ("gfs", "icon"):
                continue
            seen.add(key)
            try:
                ts = row["hourly_temps"]
                temps = json.loads(ts) if ts.startswith("[") else [float(x) for x in ts.split(",") if x.strip()]
                if temps:
                    date_forecasts.setdefault(row["event_date"], {})[row["model_name"]] = max(temps)
            except Exception:
                pass

        # Run backtest day by day
        brier_scores = []
        bucket_hits = []
        daily_results = []
        calibration_bins = {}  # {bin_label: [predicted_prob, actual_hit]}

        for row in cal_rows:
            settle_date_str = row["settle_date"]
            actual_temp = row["wu_temp"]

            if settle_date_str not in date_forecasts:
                continue

            model_forecasts = date_forecasts[settle_date_str]
            if not model_forecasts:
                continue

            # Build a minimal NormalizedForecast for the prob engine
            forecast = NormalizedForecast(
                city=city,
                event_date=date.fromisoformat(settle_date_str),
                model_forecasts=model_forecasts,
                hours_to_settlement=24.0,  # simulate day-ahead
                updated_at=date.fromisoformat(settle_date_str),
            )

            # Get probability predictions
            fair_result = self.prob_engine.estimate(forecast, DEFAULT_BUCKETS)
            bucket_probs = fair_result.bucket_probs

            # Determine which bucket actually won
            actual_bucket = self._temp_to_bucket(actual_temp, DEFAULT_BUCKETS)

            # Compute Brier Score: sum of (p_i - o_i)^2 for all buckets
            brier = 0.0
            for bucket in DEFAULT_BUCKETS:
                label = bucket["label"]
                predicted = bucket_probs.get(label, 0.0)
                actual = 1.0 if label == actual_bucket else 0.0
                brier += (predicted - actual) ** 2

                # Track calibration
                bin_key = self._calibration_bin(predicted)
                calibration_bins.setdefault(bin_key, {"predicted": [], "actual": []})
                calibration_bins[bin_key]["predicted"].append(predicted)
                calibration_bins[bin_key]["actual"].append(actual)

            brier_scores.append(brier)

            # Hit rate: did the highest-prob bucket match actual?
            top_bucket = max(bucket_probs, key=bucket_probs.get)
            hit = top_bucket == actual_bucket
            bucket_hits.append(hit)

            daily_results.append({
                "date": settle_date_str,
                "actual_temp": actual_temp,
                "actual_bucket": actual_bucket,
                "predicted_mean": round(fair_result.weighted_mean_temp, 1),
                "top_bucket": top_bucket,
                "top_prob": round(bucket_probs.get(top_bucket, 0), 3),
                "brier": round(brier, 4),
                "hit": hit,
            })

        if not brier_scores:
            return {"error": "No dates with both forecasts and settlements", "n_days": 0}

        # Calibration: for each probability bin, compare mean predicted vs actual frequency
        calibration = {}
        for bin_key, data in sorted(calibration_bins.items()):
            if data["predicted"]:
                calibration[bin_key] = {
                    "mean_predicted": round(statistics.mean(data["predicted"]), 3),
                    "actual_frequency": round(statistics.mean(data["actual"]), 3),
                    "n": len(data["predicted"]),
                }

        return {
            "n_days": len(brier_scores),
            "mean_brier_score": round(statistics.mean(brier_scores), 4),
            "brier_std": round(statistics.stdev(brier_scores), 4) if len(brier_scores) > 1 else 0,
            "bucket_hit_rate": round(sum(bucket_hits) / len(bucket_hits), 3),
            "calibration": calibration,
            "daily": daily_results,
            "interpretation": self._interpret_brier(statistics.mean(brier_scores)),
        }

    @staticmethod
    def _temp_to_bucket(temp: int, buckets: list[dict]) -> str:
        """Map a temperature to its bucket label."""
        for b in buckets:
            low, high = b["low"], b["high"]
            if low == -999 and temp <= high:
                return b["label"]
            elif high == 999 and temp >= low:
                return b["label"]
            elif low <= temp <= high:
                return b["label"]
        return buckets[-1]["label"]  # fallback to highest

    @staticmethod
    def _calibration_bin(prob: float) -> str:
        """Assign a probability to a calibration bin."""
        if prob < 0.05:
            return "0-5%"
        elif prob < 0.15:
            return "5-15%"
        elif prob < 0.30:
            return "15-30%"
        elif prob < 0.50:
            return "30-50%"
        elif prob < 0.70:
            return "50-70%"
        else:
            return "70%+"

    @staticmethod
    def _interpret_brier(score: float) -> str:
        """Human-readable interpretation of Brier Score."""
        if score < 0.10:
            return "优秀 — 模型预测非常准确"
        elif score < 0.20:
            return "良好 — 模型有明显预测能力"
        elif score < 0.35:
            return "一般 — 模型有一定预测能力，但有改进空间"
        elif score < 0.50:
            return "较差 — 模型预测能力有限"
        else:
            return "很差 — 模型可能不如随机猜测"
