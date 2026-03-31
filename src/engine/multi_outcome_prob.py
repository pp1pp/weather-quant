"""
Multi-Outcome Probability Engine for Temperature Bucket Markets.

Enhanced with:
- Conditional bias correction (wind, cloud, precipitation, pressure)
- Student-t distribution for heavier tails
- Ensemble-based probability estimation
- Proper σ calculation (stdev instead of spread/2)
- ECMWF removed (not available for Shanghai), GFS+ICON only
"""

import json
import math
import statistics
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from scipy.stats import norm, t as student_t

from src.data import city_registry
from src.data.schemas import MultiOutcomeFairResult, NormalizedForecast, WeatherFactors
from src.utils.db import get_connection
from src.utils.logger import logger


# Bucket boundary definitions matching Polymarket Shanghai market
DEFAULT_BUCKETS = [
    {"label": "11°C or below", "low": -999, "high": 11},
    {"label": "12°C", "low": 12, "high": 12},
    {"label": "13°C", "low": 13, "high": 13},
    {"label": "14°C", "low": 14, "high": 14},
    {"label": "15°C", "low": 15, "high": 15},
    {"label": "16°C", "low": 16, "high": 16},
    {"label": "17°C", "low": 17, "high": 17},
    {"label": "18°C", "low": 18, "high": 18},
    {"label": "19°C", "low": 19, "high": 19},
    {"label": "20°C", "low": 20, "high": 20},
    {"label": "21°C or higher", "low": 21, "high": 999},
]


class MultiOutcomeProbEngine:
    """
    Fair probability engine for multi-outcome temperature bucket markets.

    Key improvements over v1:
    1. Conditional bias: adjusts based on weather conditions (wind, cloud, precip)
    2. Student-t distribution: heavier tails for rare outcomes (df=6)
    3. Ensemble integration: uses 69-member ensemble spread for calibrated σ
    4. Proper model spread σ: uses stdev, not spread/2
    5. Model weights: GFS 55% + ICON 45% (ECMWF unavailable for Shanghai)
    6. Time-of-day observation boost: after daily max peak time, WU obs ≈ final settlement
    7. WU lower-bound floor: when obs is likely final, probability mass below obs → 0
    """

    # Default Student-t degrees of freedom (overridden by config)
    # FIXED: df=6 has 9.2% tail mass beyond 2σ, but observed data has 0% outliers.
    # df=30 approximates Gaussian (4.6%) which matches our light-tailed empirical data.
    STUDENT_T_DF = 30

    # City timezones and peak hours now loaded from city_registry

    def __init__(self, config: dict):
        mo_cfg = config.get("multi_outcome", {})
        self.base_uncertainty_std = mo_cfg.get("bucket_uncertainty_std", 1.5)
        self.obs_start_hours = mo_cfg.get("observation_blend_start_hours", 6)
        self.max_obs_weight = mo_cfg.get("max_observation_weight", 0.6)
        self.prob_clamp_min = mo_cfg.get("prob_clamp_min", 0.002)
        self.prob_clamp_max = mo_cfg.get("prob_clamp_max", 0.998)
        self.data_source_bias = mo_cfg.get("data_source_bias_std", 0.5)
        self.bias_correction = mo_cfg.get("bias_correction", {})
        # Updated default weights: ECMWF unavailable for Shanghai
        self.model_weights = config.get("model_weights", {
            "gfs": 0.55, "icon": 0.45,
        })
        self.use_adaptive_weights = config.get("multi_outcome", {}).get("adaptive_weights", True)
        self.STUDENT_T_DF = mo_cfg.get("student_t_df", self.STUDENT_T_DF)
        self._adaptive_cache = None
        self._adaptive_cache_time = 0
        # Per-model bias learned from data (populated by _learn_per_model_bias)
        self._per_model_bias: dict[str, float] = {}
        self._per_model_bias_time = 0

        # Conditional bias adjustment coefficients
        # Start with config defaults, then override with data-learned values
        self.conditional_bias_cfg = mo_cfg.get("conditional_bias", {
            "sea_breeze_adj": -0.5,
            "high_cloud_adj": -0.3,
            "precipitation_adj": -0.8,
            "high_pressure_adj": 0.3,
            "low_humidity_adj": 0.2,
        })
        self._learned_bias_cache = None
        self._learned_bias_cache_time = 0

    def estimate(
        self,
        forecast: NormalizedForecast,
        buckets: list[dict] | None = None,
    ) -> MultiOutcomeFairResult:
        """Estimate fair probabilities for all temperature buckets."""
        if buckets is None:
            buckets = DEFAULT_BUCKETS

        city = getattr(forecast, "city", "")
        active_weights = self.model_weights
        if self.use_adaptive_weights:
            adaptive = self._get_adaptive_weights(city)
            if adaptive:
                active_weights = adaptive

        # Dynamic model downweight: when models strongly disagree,
        # penalize the model with worse recent MAE
        active_weights = self._apply_divergence_downweight(
            forecast.model_forecasts, active_weights, city
        )

        mean_temp = self._weighted_mean(forecast.model_forecasts, weights_override=active_weights, city=city)

        # Update conditional bias with learned values if available
        import time as _time
        if _time.time() - self._learned_bias_cache_time > 3600:
            learned = self._learn_conditional_bias(city)
            if learned:
                # Blend 60% learned + 40% config defaults
                for k, v in learned.items():
                    default = self.conditional_bias_cfg.get(k, 0)
                    self.conditional_bias_cfg[k] = round(0.6 * v + 0.4 * default, 2)
                self._learned_bias_cache = learned
            self._learned_bias_cache_time = _time.time()

        # Conditional bias correction
        base_bias = self.bias_correction.get(city, 0.0)

        # NOTE: Per-model debiasing (in _weighted_mean) already removes the major
        # systematic component. Stratified bias and seasonal bias were learned from
        # RAW residuals that included per-model offset. After per-model correction,
        # the residual bias is near zero (~+0.03°C empirically), so these additional
        # corrections now risk double-counting.
        #
        # DISABLED: stratified_bias, seasonal_bias — until we accumulate post-debiasing
        # calibration data to re-learn these adjustments.
        # temp_stratified_adj = self._stratified_bias(mean_temp, city)
        # base_bias += temp_stratified_adj

        conditional_adj = self._compute_conditional_bias(forecast)
        total_bias = base_bias + conditional_adj

        # event_date = getattr(forecast, "event_date", None)
        # seasonal_adj = self._get_seasonal_bias(city, event_date)
        # total_bias += seasonal_adj

        if total_bias != 0.0:
            logger.info(
                f"Bias correction for {city}: base={base_bias:+.2f}, "
                f"conditional={conditional_adj:+.2f}, "
                f"total={total_bias:+.2f}°C ({mean_temp:.1f} → {mean_temp + total_bias:.1f})"
            )
            mean_temp += total_bias

        # Dynamic uncertainty - use ensemble if available, else model spread
        if forecast.ensemble_maxes and len(forecast.ensemble_maxes) >= 10:
            effective_std = self._ensemble_sigma(forecast.ensemble_maxes)
            logger.info(f"Using ensemble σ: {effective_std:.2f}°C from {len(forecast.ensemble_maxes)} members")
        else:
            effective_std = self._dynamic_sigma(forecast.model_forecasts)

        # Observation blending (near settlement)
        #
        # KEY INSIGHT from Polymarket analysis:
        #   Polymarket's final probabilities closely match WU actuals because traders
        #   observe WU data in real-time. Our model must do the same.
        #
        # WU observation = RUNNING DAILY MAX (monotone non-decreasing).
        # After the daily peak temperature time (~15:00 Shanghai), the running max
        # IS essentially the final settlement value. Model forecasts become irrelevant.
        #
        # THREE REGIMES:
        #   A. Before peak (morning): WU is a floor, model drives upside estimate
        #   B. After peak, before settlement: WU ≈ final value, model almost irrelevant
        #   C. Far from settlement (>6h): Pure model, no observation
        wu_obs = forecast.latest_observation
        obs_weight = 0.0
        if wu_obs is not None and forecast.hours_to_settlement < self.obs_start_hours:
            h = forecast.hours_to_settlement
            time_boost = self._time_of_day_obs_boost(forecast.city)
            past_peak = time_boost >= 0.10  # after ~15:30 local (peak + 0.5h)
            near_peak = time_boost >= 0.01  # after ~14:00 local

            model_mean_pre_blend = mean_temp

            if past_peak:
                # === REGIME B: PAST PEAK — WU IS THE ANSWER ===
                # After peak temp time, temperature only drops.
                # WU running max = final settlement with very high certainty.
                # Model forecast is IRRELEVANT — use WU directly.
                #
                # Polymarket converges to ~99% on WU bucket after peak.
                # We must do the same to avoid trading against reality.
                mean_temp = wu_obs
                # Extremely tight σ: WU integer rounding is the only uncertainty.
                # σ=0.25 → P(wu_obs bucket) ≈ 80-90%, matching Polymarket behavior.
                effective_std = max(0.25, 0.20 + h * 0.03)  # 0.25-0.40°C
                obs_weight = 0.95

                logger.info(
                    f"WU obs (PAST PEAK): wu_obs={wu_obs:.1f}°C → mean={mean_temp:.1f}°C, "
                    f"σ={effective_std:.2f} (model={model_mean_pre_blend:.1f} IGNORED)"
                )

            elif near_peak:
                # === REGIME A2: NEAR PEAK — WU dominant, small model upside ===
                # Peak is happening now. WU running max is close to final.
                # Model can add small upside if it predicts higher.
                if mean_temp > wu_obs:
                    # Model predicts higher — allow small upside premium
                    upside = min(1.0, (mean_temp - wu_obs) * 0.3)
                    mean_temp = wu_obs + upside
                else:
                    mean_temp = wu_obs

                effective_std = max(0.5, effective_std * 0.35)
                obs_weight = 0.80

                logger.info(
                    f"WU obs (NEAR PEAK): wu_obs={wu_obs:.1f}°C, upside={mean_temp - wu_obs:.1f}, "
                    f"mean={mean_temp:.1f}°C, σ={effective_std:.2f}"
                )

            else:
                # === REGIME A1: BEFORE PEAK — WU is floor, model drives upside ===
                # Morning/early afternoon. Temperature still rising.
                # WU is a hard floor, but final temp could be wu_obs + 1-3°C.
                if mean_temp < wu_obs:
                    mean_temp = wu_obs  # Can't go below observed max
                # Model upside is plausible: temp hasn't peaked yet
                # Keep meaningful σ for upside potential
                effective_std = max(0.8, effective_std * 0.6)
                obs_weight = 0.5

                logger.info(
                    f"WU obs (PRE-PEAK): wu_obs={wu_obs:.1f}°C, model={model_mean_pre_blend:.1f}, "
                    f"mean={mean_temp:.1f}°C, σ={effective_std:.2f}"
                )

        # Compute bucket probabilities using Student-t (heavier tails)
        bucket_probs = self.compute_bucket_probs_t(mean_temp, effective_std, buckets)

        # Lower-bound floor: the WU running daily max is MONOTONE INCREASING.
        # The final settlement temperature CANNOT be less than the current running max.
        # This is a MATHEMATICAL FACT, not probabilistic — apply at full strength always.
        if wu_obs is not None:
            bucket_probs = self._apply_obs_lower_bound(
                bucket_probs, wu_obs, 1.0, buckets  # force floor_strength=1.0
            )

        # Clamp probabilities
        for label in bucket_probs:
            bucket_probs[label] = max(
                self.prob_clamp_min,
                min(self.prob_clamp_max, bucket_probs[label]),
            )

        # Re-normalize after clamping
        total = sum(bucket_probs.values())
        if total > 0:
            bucket_probs = {k: v / total for k, v in bucket_probs.items()}

        confidence = self._compute_confidence(forecast.model_forecasts)

        logger.info(
            f"MultiOutcome fair probs: mean={mean_temp:.1f}°C, std={effective_std:.2f}, "
            f"bias={total_bias:+.2f}, top3={self._top_n(bucket_probs, 3)}"
        )

        return MultiOutcomeFairResult(
            bucket_probs=bucket_probs,
            weighted_mean_temp=mean_temp,
            uncertainty_std=effective_std,
            model_forecasts=forecast.model_forecasts,
            confidence=confidence,
        )

    def _compute_conditional_bias(self, forecast: NormalizedForecast) -> float:
        """Compute conditional bias adjustment based on weather factors.

        Adjusts the base bias using weather conditions that systematically
        affect the Open-Meteo vs WU settlement difference.
        """
        if not forecast.weather_factors:
            return 0.0

        # Average factors across available models
        all_factors = list(forecast.weather_factors.values())
        if not all_factors:
            return 0.0

        avg_cloud = statistics.mean(f.mean_cloud_cover for f in all_factors)
        avg_precip = statistics.mean(f.total_precipitation for f in all_factors)
        avg_humidity = statistics.mean(f.mean_humidity for f in all_factors)
        avg_pressure = statistics.mean(f.mean_pressure for f in all_factors)
        any_sea_breeze = any(f.is_sea_breeze for f in all_factors)

        adj = 0.0
        cfg = self.conditional_bias_cfg

        # Sea breeze at ZSPD: east wind brings cooler maritime air
        if any_sea_breeze:
            adj += cfg.get("sea_breeze_adj", -0.5)

        # High cloud cover (>70%): models tend to overestimate max temp
        if avg_cloud > 70:
            adj += cfg.get("high_cloud_adj", -0.3)

        # Precipitation: rain significantly lowers max temp vs model forecast
        if avg_precip > 1.0:
            adj += cfg.get("precipitation_adj", -0.8)
        elif avg_precip > 0.1:
            adj += cfg.get("precipitation_adj", -0.8) * 0.4  # light rain

        # High pressure (>1020 hPa): clear skies, model underestimates warming
        if avg_pressure > 1020:
            adj += cfg.get("high_pressure_adj", 0.3)

        # Low humidity (<50%): dry air allows faster warming
        if avg_humidity < 50:
            adj += cfg.get("low_humidity_adj", 0.2)

        # Clamp to ±1.5°C total conditional adjustment
        adj = max(-1.5, min(1.5, adj))

        if abs(adj) > 0.05:
            logger.info(
                f"Conditional bias: cloud={avg_cloud:.0f}% precip={avg_precip:.1f}mm "
                f"humidity={avg_humidity:.0f}% pressure={avg_pressure:.0f}hPa "
                f"sea_breeze={any_sea_breeze} → adj={adj:+.2f}°C"
            )

        return round(adj, 2)

    @staticmethod
    def compute_bucket_probs(
        mean_temp: float,
        std: float,
        buckets: list[dict] | None = None,
    ) -> dict[str, float]:
        """Compute bucket probabilities using Gaussian CDF (legacy compatibility)."""
        if buckets is None:
            buckets = DEFAULT_BUCKETS

        probs = {}
        for b in buckets:
            low, high = b["low"], b["high"]
            if low == -999:
                p = norm.cdf(high + 0.5, loc=mean_temp, scale=std)
            elif high == 999:
                p = 1.0 - norm.cdf(low - 0.5, loc=mean_temp, scale=std)
            else:
                p = norm.cdf(high + 0.5, loc=mean_temp, scale=std) - \
                    norm.cdf(low - 0.5, loc=mean_temp, scale=std)
            probs[b["label"]] = max(0.0, p)

        return probs

    def compute_bucket_probs_t(
        self,
        mean_temp: float,
        std: float,
        buckets: list[dict] | None = None,
    ) -> dict[str, float]:
        """Compute bucket probabilities using Student-t distribution.

        Student-t with df=6 gives heavier tails than Gaussian:
        - P(|X| > 2σ) ≈ 9.1% vs 4.6% for Gaussian
        - Better captures rare temperature events (3/20 = +3.4°C outlier in our data)
        """
        if buckets is None:
            buckets = DEFAULT_BUCKETS

        df = self.STUDENT_T_DF
        probs = {}
        for b in buckets:
            low, high = b["low"], b["high"]
            if low == -999:
                p = student_t.cdf(high + 0.5, df, loc=mean_temp, scale=std)
            elif high == 999:
                p = 1.0 - student_t.cdf(low - 0.5, df, loc=mean_temp, scale=std)
            else:
                p = student_t.cdf(high + 0.5, df, loc=mean_temp, scale=std) - \
                    student_t.cdf(low - 0.5, df, loc=mean_temp, scale=std)
            probs[b["label"]] = max(0.0, p)

        return probs

    def _weighted_mean(self, model_forecasts: dict[str, float], weights_override: dict[str, float] | None = None, city: str = "") -> float:
        """Compute weighted average temperature across forecast models.

        NEW: Applies per-model bias correction BEFORE blending.
        Each model's systematic offset is learned from historical data,
        so that ICON's consistent -2°C underprediction is corrected before
        being combined with GFS.
        """
        weights = weights_override or self.model_weights

        # Learn per-model bias from calibration data (cached for 1h)
        if city:
            self._ensure_per_model_bias(city)

        total_weight = 0.0
        weighted_sum = 0.0

        for model, temp in model_forecasts.items():
            w = weights.get(model, 0)
            # Apply per-model debiasing: add the correction to each model's forecast
            bias_adj = self._per_model_bias.get(model, 0.0)
            debiased_temp = temp + bias_adj
            if abs(bias_adj) > 0.01:
                logger.debug(f"Model debias: {model} {temp:.1f} + {bias_adj:+.2f} = {debiased_temp:.1f}")
            weighted_sum += w * debiased_temp
            total_weight += w

        if total_weight == 0:
            return sum(model_forecasts.values()) / len(model_forecasts)

        return weighted_sum / total_weight

    def _ensure_per_model_bias(self, city: str) -> None:
        """Learn per-model bias from calibration data (cached 1 hour).

        Empirical finding from Shanghai data:
        - GFS raw bias ≈ -0.15°C (near zero, minimal correction needed)
        - ICON raw bias ≈ -2.10°C (severe systematic underprediction)
        After per-model debiasing, ICON becomes the most accurate model
        (debiased MAE=0.25 vs GFS debiased MAE=0.95).
        """
        import time as _time
        if _time.time() - self._per_model_bias_time < 3600 and self._per_model_bias:
            return

        try:
            conn = get_connection()
            # Get WU actuals
            cal_rows = conn.execute(
                """SELECT settle_date, wu_temp FROM bias_calibration
                   WHERE city = ? AND is_reference = 1
                   ORDER BY settle_date DESC LIMIT 14""",
                (city,),
            ).fetchall()

            if len(cal_rows) < 3:
                # Not enough data — use empirical defaults for Shanghai
                if city == "shanghai":
                    self._per_model_bias = {"gfs": 0.0, "icon": 2.0, "ensemble": 1.0}
                    logger.info(f"Per-model bias: using defaults (insufficient data) → {self._per_model_bias}")
                self._per_model_bias_time = _time.time()
                conn.close()
                return

            actuals = {r["settle_date"]: r["wu_temp"] for r in cal_rows}

            # Get per-model max forecasts
            rows = conn.execute(
                """SELECT event_date, model_name, hourly_temps
                   FROM raw_forecasts
                   WHERE city = ?
                   ORDER BY event_date DESC, fetched_at DESC""",
                (city,),
            ).fetchall()
            conn.close()

            model_maxes: dict[str, dict[str, float]] = {}
            seen: set[tuple[str, str]] = set()
            for row in rows:
                d, m = row["event_date"], row["model_name"]
                key = (d, m)
                if key in seen or m not in ("gfs", "icon", "ensemble"):
                    continue
                seen.add(key)
                try:
                    temps_str = row["hourly_temps"]
                    temps = json.loads(temps_str) if temps_str.startswith("[") else [float(x) for x in temps_str.split(",") if x.strip()]
                    if temps:
                        model_maxes.setdefault(d, {})[m] = max(temps)
                except Exception:
                    pass

            # Compute signed residuals per model: actual - forecast (positive = model underpredicts)
            model_residuals: dict[str, list[float]] = {"gfs": [], "icon": [], "ensemble": []}
            for date_str, actual in actuals.items():
                if date_str in model_maxes:
                    for m, temp in model_maxes[date_str].items():
                        if m in model_residuals:
                            model_residuals[m].append(actual - temp)

            # Mean residual = the bias correction to add to each model
            new_bias: dict[str, float] = {}
            for m, resids in model_residuals.items():
                if len(resids) >= 2:
                    mean_resid = statistics.mean(resids)
                    # Cap per-model bias at ±4°C (sanity check)
                    new_bias[m] = round(max(-4.0, min(4.0, mean_resid)), 2)

            if new_bias:
                self._per_model_bias = new_bias
                logger.info(f"Per-model bias learned for {city}: {new_bias} (from {len(actuals)} settlements)")
            else:
                if city == "shanghai":
                    self._per_model_bias = {"gfs": 0.0, "icon": 2.0, "ensemble": 1.0}

            self._per_model_bias_time = _time.time()

        except Exception as e:
            logger.warning(f"Per-model bias learning failed: {e}")
            if city == "shanghai" and not self._per_model_bias:
                self._per_model_bias = {"gfs": 0.0, "icon": 2.0, "ensemble": 1.0}

    def _dynamic_sigma(self, model_forecasts: dict[str, float]) -> float:
        """Compute dynamic uncertainty σ using proper standard deviation.

        Uses DEBIASED model forecasts for spread calculation, so that
        ICON's systematic -2°C offset doesn't inflate disagreement σ.
        Combined via quadrature: σ = sqrt(base² + model_spread² + bias²)
        """
        if len(model_forecasts) >= 2:
            # Use debiased temps for spread calculation
            debiased = {m: t + self._per_model_bias.get(m, 0.0) for m, t in model_forecasts.items()}
            temps = list(debiased.values())
            spread_std = statistics.stdev(temps) if len(temps) > 1 else 0.3
        else:
            spread_std = 0.3

        combined = (
            self.base_uncertainty_std ** 2
            + spread_std ** 2
            + self.data_source_bias ** 2
        ) ** 0.5

        # Cap sigma: empirical debiased residual stdev is ~0.54°C
        combined = min(1.5, combined)

        logger.debug(
            f"Dynamic σ: base={self.base_uncertainty_std:.2f}, "
            f"spread={spread_std:.2f}, bias={self.data_source_bias:.2f} "
            f"→ combined={combined:.2f}"
        )
        return combined

    def _ensemble_sigma(self, ensemble_maxes: list[float]) -> float:
        """Compute σ from ensemble member spread.

        FIXED: Use IQR (interquartile range) instead of full std.
        Full std from 69 members is ~2.5°C which is 74% wider than observed
        residuals (1.64°C). IQR/1.35 better approximates the useful spread
        and avoids outlier ensemble members inflating σ.

        Cap at 2.0°C to prevent noise from swamping the bias signal.
        """
        if len(ensemble_maxes) < 5:
            return self.base_uncertainty_std

        sorted_ens = sorted(ensemble_maxes)
        n = len(sorted_ens)

        # IQR-based σ estimate (more robust than full stdev)
        p25 = sorted_ens[int(n * 0.25)]
        p75 = sorted_ens[int(n * 0.75)]
        iqr = p75 - p25
        iqr_sigma = iqr / 1.35  # IQR / 1.35 ≈ σ for normal distribution

        # Combine with data source bias (WU vs model grid)
        combined = (
            iqr_sigma ** 2
            + self.data_source_bias ** 2
        ) ** 0.5

        # Cap at 1.5°C (debiased blend σ ≈ 0.54°C; 1.5 for ensemble disagreement)
        combined = min(1.5, combined)

        # Floor at 0.5°C (calibrated debiased σ = 0.54°C)
        combined = max(0.5, combined)

        logger.debug(
            f"Ensemble σ: full_std={statistics.stdev(ensemble_maxes):.2f}, "
            f"IQR={iqr:.2f}, iqr_sigma={iqr_sigma:.2f}, "
            f"combined={combined:.2f} (capped at 2.0)"
        )

        return combined

    def _compute_confidence(self, model_forecasts: dict[str, float]) -> float:
        """Confidence based on model agreement (low spread = high confidence)."""
        if len(model_forecasts) <= 1:
            return 0.5

        temps = list(model_forecasts.values())
        mean = sum(temps) / len(temps)
        variance = sum((t - mean) ** 2 for t in temps) / len(temps)
        std = variance ** 0.5

        return max(0.2, min(1.0, 1.0 - std / 4.0))

    def _get_adaptive_weights(self, city: str) -> dict[str, float] | None:
        """Compute adaptive model weights based on recent backtest MAE.

        Uses inverse-MAE weighting: models with lower recent error get more weight.
        """
        import time
        now = time.time()
        if self._adaptive_cache and (now - self._adaptive_cache_time) < 3600:
            return self._adaptive_cache

        try:
            conn = get_connection()
            rows = conn.execute(
                """SELECT event_date, model_name, hourly_temps
                   FROM raw_forecasts
                   WHERE city = ?
                   ORDER BY event_date DESC, fetched_at DESC""",
                (city,),
            ).fetchall()

            cal_rows = conn.execute(
                """SELECT settle_date, wu_temp FROM bias_calibration
                   WHERE city = ? AND is_reference = 1
                   ORDER BY settle_date DESC LIMIT 14""",
                (city,),
            ).fetchall()
            conn.close()

            if len(cal_rows) < 3:
                return None

            actuals = {r["settle_date"]: r["wu_temp"] for r in cal_rows}

            model_maxes = {}
            seen = set()
            for row in rows:
                d = row["event_date"]
                m = row["model_name"]
                key = (d, m)
                if key in seen or m not in ("gfs", "icon", "ensemble"):
                    continue
                seen.add(key)
                try:
                    temps_str = row["hourly_temps"]
                    if temps_str.startswith("["):
                        temps = json.loads(temps_str)
                    else:
                        temps = [float(x) for x in temps_str.split(",") if x.strip()]
                    if temps:
                        model_maxes.setdefault(d, {})[m] = max(temps)
                except Exception:
                    pass

            # Compute DEBIASED errors: first remove systematic bias, then measure MAE
            # This prevents penalizing ICON for its consistent -2°C offset
            model_signed_errs: dict[str, list[float]] = {"gfs": [], "icon": [], "ensemble": []}
            for date_str, actual in actuals.items():
                if date_str in model_maxes:
                    for m, temp in model_maxes[date_str].items():
                        if m in model_signed_errs:
                            model_signed_errs[m].append(actual - temp)

            inv_maes = {}
            for m, errs in model_signed_errs.items():
                if len(errs) >= 2:
                    mean_bias = statistics.mean(errs)
                    # Debiased MAE: remove systematic component, measure noise
                    debiased_mae = statistics.mean([abs(e - mean_bias) for e in errs])
                    # Floor at 0.1 to avoid division by zero
                    debiased_mae = max(0.1, debiased_mae)
                    inv_maes[m] = 1.0 / debiased_mae
                    logger.debug(f"Adaptive: {m} raw_bias={mean_bias:+.2f}, debiased_MAE={debiased_mae:.2f}")

            if not inv_maes:
                return None

            total = sum(inv_maes.values())
            adaptive = {m: round(v / total, 3) for m, v in inv_maes.items()}

            # Blend 70% adaptive + 30% fixed
            blended = {}
            for m in ("gfs", "icon"):
                fixed = self.model_weights.get(m, 0.5)
                adap = adaptive.get(m, fixed)
                blended[m] = round(0.7 * adap + 0.3 * fixed, 3)

            total_b = sum(blended.values())
            if total_b > 0:
                blended = {k: round(v / total_b, 3) for k, v in blended.items()}

            logger.info(f"Adaptive weights for {city}: {blended} (from {len(actuals)} samples)")
            self._adaptive_cache = blended
            self._adaptive_cache_time = now
            return blended

        except Exception as e:
            logger.warning(f"Adaptive weights failed: {e}")
            return None

    def _apply_divergence_downweight(
        self,
        model_forecasts: dict[str, float],
        weights: dict[str, float],
        city: str,
    ) -> dict[str, float]:
        """When DEBIASED models diverge by >3°C, reduce the less accurate model's weight.

        Uses debiased spread (after per-model bias correction) so that ICON's
        systematic -2°C offset doesn't trigger false divergence penalties.
        """
        if len(model_forecasts) < 2:
            return weights

        # Use DEBIASED temps for spread check
        debiased = {m: t + self._per_model_bias.get(m, 0.0) for m, t in model_forecasts.items()}
        temps = list(debiased.values())
        spread = max(temps) - min(temps)

        if spread < 3.0:
            return weights

        # Identify which model to penalize using recent MAE
        try:
            conn = get_connection()
            cal_rows = conn.execute(
                """SELECT settle_date, wu_temp FROM bias_calibration
                   WHERE city = ? AND is_reference = 1
                   ORDER BY settle_date DESC LIMIT 10""",
                (city,),
            ).fetchall()

            forecast_rows = conn.execute(
                """SELECT event_date, model_name, hourly_temps
                   FROM raw_forecasts WHERE city = ?
                   ORDER BY event_date DESC, fetched_at DESC""",
                (city,),
            ).fetchall()
            conn.close()

            if len(cal_rows) < 3:
                return weights

            actuals = {r["settle_date"]: r["wu_temp"] for r in cal_rows}
            model_maxes = {}
            seen = set()
            for row in forecast_rows:
                key = (row["event_date"], row["model_name"])
                if key in seen:
                    continue
                seen.add(key)
                try:
                    ts = row["hourly_temps"]
                    t = json.loads(ts) if ts.startswith("[") else [float(x) for x in ts.split(",") if x.strip()]
                    if t:
                        model_maxes.setdefault(row["event_date"], {})[row["model_name"]] = max(t)
                except Exception:
                    pass

            # Use debiased MAE for identifying worst model
            model_signed = {}
            for d, actual in actuals.items():
                if d in model_maxes:
                    for m, temp in model_maxes[d].items():
                        model_signed.setdefault(m, []).append(actual - temp)

            avg_mae = {}
            for m, errs in model_signed.items():
                if len(errs) >= 2:
                    mean_bias = sum(errs) / len(errs)
                    debiased_mae = sum(abs(e - mean_bias) for e in errs) / len(errs)
                    avg_mae[m] = max(0.1, debiased_mae)
            if len(avg_mae) < 2:
                return weights

            # Find worst model
            worst_model = max(avg_mae, key=avg_mae.get)
            best_model = min(avg_mae, key=avg_mae.get)
            mae_ratio = avg_mae[worst_model] / max(avg_mae[best_model], 0.1)

            if mae_ratio > 1.5:
                # Reduce worst model weight by 30-60% based on divergence
                penalty = min(0.6, (spread - 3.0) * 0.15 + 0.3)
                new_weights = dict(weights)
                new_weights[worst_model] = weights.get(worst_model, 0.5) * (1 - penalty)
                # Renormalize
                total = sum(new_weights.values())
                if total > 0:
                    new_weights = {k: round(v / total, 3) for k, v in new_weights.items()}
                logger.info(
                    f"Divergence downweight: {worst_model} MAE={avg_mae[worst_model]:.1f}°C "
                    f"(spread={spread:.1f}°C, penalty={penalty:.0%}) → weights={new_weights}"
                )
                return new_weights

        except Exception as e:
            logger.debug(f"Divergence downweight failed: {e}")

        return weights

    def _time_of_day_obs_boost(self, city: str) -> float:
        """Compute observation weight boost based on local time of day.

        After the daily peak temperature time, the WU running max is essentially
        the final settlement value — confidence in observation increases sharply.

        Returns a boost value (0.0-0.35) to add to the base observation weight.
        """
        tz_name = city_registry.get_timezone(city)
        if not tz_name or tz_name == "UTC":
            return 0.0

        try:
            now_local = datetime.now(ZoneInfo(tz_name))
            local_hour = now_local.hour + now_local.minute / 60.0
            peak_hour = city_registry.get_peak_temp_hour(city)

            if local_hour >= peak_hour + 2.0:
                # Well past peak (e.g., after 5pm for Shanghai): max is final
                boost = 0.35
            elif local_hour >= peak_hour:
                # At/past peak time: gradually boost from 0.05 → 0.35 over 2h
                # min 0.05 ensures peak hour itself triggers NEAR PEAK regime
                boost = max(0.05, 0.35 * (local_hour - peak_hour) / 2.0)
            elif local_hour >= peak_hour - 1.0:
                # 1h before peak: small boost — max likely very close
                # min 0.01 ensures boundary (peak-1h) triggers NEAR PEAK
                boost = max(0.01, 0.10 * (local_hour - (peak_hour - 1.0)))
            else:
                boost = 0.0

            if boost > 0.05:
                logger.info(
                    f"Time-of-day obs boost: city={city}, local_hour={local_hour:.1f}, "
                    f"peak_hour={peak_hour:.0f} → boost={boost:+.2f}"
                )
            return round(boost, 3)

        except Exception as e:
            logger.debug(f"Time-of-day boost failed: {e}")
            return 0.0

    def _apply_obs_lower_bound(
        self,
        bucket_probs: dict[str, float],
        wu_obs: float,
        obs_weight: float,
        buckets: list[dict],
    ) -> dict[str, float]:
        """Apply lower-bound constraint: WU running max cannot decrease.

        When obs_weight is high, the running WU max is likely the final settlement.
        The final temperature CANNOT be lower than the current running max — so we
        zero out probability mass below (wu_obs - 0.5) and redistribute upward.

        The floor is softened by (1 - obs_weight) to avoid hard cutoffs when
        there's still meaningful model uncertainty.
        """
        # Floor is wu_obs - 0.5°C (half-bucket tolerance for WU rounding)
        floor_temp = wu_obs - 0.5

        # Strength of floor constraint scales with obs_weight:
        # - obs_weight=0.20: floor_strength=0.5 (WU is a soft bound, temp may still rise)
        # - obs_weight=0.50: floor_strength=0.8 (strong bound)
        # - obs_weight=0.80+: floor_strength=1.0 (WU IS the final value)
        # Using a steep sigmoid-like ramp starting from 0
        floor_strength = min(1.0, obs_weight * 2.0)  # 0→0, 0.5→1.0, clips at 1.0

        eliminated_mass = 0.0
        new_probs = {}

        for b in buckets:
            label = b["label"]
            p = bucket_probs.get(label, 0.0)
            high = b["high"]

            if high == 999:
                # "X or higher" bucket — always above floor
                new_probs[label] = p
            elif high < floor_temp:
                # This bucket's high end is below our floor — eliminate with floor_strength
                eliminated = p * floor_strength
                new_probs[label] = p - eliminated
                eliminated_mass += eliminated
            else:
                new_probs[label] = p

        # Redistribute eliminated mass to buckets at or above wu_obs
        if eliminated_mass > 0.001:
            above_mass = sum(
                new_probs.get(b["label"], 0.0)
                for b in buckets
                if b["low"] >= wu_obs - 0.5
            )
            if above_mass > 0:
                scale = (above_mass + eliminated_mass) / above_mass
                for b in buckets:
                    if b["low"] >= wu_obs - 0.5:
                        new_probs[b["label"]] = new_probs.get(b["label"], 0.0) * scale

            logger.info(
                f"Lower-bound floor: wu_obs={wu_obs:.1f}°C, floor={floor_temp:.1f}°C, "
                f"strength={floor_strength:.2f}, eliminated={eliminated_mass:.3f}"
            )

        return new_probs

    def _learn_conditional_bias(self, city: str) -> dict[str, float] | None:
        """Learn conditional bias coefficients from historical weather factors + residuals.

        Uses simple OLS regression: residual ~ cloud + precip + humidity + pressure + sea_breeze
        Requires at least 15 data points with both weather factors and settlement residuals.
        """
        try:
            conn = get_connection()
            # Join weather_factors with bias_calibration
            rows = conn.execute(
                """SELECT bc.settle_date, bc.residual,
                          wf.mean_cloud_cover, wf.total_precipitation,
                          wf.mean_humidity, wf.mean_pressure,
                          wf.is_sea_breeze, wf.max_wind_speed
                   FROM bias_calibration bc
                   JOIN weather_factors wf
                     ON bc.settle_date = wf.event_date AND wf.city = bc.city
                   WHERE bc.city = ? AND bc.is_reference = 1
                     AND wf.model_name = 'gfs'
                   GROUP BY bc.settle_date
                   ORDER BY bc.settle_date DESC
                   LIMIT 30""",
                (city,),
            ).fetchall()
            conn.close()

            if len(rows) < 15:
                return None

            # Simple OLS: compute coefficients for each factor
            # Using direct correlation (single-variable regression) for each factor
            residuals = [r["residual"] for r in rows]
            mean_r = sum(residuals) / len(residuals)

            learned = {}
            factor_cols = {
                "high_cloud_adj": ("mean_cloud_cover", lambda v: 1 if v > 70 else 0),
                "precipitation_adj": ("total_precipitation", lambda v: 1 if v > 1.0 else (0.4 if v > 0.1 else 0)),
                "low_humidity_adj": ("mean_humidity", lambda v: 1 if v < 50 else 0),
                "high_pressure_adj": ("mean_pressure", lambda v: 1 if v > 1020 else 0),
                "sea_breeze_adj": ("is_sea_breeze", lambda v: 1 if v else 0),
            }

            for adj_name, (col_name, binarize) in factor_cols.items():
                # Split residuals by condition
                cond_residuals = []
                no_cond_residuals = []
                for r in rows:
                    val = r[col_name]
                    if val is None:
                        continue
                    if binarize(val):
                        cond_residuals.append(r["residual"])
                    else:
                        no_cond_residuals.append(r["residual"])

                if len(cond_residuals) >= 3 and len(no_cond_residuals) >= 3:
                    cond_mean = sum(cond_residuals) / len(cond_residuals)
                    no_cond_mean = sum(no_cond_residuals) / len(no_cond_residuals)
                    # The conditional adjustment is the difference
                    adj = cond_mean - no_cond_mean
                    # Clamp to reasonable range
                    adj = max(-1.5, min(1.5, adj))
                    learned[adj_name] = round(adj, 2)

            if learned:
                logger.info(f"Learned conditional bias from {len(rows)} samples: {learned}")
            return learned if learned else None

        except Exception as e:
            logger.debug(f"Conditional bias learning failed: {e}")
            return None

    def _get_seasonal_bias(self, city: str, event_date=None) -> float:
        """Compute seasonal bias adjustment based on month."""
        if event_date is None:
            return 0.0

        try:
            conn = get_connection()
            month = event_date.month if hasattr(event_date, 'month') else int(str(event_date).split('-')[1])

            rows = conn.execute(
                """SELECT residual FROM bias_calibration
                   WHERE city = ? AND is_reference = 1
                   AND CAST(SUBSTR(settle_date, 6, 2) AS INTEGER) = ?""",
                (city, month),
            ).fetchall()
            conn.close()

            if len(rows) < 3:
                return 0.0

            residuals = [r["residual"] for r in rows]
            seasonal_mean = sum(residuals) / len(residuals)
            overall_bias = self.bias_correction.get(city, 0.0)
            adjustment = seasonal_mean - overall_bias
            adjustment = max(-1.0, min(1.0, adjustment))

            if abs(adjustment) > 0.1:
                logger.info(f"Seasonal bias adj for {city} month={month}: {adjustment:+.2f}°C")

            return round(adjustment, 2)

        except Exception as e:
            logger.debug(f"Seasonal bias failed: {e}")
            return 0.0

    def _stratified_bias(self, forecast_mean: float, city: str) -> float:
        """Temperature-stratified bias correction.

        Empirical finding: warmer days have larger forecast underprediction.
        Learn from historical data if available; otherwise use data-informed defaults.
        """
        try:
            conn = get_connection()
            rows = conn.execute(
                """SELECT forecast_mean, wu_temp, residual
                   FROM bias_calibration
                   WHERE city = ? AND is_reference = 1
                   ORDER BY settle_date DESC LIMIT 30""",
                (city,),
            ).fetchall()
            conn.close()

            if len(rows) >= 6:
                # Learn stratified bias from data
                cold_resids = [r["residual"] for r in rows if r["forecast_mean"] < 15]
                warm_resids = [r["residual"] for r in rows if 15 <= r["forecast_mean"] < 20]
                hot_resids = [r["residual"] for r in rows if r["forecast_mean"] >= 20]

                base = self.bias_correction.get(city, 0.0)

                if forecast_mean < 15 and len(cold_resids) >= 2:
                    learned = statistics.mean(cold_resids)
                    adj = learned - base
                    logger.debug(f"Stratified bias (cold <15): learned={learned:+.2f}, adj={adj:+.2f}")
                    return max(-1.0, min(1.0, adj))
                elif forecast_mean >= 20 and len(hot_resids) >= 2:
                    learned = statistics.mean(hot_resids)
                    adj = learned - base
                    logger.debug(f"Stratified bias (hot ≥20): learned={learned:+.2f}, adj={adj:+.2f}")
                    return max(-1.0, min(1.0, adj))
                elif 15 <= forecast_mean < 20 and len(warm_resids) >= 2:
                    learned = statistics.mean(warm_resids)
                    adj = learned - base
                    logger.debug(f"Stratified bias (warm 15-20): learned={learned:+.2f}, adj={adj:+.2f}")
                    return max(-1.0, min(1.0, adj))

        except Exception as e:
            logger.debug(f"Stratified bias lookup failed: {e}")

        # Default: warmer forecasts tend to underpredict more
        if forecast_mean >= 20:
            return 0.5  # +0.5°C extra on warm days
        elif forecast_mean < 13:
            return -0.3  # slightly less correction on cold days

        return 0.0

    @staticmethod
    def _top_n(probs: dict[str, float], n: int) -> str:
        sorted_probs = sorted(probs.items(), key=lambda x: -x[1])[:n]
        return ", ".join(f"{k}={v:.1%}" for k, v in sorted_probs)
