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
        # Per-model bias learned from data, keyed by city
        self._per_model_bias: dict[str, dict[str, float]] = {}
        self._per_model_bias_time: dict[str, float] = {}
        # Consolidated calibration data cache (city -> {actuals, model_maxes})
        self._cal_data_cache: dict[str, dict] = {}
        self._cal_data_cache_time: dict[str, float] = {}

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

        # Temperature-dependent weight adjustment:
        # On warm days (>20°C), ICON has catastrophic MAE (3.05 vs GFS 1.05).
        # Shift weight heavily toward GFS for warm forecasts.
        raw_mean = sum(forecast.model_forecasts.values()) / max(1, len(forecast.model_forecasts))
        if raw_mean > 22:
            # Very warm: 85% GFS, 15% ICON
            active_weights = {"gfs": 0.85, "icon": 0.15}
        elif raw_mean > 19:
            # Warm: interpolate — GFS gets (raw_mean-19)/3 * 0.35 extra
            gfs_boost = min(0.35, (raw_mean - 19) / 3 * 0.35)
            gfs_w = active_weights.get("gfs", 0.5) + gfs_boost
            icon_w = max(0.15, active_weights.get("icon", 0.5) - gfs_boost)
            total = gfs_w + icon_w
            active_weights = {"gfs": round(gfs_w / total, 3), "icon": round(icon_w / total, 3)}

        # Dynamic model downweight: when models strongly disagree,
        # penalize the model with worse recent MAE
        active_weights = self._apply_divergence_downweight(
            forecast.model_forecasts, active_weights, city
        )

        # Extract cloud cover for weather-stratified bias
        _cloud = None
        if forecast.weather_factors:
            _cloud = forecast.weather_factors.get("cloud_cover") or forecast.weather_factors.get("mean_cloud_cover")
        mean_temp = self._weighted_mean(
            forecast.model_forecasts, weights_override=active_weights,
            city=city, cloud_cover=_cloud,
        )

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
        #
        # FIX: per_model_bias (applied in _weighted_mean) already corrects each
        # model's systematic offset from the SAME calibration data that city
        # bias_correction was learned from.  Applying both causes double-counting.
        # When per_model_bias is active FOR THIS CITY, skip the city-level base_bias.
        city_per_model = self._per_model_bias.get(city, {})
        has_per_model_bias = bool(city_per_model)
        if has_per_model_bias:
            base_bias = 0.0
            logger.info(
                f"Skipping city bias_correction for {city} "
                f"(per_model_bias active: {city_per_model})"
            )
        else:
            base_bias = self.bias_correction.get(city, 0.0)

        conditional_adj = self._compute_conditional_bias(forecast)

        # Temperature-stratified bias: warmer days underpredict more (dampened on cloudy days)
        stratified_adj = self._stratified_bias(mean_temp + base_bias, city, cloud_cover=_cloud)

        total_bias = base_bias + conditional_adj + stratified_adj

        if total_bias != 0.0:
            logger.info(
                f"Bias correction for {city}: base={base_bias:+.2f}, "
                f"conditional={conditional_adj:+.2f}, stratified={stratified_adj:+.2f}, "
                f"total={total_bias:+.2f}°C ({mean_temp:.1f} → {mean_temp + total_bias:.1f})"
            )
            mean_temp += total_bias

        # Dynamic uncertainty - use ensemble if available, else model spread
        if forecast.ensemble_maxes and len(forecast.ensemble_maxes) >= 10:
            effective_std = self._ensemble_sigma(forecast.ensemble_maxes)
            logger.info(f"Using ensemble σ: {effective_std:.2f}°C from {len(forecast.ensemble_maxes)} members")
        else:
            effective_std = self._dynamic_sigma(forecast.model_forecasts, city)

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
        # FOUR REGIMES:
        #   A. Before peak (morning): WU is a floor, model drives upside estimate
        #   B. After peak, before settlement: WU ≈ final value, model almost irrelevant
        #   C. Early day (6-10h out): WU as soft floor + model blend
        #   D. Far from settlement (>10h): Pure model, no observation
        wu_obs = forecast.latest_observation
        obs_weight = 0.0
        # FIX: Extended from 6h to 10h. Even in early morning, WU running max
        # provides a hard floor (temp can only go up during the day).
        obs_window = max(self.obs_start_hours, 10)
        if wu_obs is not None and forecast.hours_to_settlement < obs_window:
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
                effective_std = max(1.0, effective_std * 0.7)
                obs_weight = 0.5

                logger.info(
                    f"WU obs (PRE-PEAK): wu_obs={wu_obs:.1f}°C, model={model_mean_pre_blend:.1f}, "
                    f"mean={mean_temp:.1f}°C, σ={effective_std:.2f}"
                )

        elif wu_obs is not None and forecast.hours_to_settlement < 14:
            # === REGIME C: EARLY DAY (10-14h out) — WU as soft constraint ===
            # Very early morning. WU running max is a lower bound but temp will
            # rise significantly. Use WU as floor only, keep model σ wide.
            model_mean_pre_blend = mean_temp
            if mean_temp < wu_obs:
                mean_temp = wu_obs
            obs_weight = 0.15
            logger.info(
                f"WU obs (EARLY DAY): wu_obs={wu_obs:.1f}°C, model={model_mean_pre_blend:.1f}, "
                f"mean={mean_temp:.1f}°C (floor only, σ unchanged)"
            )

        # Time-decay σ: uncertainty shrinks as settlement approaches
        # This applies ON TOP of observation regime adjustments above.
        hours = forecast.hours_to_settlement
        if hours <= 0:
            time_decay = 0.40
        elif hours <= 2:
            time_decay = 0.50
        elif hours <= 6:
            time_decay = 0.55 + (hours - 2) * 0.075  # 0.55 → 0.85
        elif hours <= 12:
            time_decay = 0.85 + (hours - 6) * 0.025  # 0.85 → 1.0
        else:
            time_decay = 1.0  # Far out: keep original σ

        effective_std_pre_decay = effective_std
        # FIX: Floor at 0.8°C (was 0.3). Historical debiased residual std ≈ 1.3°C.
        # 0.3 was far too tight, causing extreme overconfidence on single buckets.
        # Only allow tighter σ when WU obs regime is active (past_peak / near_peak).
        sigma_floor = 0.35 if obs_weight >= 0.8 else 0.8
        effective_std = max(sigma_floor, effective_std * time_decay)

        if time_decay < 1.0:
            logger.info(
                f"Time-decay σ: {hours:.1f}h to settlement, "
                f"decay={time_decay:.2f}, σ={effective_std_pre_decay:.2f} → {effective_std:.2f}"
            )

        # Compute bucket probabilities using Student-t (heavier tails)
        bucket_probs = self.compute_bucket_probs_t(mean_temp, effective_std, buckets)

        # Lower-bound floor: the WU running daily max is MONOTONE INCREASING.
        # The final settlement temperature CANNOT be less than the current running max.
        # This is a MATHEMATICAL FACT, not probabilistic — apply at full strength always.
        # Buckets whose HIGH end is definitively below WU obs get HARD ZERO (not soft reduce).
        hard_zero_labels: set[str] = set()
        if wu_obs is not None:
            bucket_probs, hard_zero_labels = self._apply_obs_lower_bound(
                bucket_probs, wu_obs, 1.0, buckets  # force floor_strength=1.0
            )

        # Clamp probabilities — but skip buckets that were hard-zeroed by WU floor
        for label in bucket_probs:
            if label in hard_zero_labels:
                continue  # Keep at 0.0 — physically impossible
            bucket_probs[label] = max(
                self.prob_clamp_min,
                min(self.prob_clamp_max, bucket_probs[label]),
            )

        # Re-normalize after clamping (hard zeros stay zero)
        total = sum(bucket_probs.values())
        if total > 0:
            bucket_probs = {k: (v / total if v > 0 else 0.0) for k, v in bucket_probs.items()}

        # Probability calibration: de-sharpen overconfident predictions.
        # Historical analysis: model assigns 16.7% avg probability to correct bucket,
        # but wins 13.3% of the time → model is overconfident on top picks.
        # Apply entropy-boosting: soften extreme probabilities toward uniform.
        bucket_probs = self._calibrate_probabilities(bucket_probs, hard_zero_labels)

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

    def _calibrate_probabilities(
        self,
        bucket_probs: dict[str, float],
        hard_zero_labels: set[str],
    ) -> dict[str, float]:
        """De-sharpen overconfident model probabilities.

        Historical finding: model assigns 16.7% average probability to the
        winning bucket but only hits 13.3%. The model is overconfident on
        its top-1 pick and underestimates tail buckets.

        Method: power-law softening. Raise probs to power γ<1, which
        compresses high probs down and lifts low probs up, increasing entropy.
        γ=0.85 empirically reduces overconfidence without destroying signal.

        Hard-zero buckets (below WU floor) are preserved at 0.
        """
        GAMMA = 0.85  # <1 = soften; tune with backtest

        calibrated = {}
        for label, p in bucket_probs.items():
            if label in hard_zero_labels or p <= 0:
                calibrated[label] = 0.0
            else:
                calibrated[label] = p ** GAMMA

        # Re-normalize
        total = sum(calibrated.values())
        if total > 0:
            calibrated = {k: v / total for k, v in calibrated.items()}

        return calibrated

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

    def _weighted_mean(
        self,
        model_forecasts: dict[str, float],
        weights_override: dict[str, float] | None = None,
        city: str = "",
        cloud_cover: float | None = None,
    ) -> float:
        """Compute weighted average temperature across forecast models.

        Applies WEATHER-STRATIFIED per-model bias correction BEFORE blending.
        On cloudy days (cloud>60%), ICON's bias is smaller; on clear days, larger.
        Falls back to overall bias when stratified data is insufficient.
        """
        weights = weights_override or self.model_weights

        # Learn per-model bias from calibration data (cached for 1h)
        if city:
            self._ensure_per_model_bias(city)

        total_weight = 0.0
        weighted_sum = 0.0

        # Choose stratified bias if available, else overall
        city_bias = self._per_model_bias.get(city, {})
        if cloud_cover is not None and cloud_cover > 60:
            stratified = self._per_model_bias.get(f"{city}:cloudy", {})
            if stratified:
                city_bias = stratified
                logger.debug(f"Using CLOUDY per-model bias for {city}: {city_bias}")
        elif cloud_cover is not None and cloud_cover <= 60:
            stratified = self._per_model_bias.get(f"{city}:clear", {})
            if stratified:
                city_bias = stratified
                logger.debug(f"Using CLEAR per-model bias for {city}: {city_bias}")

        for model, temp in model_forecasts.items():
            w = weights.get(model, 0)
            bias_adj = city_bias.get(model, 0.0)
            debiased_temp = temp + bias_adj
            if abs(bias_adj) > 0.01:
                logger.debug(f"Model debias: {model} {temp:.1f} + {bias_adj:+.2f} = {debiased_temp:.1f}")
            weighted_sum += w * debiased_temp
            total_weight += w

        if total_weight == 0:
            return sum(model_forecasts.values()) / len(model_forecasts)

        return weighted_sum / total_weight

    def _load_calibration_data(self, city: str) -> dict:
        """Load and cache calibration data for a city (1-hour TTL).

        Consolidates DB queries that were previously scattered across
        _ensure_per_model_bias, _get_adaptive_weights, _apply_divergence_downweight.
        Returns: {actuals: {date: wu_temp}, model_maxes: {date: {model: max_temp}}}
        """
        import time as _time
        cache_time = self._cal_data_cache_time.get(city, 0)
        if _time.time() - cache_time < 3600 and city in self._cal_data_cache:
            return self._cal_data_cache[city]

        try:
            conn = get_connection()
            cal_rows = conn.execute(
                """SELECT settle_date, wu_temp FROM bias_calibration
                   WHERE city = ? AND is_reference = 1
                   ORDER BY settle_date DESC LIMIT 20""",
                (city,),
            ).fetchall()

            actuals = {r["settle_date"]: r["wu_temp"] for r in cal_rows}

            rows = conn.execute(
                """SELECT event_date, model_name, hourly_temps
                   FROM raw_forecasts
                   WHERE city = ?
                     AND fetched_at < (event_date || 'T12:00:00')
                   ORDER BY event_date DESC, fetched_at DESC""",
                (city,),
            ).fetchall()
            # Fallback: if too few forecasts found, use all forecasts
            if len(rows) < 5:
                conn2 = get_connection()
                rows = conn2.execute(
                    """SELECT event_date, model_name, hourly_temps
                       FROM raw_forecasts
                       WHERE city = ?
                       ORDER BY event_date DESC, fetched_at DESC""",
                    (city,),
                ).fetchall()
                conn2.close()
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

            result = {"actuals": actuals, "model_maxes": model_maxes}
            self._cal_data_cache[city] = result
            self._cal_data_cache_time[city] = _time.time()
            return result

        except Exception as e:
            logger.warning(f"Failed to load calibration data for {city}: {e}")
            return {"actuals": {}, "model_maxes": {}}

    def _ensure_per_model_bias(self, city: str) -> None:
        """Learn per-model bias from consolidated calibration data cache.

        Also learns weather-stratified bias: separate corrections for
        cloudy days (cloud>60%) vs clear days (cloud≤60%).
        """
        import time as _time
        city_cache_time = self._per_model_bias_time.get(city, 0)
        if _time.time() - city_cache_time < 1800 and city in self._per_model_bias:
            return

        try:
            cal = self._load_calibration_data(city)
            actuals = cal["actuals"]
            model_maxes = cal["model_maxes"]

            if len(actuals) < 3:
                if city == "shanghai":
                    self._per_model_bias[city] = {"gfs": 0.0, "icon": 2.0, "ensemble": 1.0}
                    logger.info(f"Per-model bias: using defaults for {city} (insufficient data)")
                self._per_model_bias_time[city] = _time.time()
                return

            # Load cloud cover per date for weather-stratified bias
            cloud_by_date: dict[str, float] = {}
            try:
                conn = get_connection()
                wf_rows = conn.execute(
                    """SELECT event_date, AVG(mean_cloud_cover) as avg_cloud
                       FROM weather_factors
                       WHERE city = ? AND model_name IN ('gfs','icon')
                       GROUP BY event_date""",
                    (city,),
                ).fetchall()
                conn.close()
                cloud_by_date = {r["event_date"]: r["avg_cloud"] for r in wf_rows}
            except Exception:
                pass

            # Compute signed residuals per model, split by weather type
            model_residuals: dict[str, list[float]] = {"gfs": [], "icon": [], "ensemble": []}
            cloudy_residuals: dict[str, list[float]] = {"gfs": [], "icon": [], "ensemble": []}
            clear_residuals: dict[str, list[float]] = {"gfs": [], "icon": [], "ensemble": []}

            for date_str, actual in actuals.items():
                if date_str in model_maxes:
                    cloud = cloud_by_date.get(date_str, 50)
                    for m, temp in model_maxes[date_str].items():
                        if m in model_residuals:
                            resid = actual - temp
                            model_residuals[m].append(resid)
                            if cloud > 60:
                                cloudy_residuals[m].append(resid)
                            else:
                                clear_residuals[m].append(resid)

            # Overall bias
            new_bias: dict[str, float] = {}
            for m, resids in model_residuals.items():
                if len(resids) >= 2:
                    mean_resid = statistics.mean(resids)
                    new_bias[m] = round(max(-4.0, min(4.0, mean_resid)), 2)

            # Weather-stratified bias
            cloudy_bias: dict[str, float] = {}
            clear_bias: dict[str, float] = {}
            for m in ("gfs", "icon", "ensemble"):
                if len(cloudy_residuals[m]) >= 2:
                    cloudy_bias[m] = round(max(-4.0, min(4.0, statistics.mean(cloudy_residuals[m]))), 2)
                if len(clear_residuals[m]) >= 2:
                    clear_bias[m] = round(max(-4.0, min(4.0, statistics.mean(clear_residuals[m]))), 2)

            if new_bias:
                self._per_model_bias[city] = new_bias
                # Store stratified bias as sub-keys
                self._per_model_bias[f"{city}:cloudy"] = cloudy_bias
                self._per_model_bias[f"{city}:clear"] = clear_bias
                logger.info(
                    f"Per-model bias for {city}: overall={new_bias}, "
                    f"cloudy={cloudy_bias} (n={len(cloudy_residuals.get('gfs',[]))}), "
                    f"clear={clear_bias} (n={len(clear_residuals.get('gfs',[]))})"
                )
            else:
                if city == "shanghai":
                    self._per_model_bias[city] = {"gfs": 0.0, "icon": 2.0, "ensemble": 1.0}

            self._per_model_bias_time[city] = _time.time()

        except Exception as e:
            logger.warning(f"Per-model bias learning failed for {city}: {e}")
            if city == "shanghai" and city not in self._per_model_bias:
                self._per_model_bias[city] = {"gfs": 0.0, "icon": 2.0, "ensemble": 1.0}

    def _dynamic_sigma(self, model_forecasts: dict[str, float], city: str = "") -> float:
        """Compute dynamic uncertainty σ using proper standard deviation.

        Uses DEBIASED model forecasts for spread calculation, so that
        ICON's systematic -2°C offset doesn't inflate disagreement σ.
        Combined via quadrature: σ = sqrt(base² + model_spread² + bias²)
        """
        city_bias = self._per_model_bias.get(city, {})
        if len(model_forecasts) >= 2:
            # Use debiased temps for spread calculation
            debiased = {m: t + city_bias.get(m, 0.0) for m, t in model_forecasts.items()}
            temps = list(debiased.values())
            spread_std = statistics.stdev(temps) if len(temps) > 1 else 0.3
        else:
            spread_std = 0.3

        combined = (
            self.base_uncertainty_std ** 2
            + spread_std ** 2
            + self.data_source_bias ** 2
        ) ** 0.5

        # Cap sigma at 2.0; floor at 0.8 (historical residual std ≈ 1.3°C)
        combined = max(0.8, min(2.0, combined))

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

        # Cap at 2.0°C (allow wider spread when ensemble disagrees)
        combined = min(2.0, combined)

        # Floor at 0.8°C — historical residual std ≈ 1.3°C, so 0.5 was too tight.
        # Ensemble agreement doesn't mean the forecast is right (systematic bias exists).
        combined = max(0.8, combined)

        # Detect bimodal distribution: if there's a gap in the middle,
        # the ensemble is "split" and a single Gaussian will miss the gap region.
        # Widen sigma to ensure the gap temperatures get non-zero probability.
        bimodal_penalty = self._detect_bimodality(sorted_ens)
        if bimodal_penalty > 0:
            combined = max(combined, combined + bimodal_penalty)
            combined = min(2.5, combined)  # Allow wider cap for bimodal

        full_std = statistics.stdev(ensemble_maxes)
        logger.debug(
            f"Ensemble σ: full_std={full_std:.2f}, "
            f"IQR={iqr:.2f}, iqr_sigma={iqr_sigma:.2f}, "
            f"combined={combined:.2f}, bimodal_penalty={bimodal_penalty:.2f}"
        )

        return combined

    @staticmethod
    def _detect_bimodality(sorted_vals: list[float]) -> float:
        """Detect bimodal distribution and return sigma penalty.

        When ensemble members cluster into two groups with a gap in between
        (e.g., 17-19°C and 22-24°C with nothing at 20-21°C), a single
        Gaussian badly underestimates probability at the gap temperatures.

        Returns penalty to add to sigma (0.0 if unimodal, 0.3-0.8 if bimodal).
        """
        if len(sorted_vals) < 10:
            return 0.0

        n = len(sorted_vals)
        # Find largest gap between consecutive sorted values
        max_gap = 0.0
        gap_center = 0.0
        for i in range(1, n):
            gap = sorted_vals[i] - sorted_vals[i - 1]
            if gap > max_gap:
                max_gap = gap
                gap_center = (sorted_vals[i] + sorted_vals[i - 1]) / 2

        # Compare gap to overall spread
        spread = sorted_vals[-1] - sorted_vals[0]
        if spread < 1.0:
            return 0.0

        gap_ratio = max_gap / spread

        if gap_ratio > 0.25 and max_gap > 1.5:
            # Strong bimodality: gap > 25% of spread and > 1.5°C
            penalty = min(0.8, max_gap * 0.3)
            logger.info(
                f"Bimodal ensemble detected: gap={max_gap:.1f}°C at ~{gap_center:.1f}°C "
                f"(ratio={gap_ratio:.2f}), σ penalty={penalty:+.2f}"
            )
            return penalty
        elif gap_ratio > 0.15 and max_gap > 1.0:
            # Mild bimodality
            penalty = min(0.4, max_gap * 0.15)
            return penalty

        return 0.0

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
            degradation_penalty: dict[str, float] = {}
            for m, errs in model_signed_errs.items():
                if len(errs) >= 2:
                    mean_bias = statistics.mean(errs)
                    debiased_mae = statistics.mean([abs(e - mean_bias) for e in errs])
                    debiased_mae = max(0.1, debiased_mae)
                    inv_maes[m] = 1.0 / debiased_mae

                    # Trend detection: 3-day vs 7-day MAE
                    # If recent errors are growing, apply a penalty
                    if len(errs) >= 5:
                        recent_3 = statistics.mean([abs(e - mean_bias) for e in errs[:3]])
                        older_7 = statistics.mean([abs(e - mean_bias) for e in errs[:7]])
                        if older_7 > 0 and recent_3 > older_7 * 1.3:
                            # Model degrading: recent 3-day MAE >30% worse than 7-day
                            penalty = min(0.6, (recent_3 / older_7 - 1.0))
                            inv_maes[m] *= (1.0 - penalty)
                            degradation_penalty[m] = penalty
                            logger.warning(
                                f"Model {m} degrading for {city}: "
                                f"3d_MAE={recent_3:.2f} vs 7d_MAE={older_7:.2f} "
                                f"→ penalty={penalty:.0%}"
                            )

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
        city_bias = self._per_model_bias.get(city, {})
        debiased = {m: t + city_bias.get(m, 0.0) for m, t in model_forecasts.items()}
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

            if local_hour >= peak_hour + 1.5:
                # Well past peak (e.g., after 4:30pm for Shanghai): max is final
                boost = 0.35
            elif local_hour >= peak_hour:
                # At/past peak time: rapidly boost 0.10 → 0.35 over 1.5h
                boost = max(0.10, 0.35 * (local_hour - peak_hour) / 1.5)
            elif local_hour >= peak_hour - 1.5:
                # 1.5h before peak (13:30 for Shanghai): WU is very close to final
                # Temperature rise slows significantly near peak
                progress = (local_hour - (peak_hour - 1.5)) / 1.5  # 0→1
                boost = max(0.02, 0.10 * progress)
            elif local_hour >= peak_hour - 3.0:
                # 3h before peak (12:00 for Shanghai): approaching peak window
                # WU running max is informative but upside still possible
                progress = (local_hour - (peak_hour - 3.0)) / 1.5  # 0→1
                boost = 0.005 * progress  # Very small, just triggers floor logic
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
    ) -> tuple[dict[str, float], set[str]]:
        """Apply lower-bound constraint: WU running max cannot decrease.

        WU running daily max is MONOTONE INCREASING — the final settlement
        temperature CANNOT be lower than the current running max.

        Returns (new_probs, hard_zero_labels):
        - hard_zero_labels: bucket labels that are physically impossible (prob=0.0)
          These should NOT be clamped back to prob_clamp_min.
        """
        # Hard floor: buckets entirely below WU obs are physically impossible
        hard_floor = wu_obs - 0.5  # WU integer rounding tolerance

        eliminated_mass = 0.0
        new_probs = {}
        hard_zero_labels: set[str] = set()

        for b in buckets:
            label = b["label"]
            p = bucket_probs.get(label, 0.0)
            high = b["high"]

            if high == 999:
                # "X or higher" bucket — always above floor
                new_probs[label] = p
            elif high < hard_floor:
                # Bucket's high end is definitively below WU observation.
                # Physically impossible for daily max to be in this bucket.
                # HARD ZERO — no probability residue.
                eliminated_mass += p
                new_probs[label] = 0.0
                hard_zero_labels.add(label)
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
                f"Lower-bound floor: wu_obs={wu_obs:.1f}°C, hard_floor={hard_floor:.1f}°C, "
                f"zeroed={len(hard_zero_labels)} buckets, eliminated={eliminated_mass:.3f}"
            )

        return new_probs, hard_zero_labels

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

    def _stratified_bias(self, forecast_mean: float, city: str, cloud_cover: float | None = None) -> float:
        """Temperature-stratified bias correction.

        Empirical finding: warmer days have larger forecast underprediction,
        BUT this effect is weaker on cloudy days (clouds cap max temp).
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
                cold_resids = [r["residual"] for r in rows if r["forecast_mean"] < 15]
                warm_resids = [r["residual"] for r in rows if 15 <= r["forecast_mean"] < 20]
                hot_resids = [r["residual"] for r in rows if r["forecast_mean"] >= 20]

                base = self.bias_correction.get(city, 0.0)
                adj = 0.0

                if forecast_mean < 15 and len(cold_resids) >= 2:
                    learned = statistics.mean(cold_resids)
                    adj = learned - base
                    logger.debug(f"Stratified bias (cold <15): learned={learned:+.2f}, adj={adj:+.2f}")
                elif forecast_mean >= 20 and len(hot_resids) >= 2:
                    learned = statistics.mean(hot_resids)
                    adj = learned - base
                    logger.debug(f"Stratified bias (hot ≥20): learned={learned:+.2f}, adj={adj:+.2f}")
                elif 15 <= forecast_mean < 20 and len(warm_resids) >= 2:
                    learned = statistics.mean(warm_resids)
                    adj = learned - base
                    logger.debug(f"Stratified bias (warm 15-20): learned={learned:+.2f}, adj={adj:+.2f}")

                if adj != 0.0:
                    # Dampen warm-day upward bias on cloudy days
                    # Cloud cover r=-0.65 means clouds reduce the underprediction
                    if cloud_cover is not None and cloud_cover > 60 and adj > 0:
                        dampen = max(0.2, 1.0 - (cloud_cover - 60) / 80)  # 60%→1.0, 100%→0.5
                        adj *= dampen
                        logger.debug(f"Stratified bias dampened by cloud={cloud_cover:.0f}%: adj={adj:+.2f}")
                    return max(-1.0, min(1.0, adj))

        except Exception as e:
            logger.debug(f"Stratified bias lookup failed: {e}")

        # Default: warmer forecasts tend to underpredict more
        if forecast_mean >= 20:
            default_adj = 0.5
            # Dampen on cloudy days
            if cloud_cover is not None and cloud_cover > 60:
                default_adj *= max(0.2, 1.0 - (cloud_cover - 60) / 80)
            return default_adj
        elif forecast_mean < 13:
            return -0.3

        return 0.0

    @staticmethod
    def _top_n(probs: dict[str, float], n: int) -> str:
        sorted_probs = sorted(probs.items(), key=lambda x: -x[1])[:n]
        return ", ".join(f"{k}={v:.1%}" for k, v in sorted_probs)
