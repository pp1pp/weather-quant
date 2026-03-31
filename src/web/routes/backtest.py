"""GET /api/backtest — Backtest validation data for model accuracy analysis."""

import json
import math
import sqlite3
from datetime import datetime, timezone

from fastapi import APIRouter, Request

from src.web.cache import cache
from src.utils.db import get_connection
from src.utils.logger import logger
from src.web.request_context import get_force_refresh, get_scoped_cache_key, get_selected_city, get_selected_date

router = APIRouter()

# Model weights for weighted mean computation
DEFAULT_WEIGHTS = {"ecmwf": 0.45, "gfs": 0.30, "icon": 0.25}


def _load_bias_rows(conn, city: str = "shanghai"):
    """Load bias calibration rows grouped by date for a city."""
    rows = conn.execute(
        """SELECT settle_date, wu_temp, forecast_mean, residual,
                  source, is_reference, notes
           FROM bias_calibration
           WHERE city = ?
           ORDER BY settle_date DESC""",
        (city,),
    ).fetchall()
    grouped = {}
    for row in rows:
        grouped.setdefault(row["settle_date"], []).append(row)
    return grouped


def _load_raw_forecasts(conn, city: str = "shanghai"):
    """Load raw forecast data per model per date for a city."""
    rows = conn.execute(
        """SELECT event_date, model_name, hourly_temps, fetched_at
           FROM raw_forecasts
           WHERE city = ?
           ORDER BY event_date DESC, fetched_at DESC""",
        (city,),
    ).fetchall()

    # Per-date, per-model: keep the latest fetch
    grouped = {}
    for row in rows:
        d = row["event_date"]
        m = row["model_name"]
        grouped.setdefault(d, {})
        if m not in grouped[d]:
            try:
                temps_str = row["hourly_temps"]
                if temps_str.startswith("["):
                    temps = json.loads(temps_str)
                else:
                    temps = [float(x) for x in temps_str.split(",") if x.strip()]
                if temps:
                    grouped[d][m] = {
                        "daily_max": round(max(temps), 1),
                        "fetched_at": row["fetched_at"],
                    }
            except Exception:
                pass
    return grouped


def _pick_best_row(rows, *, reference_only=False):
    """Pick the best calibration row by source priority."""
    SOURCE_PRIORITY = {
        "verified_market": 0, "live_replay": 1,
        "verified_settlement_seed_forecast": 2, "seed": 3, "legacy": 4,
    }
    candidates = rows
    if reference_only:
        candidates = [r for r in candidates if r["is_reference"]]
    if not candidates:
        return None
    return min(candidates, key=lambda r: SOURCE_PRIORITY.get(r["source"], 99))


def _mae(errors):
    return sum(abs(e) for e in errors) / len(errors) if errors else 0.0


def _rmse(errors):
    return math.sqrt(sum(e * e for e in errors) / len(errors)) if errors else 0.0


@router.get("/backtest")
def get_backtest(request: Request):
    cache_key = get_scoped_cache_key("backtest", request)
    force_refresh = get_force_refresh(request)
    if not force_refresh:
        cached = cache.get(cache_key)
        if cached:
            return cached

    config = request.app.state.config
    city = get_selected_city(request)
    weights = config.get("model_weights", DEFAULT_WEIGHTS)
    bias_cfg = config.get("multi_outcome", {}).get("bias_correction", {}).get(city, 0.0)
    selected_date = get_selected_date(request)

    conn = get_connection()
    try:
        bias_rows = _load_bias_rows(conn, city)
        raw_forecasts = _load_raw_forecasts(conn, city)
    finally:
        conn.close()

    # Build daily comparison
    daily = []
    raw_errors = []
    corrected_errors = []
    walk_forward_errors = []
    expanding_residuals = []
    model_errors = {"ecmwf": [], "gfs": [], "icon": []}

    # Sort dates chronologically
    all_dates = sorted(set(list(bias_rows.keys()) + list(raw_forecasts.keys())))

    for date_str in all_dates:
        b_rows = bias_rows.get(date_str, [])
        r_data = raw_forecasts.get(date_str, {})

        best_row = _pick_best_row(b_rows) if b_rows else None
        actual = best_row["wu_temp"] if best_row else None

        # Compute weighted mean from raw forecasts
        raw_mean = None
        model_temps = {}
        if r_data:
            total_w = 0
            wsum = 0
            for m_name in ("ecmwf", "gfs", "icon"):
                if m_name in r_data:
                    w = weights.get(m_name, 0)
                    wsum += w * r_data[m_name]["daily_max"]
                    total_w += w
                    model_temps[m_name] = r_data[m_name]["daily_max"]
            if total_w > 0:
                raw_mean = round(wsum / total_w, 2)

        # Use forecast_mean from bias row as fallback
        if raw_mean is None and best_row:
            raw_mean = best_row["forecast_mean"]

        corrected_mean = round(raw_mean + bias_cfg, 2) if raw_mean is not None else None

        # Walk-forward bias: use expanding window of past residuals
        wf_bias = (sum(expanding_residuals) / len(expanding_residuals)) if expanding_residuals else 0.0
        wf_pred = round(raw_mean + wf_bias, 2) if raw_mean is not None else None

        entry = {
            "date": date_str,
            "actual": actual,
            "raw_forecast": raw_mean,
            "bias_corrected": corrected_mean,
            "walk_forward_pred": wf_pred,
            "walk_forward_bias": round(wf_bias, 3),
            "models": model_temps,
            "source": best_row["source"] if best_row else None,
            "is_reference": bool(best_row["is_reference"]) if best_row else False,
        }

        if actual is not None and raw_mean is not None:
            raw_err = actual - raw_mean
            raw_errors.append(raw_err)
            corrected_errors.append(actual - corrected_mean)
            expanding_residuals.append(raw_err)

            if wf_pred is not None:
                walk_forward_errors.append(actual - wf_pred)

            entry["raw_error"] = round(raw_err, 2)
            entry["corrected_error"] = round(actual - corrected_mean, 2)
            entry["wf_error"] = round(actual - wf_pred, 2) if wf_pred is not None else None

            # Per-model errors
            for m_name, m_temp in model_temps.items():
                if m_name in model_errors:
                    model_errors[m_name].append(actual - m_temp)

        daily.append(entry)

    # Compute aggregate metrics
    def _metrics(errors_list):
        if not errors_list:
            return {"bias": 0, "mae": 0, "rmse": 0, "n": 0, "bucket_hit": 0}
        bias = sum(errors_list) / len(errors_list)
        return {
            "bias": round(bias, 3),
            "mae": round(_mae(errors_list), 3),
            "rmse": round(_rmse(errors_list), 3),
            "n": len(errors_list),
        }

    # Per-model metrics
    per_model = {}
    for m_name, errs in model_errors.items():
        per_model[m_name] = _metrics(errs)

    # Adaptive weight suggestion: inverse MAE weighting
    adaptive_weights = {}
    total_inv_mae = 0
    for m_name in ("ecmwf", "gfs", "icon"):
        m = per_model.get(m_name, {})
        if m.get("mae", 0) > 0 and m.get("n", 0) >= 3:
            inv = 1.0 / m["mae"]
            adaptive_weights[m_name] = inv
            total_inv_mae += inv
    if total_inv_mae > 0:
        adaptive_weights = {k: round(v / total_inv_mae, 3) for k, v in adaptive_weights.items()}
    else:
        adaptive_weights = dict(weights)

    # Compute walk-forward bias as a calibration suggestion
    wf_bias_suggestion = None
    if walk_forward_errors and len(walk_forward_errors) >= 3:
        wf_bias_suggestion = round(sum(walk_forward_errors) / len(walk_forward_errors) + bias_cfg, 3)

    # Bucket hit rate: percent of days where round(forecast) == actual
    bucket_hits = sum(
        1 for d in daily
        if d.get("actual") is not None and d.get("bias_corrected") is not None
        and round(d["bias_corrected"]) == d["actual"]
    )
    n_with_actual = len(raw_errors)
    bucket_hit_rate = round(bucket_hits / n_with_actual, 3) if n_with_actual > 0 else 0

    # Config suggestions based on backtest results
    suggestions = []
    raw_m = _metrics(raw_errors)
    corr_m = _metrics(corrected_errors)
    wf_m = _metrics(walk_forward_errors)

    if raw_m["n"] >= 3 and abs(raw_m["bias"]) > 0.3:
        direction = "正偏差(模型偏低)" if raw_m["bias"] > 0 else "负偏差(模型偏高)"
        suggestions.append(f"原始预测{direction} {raw_m['bias']:+.2f}°C，建议保持/更新偏差修正")

    if adaptive_weights != dict(weights) and len(raw_errors) >= 5:
        best_model = min(per_model.items(), key=lambda x: x[1].get("mae", 99) if x[1].get("n",0)>0 else 99)
        suggestions.append(f"最优模型: {best_model[0].upper()} (MAE={best_model[1]['mae']:.2f}°C)，建议启用自适应权重")

    if bucket_hit_rate > 0:
        if bucket_hit_rate >= 0.6:
            suggestions.append(f"落桶命中率 {bucket_hit_rate:.0%} — 模型校准良好")
        else:
            suggestions.append(f"落桶命中率 {bucket_hit_rate:.0%} — 可考虑调整偏差修正提高命中率")

    selected_entry = None
    if selected_date:
        for item in daily:
            if item["date"] == selected_date:
                selected_entry = item
                break

    result = {
        "daily": daily,
        "metrics": {
            "raw": _metrics(raw_errors),
            "bias_corrected": _metrics(corrected_errors),
            "walk_forward": _metrics(walk_forward_errors),
        },
        "per_model": per_model,
        "current_weights": weights,
        "adaptive_weights": adaptive_weights,
        "current_bias": bias_cfg,
        "suggested_bias": wf_bias_suggestion,
        "bucket_hit_rate": bucket_hit_rate,
        "suggestions": suggestions,
        "n_dates": len(daily),
        "n_with_actual": n_with_actual,
        "selected_date": selected_date,
        "selected_entry": selected_entry,
    }
    cache.set(cache_key, result, ttl=300)
    return result


@router.get("/backtest/brier")
def get_brier_score(request: Request):
    """Run Brier Score backtest evaluation of the probability model."""
    city = get_selected_city(request)
    brier_key = f"brier_backtest:{city}"
    cached = cache.get(brier_key)
    if cached:
        return cached

    config = request.app.state.config
    from src.engine.backtester import Backtester
    bt = Backtester(config)
    result = bt.run(city)
    if "error" not in result:
        cache.set(brier_key, result, ttl=600)
    return result


@router.post("/config/apply-weights")
def apply_adaptive_weights(request: Request):
    """Apply adaptive model weights from backtest to config and running engine."""
    import yaml
    import os

    # Get latest backtest data
    result = get_backtest(request)
    adaptive = result.get("adaptive_weights", {})
    if not adaptive:
        return {"error": "No adaptive weights computed (need ≥3 data points)"}

    # Update settings.yaml
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "config", "settings.yaml"
    )
    try:
        with open(config_path, "r") as f:
            settings = yaml.safe_load(f)
        settings["model_weights"] = adaptive
        with open(config_path, "w") as f:
            yaml.dump(settings, f, default_flow_style=False, allow_unicode=True)
    except Exception as e:
        return {"error": f"Failed to update settings.yaml: {e}"}

    # Update live config and engine
    request.app.state.config["model_weights"] = adaptive
    engine = request.app.state.modules.get("multi_prob_engine")
    if engine:
        engine.model_weights = adaptive
        engine._adaptive_cache = None  # Force recompute

    cache.clear()
    logger.info(f"Applied adaptive weights: {adaptive}")
    return {"applied": adaptive, "success": True}
