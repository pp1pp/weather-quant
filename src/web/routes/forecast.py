"""GET /api/forecast — Current weather forecast data."""

from datetime import datetime, timezone

from fastapi import APIRouter, Request

from src.web.cache import cache
from src.utils.logger import logger
from src.web.request_context import (
    get_event_context,
    get_force_refresh,
    get_forecast_bundle,
    get_scoped_cache_key,
    get_selected_city,
)

router = APIRouter()


@router.get("/forecast")
def get_forecast(request: Request):
    cache_key = get_scoped_cache_key("forecast", request)
    force_refresh = get_force_refresh(request)
    if not force_refresh:
        cached = cache.get(cache_key)
        if cached:
            return cached

    config = request.app.state.config
    ctx = get_event_context(request)
    if not ctx:
        return {"error": "No active event found"}

    try:
        bundle = get_forecast_bundle(request, include_ensemble=False)
    except Exception as exc:
        logger.warning(f"API forecast: weather context error: {exc}")
        return {"error": f"Fetch failed: {exc}"}

    event = ctx["event"]
    settle_utc = ctx["settle_utc"]
    event_date = ctx["event_date"]
    end_date_str = event.get("endDate", "")
    forecast = bundle["forecast"]
    model_forecasts = {k: round(v, 1) for k, v in forecast.model_forecasts.items()}
    fetched_at = bundle.get("fetched_at") or datetime.now(timezone.utc).isoformat()

    # Compute weighted mean with per-model debiasing (matches probability engine)
    city = get_selected_city(request)
    prob_engine = request.app.state.modules.get("multi_outcome_prob")
    if prob_engine:
        # Use the engine's debiased weighted mean for consistency
        weighted_mean = prob_engine._weighted_mean(
            {m: t for m, t in model_forecasts.items() if t is not None},
            city=city,
        )
        # Per-model bias is already applied; show residual city bias if no per-model
        city_per_model = prob_engine._per_model_bias.get(city, {})
        if city_per_model:
            bias = 0.0  # Already handled by per-model debiasing
        else:
            bias = config.get("multi_outcome", {}).get("bias_correction", {}).get(city, 0.0)
    else:
        weights = config.get("model_weights", {"gfs": 0.50, "icon": 0.50})
        total_w = sum(weights.get(m, 0) for m in model_forecasts if model_forecasts[m] is not None)
        weighted_mean = (
            sum(weights.get(m, 0) * t for m, t in model_forecasts.items() if t is not None) / total_w
            if total_w > 0
            else sum(v for v in model_forecasts.values() if v is not None) / len(model_forecasts)
        )
        bias = config.get("multi_outcome", {}).get("bias_correction", {}).get(city, 0.0)
    hours_to_settlement = max(0, (settle_utc - datetime.now(timezone.utc)).total_seconds() / 3600)

    result = {
        "city": city,
        "event_date": event_date.isoformat(),
        "event_slug": event.get("slug", ""),
        "settlement_time": end_date_str,
        "model_forecasts": model_forecasts,
        "weighted_mean": round(weighted_mean, 1),
        "bias_correction": bias,
        "bias_corrected_mean": round(weighted_mean + bias, 1),
        "hours_to_settlement": round(hours_to_settlement, 1),
        "fetched_at": fetched_at,
        "data_source": bundle.get("data_source"),
    }
    cache.set(cache_key, result, ttl=60)
    return result
