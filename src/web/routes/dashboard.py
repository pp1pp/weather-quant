"""GET /api/dashboard — Aggregated endpoint returning all dashboard data."""

import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Request, Query

from src.data import city_registry
from src.utils.logger import logger
from src.web.request_context import get_force_refresh, get_forecast_bundle, get_market_bundle, get_selected_city
from src.web.routes import bias as bias_mod
from src.web.routes import mode as mode_mod
from src.web.routes import forecast as forecast_mod
from src.web.routes import market as market_mod
from src.web.routes import probabilities as prob_mod
from src.web.routes import positions as pos_mod
from src.web.routes import stats as stats_mod
from src.web.routes import timing as timing_mod
from src.web.routes import backtest as backtest_mod

router = APIRouter()


@router.get("/dashboard")
def get_dashboard(
    request: Request,
    date: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
):
    """Single call that returns all panel data for the dashboard."""
    live_trading = getattr(request.app.state, "live_trading", None)
    if live_trading is None:
        live_trading = os.getenv("LIVE_TRADING", "false").lower() == "true"

    # Store city and date in request state for sub-routes
    if city and city in city_registry.all_city_keys():
        request.state.selected_city = city
    else:
        city = "shanghai"
        request.state.selected_city = city

    # If date is specified, find that specific event
    selected_event = None
    if date:
        tracker = request.app.state.modules["series_tracker"]
        try:
            selected_event = tracker.find_event_by_date(date, city=city)
        except Exception:
            selected_event = None

    request.state.selected_event = selected_event
    request.state.selected_date = date
    request.state._event_resolved = bool(date)

    # Get available dates for the date picker (per city)
    tracker = request.app.state.modules["series_tracker"]
    try:
        available_dates = tracker.list_available_dates(city=city)
    except Exception:
        available_dates = []

    # Build available cities list for the city selector
    available_cities = []
    for key in city_registry.all_city_keys():
        cfg = city_registry.get_city(key)
        available_cities.append({
            "key": key,
            "name": cfg.get("name", key.capitalize()) if cfg else key.capitalize(),
            "unit": city_registry.get_unit(key),
        })

    # Pre-warm shared forecast context so all sub-panels use the same event/forecast snapshot.
    bundle = None
    try:
        bundle = get_forecast_bundle(request, include_ensemble=True)
    except Exception:
        bundle = None

    market_bundle = None
    try:
        market_bundle = get_market_bundle(request)
    except Exception:
        market_bundle = None

    mode_payload = mode_mod.build_mode_response(request)
    weather_source = bundle.get("data_source") if bundle else None
    if weather_source == "live":
        weather_label = "实时抓取"
    elif weather_source == "db":
        weather_label = "DB快照回退"
    else:
        weather_label = None

    market_source = market_bundle.get("data_source") if market_bundle else None
    if market_source == "gamma":
        market_label = "Gamma 实时盘口"
    else:
        market_label = None

    view_mode = "HISTORICAL" if date else "LATEST"
    city_name = next((c["name"] for c in available_cities if c["key"] == city), city.capitalize())
    view_label = f"历史市场 {date}" if date else "当前市场"

    def safe_panel(name: str, handler):
        try:
            return handler(request)
        except Exception as exc:
            logger.warning(f"Dashboard panel '{name}' failed: {exc}")
            return {"error": f"{name} unavailable"}

    return {
        "system": {
            "mode": "LIVE" if live_trading else "DRY_RUN",
            "mode_label": mode_payload["mode_label"],
            "mode_badge": mode_payload["mode_badge"],
            "mode_description": mode_payload["mode_description"],
            "target_mode": mode_payload["target_mode"],
            "target_mode_label": mode_payload["target_mode_label"],
            "mode_source": mode_payload["source"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "selected_date": date,
            "selected_city": city,
            "available_dates": available_dates,
            "available_cities": available_cities,
            "view_mode": view_mode,
            "view_label": view_label,
            "weather_data_source": weather_source,
            "weather_data_label": weather_label,
            "weather_fetched_at": bundle.get("fetched_at") if bundle else None,
            "market_data_source": market_source,
            "market_data_label": market_label,
            "market_fetched_at": market_bundle.get("fetched_at") if market_bundle else None,
            "refresh_bypassed_cache": get_force_refresh(request),
        },
        "forecast": safe_panel("forecast", forecast_mod.get_forecast),
        "market": safe_panel("market", market_mod.get_market),
        "probabilities": safe_panel("probabilities", prob_mod.get_probabilities),
        "positions": safe_panel("positions", pos_mod.get_positions),
        "timing": safe_panel("timing", timing_mod.get_timing),
        "bias": safe_panel("bias", bias_mod.get_bias),
        "stats": safe_panel("stats", stats_mod.get_stats),
        "backtest": safe_panel("backtest", backtest_mod.get_backtest),
    }
