"""GET /api/timing — Timing strategy curve data."""

from datetime import datetime, timezone

from fastapi import APIRouter, Request

from src.engine.timing_strategy import TimingStrategy
from src.web.cache import cache
from src.web.request_context import get_event_context, get_force_refresh, get_scoped_cache_key

router = APIRouter()


@router.get("/timing")
def get_timing(request: Request):
    cache_key = get_scoped_cache_key("timing", request)
    force_refresh = get_force_refresh(request)
    if not force_refresh:
        cached = cache.get(cache_key)
        if cached:
            return cached

    config = request.app.state.config
    mo_cfg = config.get("multi_outcome", {})
    timing = TimingStrategy(config)

    # Generate curve at 100 points from 0 to 48h
    curve = []
    for i in range(101):
        hours = i * 48 / 100
        mult = timing.get_multiplier(hours)
        curve.append({"hours": round(hours, 1), "multiplier": round(mult, 3)})

    # Current position
    current_hours = None
    current_mult = None
    try:
        ctx = get_event_context(request)
        if ctx:
            settle = ctx["settle_utc"]
            current_hours = max(0, (settle - datetime.now(timezone.utc)).total_seconds() / 3600)
            current_mult = timing.get_multiplier(current_hours)
    except Exception:
        pass

    resp = {
        "curve": curve,
        "current_hours": round(current_hours, 1) if current_hours is not None else None,
        "current_multiplier": round(current_mult, 2) if current_mult is not None else None,
        "config": {
            "min_hours": mo_cfg.get("min_hours_to_settle", 2),
            "max_hours": mo_cfg.get("max_hours_to_settle", 36),
            "sweet_spot_low": mo_cfg.get("sweet_spot_hours_low", 6),
            "sweet_spot_high": mo_cfg.get("sweet_spot_hours_high", 18),
        },
    }
    cache.set(cache_key, resp, ttl=300)
    return resp
