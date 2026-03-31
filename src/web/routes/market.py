"""GET /api/market — Live Polymarket bucket prices."""

from fastapi import APIRouter, Request

from src.web.cache import cache
from src.utils.logger import logger
from src.web.request_context import (
    get_event_context,
    get_force_refresh,
    get_market_bundle,
    get_scoped_cache_key,
)

router = APIRouter()


@router.get("/market")
def get_market(request: Request):
    cache_key = get_scoped_cache_key("market", request)
    force_refresh = get_force_refresh(request)
    if not force_refresh:
        cached = cache.get(cache_key)
        if cached:
            return cached

    ctx = get_event_context(request)
    if not ctx:
        return {"error": "No active event"}

    try:
        bundle = get_market_bundle(request)
        buckets = bundle.get("buckets", [])
    except Exception as e:
        logger.warning(f"API market: bucket extraction error: {e}")
        return {"error": "Failed to fetch market data"}
    if not buckets:
        return {"error": "No buckets found"}

    event = ctx["event"]
    total_yes = sum(b.get("yes_price", 0) for b in buckets)

    result = {
        "event_slug": event.get("slug", ""),
        "settlement_time": event.get("endDate", ""),
        "buckets": [
            {
                "label": b["label"],
                "yes_price": b.get("yes_price", 0),
                "no_price": b.get("no_price", 0),
                "spread": b.get("spread", 0),
                "volume": b.get("volume", 0),
                "liquidity": b.get("liquidity", 0),
            }
            for b in buckets
        ],
        "total_price_sum": round(total_yes, 4),
        "data_source": bundle.get("data_source"),
        "fetched_at": bundle.get("fetched_at"),
    }
    cache.set(cache_key, result, ttl=30)
    return result
