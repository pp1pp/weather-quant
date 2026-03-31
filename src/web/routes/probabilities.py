"""GET /api/probabilities — Fair probabilities + edge signals + limit orders."""

from datetime import datetime, timezone

from fastapi import APIRouter, Request

from src.web.cache import cache
from src.engine.timing_strategy import TimingStrategy
from src.engine.multi_outcome_edge import MultiOutcomeEdgeDetector
from src.data.schemas import BucketPrice, MultiMarketSnapshot
from src.utils.logger import logger
from src.web.request_context import (
    get_event_context,
    get_force_refresh,
    get_forecast_bundle,
    get_live_buckets,
    get_scoped_cache_key,
)

router = APIRouter()


@router.get("/probabilities")
def get_probabilities(request: Request):
    cache_key = get_scoped_cache_key("probabilities", request)
    force_refresh = get_force_refresh(request)
    if not force_refresh:
        cached = cache.get(cache_key)
        if cached:
            return cached

    modules = request.app.state.modules
    config = request.app.state.config
    multi_prob = modules["multi_prob_engine"]

    mo_cfg = config.get("multi_outcome", {})
    min_edge = mo_cfg.get("min_bucket_edge", 0.05)
    strong_edge = mo_cfg.get("strong_bucket_edge", 0.10)

    ctx = get_event_context(request)
    if not ctx:
        return {"error": "No active event"}

    try:
        live_buckets = get_live_buckets(request)
    except Exception as exc:
        logger.warning(f"Probabilities route failed to resolve buckets: {exc}")
        return {"error": "No buckets"}
    if not live_buckets:
        return {"error": "No buckets"}

    try:
        bundle = get_forecast_bundle(request, include_ensemble=True)
    except Exception as exc:
        return {"error": f"Weather fetch failed: {exc}"}

    event = ctx["event"]
    forecast = bundle["forecast"]
    ensemble_maxes = bundle.get("ensemble_maxes") or []

    # Compute fair probabilities
    bucket_defs = [
        {"label": b["label"], "low": b.get("temp_low", 0), "high": b.get("temp_high", 0)}
        for b in live_buckets
    ]
    fair = multi_prob.estimate(forecast, bucket_defs)

    # Timing
    timing = TimingStrategy(config)
    time_mult = timing.get_multiplier(forecast.hours_to_settlement)

    # Build snapshot for edge detector
    bucket_prices = []
    for b in live_buckets:
        bp = BucketPrice(
            label=b["label"],
            yes_price=b.get("yes_price", 0),
            no_price=b.get("no_price", 0),
            best_bid=b.get("best_bid", b.get("yes_price", 0) - 0.01),
            best_ask=b.get("best_ask", b.get("yes_price", 0) + 0.01),
            spread=b.get("spread", 0.02),
            volume=b.get("volume", 0),
            liquidity=b.get("liquidity", 0),
        )
        bucket_prices.append(bp)

    snapshot = MultiMarketSnapshot(
        event_slug=event.get("slug", ""),
        buckets=bucket_prices,
        total_price_sum=sum(bp.yes_price for bp in bucket_prices),
        fetched_at=datetime.now(timezone.utc),
    )

    # Edge detection with limit order pricing
    edge_detector = MultiOutcomeEdgeDetector(config)
    edge_result = edge_detector.detect(fair, snapshot)

    # Generate signals
    total_capital = config.get("position", {}).get("total_capital", 200)
    signals = edge_detector.generate_signals(edge_result, total_capital, fair.confidence * time_mult)

    # Compute scenario P&L
    scenario_pnl = edge_detector.compute_scenario_pnl(signals, snapshot) if signals else {}

    # Build edges response with limit order info
    edges = []
    for be in edge_result.bucket_edges:
        strength = "STRONG" if abs(be.edge) >= strong_edge else "LEAN" if abs(be.edge) >= min_edge else "NONE"
        edges.append({
            "label": be.label,
            "fair_prob": round(be.fair_prob, 4),
            "market_price": round(be.market_price, 4),
            "edge": round(be.edge, 4),
            "direction": be.direction,
            "strength": strength,
            "limit_price": round(be.limit_price, 4),
            "fill_prob": round(be.expected_fill_prob, 3),
            "risk_reward": round(be.risk_reward_ratio, 2),
            "kelly": round(be.kelly_fraction, 4),
        })

    # Weather factors summary
    factors_summary = None
    if forecast.weather_factors:
        all_f = list(forecast.weather_factors.values())
        if all_f:
            import statistics
            factors_summary = {
                "cloud_cover": round(statistics.mean(f.mean_cloud_cover for f in all_f), 1),
                "max_wind": round(max(f.max_wind_speed for f in all_f), 1),
                "wind_dir": round(statistics.mean(f.dominant_wind_dir for f in all_f), 0),
                "sea_breeze": any(f.is_sea_breeze for f in all_f),
                "precipitation": round(sum(f.total_precipitation for f in all_f) / len(all_f), 2),
                "humidity": round(statistics.mean(f.mean_humidity for f in all_f), 1),
                "pressure": round(statistics.mean(f.mean_pressure for f in all_f), 1),
                "diurnal_range": round(statistics.mean(f.diurnal_range for f in all_f), 1),
            }

    # Ensemble stats
    ensemble_stats = None
    if ensemble_maxes and len(ensemble_maxes) >= 5:
        import statistics as st
        ensemble_stats = {
            "n_members": len(ensemble_maxes),
            "mean": round(st.mean(ensemble_maxes), 2),
            "std": round(st.stdev(ensemble_maxes), 2),
            "min": round(min(ensemble_maxes), 1),
            "max": round(max(ensemble_maxes), 1),
            "p10": round(sorted(ensemble_maxes)[int(len(ensemble_maxes) * 0.1)], 1),
            "p90": round(sorted(ensemble_maxes)[int(len(ensemble_maxes) * 0.9)], 1),
        }

    result = {
        "bucket_probs": {k: round(v, 4) for k, v in fair.bucket_probs.items()},
        "weighted_mean_temp": round(fair.weighted_mean_temp, 1),
        "uncertainty_std": round(fair.uncertainty_std, 2),
        "confidence": round(fair.confidence, 3),
        "model_forecasts": {k: round(v, 1) for k, v in fair.model_forecasts.items()},
        "hours_to_settlement": round(forecast.hours_to_settlement, 1),
        "timing_multiplier": round(time_mult, 2),
        "observation": forecast.latest_observation,
        "edges": edges,
        "sum_to_one_gap": edge_result.sum_to_one_gap,
        "signals": [
            {
                "label": s.label,
                "direction": s.direction,
                "edge": round(s.edge, 4),
                "amount": s.amount,
                "fair_prob": round(s.fair_prob, 4),
                "market_price": round(s.market_price, 4),
            }
            for s in signals
        ],
        "scenario_pnl": scenario_pnl,
        "weather_factors": factors_summary,
        "ensemble": ensemble_stats,
        "data_source": bundle.get("data_source"),
    }
    cache.set(cache_key, result, ttl=60)
    return result
