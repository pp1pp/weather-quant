"""GET /api/bias — Bias calibration history."""

from fastapi import APIRouter, Request

from src.engine.bias_calibrator import BiasCalibrator, ensure_calibration_table
from src.utils.db import get_connection
from src.web.cache import cache
from src.web.request_context import get_selected_city, get_scoped_cache_key

router = APIRouter()
RESEARCH_SOURCES = ("seed", "legacy", "verified_settlement_seed_forecast")


@router.get("/bias")
def get_bias(request: Request):
    cache_key = get_scoped_cache_key("bias", request)
    cached_val = cache.get(cache_key)
    if cached_val:
        return cached_val

    city = get_selected_city(request)
    config = request.app.state.config
    current_bias = config.get("multi_outcome", {}).get("bias_correction", {}).get(city, 0.0)

    # Compute from calibrator
    try:
        ensure_calibration_table()
        cal = BiasCalibrator()
        trusted = cal.compute_bias(city, reference_only=True)
        research = cal.compute_bias(city, sources=RESEARCH_SOURCES)
    except Exception:
        trusted = {"bias": current_bias, "std": 0.5, "n": 0, "samples": []}
        research = {"bias": 0.0, "std": 1.5, "n": 0, "samples": []}

    # Full history from DB
    history = []
    source_counts = {}
    try:
        conn = get_connection()
        try:
            cursor = conn.execute(
                """SELECT settle_date, wu_temp, forecast_mean, residual,
                          source, is_reference, notes
                   FROM bias_calibration
                   WHERE city = ?
                   ORDER BY settle_date ASC""",
                (city,),
            )
            for row in cursor.fetchall():
                source = row["source"]
                source_counts[source] = source_counts.get(source, 0) + 1
                history.append({
                    "date": row["settle_date"],
                    "actual": row["wu_temp"],
                    "forecast": row["forecast_mean"],
                    "residual": row["residual"],
                    "source": source,
                    "is_reference": bool(row["is_reference"]),
                    "notes": row["notes"],
                })
        finally:
            conn.close()
    except Exception:
        pass

    trusted_samples = sum(1 for row in history if row["is_reference"])
    research_samples = len(history) - trusted_samples

    resp = {
        "current_bias": current_bias,
        "computed_bias": trusted["bias"],
        "residual_std": trusted["std"],
        "n_samples": trusted["n"],
        "trusted_bias": trusted["bias"],
        "trusted_residual_std": trusted["std"],
        "trusted_n_samples": trusted["n"],
        "research_bias": research["bias"],
        "research_residual_std": research["std"],
        "research_n_samples": research["n"],
        "total_n_samples": len(history),
        "source_counts": source_counts,
        "trusted_history_samples": trusted_samples,
        "research_history_samples": research_samples,
        "history": history,
    }
    cache.set(cache_key, resp, ttl=300)
    return resp
