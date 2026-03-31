"""POST /api/calibrate — Settlement collection and bias calibration management."""

import json
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Request

from src.data.fetcher_wunderground import WundergroundFetcher
from src.engine.bias_calibrator import BiasCalibrator
from src.utils.db import get_connection
from src.utils.logger import logger
from src.web.cache import cache
from src.web.request_context import get_selected_city

router = APIRouter()


@router.post("/calibrate/collect-settlements")
def collect_settlements(request: Request, days_back: int = 14):
    """
    Auto-fetch WU settlement temperatures for past N days and record them.

    This is the primary way to accumulate real settlement data. Each successful
    WU fetch adds a trusted 'live_replay' reference sample to bias_calibration,
    enabling adaptive model weights and improved seasonal bias correction.

    Args:
        days_back: How many past days to attempt to collect (default 14, max 30)
    """
    days_back = min(30, max(1, days_back))
    city = get_selected_city(request)
    wu = WundergroundFetcher()
    config = request.app.state.config
    weights = config.get("model_weights", {"gfs": 0.55, "icon": 0.45})

    conn = get_connection()
    try:
        existing = {
            row["settle_date"]
            for row in conn.execute(
                "SELECT settle_date FROM bias_calibration WHERE city=?", (city,)
            ).fetchall()
        }
    finally:
        conn.close()

    calibrator = BiasCalibrator()
    today = date.today()
    collected = []
    failed = []
    skipped = []

    for i in range(1, days_back + 1):
        check_date = today - timedelta(days=i)
        date_str = check_date.isoformat()

        if date_str in existing:
            skipped.append(date_str)
            continue

        # Fetch WU temperature for this date
        wu_temp_float = wu.fetch_daily_high(city, check_date)
        if wu_temp_float is None:
            failed.append(date_str)
            continue

        wu_temp = round(wu_temp_float)

        # Fetch raw forecast from DB — only use forecasts fetched within 2 days of event.
        # Long-range forecasts (5+ days ahead) are much less accurate and corrupt bias.
        conn = get_connection()
        try:
            # date_str is the event date; we want forecasts fetched on event_date-1 or event_date
            date_minus1 = (check_date - timedelta(days=1)).isoformat()
            rows = conn.execute(
                """SELECT model_name, hourly_temps, fetched_at FROM raw_forecasts
                   WHERE city=? AND event_date=?
                   AND DATE(fetched_at) >= ?
                   ORDER BY fetched_at DESC""",
                (city, date_str, date_minus1),
            ).fetchall()
        finally:
            conn.close()

        # Compute weighted mean from stored recent forecasts
        raw_mean = None
        seen_models = set()
        model_maxes = {}
        for row in rows:
            m = row["model_name"]
            if m in seen_models or m not in ("gfs", "icon", "ecmwf"):
                continue
            seen_models.add(m)
            try:
                temps_str = row["hourly_temps"]
                if temps_str.startswith("["):
                    temps = json.loads(temps_str)
                else:
                    temps = [float(x) for x in temps_str.split(",") if x.strip()]
                if temps:
                    model_maxes[m] = max(temps)
            except Exception:
                pass

        if model_maxes:
            total_w = sum(weights.get(m, 0) for m in model_maxes)
            if total_w > 0:
                raw_mean = sum(
                    weights.get(m, 0) * t for m, t in model_maxes.items()
                ) / total_w

        if raw_mean is None:
            # Skip: no valid raw forecast in DB for this date.
            # Recording with forecast_mean=0 would create a spurious large residual.
            logger.info(f"Skipping {date_str}: no raw forecast in DB for bias computation")
            failed.append(date_str + " (no forecast)")
            continue

        calibrator.record_settlement(
            city,
            check_date,
            wu_temp,
            raw_mean,
            source="live_replay",
            notes=f"Auto-collected via API. WU={wu_temp_float:.1f}°C rounded={wu_temp}. "
                  f"Models: {list(model_maxes.keys())}",
        )

        collected.append({
            "date": date_str,
            "wu_temp": wu_temp,
            "raw_forecast": round(raw_mean, 2) if raw_mean else None,
            "residual": round(wu_temp - raw_mean, 2) if raw_mean else None,
        })
        logger.info(f"Collected settlement: {date_str} WU={wu_temp}°C raw={raw_mean}")

    # Update bias config if we collected new reference samples
    if collected:
        try:
            calibrator.update_config(city)
            # Reload config in running server
            import yaml
            with open(calibrator.config_path) as f:
                new_cfg = yaml.safe_load(f)
            if new_cfg:
                request.app.state.config.update(new_cfg)
            cache.clear()
            new_bias = (
                request.app.state.config
                .get("multi_outcome", {})
                .get("bias_correction", {})
                .get(city, 0.0)
            )
        except Exception as e:
            logger.warning(f"Bias update after collection failed: {e}")
            new_bias = None
    else:
        new_bias = None

    return {
        "collected": collected,
        "n_collected": len(collected),
        "n_skipped": len(skipped),
        "n_failed": len(failed),
        "failed_dates": failed,
        "new_bias": new_bias,
        "message": (
            f"Collected {len(collected)} new settlement records. "
            f"Skipped {len(skipped)} existing. Failed {len(failed)} WU fetches."
        ),
    }


@router.post("/calibrate/add-settlement")
def add_settlement(request: Request, settle_date: str, wu_temp: int, notes: str = ""):
    """
    Manually add a verified settlement record.

    Use this to add settlement data you've verified from Polymarket or WU directly.
    Source is set to 'verified_market' (highest trust level).

    Args:
        settle_date: ISO date string (e.g. "2026-03-20")
        wu_temp: WU observed maximum temperature in whole °C (settlement value)
        notes: Optional description
    """
    try:
        check_date = date.fromisoformat(settle_date)
    except ValueError:
        return {"error": f"Invalid date format: {settle_date}. Use YYYY-MM-DD"}

    city = get_selected_city(request)
    config = request.app.state.config
    weights = config.get("model_weights", {"gfs": 0.55, "icon": 0.45})

    # Fetch raw forecast from DB — only recent (within 2 days) to avoid long-range bias
    conn = get_connection()
    try:
        date_minus1 = (check_date - timedelta(days=1)).isoformat()
        rows = conn.execute(
            """SELECT model_name, hourly_temps FROM raw_forecasts
               WHERE city=? AND event_date=?
               AND DATE(fetched_at) >= ?
               ORDER BY fetched_at DESC""",
            (city, settle_date, date_minus1),
        ).fetchall()
    finally:
        conn.close()

    model_maxes = {}
    seen_models = set()
    for row in rows:
        m = row["model_name"]
        if m in seen_models or m not in ("gfs", "icon", "ecmwf"):
            continue
        seen_models.add(m)
        try:
            import json
            temps_str = row["hourly_temps"]
            if temps_str.startswith("["):
                temps = json.loads(temps_str)
            else:
                temps = [float(x) for x in temps_str.split(",") if x.strip()]
            if temps:
                model_maxes[m] = max(temps)
        except Exception:
            pass

    raw_mean = None
    if model_maxes:
        total_w = sum(weights.get(m, 0) for m in model_maxes)
        if total_w > 0:
            raw_mean = sum(weights.get(m, 0) * t for m, t in model_maxes.items()) / total_w

    calibrator = BiasCalibrator()
    # Use 0.0 as forecast_mean only if none available — bias is still computable
    # but this record won't contribute to bias since residual = wu_temp - 0.0 = wu_temp
    # The bias_calibrator.update_config now caps bias at ±2°C to prevent corruption
    calibrator.record_settlement(
        city,
        check_date,
        wu_temp,
        raw_mean if raw_mean is not None else 0.0,
        source="verified_market",
        notes=notes or f"Manually added via API. Models: {list(model_maxes.keys()) if model_maxes else 'none'}",
        is_reference=True,
    )

    # Update bias
    try:
        calibrator.update_config(city)
        import yaml
        with open(calibrator.config_path) as f:
            new_cfg = yaml.safe_load(f)
        if new_cfg:
            request.app.state.config.update(new_cfg)
        cache.clear()
    except Exception as e:
        logger.warning(f"Bias update failed: {e}")

    return {
        "success": True,
        "date": settle_date,
        "wu_temp": wu_temp,
        "raw_forecast": round(raw_mean, 2) if raw_mean else None,
        "residual": round(wu_temp - raw_mean, 2) if raw_mean else None,
    }


@router.get("/calibrate/status")
def get_calibration_status(request: Request):
    """Summary of current calibration data and quality."""
    city = get_selected_city(request)
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT settle_date, wu_temp, forecast_mean, residual, source, is_reference
               FROM bias_calibration
               WHERE city = ?
               ORDER BY settle_date DESC""",
            (city,),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {"n_samples": 0, "message": "No calibration data"}

    residuals = [r["residual"] for r in rows if r["residual"] is not None]
    ref_residuals = [r["residual"] for r in rows if r["is_reference"] and r["residual"] is not None]

    source_counts = {}
    for r in rows:
        source_counts[r["source"]] = source_counts.get(r["source"], 0) + 1

    current_bias = (
        request.app.state.config
        .get("multi_outcome", {})
        .get("bias_correction", {})
        .get(city, 0.0)
    )

    return {
        "n_total": len(rows),
        "n_reference": len(ref_residuals),
        "source_counts": source_counts,
        "current_bias": current_bias,
        "all_mean_residual": round(sum(residuals) / len(residuals), 3) if residuals else None,
        "ref_mean_residual": round(sum(ref_residuals) / len(ref_residuals), 3) if ref_residuals else None,
        "recent_dates": [r["settle_date"] for r in rows[:5]],
        "recommendation": (
            "需要更多真实结算数据 (< 10个)" if len(ref_residuals) < 10
            else "校准数据充足，可信赖自适应权重" if len(ref_residuals) >= 20
            else "数据量适中，持续积累中"
        ),
    }
