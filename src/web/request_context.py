"""Shared request-scoped helpers for dashboard API routes."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from src.data import city_registry
from src.data.schemas import NormalizedForecast, RawWeatherData
from src.utils.db import get_connection
from src.utils.logger import logger


def get_selected_city(request) -> str:
    """Read the selected city from request state or query params. Default: shanghai."""
    cached = getattr(request.state, "selected_city", None)
    if cached is not None:
        return cached

    query_city = request.query_params.get("city")
    city = query_city if query_city and query_city in city_registry.all_city_keys() else "shanghai"
    request.state.selected_city = city
    return city


def get_selected_date(request) -> str | None:
    """Read the selected date from request state or query params."""
    selected_date = getattr(request.state, "selected_date", None)
    if selected_date is not None:
        return selected_date

    query_date = request.query_params.get("date")
    request.state.selected_date = query_date
    return query_date


def get_scoped_cache_key(base: str, request) -> str:
    """Build a cache key scoped to the selected city + event/date."""
    city = get_selected_city(request)
    selected_date = get_selected_date(request)
    return f"{base}:{city}:{selected_date or 'latest'}"


def get_force_refresh(request) -> bool:
    """Whether this request explicitly bypasses the shared API cache."""
    cached = getattr(request.state, "_force_refresh", None)
    if cached is not None:
        return cached

    raw_value = request.query_params.get("refresh")
    force_refresh = False
    if raw_value is not None:
        normalized = str(raw_value).strip().lower()
        force_refresh = normalized not in ("", "0", "false", "no")

    request.state._force_refresh = force_refresh
    return force_refresh


def get_selected_event(request):
    """Resolve the selected event for the current city, once per request."""
    if getattr(request.state, "_event_resolved", False):
        return getattr(request.state, "selected_event", None)

    selected_date = get_selected_date(request)
    if selected_date is not None and hasattr(request.state, "selected_event"):
        request.state._event_resolved = True
        return getattr(request.state, "selected_event", None)

    city = get_selected_city(request)
    tracker = request.app.state.modules["series_tracker"]
    event = None
    try:
        if selected_date:
            event = tracker.find_event_by_date(selected_date, city=city)
        else:
            event = tracker.find_latest_event(city=city)
    except Exception as exc:
        logger.warning(f"Failed to resolve event for {city}/{selected_date}: {exc}")

    request.state.selected_event = event
    request.state._event_resolved = True
    return event


def get_event_context(request) -> dict | None:
    """Return event, settlement time, and event date for the current request."""
    cached = getattr(request.state, "_event_context", None)
    if cached is not None:
        return cached

    event = get_selected_event(request)
    if not event:
        return None

    end_date_str = event.get("endDate", "")
    if not end_date_str:
        return None

    settle_utc = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
    ctx = {
        "event": event,
        "settle_utc": settle_utc,
        "event_date": settle_utc.date(),
    }
    request.state._event_context = ctx
    return ctx


def get_live_buckets(request) -> list[dict] | None:
    """Extract bucket metadata once per request."""
    bundle = get_market_bundle(request)
    return bundle.get("buckets")


def get_market_bundle(request) -> dict:
    """Build a request-scoped market snapshot bundle."""
    cached = getattr(request.state, "_market_bundle", None)
    if cached is not None:
        return cached

    ctx = get_event_context(request)
    if not ctx:
        raise ValueError("No active event found")

    tracker = request.app.state.modules["series_tracker"]
    fetched_at = datetime.now(timezone.utc).isoformat()
    buckets = tracker.extract_buckets(ctx["event"])

    bundle = {
        "buckets": buckets,
        "data_source": "gamma",
        "fetched_at": fetched_at,
    }
    request.state._market_bundle = bundle
    return bundle


def _load_db_raw_data(city: str, event_date) -> list[RawWeatherData]:
    """Load the latest stored raw forecast snapshot for each model from SQLite."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT model_name, hourly_temps, source, fetched_at
               FROM raw_forecasts
               WHERE city = ? AND event_date = ?
               ORDER BY fetched_at DESC""",
            (city, event_date.isoformat()),
        ).fetchall()
    finally:
        conn.close()

    raw_data: list[RawWeatherData] = []
    seen_models: set[str] = set()
    for row in rows:
        model_name = row["model_name"]
        if model_name in seen_models:
            continue

        try:
            hourly_temps = json.loads(row["hourly_temps"])
        except Exception:
            continue
        if not hourly_temps:
            continue

        fetched_at = datetime.fromisoformat(row["fetched_at"])
        raw_data.append(
            RawWeatherData(
                city=city,
                event_date=event_date,
                source=row["source"],
                model_name=model_name,
                hourly_temps=hourly_temps,
                fetched_at=fetched_at,
                raw_response={},
            )
        )
        seen_models.add(model_name)

    return raw_data


def get_forecast_bundle(request, *, include_ensemble: bool = False) -> dict:
    """
    Build a request-scoped forecast bundle for the selected city.

    Strategy:
    1. Prefer a live Open-Meteo fetch.
    2. Fall back to the latest stored DB snapshot if the network fetch fails.
    3. Optionally fetch ensemble members and inject current WU observation.
    """
    existing = getattr(request.state, "_forecast_bundle", None)
    if existing is not None:
        has_ensemble = bool(existing.get("ensemble_maxes"))
        if not include_ensemble or has_ensemble:
            return existing

    ctx = get_event_context(request)
    if not ctx:
        raise ValueError("No active event found")

    city = get_selected_city(request)
    modules = request.app.state.modules
    config = request.app.state.config
    fetcher = modules["fetcher"]
    normalizer = modules["normalizer"]
    wu_fetcher = modules.get("wu_fetcher")

    event_date = ctx["event_date"]
    settle_utc = ctx["settle_utc"]

    raw_data = existing["raw_data"] if existing else None
    data_source = existing["data_source"] if existing else None
    fetched_at = existing["fetched_at"] if existing else None

    if raw_data is None:
        try:
            raw_data = fetcher.fetch_forecast(city, event_date)
            data_source = "live"
            fetched_at = max((raw.fetched_at for raw in raw_data), default=datetime.now(timezone.utc)).isoformat()
        except Exception as exc:
            logger.warning(f"Live weather fetch failed for {city}/{event_date}: {exc}; falling back to DB snapshot")
            raw_data = _load_db_raw_data(city, event_date)
            if not raw_data:
                raise ValueError(f"Weather fetch failed and no DB snapshot available: {exc}") from exc
            data_source = "db"
            fetched_at = max((raw.fetched_at for raw in raw_data), default=datetime.now(timezone.utc)).isoformat()

    ensemble_maxes = existing.get("ensemble_maxes", []) if existing else []
    if include_ensemble and not ensemble_maxes:
        try:
            coords = city_registry.get_coordinates(city)
            ensemble_maxes = fetcher.fetch_ensemble(city, event_date, coords)
        except Exception as exc:
            logger.warning(f"Ensemble fetch failed for {city}/{event_date}: {exc}")
            ensemble_maxes = []

    forecast: NormalizedForecast = normalizer.normalize(
        raw_data,
        event_date,
        settle_utc,
        ensemble_maxes=ensemble_maxes or None,
    )

    obs_start = config.get("multi_outcome", {}).get("observation_blend_start_hours", 12)
    if wu_fetcher and forecast.hours_to_settlement < obs_start:
        try:
            wu_temp = wu_fetcher.fetch_current_temp(city)
            if wu_temp is not None:
                forecast.latest_observation = wu_temp
        except Exception as exc:
            logger.debug(f"WU observation fetch failed for {city}: {exc}")

    bundle = {
        "raw_data": raw_data,
        "forecast": forecast,
        "ensemble_maxes": ensemble_maxes,
        "data_source": data_source,
        "fetched_at": fetched_at,
    }
    request.state._forecast_bundle = bundle
    return bundle
