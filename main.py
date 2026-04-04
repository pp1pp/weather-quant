"""
weather-quant/main.py

Polymarket Weather Market Quantitative Trading System.

Usage:
    python3 main.py                  # Run with scheduler (24/7 mode)
    python3 main.py --once           # Run one cycle then exit
    python3 main.py --search         # Search for weather markets on Polymarket
    python3 main.py --status         # Show current positions and stats

Environment:
    config/.env controls credentials and mode:
    - LIVE_TRADING=false  → dry run (default, safe)
    - LIVE_TRADING=true   → real money trading
"""

import argparse
import json
import os
import signal
import sys

import yaml
from dotenv import load_dotenv

from src.data import city_registry
from src.data.fetcher_openmeteo import OpenMeteoFetcher
from src.data.fetcher_wunderground import WundergroundFetcher
from src.data.normalizer import Normalizer
from src.data.schemas import RawWeatherData
from src.engine.edge_detector import EdgeDetector
from src.engine.event_mapper import EventMapper
from src.engine.fair_prob import FairProbEngine
from src.engine.bias_calibrator import BiasCalibrator, ensure_calibration_table
from src.engine.multi_outcome_prob import MultiOutcomeProbEngine
from src.engine.timing_strategy import TimingStrategy
from src.engine.signal_generator import SignalGenerator
from src.review.settlement_review import SettlementReview
from src.trading.executor import Executor
from src.trading.market_scanner import MarketScanner
from src.trading.multi_market_scanner import MultiMarketScanner
from src.trading.multi_outcome_trade import (
    build_bucket_market_id,
    build_bucket_trade_meta,
    parse_iso_datetime,
    side_price_from_yes_price,
)
from src.trading.position_manager import PositionManager
from src.trading.risk_control import RiskControl
from src.trading.series_tracker import SeriesTracker
from src.utils.circuit_breaker import CircuitBreaker
from src.utils.db import init_db
from src.utils.logger import logger

# Circuit breakers for external APIs
_breakers = {
    "open_meteo": CircuitBreaker("open_meteo", max_failures=3, base_delay=30),
    "polymarket": CircuitBreaker("polymarket", max_failures=5, base_delay=15),
    "wu": CircuitBreaker("wu", max_failures=3, base_delay=60),
}

# Load environment
load_dotenv("config/.env")

# Load config
with open("config/settings.yaml", "r") as f:
    config = yaml.safe_load(f)


def create_clob_client():
    """Create authenticated Polymarket CLOB client if credentials are available."""
    private_key = os.getenv("PRIVATE_KEY")
    api_key = os.getenv("POLY_API_KEY")
    api_secret = os.getenv("POLY_API_SECRET")
    api_passphrase = os.getenv("POLY_API_PASSPHRASE")
    funder = os.getenv("FUNDER")

    if not all([private_key, api_key, api_secret, api_passphrase]):
        logger.warning("Polymarket credentials not found in .env, using mock mode")
        return None

    try:
        from py_clob_client import ClobClient, ApiCreds
        from py_clob_client.constants import POLYGON

        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )
        client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=POLYGON,
            key=private_key,
            creds=creds,
            funder=funder,
        )

        # Verify connection
        ok = client.get_ok()
        if ok == "OK":
            logger.info(f"Polymarket CLOB connected. Address: {client.get_address()}")
            return client
        else:
            logger.error(f"CLOB connection check failed: {ok}")
            return None
    except Exception as e:
        logger.error(f"Failed to create CLOB client: {e}")
        return None


def init_system():
    """Initialize all system modules."""
    db = init_db()
    ensure_calibration_table()

    # Determine mode
    live_trading = os.getenv("LIVE_TRADING", "false").lower() == "true"
    clob_client = create_clob_client()

    # If no CLOB client, force mock/dry-run
    mock_mode = clob_client is None
    dry_run = not live_trading or clob_client is None

    if live_trading and dry_run:
        logger.warning(
            "LIVE_TRADING=true but no CLOB client available. "
            "Falling back to DRY RUN mode."
        )

    mode_str = "LIVE" if not dry_run else "DRY RUN"
    data_str = "REAL" if not mock_mode else "MOCK"
    logger.info(f"Mode: {mode_str} trading, {data_str} market data")

    fetcher = OpenMeteoFetcher(config["data_sources"])
    wu_fetcher = WundergroundFetcher()
    normalizer = Normalizer()
    # EventMapper is for old binary market format; may have zero markets
    # when markets.yaml uses the bucket format for multi-outcome trading
    try:
        mapper = EventMapper("config/markets.yaml", model_weights=config["model_weights"])
    except Exception:
        mapper = None
        logger.info("EventMapper: no binary markets configured (using multi-outcome mode)")
    fair_prob_engine = FairProbEngine(config)
    multi_prob_engine = MultiOutcomeProbEngine(config)
    edge_detector = EdgeDetector(config)
    signal_generator = SignalGenerator(config)
    position_manager = PositionManager(config, db)
    risk_control = RiskControl(config, db)
    scanner = MarketScanner(config, mock_mode=mock_mode, clob_client=clob_client)
    multi_scanner = MultiMarketScanner(config)
    executor = Executor(config, db, dry_run=dry_run, clob_client=clob_client)
    reviewer = SettlementReview(db)
    series_tracker = SeriesTracker()
    bias_calibrator = BiasCalibrator()

    return {
        "db": db,
        "fetcher": fetcher,
        "wu_fetcher": wu_fetcher,
        "normalizer": normalizer,
        "mapper": mapper,
        "fair_prob_engine": fair_prob_engine,
        "multi_prob_engine": multi_prob_engine,
        "edge_detector": edge_detector,
        "signal_generator": signal_generator,
        "position_manager": position_manager,
        "risk_control": risk_control,
        "scanner": scanner,
        "multi_scanner": multi_scanner,
        "executor": executor,
        "reviewer": reviewer,
        "series_tracker": series_tracker,
        "bias_calibrator": bias_calibrator,
        "clob_client": clob_client,
    }


def load_bucket_config() -> list[dict]:
    with open("config/markets.yaml", "r") as f:
        markets_cfg = yaml.safe_load(f) or {}
    return markets_cfg.get("buckets", [])


def inject_wu_observation(
    forecast,
    wu_fetcher: WundergroundFetcher,
    city: str,
    max_hours_to_settlement: float = 8.0,
):
    """Inject a station observation only when we are near settlement."""
    if forecast.hours_to_settlement >= max_hours_to_settlement:
        return

    try:
        wu_temp = wu_fetcher.fetch_current_temp(city)
        if wu_temp is not None:
            forecast.latest_observation = wu_temp
            logger.info(f"WU observation: {wu_temp:.1f}°C (injected)")
    except Exception as e:
        logger.debug(f"WU fetch failed: {e}")


def get_latest_raw_data(db, event_spec) -> list[RawWeatherData]:
    """Get latest raw forecasts from DB for an event."""
    from datetime import datetime, timezone

    cursor = db.execute(
        """SELECT city, event_date, source, model_name, hourly_temps, fetched_at, raw_response
           FROM raw_forecasts
           WHERE city = ? AND event_date = ?
           ORDER BY fetched_at DESC""",
        (event_spec.city, event_spec.settlement_time_utc.date().isoformat()),
    )
    rows = cursor.fetchall()

    seen_models = set()
    results = []
    for row in rows:
        model = row["model_name"]
        if model not in seen_models:
            seen_models.add(model)
            fetched_at = row["fetched_at"]
            if isinstance(fetched_at, str):
                if "+" in fetched_at or fetched_at.endswith("Z"):
                    fetched_at = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
                else:
                    fetched_at = datetime.fromisoformat(fetched_at).replace(tzinfo=timezone.utc)
            results.append(RawWeatherData(
                city=row["city"],
                event_date=row["event_date"],
                source=row["source"],
                model_name=model,
                hourly_temps=json.loads(row["hourly_temps"]),
                fetched_at=fetched_at,
                raw_response=json.loads(row["raw_response"]) if row["raw_response"] else {},
            ))

    return results


def fetch_weather(modules):
    """Fetch forecast data for all active city events."""
    logger.info("=== Fetching weather data (all cities) ===")
    fetcher = modules["fetcher"]
    tracker = modules["series_tracker"]

    for city in city_registry.all_city_keys():
        try:
            event = tracker.find_latest_event(city)
            if not event:
                logger.debug(f"No active event for {city}, skipping weather fetch")
                continue

            end_date_str = event.get("endDate", "")
            if not end_date_str:
                continue

            settle_utc = parse_iso_datetime(end_date_str)
            if settle_utc is None:
                continue

            raw_data = fetcher.fetch_forecast(city, settle_utc.date())
            logger.info(f"Fetched {len(raw_data)} {city} model forecasts")
        except Exception as e:
            logger.error(f"Failed to fetch {city} weather: {e}")


def run_pipeline(modules):
    """L2-L7: Full trading pipeline."""
    logger.info("=== Running trading pipeline ===")
    db = modules["db"]
    normalizer = modules["normalizer"]
    mapper = modules["mapper"]
    fair_prob_engine = modules["fair_prob_engine"]
    edge_detector = modules["edge_detector"]
    signal_generator = modules["signal_generator"]
    position_manager = modules["position_manager"]
    scanner = modules["scanner"]
    executor = modules["executor"]

    for event_spec in mapper.get_all_events():
        try:
            raw_data = get_latest_raw_data(db, event_spec)
            if not raw_data:
                logger.warning(f"[{event_spec.market_id}] No raw data, skipping")
                continue

            forecast = normalizer.normalize(
                raw_data,
                event_spec.settlement_time_utc.date(),
                event_spec.settlement_time_utc,
            )

            fair_result = fair_prob_engine.estimate(forecast, event_spec)
            logger.info(f"[{event_spec.market_id}] fair_prob={fair_result.fair_prob:.3f}")

            market_ctx = scanner.get_market_context(event_spec.market_id)
            edge_result = edge_detector.detect(fair_result.fair_prob, market_ctx.current_price)
            logger.info(
                f"[{event_spec.market_id}] edge={edge_result.edge:.3f} "
                f"dir={edge_result.direction}"
            )

            signal = signal_generator.generate(edge_result, market_ctx)
            logger.info(f"[{event_spec.market_id}] signal={signal.level}")

            if signal.level != "NO_TRADE":
                order = position_manager.calculate_order(signal, market_ctx)
                if order:
                    result = executor.execute(order)
                    logger.info(f"[{event_spec.market_id}] order: {result['status']}")
                else:
                    logger.info(f"[{event_spec.market_id}] position limit reached")

        except Exception as e:
            logger.error(f"Pipeline error for {event_spec.market_id}: {e}")
            continue


def risk_check(modules):
    """Run risk checks for active multi-outcome bucket positions across all cities."""
    logger.info("=== Multi-outcome risk control check ===")
    fetcher = modules["fetcher"]
    wu_fetcher = modules["wu_fetcher"]
    normalizer = modules["normalizer"]
    multi_prob_engine = modules["multi_prob_engine"]
    risk_control = modules["risk_control"]
    multi_scanner = modules["multi_scanner"]
    executor = modules["executor"]

    positions = [
        pos
        for pos in executor.get_open_positions()
        if (pos.get("trade_meta") or {}).get("market_type") == "multi_outcome_bucket"
    ]
    if not positions:
        logger.info("No active bucket positions")
        return

    # Group by (city, event_slug, settlement_time)
    grouped_positions: dict[tuple[str, str, str], list[dict]] = {}
    for pos in positions:
        meta = pos.get("trade_meta") or {}
        event_slug = meta.get("event_slug")
        settlement_time = meta.get("settlement_time_utc")
        city = meta.get("city", "shanghai")
        if not event_slug or not settlement_time:
            logger.warning(f"[{pos['market_id']}] Missing event metadata, skipping risk check")
            continue
        grouped_positions.setdefault((city, event_slug, settlement_time), []).append(pos)

    for (city, event_slug, settlement_time_str), event_positions in grouped_positions.items():
        try:
            settle_utc = parse_iso_datetime(settlement_time_str)
            if settle_utc is None:
                logger.warning(f"[{city}/{event_slug}] Invalid settlement time, skipping risk check")
                continue

            snapshot = multi_scanner.fetch_snapshot(event_slug)
            if not snapshot:
                logger.warning(f"[{city}/{event_slug}] Missing live prices, skipping risk check")
                continue

            # Load bucket defs from per-city markets file
            markets_path = f"config/markets_{city}.yaml"
            try:
                with open(markets_path, "r") as f:
                    markets_cfg = yaml.safe_load(f) or {}
                bucket_defs = [
                    {"label": b["label"], "low": b.get("temp_low", 0), "high": b.get("temp_high", 0)}
                    for b in markets_cfg.get("buckets", [])
                ]
            except FileNotFoundError:
                bucket_defs = [
                    {"label": b["label"], "low": b.get("temp_low", 0), "high": b.get("temp_high", 0)}
                    for b in load_bucket_config()
                ]

            raw_data = fetcher.fetch_forecast(city, settle_utc.date())
            ensemble_maxes = []
            try:
                coords = city_registry.get_coordinates(city)
                ensemble_maxes = fetcher.fetch_ensemble(city, settle_utc.date(), coords)
            except Exception:
                pass
            forecast = normalizer.normalize(raw_data, settle_utc.date(), settle_utc, ensemble_maxes=ensemble_maxes or None)
            inject_wu_observation(forecast, wu_fetcher, city)
            fair_result = multi_prob_engine.estimate(forecast, bucket_defs)
            price_map = {bucket.label: bucket for bucket in snapshot.buckets}

            for pos in event_positions:
                meta = pos.get("trade_meta") or {}
                label = meta.get("label")
                live_bucket = price_map.get(label)
                if live_bucket is None:
                    logger.warning(f"[{pos['market_id']}] Bucket '{label}' missing in snapshot")
                    continue

                current_yes_prob = fair_result.bucket_probs.get(label, 0.0)
                current_side_price = side_price_from_yes_price(
                    live_bucket.yes_price,
                    pos["side"],
                    no_price=live_bucket.no_price,
                )
                position = {
                    "market_id": pos["market_id"],
                    "side": pos["side"],
                    "amount": pos["amount"],
                    "entry_price": pos["entry_price"],
                    "entry_fair_prob": (
                        pos.get("entry_fair_prob")
                        if pos.get("entry_fair_prob") is not None
                        else meta.get("entry_fair_prob", current_yes_prob)
                    ),
                }
                state = {
                    "current_price": current_side_price,
                    "spread": live_bucket.spread,
                    "hours_to_settlement": forecast.hours_to_settlement,
                }

                action = risk_control.check(position, state, current_yes_prob)

                if action.action != "HOLD":
                    logger.warning(
                        f"[{pos['market_id']}] Risk: {action.action} - {action.reason}"
                    )
                    if action.action == "CLOSE":
                        executor.close_position(pos, current_price=current_side_price)
                    elif action.action == "REDUCE":
                        executor.reduce_position(
                            pos,
                            action.reduce_pct,
                            current_price=current_side_price,
                        )
        except Exception as e:
            logger.error(f"Risk check error for {event_slug}: {e}")


def _check_settlement_gaps(modules, city: str = "shanghai"):
    """Alert if WU settlement data is missing for recent days."""
    from datetime import date, timedelta
    from src.utils.db import get_connection

    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT settle_date FROM bias_calibration WHERE city=? AND is_reference=1",
            (city,),
        ).fetchall()
        conn.close()

        existing_dates = {r["settle_date"] for r in rows}
        today = date.today()
        missing = []
        for i in range(1, 4):  # Check last 3 days
            d = (today - timedelta(days=i)).isoformat()
            if d not in existing_dates:
                missing.append(d)

        if missing:
            logger.warning(
                f"[{city}] SETTLEMENT ALERT: Missing WU data for {len(missing)} recent days: "
                f"{', '.join(missing)}."
            )
        if len(missing) >= 3:
            logger.error(
                f"[{city}] CRITICAL: No settlement data for 3+ consecutive days. "
                "WU data source may be down — check WU_API_KEY and network."
            )
    except Exception as e:
        logger.debug(f"[{city}] Settlement gap check failed: {e}")


def daily_review(modules):
    """L9: Daily settlement review + bias calibration for all cities."""
    logger.info("=== Daily settlement review (all cities) ===")
    reviewer = modules["reviewer"]
    stats = reviewer.get_cumulative_stats()
    logger.info(f"Cumulative stats: {stats}")

    from datetime import date, timedelta
    calibrator = BiasCalibrator()
    wu_fetcher = modules["wu_fetcher"]
    db = modules["db"]
    yesterday = date.today() - timedelta(days=1)

    for city in city_registry.all_city_keys():
        try:
            # Settlement alert: check last N days for missing settlements
            _check_settlement_gaps(modules, city)

            unit = city_registry.get_unit(city)
            unit_symbol = "°F" if unit == "fahrenheit" else "°C"

            wu_high = wu_fetcher.get_settlement_result(city, yesterday)
            if wu_high is None:
                logger.debug(f"[{city}] No WU settlement data for {yesterday}")
                continue

            # Get our raw forecast from DB
            cursor = db.execute(
                """SELECT model_name, hourly_temps FROM raw_forecasts
                   WHERE city = ? AND event_date = ?
                   ORDER BY fetched_at DESC""",
                (city, yesterday.isoformat()),
            )
            rows = cursor.fetchall()
            model_maxes = {}
            for row in rows:
                name = row["model_name"]
                if name in ("gfs", "icon", "ecmwf", "ensemble") and name not in model_maxes:
                    temps = json.loads(row["hourly_temps"])
                    model_maxes[name] = max(temps) if temps else None

            if not model_maxes:
                logger.debug(f"[{city}] No forecast data for {yesterday}")
                continue

            weights = config.get("model_weights", {"gfs": 0.35, "icon": 0.60, "ensemble": 0.05})
            total_w = sum(weights.get(m, 0) for m in model_maxes)
            if total_w > 0:
                models_used = sorted(model_maxes)
                raw_mean = sum(
                    weights.get(m, 0) * t
                    for m, t in model_maxes.items()
                ) / total_w

                # For °F cities, wu_high is already in °F; raw_mean is °C
                # Convert raw_mean to settlement unit for calibration comparison
                if unit == "fahrenheit":
                    raw_mean_unit = city_registry.c_to_f(raw_mean)
                else:
                    raw_mean_unit = raw_mean

                calibrator.record_settlement(
                    city,
                    yesterday,
                    wu_high,
                    raw_mean_unit,
                    source="live_replay",
                    notes=(
                        f"Auto-recorded from WU settlement ({unit_symbol}) "
                        f"using models={','.join(models_used)}."
                    ),
                )
                logger.info(
                    f"[{city}] Settlement recorded: {yesterday} WU={wu_high}{unit_symbol}, "
                    f"forecast_mean={raw_mean_unit:.1f}{unit_symbol}, "
                    f"residual={wu_high - raw_mean_unit:+.1f}{unit_symbol}"
                )

        except Exception as e:
            logger.warning(f"[{city}] Bias calibration failed: {e}")

    # Run auto-parameter optimization after recording all settlements
    _auto_optimize_parameters(modules)


def _auto_optimize_parameters(modules):
    """
    Auto-optimize model parameters from accumulated calibration data.

    Runs daily after settlement recording. Per-city optimization:
    1. Per-model bias (learned in MultiOutcomeProbEngine, cached 1h)
    2. Model weights in settings.yaml (based on debiased inverse-MAE)
    3. Base uncertainty σ (from debiased residual stdev)

    Uses the city with the most calibration data (typically shanghai) for
    global weight optimization. Per-city bias is handled by MultiOutcomeProbEngine.
    """
    import statistics as stats_mod
    from datetime import datetime, timezone

    logger.info("=== Auto-optimizing parameters ===")

    try:
        db = modules["db"]

        # Aggregate calibration data across all cities
        all_model_residuals = {"gfs": [], "icon": [], "ecmwf": [], "ensemble": []}
        total_cal_rows = 0

        for city in city_registry.all_city_keys():
            cal_rows = db.execute(
                """SELECT settle_date, wu_temp FROM bias_calibration
                   WHERE city = ? AND is_reference = 1
                   ORDER BY settle_date DESC LIMIT 14""",
                (city,),
            ).fetchall()

            if not cal_rows:
                continue

            total_cal_rows += len(cal_rows)
            actuals = {r["settle_date"]: r["wu_temp"] for r in cal_rows}

            # Get per-model forecasts for this city
            forecast_rows = db.execute(
                """SELECT event_date, model_name, hourly_temps
                   FROM raw_forecasts WHERE city = ?
                   ORDER BY event_date DESC, fetched_at DESC""",
                (city,),
            ).fetchall()

            model_maxes = {}
            seen = set()
            for row in forecast_rows:
                d, m = row["event_date"], row["model_name"]
                key = (d, m)
                if key in seen or m not in ("gfs", "icon", "ecmwf", "ensemble"):
                    continue
                seen.add(key)
                try:
                    temps = json.loads(row["hourly_temps"])
                    if temps:
                        temp_max_c = max(temps)
                        # Convert to settlement unit for comparison
                        if city_registry.is_fahrenheit(city):
                            temp_max_unit = city_registry.c_to_f(temp_max_c)
                        else:
                            temp_max_unit = temp_max_c
                        model_maxes.setdefault(d, {})[m] = temp_max_unit
                except Exception:
                    pass

            for date_str, actual in actuals.items():
                if date_str in model_maxes:
                    for m, temp in model_maxes[date_str].items():
                        if m in all_model_residuals:
                            all_model_residuals[m].append(actual - temp)

            logger.info(f"  [{city}] {len(cal_rows)} calibration days collected")

        if total_cal_rows < 5:
            logger.info(f"Not enough calibration data ({total_cal_rows}/5), skipping optimization")
            return

        # Compute debiased MAE per model (across all cities)
        model_debiased_mae = {}
        model_bias = {}
        for m, resids in all_model_residuals.items():
            if len(resids) >= 3:
                mean_bias = stats_mod.mean(resids)
                debiased_mae = stats_mod.mean([abs(r - mean_bias) for r in resids])
                model_debiased_mae[m] = max(0.1, debiased_mae)
                model_bias[m] = round(mean_bias, 2)
                logger.info(
                    f"  {m}: n={len(resids)}, bias={mean_bias:+.2f}, "
                    f"debiased_MAE={debiased_mae:.2f}"
                )

        if len(model_debiased_mae) < 2:
            logger.info("Not enough per-model data, skipping weight optimization")
            return

        # Compute optimal weights (inverse debiased MAE)
        inv_maes = {m: 1.0 / mae for m, mae in model_debiased_mae.items()}
        total_inv = sum(inv_maes.values())
        optimal_weights = {m: round(v / total_inv, 3) for m, v in inv_maes.items()}

        # Compute debiased blend stdev for sigma estimation
        blend_errors = []
        for city in city_registry.all_city_keys():
            cal_rows = db.execute(
                """SELECT settle_date, wu_temp FROM bias_calibration
                   WHERE city = ? AND is_reference = 1
                   ORDER BY settle_date DESC LIMIT 14""",
                (city,),
            ).fetchall()
            actuals = {r["settle_date"]: r["wu_temp"] for r in cal_rows}

            forecast_rows = db.execute(
                """SELECT event_date, model_name, hourly_temps
                   FROM raw_forecasts WHERE city = ?
                   ORDER BY event_date DESC, fetched_at DESC""",
                (city,),
            ).fetchall()
            model_maxes = {}
            seen = set()
            for row in forecast_rows:
                d, m = row["event_date"], row["model_name"]
                key = (d, m)
                if key in seen or m not in ("gfs", "icon", "ecmwf", "ensemble"):
                    continue
                seen.add(key)
                try:
                    temps = json.loads(row["hourly_temps"])
                    if temps:
                        temp_max_c = max(temps)
                        if city_registry.is_fahrenheit(city):
                            temp_max_unit = city_registry.c_to_f(temp_max_c)
                        else:
                            temp_max_unit = temp_max_c
                        model_maxes.setdefault(d, {})[m] = temp_max_unit
                except Exception:
                    pass

            for date_str, actual in actuals.items():
                if date_str in model_maxes:
                    w_sum = 0.0
                    blend = 0.0
                    for m, temp in model_maxes[date_str].items():
                        if m in optimal_weights and m in model_bias:
                            w = optimal_weights[m]
                            debiased_temp = temp + model_bias.get(m, 0)
                            blend += w * debiased_temp
                            w_sum += w
                    if w_sum > 0:
                        blend_errors.append(blend / w_sum - actual)

        if len(blend_errors) >= 3:
            blend_stdev = stats_mod.stdev(blend_errors)
            new_base_sigma = round(max(0.5, min(2.0, blend_stdev)), 1)
        else:
            new_base_sigma = 0.7

        # Update settings.yaml
        with open("config/settings.yaml", "r") as f:
            cfg = yaml.safe_load(f)

        old_weights = cfg.get("model_weights", {})
        cfg["model_weights"] = optimal_weights

        mo_cfg = cfg.setdefault("multi_outcome", {})
        old_sigma = mo_cfg.get("bucket_uncertainty_std", 1.2)
        mo_cfg["bucket_uncertainty_std"] = new_base_sigma

        with open("config/settings.yaml", "w") as f:
            yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        logger.info(
            f"Parameters optimized: weights {old_weights} → {optimal_weights}, "
            f"base_σ {old_sigma} → {new_base_sigma}, "
            f"per-model bias={model_bias}, "
            f"from {total_cal_rows} settlements across {len(city_registry.all_city_keys())} cities"
        )

        # Per-city auto-tuning: exponential-decay-weighted bias + σ
        calibrator = modules.get("bias_calibrator")
        if calibrator:
            for city in city_registry.all_city_keys():
                try:
                    tune_result = calibrator.auto_tune_params(city)
                    if tune_result and tune_result["applied"]:
                        logger.info(
                            f"  [{city}] auto_tune applied: "
                            f"bias {tune_result['current_bias']:+.2f} → {tune_result['suggested_bias']:+.2f}, "
                            f"σ {tune_result['current_std']:.2f} → {tune_result['suggested_std']:.2f}"
                        )
                except Exception as e:
                    logger.warning(f"  [{city}] auto_tune failed: {e}")

    except Exception as e:
        logger.warning(f"Auto-optimization failed: {e}")


def discover_markets(modules):
    """Auto-discover latest daily temperature markets for all cities."""
    logger.info("=== Auto-discovering daily markets (all cities) ===")
    tracker = modules["series_tracker"]

    for city in city_registry.all_city_keys():
        try:
            event = tracker.find_latest_event(city)
            if not event:
                logger.debug(f"No active {city} temperature event found")
                continue

            slug = event.get("slug", "unknown")
            logger.info(f"[{city}] Found event: {slug}")

            buckets = tracker.extract_buckets(event)
            if not buckets:
                logger.warning(f"[{city}] Could not extract buckets from event")
                continue

            # Per-city markets file
            markets_path = f"config/markets_{city}.yaml"
            tracker.update_markets_yaml(event, buckets, city=city, path=markets_path)

            # Also update the default markets.yaml with shanghai for backward compat
            if city == "shanghai":
                tracker.update_markets_yaml(event, buckets, city=city, path="config/markets.yaml")

            prices = tracker.get_live_prices(buckets)
            logger.info(f"[{city}] {len(buckets)} buckets, prices: {prices}")
        except Exception as e:
            logger.error(f"[{city}] Market discovery failed: {e}")


def run_multi_outcome_pipeline(modules):
    """
    Multi-outcome pipeline for all city temperature bucket markets.

    Loops over all cities in city_registry. Per city:
    1. Fetch live market prices (single Gamma API call)
    2. Fetch weather forecast from Open-Meteo
    3. Normalize (extract daily max per model)
    4. Compute fair probability per bucket (bias correction + dynamic σ)
    5. Detect edge, size via Kelly, and execute
    """
    logger.info("=== Multi-outcome pipeline (all cities) ===")
    from datetime import datetime, timezone

    for city in city_registry.all_city_keys():
        try:
            _run_city_pipeline(modules, city)
        except Exception as e:
            logger.error(f"[{city}] Pipeline error: {e}")


def _run_city_pipeline(modules, city: str):
    """Run the multi-outcome pipeline for a single city."""
    from datetime import datetime, timezone

    fetcher = modules["fetcher"]
    wu_fetcher = modules["wu_fetcher"]
    normalizer = modules["normalizer"]
    multi_prob = modules["multi_prob_engine"]
    tracker = modules["series_tracker"]
    executor = modules["executor"]
    timing = TimingStrategy(config)
    mo_cfg = config.get("multi_outcome", {})
    min_edge = mo_cfg.get("min_bucket_edge", 0.05)
    strong_edge = mo_cfg.get("strong_bucket_edge", 0.10)
    max_buckets = mo_cfg.get("max_concurrent_buckets", 4)
    per_bucket_max = mo_cfg.get("per_bucket_max_amount", 25)
    total_max = mo_cfg.get("total_multi_max_exposure", 80)
    kelly_cap = mo_cfg.get("kelly_cap", 0.25)
    capital = config["position"]["total_capital"]

    unit = city_registry.get_unit(city)
    unit_symbol = "°F" if unit == "fahrenheit" else "°C"

    # ── Step 1: Get live market prices (with circuit breaker) ──
    if not _breakers["polymarket"].allow_request():
        logger.warning(f"[{city}] Polymarket circuit breaker OPEN, skipping")
        return

    event = tracker.find_latest_event(city)
    if not event:
        logger.debug(f"[{city}] No active event, skipping pipeline")
        return

    _breakers["polymarket"].record_success()
    live_buckets = tracker.extract_buckets(event)
    if not live_buckets:
        logger.warning(f"[{city}] Could not extract buckets from event")
        return

    slug = event.get("slug", "unknown")
    end_date_str = event.get("endDate", "")
    logger.info(f"[{city}] Event: {slug}, {len(live_buckets)} buckets")

    # Parse settlement time
    if not end_date_str:
        logger.warning(f"[{city}] No end date in event")
        return
    settle_utc = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
    event_date = settle_utc.date()

    # ── Step 2: Fetch weather + ensemble (with circuit breaker) ──
    if not _breakers["open_meteo"].allow_request():
        logger.warning(f"[{city}] Open-Meteo circuit breaker OPEN, skipping")
        return

    try:
        raw_data = fetcher.fetch_forecast(city, event_date)
        _breakers["open_meteo"].record_success()
        logger.info(f"[{city}] Fetched {len(raw_data)} model forecasts")
    except Exception as e:
        _breakers["open_meteo"].record_failure()
        logger.error(f"[{city}] Weather fetch failed: {e}")
        return

    # Fetch ensemble for probability estimation (69 members)
    ensemble_maxes = []
    try:
        coords = city_registry.get_coordinates(city)
        ensemble_maxes = fetcher.fetch_ensemble(city, event_date, coords)
        if ensemble_maxes:
            logger.info(f"[{city}] Fetched {len(ensemble_maxes)} ensemble members")
    except Exception as e:
        logger.warning(f"[{city}] Ensemble fetch failed (non-fatal): {e}")

    # ── Step 3: Normalize (with ensemble) ──
    forecast = normalizer.normalize(raw_data, event_date, settle_utc, ensemble_maxes=ensemble_maxes or None)
    logger.info(
        f"[{city}] Models: {list(forecast.model_forecasts.keys())}, "
        f"hours_to_settle={forecast.hours_to_settlement:.1f}"
    )

    # Check timing — skip if too early or too late
    time_mult = timing.get_multiplier(forecast.hours_to_settlement)
    if time_mult <= 0:
        logger.info(
            f"[{city}] Timing skip: {forecast.hours_to_settlement:.1f}h to settlement "
            f"(multiplier={time_mult:.2f})"
        )
        return

    obs_start = mo_cfg.get("observation_blend_start_hours", 12)
    inject_wu_observation(forecast, wu_fetcher, city, max_hours_to_settlement=obs_start)

    # ── Step 4: Compute fair probabilities ──
    bucket_defs = [
        {"label": lb["label"], "low": lb.get("temp_low", 0), "high": lb.get("temp_high", 0)}
        for lb in live_buckets
    ]
    fair_result = multi_prob.estimate(forecast, bucket_defs)
    logger.info(
        f"[{city}] Fair probs: μ={fair_result.weighted_mean_temp:.1f}{unit_symbol}, "
        f"σ={fair_result.uncertainty_std:.2f}, conf={fair_result.confidence:.0%}"
    )

    # ── Step 5: Edge detection + Kelly sizing ──
    signals = []
    for lb in live_buckets:
        label = lb["label"]
        fair_p = fair_result.bucket_probs.get(label, 0)
        market_price = lb["yes_price"]

        if market_price <= 0.001:
            continue

        edge = fair_p - market_price

        if abs(edge) < min_edge:
            continue

        direction = "BUY_YES" if edge > 0 else "BUY_NO"
        strength = "STRONG" if abs(edge) >= strong_edge else "LEAN"

        # Kelly fraction
        if direction == "BUY_YES":
            odds = (1.0 / market_price) - 1.0
            kelly = (fair_p * odds - (1 - fair_p)) / odds if odds > 0 else 0
        else:
            no_price = lb.get("no_price", 1.0 - market_price)
            no_fair = 1.0 - fair_p
            odds = (1.0 / no_price) - 1.0 if no_price > 0 else 0
            kelly = (no_fair * odds - fair_p) / odds if odds > 0 else 0

        kelly = max(0, min(kelly_cap, kelly))
        # Scale by timing multiplier (1.0 at sweet spot, <1.0 at edges)
        amount = min(kelly * capital * fair_result.confidence * time_mult, per_bucket_max)

        signals.append({
            "bucket": lb,
            "label": label,
            "fair": fair_p,
            "market": market_price,
            "edge": edge,
            "direction": direction,
            "strength": strength,
            "amount": amount,
        })

        logger.info(
            f"  [{city}] {label}: {direction} edge={edge:+.2%} fair={fair_p:.1%} "
            f"mkt={market_price:.1%} ${amount:.1f} [{strength}]"
        )

    if not signals:
        logger.info(f"[{city}] No tradeable signals found")
        return

    # Sort by absolute edge, take top N
    signals.sort(key=lambda s: -abs(s["edge"]))
    signals = signals[:max_buckets]

    # Enforce total exposure limit against existing open exposure too
    existing_total_exposure = executor.get_current_exposure(city=city)
    total_exposure = 0.0
    for sig in signals:
        side = "YES" if sig["direction"] == "BUY_YES" else "NO"
        market_id = build_bucket_market_id(slug, sig["label"], city)
        opposite_side = "NO" if side == "YES" else "YES"
        current_bucket_exposure = executor.get_current_exposure(
            market_id=market_id,
            side=side,
        )
        opposite_bucket_exposure = executor.get_current_exposure(
            market_id=market_id,
            side=opposite_side,
        )
        if opposite_bucket_exposure > 0:
            logger.info(
                f"  [{city}] SKIP: {sig['label']} has open opposite-side exposure ${opposite_bucket_exposure:.2f}"
            )
            sig["amount"] = 0.0
            continue

        remaining_total = total_max - existing_total_exposure - total_exposure
        remaining_bucket = per_bucket_max - current_bucket_exposure
        remaining = min(remaining_total, remaining_bucket)
        if remaining <= 0:
            sig["amount"] = 0.0
            break
        sig["amount"] = min(sig["amount"], remaining)
        total_exposure += sig["amount"]
        logger.info(
            f"  [{city}] TRADE: {sig['direction']} {sig['label']} ${sig['amount']:.1f} "
            f"(edge={sig['edge']:+.2%})"
        )

    # ── Step 6: Execute ──
    from src.data.schemas import Order
    for sig in signals:
        bucket = sig["bucket"]
        min_order_size = bucket.get("min_order_size", 5.0)
        if sig["amount"] < min_order_size:
            continue
        side = "YES" if sig["direction"] == "BUY_YES" else "NO"
        price = (
            sig["market"]
            if sig["direction"] == "BUY_YES"
            else bucket.get("no_price", 1 - sig["market"])
        )
        market_id = build_bucket_market_id(slug, sig["label"], city)
        metadata = build_bucket_trade_meta(
            city=city,
            event_slug=slug,
            settlement_time_utc=settle_utc,
            bucket=bucket,
            fair_prob=sig["fair"],
            yes_price=sig["market"],
            no_price=bucket.get("no_price"),
        )
        order = Order(
            market_id=market_id,
            side=side,
            amount=sig["amount"],
            price=price,
        )
        try:
            result = executor.execute(order, metadata=metadata)
            logger.info(f"  [{city}] Executed: {sig['label']} → {result.get('status', 'unknown')}")
        except Exception as e:
            logger.error(f"  [{city}] Execution failed for {sig['label']}: {e}")


def cmd_search(modules):
    """Search for weather markets on Polymarket."""
    scanner = modules["scanner"]
    logger.info("Searching Polymarket for weather markets...")
    results = scanner.search_weather_markets()

    if not results:
        print("\n  No weather markets found on Polymarket right now.")
        print("  The system will use MOCK mode for market prices.")
        print("  Weather markets appear seasonally (summer heat waves, winter storms).")
        print("\n  You can still run the system in mock mode to test the pipeline:")
        print("    python3 main.py --once")
        return

    print(f"\n  Found {len(results)} weather market(s):\n")
    for i, m in enumerate(results, 1):
        print(f"  {i}. {m['question']}")
        print(f"     condition_id: {m['condition_id']}")
        print(f"     YES price: {m['prices']}")
        print(f"     Volume: ${m['volume']:,.0f}" if m['volume'] else "")
        print()


def cmd_status(modules):
    """Show current positions and stats."""
    executor = modules["executor"]
    reviewer = modules["reviewer"]

    positions = executor.get_open_positions()
    stats = reviewer.get_cumulative_stats()

    print("\n=== Current Positions ===")
    if positions:
        for p in positions:
            print(f"  {p['market_id']}: {p['side']} ${p['amount']:.2f} @ {p['entry_price']:.3f}")
    else:
        print("  No open positions.")

    print(f"\n=== Cumulative Stats ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print()


def graceful_shutdown(signum, frame):
    logger.info("Shutting down gracefully...")
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description="Weather Quant Trading System")
    parser.add_argument("--once", action="store_true", help="Run one cycle then exit")
    parser.add_argument("--search", action="store_true", help="Search for weather markets")
    parser.add_argument("--status", action="store_true", help="Show positions and stats")
    parser.add_argument("--web", action="store_true", help="Start web dashboard on port 8000")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    logger.info("Weather Quant System starting...")
    modules = init_system()

    if args.search:
        cmd_search(modules)
        return

    if args.status:
        cmd_status(modules)
        return

    # Start web dashboard if requested
    if args.web:
        import threading
        import uvicorn
        from src.web.app import create_app
        app = create_app(modules, config)
        web_thread = threading.Thread(
            target=uvicorn.run,
            args=(app,),
            kwargs={"host": "0.0.0.0", "port": 8000, "log_level": "warning"},
            daemon=True,
        )
        web_thread.start()
        logger.info("Web dashboard started at http://localhost:8000")

    # Auto-discover latest markets for all cities
    discover_markets(modules)

    # Run one cycle (multi-outcome pipeline for all cities)
    run_multi_outcome_pipeline(modules)

    if args.once:
        logger.info("Single cycle complete. Exiting.")
        return

    # Start scheduler for 24/7 operation
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()
    # Market discovery: every 4 hours (new daily markets appear ~midnight ET)
    scheduler.add_job(lambda: discover_markets(modules), "interval", hours=4, id="discover")
    # Weather fetch: every 1 hour (more frequent for better data collection)
    scheduler.add_job(lambda: fetch_weather(modules), "interval", hours=1, id="fetch")
    # Multi-outcome pipeline: every 20 minutes (near settlement, need fresh signals)
    scheduler.add_job(lambda: run_multi_outcome_pipeline(modules), "interval", minutes=20, id="multi_pipeline")
    # Risk check: every 15 minutes
    scheduler.add_job(lambda: risk_check(modules), "interval", minutes=15, id="risk")
    # Daily review + auto-optimization: 13:00 UTC (21:00 Shanghai, after settlement)
    scheduler.add_job(lambda: daily_review(modules), "cron", hour=13, minute=5, id="review")
    # Second calibration run at 6am UTC for any missed settlements
    scheduler.add_job(lambda: daily_review(modules), "cron", hour=6, minute=30, id="review_morning")

    cities_str = ", ".join(city_registry.all_city_keys())
    logger.info(
        f"Scheduler started. Running 24/7 for {len(city_registry.all_city_keys())} cities: {cities_str}\n"
        "  fetch_weather:      every 1h (all cities)\n"
        "  multi_pipeline:     every 20min (all cities)\n"
        "  discover_markets:   every 4h (all cities)\n"
        "  risk_check:         every 15min\n"
        "  daily_review+optim: 13:05 UTC + 06:30 UTC (all cities)"
    )
    scheduler.start()


if __name__ == "__main__":
    main()
