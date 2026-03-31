"""
weather-quant/main_shanghai.py

Shanghai Multi-Outcome Temperature Market Arbitrage System.

Dedicated entry point for trading the Polymarket Shanghai daily high
temperature bucket market (11 outcomes: ≤11°C through ≥21°C).

Usage:
    python3 main_shanghai.py --scan         # Discover available Shanghai markets
    python3 main_shanghai.py --analyze      # Show fair prob vs market prices
    python3 main_shanghai.py --once         # Run one trading cycle (dry run)
    python3 main_shanghai.py                # Continuous 24/7 mode

Environment:
    config/.env controls credentials and mode:
    - LIVE_TRADING=false  → dry run (default, safe)
    - LIVE_TRADING=true   → real money trading
"""

import argparse
import os
import signal
import sys
from datetime import datetime, timezone

import yaml
from dotenv import load_dotenv

from src.data.fetcher_openmeteo import OpenMeteoFetcher
from src.data.fetcher_wunderground import WundergroundFetcher
from src.data.normalizer import Normalizer
from src.data.schemas import BucketSignal, Order
from src.engine.multi_outcome_edge import MultiOutcomeEdgeDetector
from src.engine.multi_outcome_prob import MultiOutcomeProbEngine
from src.trading.executor import Executor
from src.trading.multi_market_scanner import MultiMarketScanner
from src.trading.multi_outcome_trade import (
    build_bucket_market_id,
    build_bucket_trade_meta,
)
from src.utils.db import init_db
from src.utils.logger import logger


def create_clob_client():
    """Create authenticated Polymarket CLOB client if credentials are available."""
    private_key = os.getenv("PRIVATE_KEY")
    api_key = os.getenv("POLY_API_KEY")
    api_secret = os.getenv("POLY_API_SECRET")
    api_passphrase = os.getenv("POLY_API_PASSPHRASE")
    funder = os.getenv("FUNDER")

    if not all([private_key, api_key, api_secret, api_passphrase]):
        logger.warning("Polymarket credentials not found in .env")
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

        ok = client.get_ok()
        if ok == "OK":
            logger.info(f"CLOB connected. Address: {client.get_address()}")
            return client
        else:
            logger.error(f"CLOB connection check failed: {ok}")
            return None
    except Exception as e:
        logger.error(f"Failed to create CLOB client: {e}")
        return None


def _get_bucket_config(label: str) -> dict | None:
    """Find bucket config from markets.yaml by label."""
    for b in markets_config.get("buckets", []):
        if b["label"] == label:
            return b
    return None

# Load environment
load_dotenv("config/.env")

# Load config
with open("config/settings.yaml", "r") as f:
    config = yaml.safe_load(f)

# Load markets config
with open("config/markets.yaml", "r") as f:
    markets_config = yaml.safe_load(f)


def get_event_slug() -> str:
    """Get the event slug from markets.yaml config."""
    return markets_config.get("event_slug", "")


def get_coordinates() -> tuple[float, float]:
    """Get Shanghai coordinates from markets config."""
    coords = markets_config.get("coordinates", [31.1434, 121.8052])
    return (coords[0], coords[1])


def get_buckets_from_config() -> list[dict]:
    """Extract bucket definitions from markets.yaml."""
    buckets = []
    for b in markets_config.get("buckets", []):
        buckets.append({
            "label": b["label"],
            "low": b["temp_low"],
            "high": b["temp_high"],
        })
    return buckets


def cmd_scan():
    """Discover available Shanghai temperature markets on Polymarket."""
    scanner = MultiMarketScanner(config)
    events = scanner.discover_shanghai_events()

    if not events:
        print("\n  ❌ No active Shanghai temperature markets found.")
        print("  Markets refresh daily — check back later.\n")
        return

    print(f"\n  🔍 Found {len(events)} Shanghai temperature event(s):\n")
    for i, e in enumerate(events, 1):
        print(f"  {i}. {e['title']}")
        print(f"     Slug: {e['slug']}")
        print(f"     End:  {e['end_date']}")
        print(f"     Vol:  ${e['volume']:,.0f}  |  Liq: ${e['liquidity']:,.0f}")
        print(f"     Sub-markets: {e['num_markets']}")
        print()


def cmd_analyze():
    """Show fair probability vs market prices with edge analysis."""
    logger.info("=== Analyzing Shanghai Temperature Market ===")

    # Step 1: Fetch weather forecast
    fetcher = OpenMeteoFetcher(config["data_sources"])
    event_date_str = markets_config.get("event_end_date", "")[:10]

    from datetime import date as date_type
    event_date = date_type.fromisoformat(event_date_str)
    coords = get_coordinates()

    print(f"\n  📡 Fetching weather forecast for Shanghai ({coords[0]}, {coords[1]})...")
    raw_data = fetcher.fetch_forecast("shanghai", event_date, coords)
    print(f"  ✅ Got {len(raw_data)} model forecasts\n")

    # Step 2: Normalize
    normalizer = Normalizer()
    settlement_time = datetime.fromisoformat(
        markets_config["event_end_date"].replace("Z", "+00:00")
    )
    forecast = normalizer.normalize(raw_data, event_date, settlement_time)

    # Step 3: Compute fair probabilities
    prob_engine = MultiOutcomeProbEngine(config)
    buckets = get_buckets_from_config()
    fair_result = prob_engine.estimate(forecast, buckets)

    # Step 4: Fetch market prices
    scanner = MultiMarketScanner(config)
    slug = get_event_slug()
    snapshot = scanner.fetch_snapshot(slug)

    if not snapshot:
        print("  ❌ Failed to fetch market prices. Using model-only analysis.\n")
        print_fair_probs_only(fair_result)
        return

    # Step 5: Edge detection
    edge_detector = MultiOutcomeEdgeDetector(config)
    edge_result = edge_detector.detect(fair_result, snapshot)

    # Step 6: Print analysis
    print_full_analysis(fair_result, snapshot, edge_result)

    # Step 7: Generate signals
    signals = edge_detector.generate_signals(
        edge_result,
        total_capital=config["position"]["total_capital"],
        confidence=fair_result.confidence,
    )

    if signals:
        print_signals(signals)
    else:
        print("  ℹ️  No actionable signals at current prices.\n")


def cmd_trade_once(clob_client=None):
    """Run one full trading cycle."""
    logger.info("=== Shanghai Trading Cycle ===")

    db = init_db()
    executor = Executor(config, db, dry_run=True, clob_client=None)
    live_trading = os.getenv("LIVE_TRADING", "false").lower() == "true"
    dry_run = not live_trading or clob_client is None
    executor.dry_run = dry_run
    executor.clob_client = clob_client
    mode_str = "LIVE" if not dry_run else "DRY RUN"
    print(f"\n  Mode: {mode_str}")

    # Step 1: Fetch weather
    fetcher = OpenMeteoFetcher(config["data_sources"])
    wu_fetcher = WundergroundFetcher()
    event_date_str = markets_config.get("event_end_date", "")[:10]
    from datetime import date as date_type
    event_date = date_type.fromisoformat(event_date_str)
    coords = get_coordinates()

    print(f"  Fetching weather data...")
    raw_data = fetcher.fetch_forecast("shanghai", event_date, coords)

    # Step 2: Normalize
    normalizer = Normalizer()
    settlement_time = datetime.fromisoformat(
        markets_config["event_end_date"].replace("Z", "+00:00")
    )
    forecast = normalizer.normalize(raw_data, event_date, settlement_time)
    if forecast.hours_to_settlement < 8:
        try:
            wu_temp = wu_fetcher.fetch_current_temp("shanghai")
            if wu_temp is not None:
                forecast.latest_observation = wu_temp
                logger.info(f"WU observation: {wu_temp:.1f}°C (injected)")
        except Exception as e:
            logger.debug(f"WU fetch failed: {e}")

    # Step 3: Fair probabilities
    prob_engine = MultiOutcomeProbEngine(config)
    buckets = get_buckets_from_config()
    fair_result = prob_engine.estimate(forecast, buckets)

    # Step 4: Market prices
    scanner = MultiMarketScanner(config)
    slug = get_event_slug()
    snapshot = scanner.fetch_snapshot(slug)

    if not snapshot:
        logger.error("Cannot fetch market prices. Aborting trade cycle.")
        return

    # Step 5: Edge detection
    edge_detector = MultiOutcomeEdgeDetector(config)
    edge_result = edge_detector.detect(fair_result, snapshot)

    # Step 6: Generate signals
    signals = edge_detector.generate_signals(
        edge_result,
        total_capital=config["position"]["total_capital"],
        confidence=fair_result.confidence,
    )

    # Step 7: Execute (or dry run)
    if not signals:
        print("  No actionable signals. Holding.\n")
        return

    print_signals(signals)

    existing_total_exposure = executor.get_current_exposure(city="shanghai")
    per_bucket_max = config["multi_outcome"].get("per_bucket_max_amount", 25)
    total_max = config["multi_outcome"].get("total_multi_max_exposure", 80)

    for sig in signals:
        bucket_cfg = _get_bucket_config(sig.label)
        if not bucket_cfg:
            logger.warning(f"No bucket config for {sig.label}, skipping")
            continue

        side = "YES" if sig.direction == "BUY_YES" else "NO"
        price = sig.market_price if side == "YES" else round(1.0 - sig.market_price, 4)
        market_id = build_bucket_market_id(get_event_slug(), sig.label, "shanghai")
        opposite_side = "NO" if side == "YES" else "YES"
        current_bucket_exposure = executor.get_current_exposure(
            market_id=market_id,
            side=side,
        )
        opposite_exposure = executor.get_current_exposure(
            market_id=market_id,
            side=opposite_side,
        )
        if opposite_exposure > 0:
            logger.info(
                f"Skipping {sig.label}: opposite-side exposure ${opposite_exposure:.2f} already open"
            )
            continue

        amount = min(
            sig.amount,
            per_bucket_max - current_bucket_exposure,
            total_max - existing_total_exposure,
        )
        if amount < bucket_cfg.get("min_order_size", 5.0):
            continue

        metadata = build_bucket_trade_meta(
            city="shanghai",
            event_slug=get_event_slug(),
            settlement_time_utc=settlement_time,
            bucket=bucket_cfg,
            fair_prob=sig.fair_prob,
            yes_price=sig.market_price,
        )
        order = Order(
            market_id=market_id,
            side=side,
            amount=round(amount, 2),
            price=price,
        )

        try:
            result = executor.execute(order, metadata=metadata)
            logger.info(f"[{mode_str}] {sig.label}: {result['status']}")
            existing_total_exposure += amount
        except Exception as e:
            logger.error(f"[{mode_str}] Failed to execute {sig.label}: {e}")

    print(f"  {'='*60}")
    print(f"  Cycle complete. {len(signals)} signal(s) processed.\n")


# ── Display helpers ──────────────────────────────────────────────────────

def print_fair_probs_only(fair_result):
    """Print model-only fair probabilities."""
    print(f"  {'='*60}")
    print(f"  WEATHER MODEL FAIR PROBABILITIES")
    print(f"  Weighted Mean: {fair_result.weighted_mean_temp:.1f}°C")
    print(f"  Uncertainty σ: {fair_result.uncertainty_std:.2f}°C")
    print(f"  Confidence:    {fair_result.confidence:.0%}")
    print(f"  {'='*60}")
    print(f"  {'Bucket':<20} {'Fair Prob':>10}")
    print(f"  {'─'*32}")

    for label, prob in sorted(
        fair_result.bucket_probs.items(),
        key=lambda x: -x[1],
    ):
        bar = "█" * int(prob * 40)
        print(f"  {label:<20} {prob:>9.1%}  {bar}")

    print(f"  {'─'*32}")
    print(f"  Models: {fair_result.model_forecasts}")
    print()


def print_full_analysis(fair_result, snapshot, edge_result):
    """Print comparison of fair prob vs market price with edge heatmap."""
    print(f"\n  {'='*78}")
    print(f"  SHANGHAI TEMPERATURE MARKET — EDGE ANALYSIS")
    print(f"  Weighted Mean: {fair_result.weighted_mean_temp:.1f}°C  |  "
          f"σ: {fair_result.uncertainty_std:.2f}°C  |  "
          f"Confidence: {fair_result.confidence:.0%}")
    print(f"  Market Price Sum: {snapshot.total_price_sum:.4f}  |  "
          f"Sum Gap: {edge_result.sum_to_one_gap:+.4f}")
    print(f"  {'='*78}")
    print(
        f"  {'Bucket':<18} {'Fair':>7} {'Market':>7} {'Edge':>7} "
        f"{'Kelly':>7} {'Signal':>10} {'Vis':>5}"
    )
    print(f"  {'─'*74}")

    for be in edge_result.bucket_edges:
        # Color coding
        if be.direction == "BUY_YES":
            signal_str = "🟢 BUY"
            edge_str = f"+{be.edge:.3f}"
        elif be.direction == "BUY_NO":
            signal_str = "🔴 SELL"
            edge_str = f"{be.edge:.3f}"
        else:
            signal_str = "⚪ —"
            edge_str = f"{be.edge:+.3f}"

        # Visual edge bar
        bar_size = min(5, int(abs(be.edge) * 50))
        if be.edge > 0:
            vis = "▲" * bar_size
        elif be.edge < 0:
            vis = "▼" * bar_size
        else:
            vis = "—"

        print(
            f"  {be.label:<18} {be.fair_prob:>6.1%} {be.market_price:>7.3f} "
            f"{edge_str:>7} {be.kelly_fraction:>6.3f} {signal_str:>10} {vis:>5}"
        )

    print(f"  {'─'*74}")
    print(
        f"  Models: "
        + ", ".join(f"{k}={v:.1f}°C" for k, v in fair_result.model_forecasts.items())
    )
    print()


def print_signals(signals: list[BucketSignal]):
    """Print actionable trading signals."""
    print(f"\n  {'='*60}")
    print(f"  📊 TRADING SIGNALS ({len(signals)})")
    print(f"  {'='*60}")

    total_amount = 0
    for i, sig in enumerate(signals, 1):
        emoji = "🟢" if sig.direction == "BUY_YES" else "🔴"
        print(
            f"  {i}. {emoji} {sig.direction} {sig.label}  "
            f"${sig.amount:.2f}  edge={sig.edge:+.3f}  "
            f"fair={sig.fair_prob:.1%} vs mkt={sig.market_price:.3f}"
        )
        total_amount += sig.amount

    print(f"  {'─'*60}")
    print(f"  Total exposure: ${total_amount:.2f}")
    print(f"  {'='*60}\n")


# ── Entry point ──────────────────────────────────────────────────────────

def graceful_shutdown(signum, frame):
    logger.info("Shutting down gracefully...")
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description="Shanghai Multi-Outcome Weather Arbitrage System"
    )
    parser.add_argument("--scan", action="store_true", help="Discover Shanghai markets")
    parser.add_argument("--analyze", action="store_true", help="Show fair prob vs market analysis")
    parser.add_argument("--once", action="store_true", help="Run one trading cycle")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    print("\n  🌡️  Shanghai Weather Arbitrage System")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    slug = get_event_slug()
    print(f"  Market: {slug}")
    print()

    if args.scan:
        cmd_scan()
        return

    if args.analyze:
        cmd_analyze()
        return

    # Create CLOB client for live trading
    clob_client = None
    live_trading = os.getenv("LIVE_TRADING", "false").lower() == "true"
    if live_trading:
        clob_client = create_clob_client()
        if clob_client:
            print(f"  CLOB connected: {clob_client.get_address()}")
        else:
            print("  WARNING: LIVE_TRADING=true but no CLOB client. Using DRY RUN.")

    if args.once:
        cmd_trade_once(clob_client)
        return

    # Continuous mode
    print("  Starting continuous trading mode...")
    print("  Schedule: analyze every 30 min")
    print("  Press Ctrl+C to stop.\n")

    from apscheduler.schedulers.blocking import BlockingScheduler
    scheduler = BlockingScheduler()

    # First run immediately
    cmd_trade_once(clob_client)

    # Then schedule
    scheduler.add_job(lambda: cmd_trade_once(clob_client), "interval", minutes=30, id="trade")
    logger.info("Scheduler started. Running 24/7.")
    scheduler.start()


if __name__ == "__main__":
    main()
