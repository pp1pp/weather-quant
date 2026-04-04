"""
Parameter Optimization via Grid Search on Historical Data.

Simulates trading over the last 14 days with different parameter combinations
and finds the configuration that maximizes Sharpe ratio and PnL.

Optimizes:
  - min_bucket_edge: minimum edge to trigger a trade (3-10%)
  - kelly_cap: max Kelly fraction per position (0.1-0.4)
  - per_bucket_max_amount: max dollar amount per bucket ($10-$50)
  - alpha (market fusion weight): model vs market blend (0.3-0.8)
  - gamma (probability calibration): power-law softening (0.7-1.0)
"""

import itertools
import json
import math
import sqlite3
import statistics
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.schemas import NormalizedForecast
from src.engine.multi_outcome_prob import MultiOutcomeProbEngine
from src.utils.db import get_connection
from src.utils.logger import logger


def load_historical_data(city: str = "shanghai", days: int = 14):
    """Load settlement actuals, model forecasts, and market prices."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row

    # Settlement actuals
    cal_rows = conn.execute(
        """SELECT settle_date, wu_temp, forecast_mean
           FROM bias_calibration
           WHERE city = ? AND is_reference = 1
           ORDER BY settle_date DESC LIMIT ?""",
        (city, days),
    ).fetchall()
    actuals = {r["settle_date"]: r["wu_temp"] for r in cal_rows}

    # Model forecasts per date
    forecast_rows = conn.execute(
        """SELECT event_date, model_name, hourly_temps, fetched_at
           FROM raw_forecasts
           WHERE city = ?
           ORDER BY event_date DESC, fetched_at DESC""",
        (city,),
    ).fetchall()

    model_maxes: dict[str, dict[str, float]] = {}
    seen: set[tuple[str, str]] = set()
    for row in forecast_rows:
        d, m = row["event_date"], row["model_name"]
        key = (d, m)
        if key in seen or m not in ("gfs", "icon"):
            continue
        seen.add(key)
        try:
            temps = json.loads(row["hourly_temps"])
            if temps:
                model_maxes.setdefault(d, {})[m] = max(temps)
        except Exception:
            pass

    # Weather factors
    wf_rows = conn.execute(
        """SELECT event_date, AVG(mean_cloud_cover) as cloud,
                  AVG(max_wind_speed) as wind, AVG(total_precipitation) as precip
           FROM weather_factors
           WHERE city = ? AND model_name IN ('gfs', 'icon')
           GROUP BY event_date""",
        (city,),
    ).fetchall()
    weather = {r["event_date"]: {"cloud_cover": r["cloud"], "max_wind_speed": r["wind"],
                                  "total_precipitation": r["precip"]} for r in wf_rows}

    conn.close()
    return actuals, model_maxes, weather


def simulate_trading(
    actuals: dict,
    model_maxes: dict,
    weather: dict,
    config: dict,
    min_edge: float,
    kelly_cap: float,
    per_bucket_max: float,
    alpha: float,
    gamma: float,
    capital: float = 200.0,
) -> dict:
    """Simulate trading over historical data and compute PnL metrics."""

    engine = MultiOutcomeProbEngine(config)

    daily_pnl = []
    total_trades = 0
    wins = 0
    losses = 0

    for date_str, wu_actual in sorted(actuals.items()):
        if date_str not in model_maxes:
            continue

        models = model_maxes[date_str]
        if "gfs" not in models:
            continue

        # Build forecast
        from datetime import datetime, timezone
        from src.data.schemas import WeatherFactors
        wf_raw = weather.get(date_str, {})
        wf_obj = None
        if wf_raw:
            wf_model = WeatherFactors(
                mean_cloud_cover=wf_raw.get("cloud_cover", 50),
                max_wind_speed=wf_raw.get("max_wind_speed", 10),
                total_precipitation=wf_raw.get("total_precipitation", 0),
            )
            wf_obj = {"gfs": wf_model, "icon": wf_model}
        forecast = NormalizedForecast(
            city="shanghai",
            event_date=date.fromisoformat(date_str),
            model_forecasts=models,
            hours_to_settlement=18.0,
            weather_factors=wf_obj,
            ensemble_maxes=[],
            latest_observation=None,
            updated_at=datetime.now(timezone.utc),
        )

        try:
            result = engine.estimate(forecast)
        except Exception:
            continue

        # Apply custom gamma calibration
        probs = {}
        for label, p in result.bucket_probs.items():
            probs[label] = p ** gamma if p > 0 else 0
        total_p = sum(probs.values())
        if total_p > 0:
            probs = {k: v / total_p for k, v in probs.items()}

        # Simulate market prices as slightly noisy version of actuals
        # (In reality we'd use historical market snapshots)
        # Use a simple heuristic: market is centered near actual with some spread
        actual_bucket = round(wu_actual)

        # Simulate trades
        day_pnl = 0.0
        for label, model_prob in probs.items():
            # Parse bucket temperature
            try:
                if "below" in label:
                    bucket_temp = int(label.split("°")[0])
                elif "higher" in label:
                    bucket_temp = int(label.split("°")[0])
                else:
                    bucket_temp = int(label.split("°")[0])
            except (ValueError, IndexError):
                continue

            # Simulate market price: higher near actual, lower further away
            dist = abs(bucket_temp - wu_actual)
            if dist == 0:
                sim_market = 0.55  # Market roughly right
            elif dist == 1:
                sim_market = 0.20
            elif dist == 2:
                sim_market = 0.08
            else:
                sim_market = 0.02

            # Fuse model with market
            fused_prob = alpha * model_prob + (1 - alpha) * sim_market
            edge = fused_prob - sim_market

            if abs(edge) < min_edge:
                continue

            # Kelly sizing
            if edge > 0 and sim_market > 0:
                odds = (1.0 / sim_market) - 1.0
                kelly = max(0, min(kelly_cap, (fused_prob * odds - (1 - fused_prob)) / odds))
                amount = min(per_bucket_max, kelly * capital)
                if amount < 1.0:
                    continue
                shares = amount / sim_market
                # Did we win?
                if bucket_temp == actual_bucket:
                    profit = shares * (1.0 - sim_market)
                    day_pnl += profit
                    wins += 1
                else:
                    day_pnl -= amount
                    losses += 1
                total_trades += 1

            elif edge < 0 and sim_market > 0.02:
                no_price = 1.0 - sim_market
                no_fair = 1.0 - fused_prob
                odds = (1.0 / no_price) - 1.0
                kelly = max(0, min(kelly_cap, (no_fair * odds - fused_prob) / odds))
                amount = min(per_bucket_max, kelly * capital)
                if amount < 1.0:
                    continue
                shares = amount / no_price
                if bucket_temp != actual_bucket:
                    profit = shares * (1.0 - no_price)
                    day_pnl += profit
                    wins += 1
                else:
                    day_pnl -= amount
                    losses += 1
                total_trades += 1

        daily_pnl.append(day_pnl)

    if not daily_pnl:
        return {"sharpe": -99, "total_pnl": 0, "trades": 0, "win_rate": 0}

    mean_pnl = statistics.mean(daily_pnl)
    std_pnl = statistics.stdev(daily_pnl) if len(daily_pnl) > 1 else 1.0
    sharpe = (mean_pnl / std_pnl * math.sqrt(365)) if std_pnl > 0 else 0
    total_pnl = sum(daily_pnl)
    win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0

    return {
        "sharpe": round(sharpe, 2),
        "total_pnl": round(total_pnl, 2),
        "mean_daily_pnl": round(mean_pnl, 2),
        "trades": total_trades,
        "win_rate": round(win_rate * 100, 1),
        "max_drawdown": round(min(daily_pnl), 2),
    }


def main():
    import yaml

    config_path = Path(__file__).resolve().parent.parent / "config" / "settings.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    print("Loading historical data...")
    actuals, model_maxes, weather = load_historical_data()
    print(f"  {len(actuals)} settlement days, {len(model_maxes)} forecast days")
    print()

    # Grid search
    param_grid = {
        "min_edge": [0.03, 0.05, 0.07, 0.10],
        "kelly_cap": [0.10, 0.20, 0.30],
        "per_bucket_max": [15, 25, 40],
        "alpha": [0.40, 0.55, 0.70],
        "gamma": [0.80, 0.85, 0.90, 1.00],
    }

    total_combos = 1
    for v in param_grid.values():
        total_combos *= len(v)
    print(f"Grid search: {total_combos} combinations")
    print("=" * 80)

    results = []
    for i, (me, kc, pbm, al, gm) in enumerate(itertools.product(
        param_grid["min_edge"],
        param_grid["kelly_cap"],
        param_grid["per_bucket_max"],
        param_grid["alpha"],
        param_grid["gamma"],
    )):
        r = simulate_trading(actuals, model_maxes, weather, config,
                             min_edge=me, kelly_cap=kc, per_bucket_max=pbm,
                             alpha=al, gamma=gm)
        r["params"] = {"min_edge": me, "kelly_cap": kc, "per_bucket_max": pbm,
                        "alpha": al, "gamma": gm}
        results.append(r)

        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{total_combos}] ...", end="\r")

    # Sort by Sharpe ratio
    results.sort(key=lambda x: x["sharpe"], reverse=True)

    print()
    print("=" * 80)
    print("TOP 10 PARAMETER COMBINATIONS (by Sharpe Ratio)")
    print("=" * 80)
    print(f"{'Rank':>4} {'Sharpe':>8} {'PnL':>8} {'Trades':>7} {'WinRate':>8} {'MaxDD':>8} | Params")
    print("-" * 80)
    for i, r in enumerate(results[:10]):
        p = r["params"]
        print(
            f"{i+1:4d} {r['sharpe']:8.2f} ${r['total_pnl']:7.2f} {r['trades']:7d} "
            f"{r['win_rate']:7.1f}% ${r['max_drawdown']:7.2f} | "
            f"edge={p['min_edge']:.0%} kelly={p['kelly_cap']:.0%} "
            f"max=${p['per_bucket_max']} α={p['alpha']:.2f} γ={p['gamma']:.2f}"
        )

    print()
    print("WORST 5 (to avoid)")
    print("-" * 80)
    for r in results[-5:]:
        p = r["params"]
        print(
            f"  Sharpe={r['sharpe']:6.2f} PnL=${r['total_pnl']:7.2f} "
            f"WR={r['win_rate']:.0f}% | "
            f"edge={p['min_edge']:.0%} kelly={p['kelly_cap']:.0%} "
            f"max=${p['per_bucket_max']} α={p['alpha']:.2f} γ={p['gamma']:.2f}"
        )

    # Report best
    best = results[0]
    bp = best["params"]
    print()
    print("=" * 80)
    print(f"RECOMMENDED PARAMETERS:")
    print(f"  min_bucket_edge: {bp['min_edge']}")
    print(f"  kelly_cap: {bp['kelly_cap']}")
    print(f"  per_bucket_max_amount: {bp['per_bucket_max']}")
    print(f"  market_fusion_alpha: {bp['alpha']}")
    print(f"  calibration_gamma: {bp['gamma']}")
    print(f"  Expected Sharpe: {best['sharpe']:.2f}")
    print(f"  Expected PnL: ${best['total_pnl']:.2f}")
    print(f"  Win Rate: {best['win_rate']:.1f}%")
    print("=" * 80)


if __name__ == "__main__":
    main()
