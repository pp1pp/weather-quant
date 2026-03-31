import os
import random

import yaml

from src.engine.signal_generator import MarketContext
from src.utils.logger import logger


class MarketScanner:
    """
    Scan Polymarket for weather market data.

    Supports 3 modes:
    - mock_mode=True: random but reasonable simulated data (for testing)
    - mock_mode=False + CLOB client available: real Polymarket API
    - mock_mode=False + no CLOB client: fallback to Gamma API (read-only)
    """

    CLOB_URL = "https://clob.polymarket.com"
    GAMMA_URL = "https://gamma-api.polymarket.com"

    def __init__(self, config: dict, mock_mode: bool = False, clob_client=None):
        self.mock_mode = mock_mode
        self.config = config
        self.clob_client = clob_client
        self._markets_config = self._load_markets()

    def _load_markets(self) -> dict:
        try:
            with open("config/markets.yaml", "r") as f:
                data = yaml.safe_load(f)
            return {m["id"]: m for m in data.get("markets", [])}
        except Exception as e:
            logger.error(f"Failed to load markets config: {e}")
            return {}

    def get_market_price(self, market_id: str) -> float:
        """Return YES price (0-1)."""
        if self.mock_mode:
            price = round(random.uniform(0.20, 0.80), 2)
            logger.debug(f"[MOCK] {market_id} price={price}")
            return price

        market_cfg = self._markets_config.get(market_id, {})
        condition_id = market_cfg.get("condition_id")
        token_id = market_cfg.get("token_id")

        # Try CLOB client first
        if self.clob_client and token_id:
            try:
                price = self.clob_client.get_last_trade_price(token_id)
                if price and "price" in price:
                    return float(price["price"])
            except Exception as e:
                logger.warning(f"CLOB price fetch failed: {e}")

        # Try CLOB midpoint
        if self.clob_client and token_id:
            try:
                mid = self.clob_client.get_midpoint(token_id)
                if mid and "mid" in mid:
                    return float(mid["mid"])
            except Exception as e:
                logger.warning(f"CLOB midpoint failed: {e}")

        # Fallback: Gamma API
        if condition_id:
            try:
                return self._gamma_price(condition_id)
            except Exception as e:
                logger.warning(f"Gamma price fetch failed: {e}")

        logger.warning(f"No price source for {market_id}, returning 0.50")
        return 0.50

    def get_market_context(self, market_id: str) -> MarketContext:
        """Return full market context (price, volume, spread, timing)."""
        if self.mock_mode:
            price = round(random.uniform(0.20, 0.80), 2)
            ctx = MarketContext(
                market_id=market_id,
                current_price=price,
                volume_24h=round(random.uniform(50, 2000), 2),
                spread=round(random.uniform(0.01, 0.06), 3),
                hours_to_settlement=round(random.uniform(1, 48), 1),
                next_model_update_hours=round(random.uniform(0.5, 6), 1),
            )
            logger.debug(
                f"[MOCK] {market_id} ctx: price={ctx.current_price}, "
                f"vol={ctx.volume_24h}, spread={ctx.spread}"
            )
            return ctx

        market_cfg = self._markets_config.get(market_id, {})
        token_id = market_cfg.get("token_id")
        condition_id = market_cfg.get("condition_id")

        price = self.get_market_price(market_id)
        spread = 0.03
        volume = 100.0

        # Try to get spread from CLOB order book
        if self.clob_client and token_id:
            try:
                sp = self.clob_client.get_spread(token_id)
                if sp and "spread" in sp:
                    spread = float(sp["spread"])
            except Exception:
                pass

        # Get volume from Gamma API
        if condition_id:
            try:
                vol = self._gamma_volume(condition_id)
                if vol is not None:
                    volume = vol
            except Exception:
                pass

        # Calculate hours to settlement from config
        from src.utils.time_utils import hours_until
        from datetime import datetime, timezone

        settle_str = market_cfg.get("settlement_time_utc", "")
        if settle_str:
            if isinstance(settle_str, str):
                settle_dt = datetime.fromisoformat(settle_str.replace("Z", "+00:00"))
            else:
                settle_dt = settle_str
                if settle_dt.tzinfo is None:
                    settle_dt = settle_dt.replace(tzinfo=timezone.utc)
            hours = max(0, hours_until(settle_dt))
        else:
            hours = 24.0

        return MarketContext(
            market_id=market_id,
            current_price=price,
            volume_24h=volume,
            spread=spread,
            hours_to_settlement=hours,
            next_model_update_hours=3.0,  # default: next model update in ~3h
        )

    def get_active_markets(self) -> list[str]:
        """Return list of configured market IDs."""
        return list(self._markets_config.keys())

    def search_weather_markets(self, keywords: list[str] = None) -> list[dict]:
        """
        Search Polymarket for weather-related markets.
        Returns list of matching market dicts from Gamma API.
        """
        import httpx

        if keywords is None:
            keywords = [
                "temperature", "weather forecast", "degrees celsius", "degrees fahrenheit",
                "heat wave", "heatwave", "freeze warning",
                "warmest day", "hottest day", "coldest day",
                "record high temp", "record low temp",
                "snowfall total", "rainfall total", "inches of rain",
            ]

        all_markets = []
        for offset in range(0, 500, 100):
            try:
                resp = httpx.get(
                    f"{self.GAMMA_URL}/markets",
                    params={"closed": "false", "limit": 100, "offset": offset, "active": "true"},
                    timeout=15,
                )
                data = resp.json()
                if not data:
                    break
                all_markets.extend(data)
            except Exception as e:
                logger.warning(f"Gamma API page {offset} failed: {e}")
                break

        results = []
        for m in all_markets:
            text = (m.get("question", "") + " " + m.get("description", "")).lower()
            for kw in keywords:
                if kw.lower() in text:
                    # Exclude false positives (sports teams, geopolitics, etc)
                    false_positives = [
                        "hurricane", "heat ", "bulls", "blackhawks", "cold war",
                        "ukraine", "ceasefire", "election", "nato", "trump",
                        "fifa", "nba", "nhl", "nfl", "mlb", "world cup",
                        "israel", "hamas", "zelenskyy", "putin",
                    ]
                    if any(fp in text for fp in false_positives):
                        continue
                    results.append({
                        "question": m.get("question"),
                        "condition_id": m.get("conditionId"),
                        "token_ids": m.get("clobTokenIds"),
                        "prices": m.get("outcomePrices"),
                        "volume": m.get("volumeNum"),
                        "end_date": m.get("endDate"),
                    })
                    break

        logger.info(f"Searched {len(all_markets)} markets, found {len(results)} weather-related")
        return results

    def _gamma_price(self, condition_id: str) -> float:
        """Get market price from Gamma API."""
        import httpx
        resp = httpx.get(
            f"{self.GAMMA_URL}/markets",
            params={"condition_id": condition_id},
            timeout=10,
        )
        data = resp.json()
        if data and isinstance(data, list) and data[0].get("outcomePrices"):
            prices = data[0]["outcomePrices"]
            if isinstance(prices, str):
                import json
                prices = json.loads(prices)
            return float(prices[0])  # YES price
        return 0.50

    def _gamma_volume(self, condition_id: str) -> float | None:
        """Get 24h volume from Gamma API."""
        import httpx
        resp = httpx.get(
            f"{self.GAMMA_URL}/markets",
            params={"condition_id": condition_id},
            timeout=10,
        )
        data = resp.json()
        if data and isinstance(data, list):
            return float(data[0].get("volumeNum", 0))
        return None
