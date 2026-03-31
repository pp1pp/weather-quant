from src.data.schemas import Order, Signal
from src.engine.signal_generator import MarketContext
from src.trading.multi_outcome_trade import ACTIVE_TRADE_STATUSES
from src.utils.logger import logger


class PositionManager:
    """L7: Position sizing based on signal strength, edge, and hard limits."""

    def __init__(self, config: dict, db):
        self.capital = config["position"]["total_capital"]
        self.sizing_rules = config["position"]["sizing"]
        self.limits = config["position"]["limits"]
        self.risk_config = config.get("risk", {})
        self.db = db

    def calculate_order(
        self, signal: Signal, market_context: MarketContext
    ) -> Order | None:
        """Calculate order size or return None if limits block it."""
        if signal.level == "NO_TRADE":
            return None

        abs_edge = abs(signal.edge)

        # Step 1: Find sizing rule by edge
        amount = self._size_by_edge(abs_edge)
        if amount is None:
            return None

        # Step 2: LEAN signal → force minimum position
        if signal.level == "LEAN":
            min_rule = self.sizing_rules[0]
            amount = min(
                min_rule["pct_of_capital"] * self.capital,
                min_rule["max_amount"],
            )

        # Step 3: Check hard limits
        market_id = market_context.market_id
        city = market_id.split("-")[0] if "-" in market_id else "unknown"

        # 3a: Single market limit
        current_market_exposure = self.get_current_exposure(market_id=market_id)
        max_single = self.limits["single_market_max_pct"] * self.capital
        if current_market_exposure + amount > max_single:
            logger.info(
                f"[{market_id}] Single market limit: "
                f"current={current_market_exposure}, new={amount}, max={max_single}"
            )
            return None

        # 3b: Same city same day limit
        current_city_exposure = self.get_current_exposure(city=city)
        max_city = self.limits["same_city_same_day_max_pct"] * self.capital
        if current_city_exposure + amount > max_city:
            logger.info(f"[{market_id}] City limit exceeded")
            return None

        # 3c: Total weather exposure limit
        total_exposure = self.get_current_exposure()
        max_total = self.limits["total_weather_exposure_pct"] * self.capital
        if total_exposure + amount > max_total:
            logger.info(f"[{market_id}] Total exposure limit exceeded")
            return None

        # Step 4: Near settlement → halve position
        near_hours = self.risk_config.get("near_settlement_reduce_hours", 3)
        near_pct = self.risk_config.get("near_settlement_reduce_pct", 0.50)
        if market_context.hours_to_settlement < near_hours:
            amount *= near_pct
            logger.info(
                f"[{market_id}] Near settlement ({market_context.hours_to_settlement:.1f}h), "
                f"reducing to {amount:.2f}"
            )

        # Determine side
        if signal.direction == "BUY_YES":
            side = "YES"
        elif signal.direction == "BUY_NO":
            side = "NO"
        else:
            return None

        return Order(
            market_id=market_id,
            side=side,
            amount=round(amount, 2),
            price=market_context.current_price,
        )

    def _size_by_edge(self, abs_edge: float) -> float | None:
        """Find the sizing rule matching the edge and compute amount."""
        for rule in self.sizing_rules:
            if rule["edge_min"] <= abs_edge < rule["edge_max"]:
                theoretical = rule["pct_of_capital"] * self.capital
                return min(theoretical, rule["max_amount"])
        return None

    def get_current_exposure(
        self, market_id: str | None = None, city: str | None = None
    ) -> float:
        """Query current open exposure from trades table."""
        statuses_sql = ", ".join(f"'{status}'" for status in ACTIVE_TRADE_STATUSES)
        try:
            cursor = self.db.cursor()
            if market_id:
                cursor.execute(
                    "SELECT COALESCE(SUM(amount), 0) FROM trades "
                    f"WHERE market_id = ? AND status IN ({statuses_sql})",
                    (market_id,),
                )
            elif city:
                cursor.execute(
                    "SELECT COALESCE(SUM(amount), 0) FROM trades "
                    f"WHERE market_id LIKE ? AND status IN ({statuses_sql})",
                    (f"{city}%",),
                )
            else:
                cursor.execute(
                    "SELECT COALESCE(SUM(amount), 0) FROM trades "
                    f"WHERE status IN ({statuses_sql})"
                )
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error querying exposure: {e}")
            return 0.0
