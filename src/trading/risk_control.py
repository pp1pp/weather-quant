from src.data.schemas import RiskAction
from src.trading.multi_outcome_trade import ACTIVE_TRADE_STATUSES
from src.utils.logger import logger


class RiskControl:
    """L8: Four-tier stop loss system for weather positions."""

    def __init__(self, config: dict, db):
        self.config = config["risk"]
        self.db = db

    def check(
        self,
        position: dict,
        market_state: dict,
        current_fair_prob: float,
    ) -> RiskAction:
        """
        Check a single position against all risk rules.

        position: {market_id, side, amount, entry_price, entry_fair_prob}
        market_state: {current_price, spread, hours_to_settlement}

        Priority order: model_reversal > time_stop > price_deviation > liquidity
        """
        entry_fair = position["entry_fair_prob"]
        entry_price = position["entry_price"]
        side = position["side"]
        current_price = market_state["current_price"]
        spread = market_state["spread"]
        hours = market_state["hours_to_settlement"]

        # Priority 1: Model reversal
        reversal_threshold = self.config["model_reversal_threshold"]
        prob_change = abs(current_fair_prob - entry_fair)
        direction_reversed = self._is_reversed(side, entry_fair, current_fair_prob)

        if prob_change > reversal_threshold and direction_reversed:
            return RiskAction(
                action="CLOSE",
                reason=(
                    f"Model reversal: entry_fair={entry_fair:.3f} → "
                    f"current_fair={current_fair_prob:.3f}"
                ),
            )

        # Priority 2: Time stop (only for losing positions)
        time_stop_hours = self.config["time_stop_hours"]
        if hours < time_stop_hours:
            is_losing = self._is_losing(side, entry_price, current_price)
            if is_losing:
                return RiskAction(
                    action="CLOSE",
                    reason=(
                        f"Time stop: {hours:.1f}h to settlement, position losing "
                        f"(entry={entry_price:.3f}, current={current_price:.3f})"
                    ),
                )

        # Priority 3: Price deviation
        price_dev_stop = self.config["price_deviation_stop"]
        price_deviation = self._price_deviation(side, entry_price, current_price)
        if price_deviation > price_dev_stop:
            return RiskAction(
                action="REDUCE",
                reason=(
                    f"Price deviation: {price_deviation:.3f} > {price_dev_stop} "
                    f"(entry={entry_price:.3f}, current={current_price:.3f})"
                ),
                reduce_pct=0.50,
            )

        # Priority 4: Liquidity stop
        liq_stop = self.config["liquidity_stop_spread"]
        if spread > liq_stop:
            return RiskAction(
                action="PAUSE",
                reason=f"Liquidity thin: spread={spread:.3f} > {liq_stop}",
            )

        return RiskAction(action="HOLD", reason="All risk checks passed")

    def _is_reversed(
        self, side: str, entry_fair: float, current_fair: float
    ) -> bool:
        """Check if the model has reversed direction relative to entry."""
        if side == "YES":
            # Bought YES because fair was high; reversal = fair drops significantly
            return current_fair < entry_fair
        else:
            # Bought NO because fair was low; reversal = fair rises significantly
            return current_fair > entry_fair

    def _is_losing(
        self, side: str, entry_price: float, current_price: float
    ) -> bool:
        """Check if position is currently losing money."""
        if side == "YES":
            return current_price < entry_price
        else:
            # NO position profits when price drops
            return current_price > entry_price

    def _price_deviation(
        self, side: str, entry_price: float, current_price: float
    ) -> float:
        """Calculate adverse price movement."""
        if side == "YES":
            return max(0, entry_price - current_price)
        else:
            return max(0, current_price - entry_price)

    def check_all_positions(self, get_fair_prob_fn, get_market_state_fn):
        """Check all open positions and return list of RiskActions."""
        actions = []
        statuses_sql = ", ".join(f"'{status}'" for status in ACTIVE_TRADE_STATUSES)
        try:
            cursor = self.db.cursor()
            cursor.execute(
                "SELECT market_id, side, amount, price as entry_price "
                f"FROM trades WHERE status IN ({statuses_sql})"
            )
            positions = [dict(row) for row in cursor.fetchall()]

            for pos in positions:
                try:
                    fair_prob = get_fair_prob_fn(pos["market_id"])
                    market_state = get_market_state_fn(pos["market_id"])
                    pos["entry_fair_prob"] = fair_prob  # simplified for now
                    action = self.check(pos, market_state, fair_prob)
                    actions.append((pos["market_id"], action))
                except Exception as e:
                    logger.error(
                        f"Risk check failed for {pos['market_id']}: {e}"
                    )
        except Exception as e:
            logger.error(f"Failed to query positions: {e}")

        return actions
