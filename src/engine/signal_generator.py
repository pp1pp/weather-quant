import json
from datetime import datetime, timezone

from pydantic import BaseModel

from src.data.schemas import EdgeResult, Signal
from src.utils.db import get_connection
from src.utils.logger import logger


class MarketContext(BaseModel):
    """Market context for filter evaluation."""
    market_id: str
    current_price: float = 0.5
    volume_24h: float  # 24h trading volume ($)
    spread: float  # current bid-ask spread
    hours_to_settlement: float
    next_model_update_hours: float


class BaseFilter:
    name: str = "base"

    def check(self, ctx: MarketContext) -> bool:
        raise NotImplementedError


class LiquidityFilter(BaseFilter):
    name = "liquidity"

    def __init__(self, min_volume: float):
        self.min_volume = min_volume

    def check(self, ctx: MarketContext) -> bool:
        return ctx.volume_24h >= self.min_volume


class SpreadFilter(BaseFilter):
    name = "spread"

    def __init__(self, max_spread: float):
        self.max_spread = max_spread

    def check(self, ctx: MarketContext) -> bool:
        return ctx.spread <= self.max_spread


class TimeFilter(BaseFilter):
    name = "time_to_settle"

    def __init__(self, min_hours: float):
        self.min_hours = min_hours

    def check(self, ctx: MarketContext) -> bool:
        return ctx.hours_to_settlement >= self.min_hours


class UpdateFilter(BaseFilter):
    name = "update_pause"

    def __init__(self, pause_hours: float):
        self.pause_hours = pause_hours

    def check(self, ctx: MarketContext) -> bool:
        return ctx.next_model_update_hours >= self.pause_hours


class SignalGenerator:
    """L6: Generate trading signals from EdgeResult + MarketContext with filter pipeline."""

    def __init__(self, config: dict):
        self.strong_threshold = config["trading"]["strong_signal_threshold"]
        self.entry_threshold = config["trading"]["entry_threshold"]
        self.filters = [
            LiquidityFilter(config["trading"]["min_liquidity_volume"]),
            SpreadFilter(config["trading"]["max_spread"]),
            TimeFilter(config["trading"]["min_hours_to_settle"]),
            UpdateFilter(config["trading"]["update_pause_hours"]),
        ]

    def generate(
        self, edge_result: EdgeResult, market_context: MarketContext
    ) -> Signal:
        """
        Generate a signal:
        1. Grade by edge magnitude
        2. Run filters (short-circuit on first failure)
        3. Save to DB
        """
        abs_edge = abs(edge_result.edge)

        # Step 1: Initial grading
        if abs_edge >= self.strong_threshold:
            level = "STRONG"
        elif abs_edge >= self.entry_threshold:
            level = "LEAN"
        else:
            level = "NO_TRADE"

        # Step 2: Filter check (only if we have a tradeable signal)
        filters_result = {}
        if level != "NO_TRADE":
            for f in self.filters:
                passed = f.check(market_context)
                filters_result[f.name] = passed
                if not passed:
                    logger.info(
                        f"[{market_context.market_id}] Filter '{f.name}' blocked signal"
                    )
                    level = "NO_TRADE"
                    break  # Short-circuit

        direction = edge_result.direction if level != "NO_TRADE" else None

        signal = Signal(
            market_id=market_context.market_id,
            level=level,
            direction=direction,
            edge=edge_result.edge,
            filters_result=filters_result,
        )

        # Step 3: Save to DB
        self._save_signal(signal, edge_result, market_context)

        logger.info(
            f"[{market_context.market_id}] Signal: level={level}, "
            f"edge={edge_result.edge:.3f}, dir={direction}"
        )

        return signal

    def _save_signal(
        self, signal: Signal, edge_result: EdgeResult, ctx: MarketContext
    ):
        try:
            conn = get_connection()
            conn.execute(
                """INSERT INTO signals
                   (market_id, signal_time, fair_prob, market_price, edge,
                    signal_level, direction, filters_passed)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    signal.market_id,
                    datetime.now(timezone.utc).isoformat(),
                    edge_result.fair_prob,
                    edge_result.market_price,
                    edge_result.edge,
                    signal.level,
                    signal.direction,
                    json.dumps(signal.filters_result),
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to save signal to DB: {e}")
