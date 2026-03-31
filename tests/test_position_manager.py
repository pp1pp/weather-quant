import pytest

from src.data.schemas import Signal
from src.engine.signal_generator import MarketContext
from src.trading.position_manager import PositionManager
from src.utils.db import init_db

CONFIG = {
    "position": {
        "total_capital": 200,
        "sizing": [
            {"edge_min": 0.08, "edge_max": 0.12, "pct_of_capital": 0.05, "max_amount": 10},
            {"edge_min": 0.12, "edge_max": 0.18, "pct_of_capital": 0.08, "max_amount": 20},
            {"edge_min": 0.18, "edge_max": 1.0, "pct_of_capital": 0.12, "max_amount": 30},
        ],
        "limits": {
            "single_market_max_pct": 0.15,
            "same_city_same_day_max_pct": 0.25,
            "total_weather_exposure_pct": 0.50,
        },
    },
    "risk": {
        "near_settlement_reduce_hours": 3,
        "near_settlement_reduce_pct": 0.50,
        "time_stop_hours": 2,
        "model_reversal_threshold": 0.10,
        "price_deviation_stop": 0.10,
        "liquidity_stop_spread": 0.08,
    },
}


def _make_signal(level="STRONG", direction="BUY_YES", edge=0.18):
    return Signal(
        market_id="test-market",
        level=level,
        direction=direction,
        edge=edge,
        filters_result={},
    )


def _make_ctx(market_id="test-market", hours=10.0, price=0.42):
    return MarketContext(
        market_id=market_id,
        current_price=price,
        volume_24h=500,
        spread=0.02,
        hours_to_settlement=hours,
        next_model_update_hours=5,
    )


@pytest.fixture
def db():
    conn = init_db()
    # Clean trades for fresh tests
    conn.execute("DELETE FROM trades")
    conn.commit()
    return conn


class TestPositionManager:
    def test_small_edge_small_position(self, db):
        """edge=0.10 → 5% of 200 = $10, max=$10 → $10"""
        pm = PositionManager(CONFIG, db)
        signal = _make_signal(level="STRONG", edge=0.10)
        order = pm.calculate_order(signal, _make_ctx())
        assert order is not None
        assert order.amount == 10.0

    def test_large_edge_larger_position(self, db):
        """edge=0.20 → 12% of 200 = $24, max=$30 → $24"""
        pm = PositionManager(CONFIG, db)
        signal = _make_signal(level="STRONG", edge=0.20)
        order = pm.calculate_order(signal, _make_ctx())
        assert order is not None
        assert order.amount == 24.0

    def test_lean_signal_forces_minimum(self, db):
        """LEAN signal with edge=0.20 still uses minimum position $10"""
        pm = PositionManager(CONFIG, db)
        signal = _make_signal(level="LEAN", edge=0.20)
        order = pm.calculate_order(signal, _make_ctx())
        assert order is not None
        assert order.amount == 10.0

    def test_single_market_limit(self, db):
        """Already $25 in market (max is 15% of 200 = $30), adding $10 = $35 > $30 → None"""
        # Insert existing position
        db.execute(
            "INSERT INTO trades (market_id, side, amount, price, executed_at, status) "
            "VALUES ('test-market', 'YES', 25, 0.42, '2026-03-24T00:00:00', 'DRY_RUN')"
        )
        db.commit()

        pm = PositionManager(CONFIG, db)
        signal = _make_signal(level="STRONG", edge=0.10)
        order = pm.calculate_order(signal, _make_ctx())
        assert order is None

    def test_near_settlement_halves(self, db):
        """hours_to_settlement=2 < 3, theoretical $20 → $10"""
        pm = PositionManager(CONFIG, db)
        signal = _make_signal(level="STRONG", edge=0.15)
        ctx = _make_ctx(hours=2.0)
        order = pm.calculate_order(signal, ctx)
        assert order is not None
        # edge=0.15 → 8% of 200 = $16, max=$20 → $16, then halved → $8
        assert order.amount == 8.0

    def test_no_trade_signal_returns_none(self, db):
        """NO_TRADE signal → None"""
        pm = PositionManager(CONFIG, db)
        signal = _make_signal(level="NO_TRADE", direction=None, edge=0.03)
        order = pm.calculate_order(signal, _make_ctx())
        assert order is None

    def test_submitted_positions_count_toward_exposure(self, db):
        db.execute(
            "INSERT INTO trades (market_id, side, amount, price, executed_at, status) "
            "VALUES ('test-market', 'YES', 25, 0.42, '2026-03-24T00:00:00', 'SUBMITTED')"
        )
        db.commit()

        pm = PositionManager(CONFIG, db)
        signal = _make_signal(level="STRONG", edge=0.10)
        order = pm.calculate_order(signal, _make_ctx())
        assert order is None
