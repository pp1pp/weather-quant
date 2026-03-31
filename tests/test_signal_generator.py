import pytest

from src.data.schemas import EdgeResult
from src.engine.signal_generator import MarketContext, SignalGenerator
from src.utils.db import init_db

CONFIG = {
    "trading": {
        "entry_threshold": 0.08,
        "strong_signal_threshold": 0.15,
        "min_liquidity_volume": 100,
        "max_spread": 0.05,
        "min_hours_to_settle": 2,
        "update_pause_hours": 1,
    },
}


def _make_edge(edge: float, fair=0.5) -> EdgeResult:
    market = fair - edge
    if edge > 0:
        direction = "BUY_YES"
    elif edge < 0:
        direction = "BUY_NO"
    else:
        direction = "NO_TRADE"
    return EdgeResult(edge=edge, direction=direction, fair_prob=fair, market_price=market)


def _good_ctx(market_id="test-market") -> MarketContext:
    """Market context where all filters pass."""
    return MarketContext(
        market_id=market_id,
        current_price=0.5,
        volume_24h=500,
        spread=0.02,
        hours_to_settlement=10,
        next_model_update_hours=5,
    )


@pytest.fixture(scope="module", autouse=True)
def setup_db():
    init_db()


class TestSignalGenerator:
    def test_strong_signal_all_filters_pass(self):
        """edge=0.18, all filters pass → STRONG"""
        gen = SignalGenerator(CONFIG)
        signal = gen.generate(_make_edge(0.18), _good_ctx())
        assert signal.level == "STRONG"
        assert signal.direction == "BUY_YES"

    def test_lean_signal(self):
        """edge=0.10, all filters pass → LEAN"""
        gen = SignalGenerator(CONFIG)
        signal = gen.generate(_make_edge(0.10), _good_ctx())
        assert signal.level == "LEAN"

    def test_below_threshold_no_trade(self):
        """edge=0.05 → NO_TRADE without checking filters"""
        gen = SignalGenerator(CONFIG)
        signal = gen.generate(_make_edge(0.05), _good_ctx())
        assert signal.level == "NO_TRADE"
        assert signal.filters_result == {}

    def test_liquidity_filter_blocks(self):
        """edge=0.15 but volume=50 → NO_TRADE"""
        gen = SignalGenerator(CONFIG)
        ctx = _good_ctx()
        ctx.volume_24h = 50
        signal = gen.generate(_make_edge(0.15), ctx)
        assert signal.level == "NO_TRADE"
        assert signal.filters_result.get("liquidity") is False

    def test_spread_filter_blocks(self):
        """edge=0.15 but spread=0.07 → NO_TRADE"""
        gen = SignalGenerator(CONFIG)
        ctx = _good_ctx()
        ctx.spread = 0.07
        signal = gen.generate(_make_edge(0.15), ctx)
        assert signal.level == "NO_TRADE"
        assert signal.filters_result.get("spread") is False

    def test_time_filter_blocks(self):
        """edge=0.15 but hours_to_settlement=1 → NO_TRADE"""
        gen = SignalGenerator(CONFIG)
        ctx = _good_ctx()
        ctx.hours_to_settlement = 1
        signal = gen.generate(_make_edge(0.15), ctx)
        assert signal.level == "NO_TRADE"
        assert signal.filters_result.get("time_to_settle") is False

    def test_update_filter_blocks(self):
        """edge=0.15 but next_model_update_hours=0.5 → NO_TRADE"""
        gen = SignalGenerator(CONFIG)
        ctx = _good_ctx()
        ctx.next_model_update_hours = 0.5
        signal = gen.generate(_make_edge(0.15), ctx)
        assert signal.level == "NO_TRADE"
        assert signal.filters_result.get("update_pause") is False

    def test_filters_short_circuit(self):
        """First filter fails → subsequent filters not checked."""
        gen = SignalGenerator(CONFIG)
        ctx = _good_ctx()
        ctx.volume_24h = 10  # liquidity fails
        ctx.spread = 0.10  # spread would also fail
        signal = gen.generate(_make_edge(0.15), ctx)
        assert signal.level == "NO_TRADE"
        # Only liquidity should be in results (short-circuited)
        assert "liquidity" in signal.filters_result
        assert "spread" not in signal.filters_result

    def test_signal_saved_to_db(self):
        """Verify signal is written to SQLite signals table."""
        from src.utils.db import get_connection

        gen = SignalGenerator(CONFIG)
        gen.generate(_make_edge(0.20), _good_ctx("db-test-market"))

        conn = get_connection()
        row = conn.execute(
            "SELECT * FROM signals WHERE market_id = 'db-test-market' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()

        assert row is not None
        assert row["signal_level"] == "STRONG"
        assert row["market_id"] == "db-test-market"
