import pytest

from src.trading.risk_control import RiskControl
from src.utils.db import init_db

CONFIG = {
    "risk": {
        "time_stop_hours": 2,
        "model_reversal_threshold": 0.10,
        "price_deviation_stop": 0.10,
        "liquidity_stop_spread": 0.08,
        "near_settlement_reduce_hours": 3,
        "near_settlement_reduce_pct": 0.50,
    },
}


@pytest.fixture
def db():
    return init_db()


def _pos(side="YES", entry_price=0.42, entry_fair=0.60):
    return {
        "market_id": "test-market",
        "side": side,
        "amount": 10,
        "entry_price": entry_price,
        "entry_fair_prob": entry_fair,
    }


def _state(current_price=0.42, spread=0.02, hours=10.0):
    return {
        "current_price": current_price,
        "spread": spread,
        "hours_to_settlement": hours,
    }


class TestRiskControl:
    def test_model_reversal_closes(self, db):
        """entry_fair=0.6 bought YES, current_fair=0.35 → CLOSE"""
        rc = RiskControl(CONFIG, db)
        action = rc.check(_pos(entry_fair=0.60), _state(), current_fair_prob=0.35)
        assert action.action == "CLOSE"
        assert "reversal" in action.reason.lower()

    def test_model_same_direction_holds(self, db):
        """entry_fair=0.6, current_fair=0.55 → HOLD (no reversal, change < threshold)"""
        rc = RiskControl(CONFIG, db)
        action = rc.check(_pos(entry_fair=0.60), _state(), current_fair_prob=0.55)
        assert action.action == "HOLD"

    def test_time_stop_losing_position(self, db):
        """hours=1.5, position losing → CLOSE"""
        rc = RiskControl(CONFIG, db)
        action = rc.check(
            _pos(side="YES", entry_price=0.42, entry_fair=0.50),
            _state(current_price=0.35, hours=1.5),
            current_fair_prob=0.45,  # no reversal
        )
        assert action.action == "CLOSE"
        assert "time" in action.reason.lower()

    def test_time_stop_winning_position(self, db):
        """hours=1.5, position winning → HOLD"""
        rc = RiskControl(CONFIG, db)
        action = rc.check(
            _pos(side="YES", entry_price=0.42, entry_fair=0.50),
            _state(current_price=0.55, hours=1.5),
            current_fair_prob=0.55,
        )
        assert action.action == "HOLD"

    def test_price_deviation_reduces(self, db):
        """Price moved adversely 12% → REDUCE 50%"""
        rc = RiskControl(CONFIG, db)
        action = rc.check(
            _pos(side="YES", entry_price=0.50, entry_fair=0.55),
            _state(current_price=0.38, hours=10),
            current_fair_prob=0.50,  # no reversal
        )
        assert action.action == "REDUCE"
        assert action.reduce_pct == 0.50

    def test_liquidity_pauses(self, db):
        """spread=0.09 → PAUSE"""
        rc = RiskControl(CONFIG, db)
        action = rc.check(
            _pos(entry_fair=0.55),
            _state(spread=0.09, hours=10),
            current_fair_prob=0.53,
        )
        assert action.action == "PAUSE"
        assert "liquidity" in action.reason.lower()

    def test_priority_order(self, db):
        """
        Both price deviation (REDUCE) and liquidity (PAUSE) triggered
        → REDUCE wins (higher priority)
        """
        rc = RiskControl(CONFIG, db)
        action = rc.check(
            _pos(side="YES", entry_price=0.50, entry_fair=0.55),
            _state(current_price=0.38, spread=0.09, hours=10),
            current_fair_prob=0.50,  # no reversal
        )
        assert action.action == "REDUCE"
