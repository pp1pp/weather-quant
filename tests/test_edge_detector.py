import pytest

from src.engine.edge_detector import EdgeDetector

CONFIG = {
    "trading": {"entry_threshold": 0.08},
}


class TestEdgeDetector:
    def test_positive_edge_buy_yes(self):
        """fair=0.57, market=0.42 → edge=0.15, BUY_YES"""
        det = EdgeDetector(CONFIG)
        result = det.detect(0.57, 0.42)
        assert result.edge == pytest.approx(0.15, abs=0.001)
        assert result.direction == "BUY_YES"

    def test_negative_edge_buy_no(self):
        """fair=0.30, market=0.55 → edge=-0.25, BUY_NO"""
        det = EdgeDetector(CONFIG)
        result = det.detect(0.30, 0.55)
        assert result.edge == pytest.approx(-0.25, abs=0.001)
        assert result.direction == "BUY_NO"

    def test_small_edge_no_trade(self):
        """fair=0.45, market=0.42 → edge=0.03, NO_TRADE"""
        det = EdgeDetector(CONFIG)
        result = det.detect(0.45, 0.42)
        assert result.edge == pytest.approx(0.03, abs=0.001)
        assert result.direction == "NO_TRADE"

    def test_edge_exactly_at_threshold(self):
        """fair=0.50, market=0.42 → edge=0.08, exactly at threshold → NO_TRADE"""
        det = EdgeDetector(CONFIG)
        result = det.detect(0.50, 0.42)
        assert result.edge == pytest.approx(0.08, abs=0.001)
        assert result.direction == "NO_TRADE"

    def test_negative_edge_exactly_at_threshold(self):
        """fair=0.34, market=0.42 → edge=-0.08, exactly at threshold → NO_TRADE"""
        det = EdgeDetector(CONFIG)
        result = det.detect(0.34, 0.42)
        assert result.edge == pytest.approx(-0.08, abs=0.001)
        assert result.direction == "NO_TRADE"
