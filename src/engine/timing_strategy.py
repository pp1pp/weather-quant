"""
Timing Strategy: Optimal entry timing for Shanghai daily temperature markets.

Key insight from backtest:
- Markets open with diffuse prices (near-uniform distribution)
- As settlement approaches, prices converge to the actual outcome
- The BEST time to bet is 8-24h before settlement when:
  1. Our model already has a strong signal
  2. Market prices haven't fully converged yet
  3. Sufficient liquidity exists

Worst time to bet:
- <2h before settlement: market already converged (no edge)
- >48h before settlement: forecast too uncertain

This module computes a timing multiplier that scales position size.
"""

from src.utils.logger import logger


class TimingStrategy:
    """Compute optimal entry timing multiplier based on hours to settlement."""

    def __init__(self, config: dict):
        mo_cfg = config.get("multi_outcome", {})
        # Don't trade within this many hours of settlement (market already efficient)
        self.min_hours = mo_cfg.get("min_hours_to_settle", 2)
        # Don't trade more than this many hours before settlement (forecast uncertain)
        self.max_hours = mo_cfg.get("max_hours_to_settle", 36)
        # Sweet spot: highest multiplier in this range
        self.sweet_low = mo_cfg.get("sweet_spot_hours_low", 6)
        self.sweet_high = mo_cfg.get("sweet_spot_hours_high", 18)

    def get_multiplier(self, hours_to_settlement: float) -> float:
        """
        Returns a multiplier [0.0, 1.0] for position sizing based on timing.

        1.0 = optimal timing (sweet spot)
        0.5 = suboptimal but still trade
        0.0 = don't trade (too early or too late)
        """
        h = hours_to_settlement

        if h < self.min_hours:
            # Too late — market already converged
            logger.debug(f"Timing: {h:.1f}h < {self.min_hours}h minimum → skip")
            return 0.0

        if h > self.max_hours:
            # Too early — forecast too uncertain
            logger.debug(f"Timing: {h:.1f}h > {self.max_hours}h maximum → skip")
            return 0.0

        if self.sweet_low <= h <= self.sweet_high:
            # Sweet spot — full position
            return 1.0

        if h < self.sweet_low:
            # Between min and sweet_low: ramp down as market converges
            # e.g., min=2, sweet_low=6: at 4h → 0.5, at 3h → 0.25
            return (h - self.min_hours) / (self.sweet_low - self.min_hours)

        # Between sweet_high and max: ramp down as forecast becomes uncertain
        return (self.max_hours - h) / (self.max_hours - self.sweet_high)

    def should_trade(self, hours_to_settlement: float) -> bool:
        """Quick check: is it a good time to trade?"""
        return self.get_multiplier(hours_to_settlement) > 0.0

    def get_edge_threshold(self, hours_to_settlement: float, base_threshold: float) -> float:
        """
        Adaptive edge threshold: require larger edge when timing is suboptimal.

        At sweet spot: use base threshold (e.g., 5%)
        At edges: require up to 2x base threshold
        """
        mult = self.get_multiplier(hours_to_settlement)
        if mult <= 0:
            return float('inf')  # Don't trade

        # Invert multiplier: low mult → high threshold
        threshold = base_threshold / max(mult, 0.3)
        return min(threshold, base_threshold * 2.5)
