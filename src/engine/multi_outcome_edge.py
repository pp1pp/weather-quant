"""
Multi-Outcome Edge Detector with Limit Order Pricing.

Enhanced with:
- Intelligent limit order pricing (挂单策略)
- Multi-bucket portfolio optimization
- Sum-to-One arbitrage exploitation
- Risk-reward ratio calculation
- Worst-case P&L analysis across all temperature scenarios
"""

import math

from src.data.schemas import (
    BucketEdge,
    BucketSignal,
    MultiOutcomeEdgeResult,
    MultiOutcomeFairResult,
    MultiMarketSnapshot,
)
from src.utils.logger import logger


class MultiOutcomeEdgeDetector:
    """
    Detect trading edges and compute optimal limit order prices.

    Strategies:
    1. Model Edge: fair_prob vs market_price per bucket
    2. Limit Order: optimal bid price between market and fair value
    3. Multi-Bucket: simultaneous positions with portfolio constraints
    4. Sum-to-One: exploit total market overpricing/underpricing
    """

    def __init__(self, config: dict):
        mo_cfg = config.get("multi_outcome", {})
        self.min_edge = mo_cfg.get("min_bucket_edge", 0.05)
        self.strong_edge = mo_cfg.get("strong_bucket_edge", 0.10)
        self.sum_to_one_threshold = mo_cfg.get("sum_to_one_threshold", 0.03)
        self.kelly_cap = mo_cfg.get("kelly_cap", 0.25)
        self.per_bucket_max = mo_cfg.get("per_bucket_max_amount", 25)
        self.total_max = mo_cfg.get("total_multi_max_exposure", 80)
        self.max_buckets = mo_cfg.get("max_concurrent_buckets", 4)
        self.min_prob = mo_cfg.get("min_prob_threshold", 0.01)

        # Limit order pricing parameters
        self.limit_order_cfg = mo_cfg.get("limit_order", {})
        # How aggressively to bid toward fair value (0=market, 1=fair)
        self.bid_aggressiveness = self.limit_order_cfg.get("bid_aggressiveness", 0.6)
        # Minimum acceptable fill probability
        self.min_fill_prob = self.limit_order_cfg.get("min_fill_prob", 0.3)
        # Spread markup for limit orders
        self.spread_markup = self.limit_order_cfg.get("spread_markup", 0.01)

    def detect(
        self,
        fair_result: MultiOutcomeFairResult,
        snapshot: MultiMarketSnapshot,
    ) -> MultiOutcomeEdgeResult:
        """Compare fair probabilities with market prices and produce edge signals
        with limit order prices.

        Uses Bayesian fusion: blends model probability with market price.
        Market price contains collective trader information — when the model
        has low confidence, defer more to the market; when confident, hold.

        α = model_weight:
          - confidence ≥ 0.9 → α = 0.75 (strongly trust model)
          - confidence ≈ 0.5 → α = 0.50 (equal blend)
          - confidence ≤ 0.3 → α = 0.35 (defer to market)
        """
        bucket_edges = []
        best_buys = []
        best_sells = []

        # Bayesian fusion weight: based on model confidence
        conf = fair_result.confidence
        alpha = max(0.35, min(0.75, 0.35 + 0.4 * conf))

        for bp in snapshot.buckets:
            label = bp.label
            raw_model_prob = fair_result.bucket_probs.get(label, 0.0)
            market_price = bp.yes_price

            # Fuse model probability with market price
            fair_prob = alpha * raw_model_prob + (1 - alpha) * market_price

            edge = fair_prob - market_price

            # Determine direction
            if edge > self.min_edge and fair_prob > self.min_prob:
                direction = "BUY_YES"
            elif edge < -self.min_edge and market_price > self.min_prob:
                direction = "BUY_NO"
            else:
                direction = "NO_TRADE"

            # Kelly criterion
            kelly = 0.0
            if direction == "BUY_YES" and 0 < market_price < 1:
                odds = (1.0 / market_price) - 1.0
                kelly = (fair_prob * odds - (1 - fair_prob)) / odds
                kelly = max(0, min(self.kelly_cap, kelly))
            elif direction == "BUY_NO" and 0 < market_price < 1:
                no_price = 1.0 - market_price
                no_fair = 1.0 - fair_prob
                odds = (1.0 / no_price) - 1.0
                kelly = (no_fair * odds - fair_prob) / odds
                kelly = max(0, min(self.kelly_cap, kelly))

            # Compute limit order price and risk-reward
            limit_price, fill_prob, rr_ratio = self._compute_limit_price(
                direction, fair_prob, market_price, bp.best_bid, bp.best_ask, bp.spread
            )

            be = BucketEdge(
                label=label,
                fair_prob=fair_prob,
                market_price=market_price,
                edge=edge,
                direction=direction,
                kelly_fraction=round(kelly, 4),
                limit_price=round(limit_price, 4),
                expected_fill_prob=round(fill_prob, 3),
                risk_reward_ratio=round(rr_ratio, 2),
            )
            bucket_edges.append(be)

            if direction == "BUY_YES":
                best_buys.append(be)
            elif direction == "BUY_NO":
                best_sells.append(be)

        # Sort by expected value (edge * fill_prob)
        best_buys.sort(key=lambda x: -(abs(x.edge) * x.expected_fill_prob))
        best_sells.sort(key=lambda x: -(abs(x.edge) * x.expected_fill_prob))

        # Sum-to-one gap
        sum_gap = snapshot.total_price_sum - 1.0

        # Enhanced arbitrage detection
        arb = (
            abs(sum_gap) > self.sum_to_one_threshold
            or len(best_buys) > 0
            or len(best_sells) > 0
        )

        result = MultiOutcomeEdgeResult(
            bucket_edges=bucket_edges,
            sum_to_one_gap=round(sum_gap, 4),
            best_buys=best_buys[:self.max_buckets],
            best_sells=best_sells[:self.max_buckets],
            arb_opportunity=arb,
        )

        logger.info(
            f"MultiEdge: sum_gap={sum_gap:+.3f}, "
            f"buys={len(best_buys)}, sells={len(best_sells)}, arb={arb}"
        )

        return result

    def _compute_limit_price(
        self,
        direction: str,
        fair_prob: float,
        market_price: float,
        best_bid: float,
        best_ask: float,
        spread: float,
    ) -> tuple[float, float, float]:
        """Compute optimal limit order price, fill probability, and risk-reward ratio.

        The limit price sits between the current market price and our fair value.
        Closer to market = higher fill probability, lower edge.
        Closer to fair value = lower fill probability, higher edge.

        Returns: (limit_price, fill_probability, risk_reward_ratio)
        """
        if direction == "NO_TRADE":
            return 0.0, 0.0, 0.0

        if direction == "BUY_YES":
            # We want to buy YES cheaper than fair_prob
            raw_limit = market_price + self.bid_aggressiveness * (fair_prob - market_price)
            min_limit = best_bid + self.spread_markup
            limit_price = max(raw_limit, min_limit)
            max_limit = fair_prob - 0.02
            limit_price = min(limit_price, max_limit)
            limit_price = max(0.01, limit_price)

            # Sigmoid fill probability model:
            # P(fill) = 1 / (1 + exp(k * (limit - midpoint)))
            # where midpoint = (best_bid + best_ask) / 2, k controls steepness
            fill_prob = self._sigmoid_fill_prob(limit_price, best_bid, best_ask, spread)

            if limit_price > 0:
                rr_ratio = (1.0 - limit_price) / limit_price
            else:
                rr_ratio = 0.0

        elif direction == "BUY_NO":
            no_price = 1.0 - market_price
            no_fair = 1.0 - fair_prob
            raw_limit = no_price + self.bid_aggressiveness * (no_fair - no_price)
            limit_price = max(raw_limit, 0.01)
            limit_price = min(limit_price, no_fair - 0.02)
            limit_price = max(0.01, limit_price)

            # For NO side, flip the sigmoid reference
            no_bid = 1.0 - best_ask
            no_ask = 1.0 - best_bid
            fill_prob = self._sigmoid_fill_prob(limit_price, no_bid, no_ask, spread)

            if limit_price > 0:
                rr_ratio = (1.0 - limit_price) / limit_price
            else:
                rr_ratio = 0.0
        else:
            return 0.0, 0.0, 0.0

        return limit_price, fill_prob, rr_ratio

    @staticmethod
    def _sigmoid_fill_prob(
        limit_price: float,
        best_bid: float,
        best_ask: float,
        spread: float,
    ) -> float:
        """Sigmoid model for limit order fill probability.

        More realistic than linear decay:
        - At best_ask: ~90% fill probability (taking the offer)
        - At midpoint: ~50% fill probability
        - At best_bid: ~15% fill probability (joining the bid)
        - Below best_bid: rapidly approaches 5%

        Uses logistic function: P = 1 / (1 + exp(-k * (x - mid)))
        """
        mid = (best_bid + best_ask) / 2.0
        effective_spread = max(spread, 0.005)
        # Steepness: higher = sharper transition around midpoint
        k = 6.0 / effective_spread

        try:
            exponent = -k * (limit_price - mid)
            exponent = max(-20, min(20, exponent))  # prevent overflow
            fill_prob = 1.0 / (1.0 + math.exp(exponent))
        except OverflowError:
            fill_prob = 0.0 if limit_price < mid else 1.0

        # Clamp to [0.05, 0.95] — never 0% or 100%
        return max(0.05, min(0.95, fill_prob))

    def generate_signals(
        self,
        edge_result: MultiOutcomeEdgeResult,
        total_capital: float = 200.0,
        confidence: float = 1.0,
    ) -> list[BucketSignal]:
        """Generate trading signals with portfolio-level optimization.

        Multi-bucket constraint: total invested across all buckets must ensure
        worst-case loss (all positions lose) stays within risk budget.
        """
        signals = []
        total_allocated = 0.0

        # Combine buys and sells, prioritized by expected value
        candidates = list(edge_result.best_buys) + list(edge_result.best_sells)

        # Filter out self-hedging signals: don't buy YES on one bucket
        # and BUY_NO on an adjacent bucket (they partially cancel out)
        candidates = self._remove_hedging_signals(candidates)

        # Sort by Kelly fraction × fill_prob (expected executable EV)
        candidates.sort(key=lambda x: -(x.kelly_fraction * x.expected_fill_prob))

        # Maximum total loss budget: 15% of capital
        max_total_loss = total_capital * 0.15

        for be in candidates[:self.max_buckets]:
            if be.direction == "NO_TRADE":
                continue

            # Position sizing: Kelly * capital * confidence, capped
            kelly_amount = be.kelly_fraction * total_capital * confidence
            amount = min(kelly_amount, self.per_bucket_max)
            amount = min(amount, self.total_max - total_allocated)

            # Worst-case loss check: if this bucket loses,
            # loss = amount (for YES) or amount (for NO)
            if total_allocated + amount > max_total_loss:
                amount = max(0, max_total_loss - total_allocated)

            amount = max(amount, 0)
            if amount < 5.0:  # Polymarket min order
                continue

            total_allocated += amount

            signal = BucketSignal(
                label=be.label,
                direction=be.direction,
                edge=be.edge,
                fair_prob=be.fair_prob,
                market_price=be.market_price,
                amount=round(amount, 2),
                confidence=confidence,
            )
            signals.append(signal)

            logger.info(
                f"Signal: {be.direction} {be.label} edge={be.edge:+.3f} "
                f"kelly={be.kelly_fraction:.3f} amount=${amount:.2f} "
                f"limit={be.limit_price:.3f} R/R={be.risk_reward_ratio:.1f} "
                f"fill_prob={be.expected_fill_prob:.0%}"
            )

        # Log portfolio summary
        if signals:
            worst_loss = sum(s.amount for s in signals)
            best_case = max(
                (s.amount / s.market_price * (1 - s.market_price) - (worst_loss - s.amount))
                for s in signals
            ) if signals else 0
            logger.info(
                f"Portfolio: {len(signals)} positions, "
                f"total=${total_allocated:.2f}, "
                f"worst_loss=${worst_loss:.2f}, "
                f"best_case=${best_case:.2f}"
            )

        return signals

    @staticmethod
    def _remove_hedging_signals(candidates: list[BucketEdge]) -> list[BucketEdge]:
        """Remove signals that hedge each other (BUY_YES + BUY_NO on adjacent buckets).

        Temperature buckets are mutually exclusive: if 21°C wins, 20°C loses.
        Having BUY_YES on 21°C and BUY_NO on 20°C is partially redundant —
        keep only the stronger signal.
        """
        buy_yes = {c.label: c for c in candidates if c.direction == "BUY_YES"}
        buy_no = {c.label: c for c in candidates if c.direction == "BUY_NO"}

        # If we have both BUY_YES and BUY_NO signals, keep the one with better EV
        to_remove = set()
        for label, yes_sig in buy_yes.items():
            for no_label, no_sig in buy_no.items():
                yes_ev = abs(yes_sig.edge) * yes_sig.expected_fill_prob * yes_sig.kelly_fraction
                no_ev = abs(no_sig.edge) * no_sig.expected_fill_prob * no_sig.kelly_fraction
                # If both exist and one is clearly weaker, drop it
                if yes_ev > no_ev * 1.5:
                    to_remove.add(no_label)
                elif no_ev > yes_ev * 1.5:
                    to_remove.add(label)

        return [c for c in candidates if c.label not in to_remove or c.direction == "NO_TRADE"]

    def compute_scenario_pnl(
        self,
        signals: list[BucketSignal],
        snapshot: MultiMarketSnapshot,
    ) -> dict[str, float]:
        """Compute P&L for every possible temperature outcome.

        Returns: {bucket_label: total_pnl_if_this_bucket_wins}
        This helps visualize the risk profile of the multi-bucket portfolio.
        """
        pnl_by_outcome = {}
        bucket_labels = [bp.label for bp in snapshot.buckets]

        for outcome_label in bucket_labels:
            total_pnl = 0.0
            for signal in signals:
                if signal.direction == "BUY_YES":
                    # Bought YES at market_price
                    price = signal.market_price
                    shares = signal.amount / price
                    if signal.label == outcome_label:
                        # This bucket won: profit = shares * (1 - price)
                        total_pnl += shares * (1.0 - price)
                    else:
                        # This bucket lost: loss = amount
                        total_pnl -= signal.amount

                elif signal.direction == "BUY_NO":
                    # Bought NO at (1 - market_price)
                    no_price = 1.0 - signal.market_price
                    shares = signal.amount / no_price
                    if signal.label == outcome_label:
                        # This bucket won (YES wins): NO loses
                        total_pnl -= signal.amount
                    else:
                        # This bucket didn't win: NO wins, profit
                        total_pnl += shares * signal.market_price

            pnl_by_outcome[outcome_label] = round(total_pnl, 2)

        return pnl_by_outcome
