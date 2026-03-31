from src.data.schemas import EdgeResult
from src.utils.logger import logger


class EdgeDetector:
    """L5: Compare fair_prob with market price to compute edge and direction."""

    def __init__(self, config: dict):
        self.entry_threshold = config["trading"]["entry_threshold"]

    def detect(self, fair_prob: float, market_price: float) -> EdgeResult:
        """
        Compute edge and trading direction.

        edge = fair_prob - market_price
        Strictly greater than threshold to trade (equal = NO_TRADE).
        """
        edge = fair_prob - market_price
        abs_edge = round(abs(edge), 10)
        threshold = round(self.entry_threshold, 10)

        if abs_edge > threshold and edge > 0:
            direction = "BUY_YES"
        elif abs_edge > threshold and edge < 0:
            direction = "BUY_NO"
        else:
            direction = "NO_TRADE"

        logger.debug(
            f"Edge: fair={fair_prob:.3f}, market={market_price:.3f}, "
            f"edge={edge:.3f}, dir={direction}"
        )

        return EdgeResult(
            edge=edge,
            direction=direction,
            fair_prob=fair_prob,
            market_price=market_price,
        )
