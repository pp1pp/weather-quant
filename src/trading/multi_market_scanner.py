"""
Multi-Market Scanner for Polymarket temperature bucket events.

Fetches all sub-market prices for a multi-outcome event in a single API call
via the Gamma events API, returning a consolidated MultiMarketSnapshot.
"""

import json
from datetime import datetime, timezone

import httpx

from src.data.schemas import BucketPrice, MultiMarketSnapshot
from src.utils.logger import logger


GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"


class MultiMarketScanner:
    """
    Fetch price data for all buckets of a multi-outcome temperature event.

    Uses Gamma events API to get all sub-markets in one call,
    avoiding N individual CLOB API calls.
    """

    def __init__(self, config: dict):
        self.config = config
        self._http = httpx.Client(timeout=20)

    def fetch_snapshot(self, event_slug: str) -> MultiMarketSnapshot | None:
        """
        Fetch all sub-market prices for the given event slug.

        Args:
            event_slug: e.g. "highest-temperature-in-shanghai-on-march-25-2026"

        Returns:
            MultiMarketSnapshot with all bucket prices, or None on failure.
        """
        try:
            resp = self._http.get(
                GAMMA_EVENTS_URL,
                params={"slug": event_slug},
            )
            resp.raise_for_status()
            events = resp.json()

            if not events:
                logger.warning(f"No event found for slug: {event_slug}")
                return None

            event = events[0]
            markets = event.get("markets", [])
            if not markets:
                logger.warning(f"No sub-markets for event: {event_slug}")
                return None

            buckets = []
            for m in markets:
                # Parse outcome prices
                prices = m.get("outcomePrices", "[]")
                if isinstance(prices, str):
                    prices = json.loads(prices)
                yes_price = float(prices[0]) if prices else 0.0
                no_price = float(prices[1]) if len(prices) > 1 else 1.0 - yes_price

                bp = BucketPrice(
                    label=m.get("groupItemTitle", m.get("question", "?")),
                    yes_price=yes_price,
                    no_price=no_price,
                    best_bid=float(m.get("bestBid", 0)),
                    best_ask=float(m.get("bestAsk", 0)),
                    spread=float(m.get("spread", 0)),
                    volume=float(m.get("volumeNum", 0)),
                    liquidity=float(m.get("liquidityNum", 0)),
                )
                buckets.append(bp)

            # Sort by groupItemThreshold to ensure correct order
            # Gamma API returns markets with groupItemThreshold 0, 1, 2, ...
            threshold_map = {}
            for m in markets:
                title = m.get("groupItemTitle", "")
                threshold = int(m.get("groupItemThreshold", 99))
                threshold_map[title] = threshold

            buckets.sort(key=lambda b: threshold_map.get(b.label, 99))

            total_sum = sum(b.yes_price for b in buckets)

            snapshot = MultiMarketSnapshot(
                event_slug=event_slug,
                buckets=buckets,
                total_price_sum=round(total_sum, 4),
                fetched_at=datetime.now(timezone.utc),
            )

            logger.info(
                f"Fetched {len(buckets)} buckets for {event_slug}, "
                f"price_sum={total_sum:.4f}"
            )
            return snapshot

        except Exception as e:
            logger.error(f"Failed to fetch multi-market snapshot: {e}")
            return None

    def discover_shanghai_events(self) -> list[dict]:
        """
        Discover all active Shanghai temperature events.

        Returns list of {slug, title, date, volume, end_date}.
        """
        try:
            # Search by Shanghai tag/series
            resp = self._http.get(
                GAMMA_EVENTS_URL,
                params={"tag": "shanghai", "closed": "false", "limit": 20},
            )
            resp.raise_for_status()
            events = resp.json()

            results = []
            for e in events:
                title = e.get("title", "")
                if "temperature" in title.lower() and "shanghai" in title.lower():
                    results.append({
                        "slug": e.get("slug"),
                        "title": title,
                        "end_date": e.get("endDate"),
                        "volume": e.get("volume", 0),
                        "liquidity": e.get("liquidity", 0),
                        "num_markets": len(e.get("markets", [])),
                    })

            results.sort(key=lambda x: x.get("end_date", ""), reverse=True)
            logger.info(f"Found {len(results)} Shanghai temperature events")
            return results

        except Exception as e:
            logger.error(f"Failed to discover Shanghai events: {e}")
            return []

    def get_latest_shanghai_slug(self) -> str | None:
        """Get the slug for the latest (nearest future) Shanghai temperature event."""
        events = self.discover_shanghai_events()
        if not events:
            return None
        # The closest event by end_date that hasn't ended yet
        now = datetime.now(timezone.utc).isoformat()
        future = [e for e in events if (e.get("end_date") or "") >= now[:10]]
        if future:
            # Sort ascending by end_date and pick the nearest
            future.sort(key=lambda x: x.get("end_date", ""))
            return future[0]["slug"]
        # If no future events, return the latest one
        return events[0]["slug"]

    def print_snapshot(self, snapshot: MultiMarketSnapshot):
        """Pretty-print a market snapshot."""
        print(f"\n{'='*80}")
        print(f"  SHANGHAI TEMPERATURE MARKET: {snapshot.event_slug}")
        print(f"  Fetched: {snapshot.fetched_at.strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"  Price Sum: {snapshot.total_price_sum:.4f} (ideal=1.0000)")
        gap = snapshot.total_price_sum - 1.0
        gap_str = f"+{gap:.4f}" if gap > 0 else f"{gap:.4f}"
        print(f"  Sum Gap: {gap_str} {'⚠️ OVERPRICED' if gap > 0.02 else '✅' if abs(gap) < 0.02 else '⚠️ UNDERPRICED'}")
        print(f"{'='*80}")
        print(f"  {'Bucket':<20} {'Price':>8} {'Bid':>8} {'Ask':>8} {'Spread':>8} {'Volume':>10}")
        print(f"  {'─'*74}")

        for b in snapshot.buckets:
            bar_len = int(b.yes_price * 40)
            bar = "█" * bar_len + "░" * (40 - bar_len)
            print(
                f"  {b.label:<20} {b.yes_price:>8.3f} {b.best_bid:>8.3f} "
                f"{b.best_ask:>8.3f} {b.spread:>8.3f} ${b.volume:>9,.0f}"
            )
            print(f"  {'':20} [{bar}]")

        print(f"{'='*80}\n")
