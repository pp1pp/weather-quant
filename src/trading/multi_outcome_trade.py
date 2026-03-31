from __future__ import annotations

from datetime import datetime
from typing import Any


ACTIVE_TRADE_STATUSES = ("OPEN", "DRY_RUN", "SUBMITTED")


def build_bucket_market_id(event_slug: str, label: str, city: str = "shanghai") -> str:
    """Create a stable internal market id for a Shanghai bucket."""
    slug = event_slug or "unknown-event"
    return f"{city}::{slug}::{label}"


def build_bucket_trade_meta(
    *,
    city: str,
    event_slug: str,
    settlement_time_utc: datetime,
    bucket: dict[str, Any],
    fair_prob: float,
    yes_price: float,
    no_price: float | None = None,
) -> dict[str, Any]:
    """Capture the metadata needed for live execution and later risk checks."""
    return {
        "market_type": "multi_outcome_bucket",
        "city": city,
        "event_slug": event_slug,
        "settlement_time_utc": settlement_time_utc.isoformat(),
        "label": bucket["label"],
        "condition_id": bucket.get("condition_id"),
        "yes_token_id": bucket.get("yes_token_id"),
        "no_token_id": bucket.get("no_token_id"),
        "temp_low": bucket.get("temp_low"),
        "temp_high": bucket.get("temp_high"),
        "tick_size": bucket.get("tick_size", 0.01),
        "min_order_size": bucket.get("min_order_size", 5.0),
        "entry_fair_prob": round(fair_prob, 6),
        "entry_yes_price": round(yes_price, 6),
        "entry_no_price": round(
            no_price if no_price is not None else max(0.0, 1.0 - yes_price),
            6,
        ),
    }


def side_price_from_yes_price(
    yes_price: float,
    side: str,
    no_price: float | None = None,
) -> float:
    """Convert the bucket YES price into the side-specific price we hold."""
    if side == "YES":
        return yes_price
    if no_price is not None:
        return no_price
    return max(0.0, 1.0 - yes_price)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
