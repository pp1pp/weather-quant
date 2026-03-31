import json
from datetime import datetime, timezone

from src.data.schemas import Order
from src.trading.multi_outcome_trade import (
    ACTIVE_TRADE_STATUSES,
    build_bucket_market_id,
)
from src.utils.logger import logger


class Executor:
    """
    Execute trading orders.

    Modes:
    - dry_run=True: log only, save to DB as DRY_RUN (safe testing)
    - dry_run=False + clob_client: execute real trades on Polymarket
    """

    def __init__(self, config: dict, db, dry_run: bool = True, clob_client=None):
        self.dry_run = dry_run
        self.db = db
        self.clob_client = clob_client
        self._market_registry = {}
        self._refresh_market_registry()

    def _refresh_market_registry(self):
        self._market_registry = self._load_market_registry()

    def _load_market_registry(self) -> dict:
        import yaml

        try:
            with open("config/markets.yaml", "r") as f:
                data = yaml.safe_load(f) or {}
        except Exception:
            return {}

        registry = {}

        for market in data.get("markets", []):
            registry[market["id"]] = market

        city = data.get("city", "shanghai")
        event_slug = data.get("event_slug", "")
        event_end_date = data.get("event_end_date", "")
        for bucket in data.get("buckets", []):
            entry = {
                **bucket,
                "city": city,
                "event_slug": event_slug,
                "event_end_date": event_end_date,
                "market_type": "multi_outcome_bucket",
            }
            keys = {
                bucket.get("label"),
                bucket.get("condition_id"),
                f"{city}-{bucket.get('label', '')}",
            }
            if event_slug and bucket.get("label"):
                keys.add(build_bucket_market_id(event_slug, bucket["label"], city))
            for key in keys:
                if key:
                    registry[key] = entry

        return registry

    def execute(self, order: Order, metadata: dict | None = None) -> dict:
        """Execute an order (or simulate in dry run mode)."""
        now = datetime.now(timezone.utc)
        metadata = metadata or {}
        shares = round(order.amount / order.price, 4) if order.price > 0 else 0.0

        if self.dry_run:
            logger.info(
                f"[DRY RUN] Would BUY {order.side} ${order.amount:.2f} "
                f"@ {order.price:.3f} on {order.market_id}"
            )
            status = "DRY_RUN"
            result = {"status": status, "order": order.model_dump(), "shares": shares}

        elif self.clob_client:
            try:
                result = self._execute_real(order, metadata)
                status = result.get("status", "SUBMITTED")
                shares = result.get("shares", shares)
            except Exception as e:
                logger.error(f"Real execution failed: {e}")
                status = "FAILED"
                result = {"status": status, "error": str(e), "shares": shares}
        else:
            logger.error("No CLOB client and dry_run=False. Cannot execute.")
            status = "NO_CLIENT"
            result = {"status": status, "shares": shares}

        self._save_trade(
            order=order,
            status=status,
            executed_at=now,
            shares=shares,
            entry_fair_prob=metadata.get("entry_fair_prob"),
            trade_meta=metadata,
        )
        return result

    def _save_trade(
        self,
        *,
        order: Order,
        status: str,
        executed_at: datetime,
        shares: float,
        entry_fair_prob: float | None,
        trade_meta: dict,
    ):
        try:
            self.db.execute(
                """INSERT INTO trades
                   (market_id, side, amount, price, executed_at, status,
                    shares, entry_fair_prob, trade_meta)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    order.market_id,
                    order.side,
                    order.amount,
                    order.price,
                    executed_at.isoformat(),
                    status,
                    shares,
                    entry_fair_prob,
                    json.dumps(trade_meta),
                ),
            )
            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to save trade: {e}")

    def _resolve_market(self, order: Order, metadata: dict | None = None) -> dict:
        self._refresh_market_registry()
        metadata = metadata or {}
        candidates = [order.market_id]

        event_slug = metadata.get("event_slug")
        label = metadata.get("label")
        city = metadata.get("city", "shanghai")
        if event_slug and label:
            candidates.append(build_bucket_market_id(event_slug, label, city))

        for key in (
            metadata.get("condition_id"),
            label,
            f"{city}-{label}" if label else None,
        ):
            if key:
                candidates.append(key)

        for key in candidates:
            if key and key in self._market_registry:
                return self._market_registry[key]

        return metadata

    def _execute_real(self, order: Order, metadata: dict | None = None) -> dict:
        """Execute a real order on Polymarket via CLOB client."""
        try:
            from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions
        except ImportError:
            from py_clob_client import OrderArgs

            PartialCreateOrderOptions = None

        market_cfg = self._resolve_market(order, metadata)

        if order.side == "YES":
            trade_token = market_cfg.get("token_id") or market_cfg.get("yes_token_id")
            action = "BUY"
        else:
            trade_token = market_cfg.get("no_token_id")
            action = "BUY"
            if not trade_token:
                trade_token = market_cfg.get("token_id")
                action = "SELL"

        if not trade_token:
            raise ValueError(
                f"No token id configured for market {order.market_id}. "
                f"Resolved metadata: {market_cfg}"
            )

        size = round(order.amount / order.price, 4) if order.price > 0 else 0.0
        if size <= 0:
            raise ValueError(f"Invalid order size for {order.market_id}: {size}")

        order_args = OrderArgs(
            token_id=trade_token,
            price=order.price,
            size=size,
            side=action,
        )

        tick_size = market_cfg.get("tick_size")
        logger.info(
            f"[LIVE] Submitting {action} {order.side} size={size} "
            f"@ {order.price:.3f} on {order.market_id}"
        )

        if PartialCreateOrderOptions is not None and tick_size is not None:
            options = PartialCreateOrderOptions(
                tick_size=str(tick_size),
                neg_risk=True,
            )
            resp = self.clob_client.create_and_post_order(order_args, options)
        else:
            resp = self.clob_client.create_and_post_order(order_args)

        logger.info(f"[LIVE] Order response: {resp}")
        return {
            "status": "SUBMITTED",
            "order": order.model_dump(),
            "response": resp,
            "shares": size,
        }

    def _submit_exit_order(
        self,
        position: dict,
        price: float,
        shares: float,
    ) -> bool:
        if not self.clob_client or shares <= 0 or price <= 0:
            return False

        try:
            from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions
        except ImportError:
            from py_clob_client import OrderArgs

            PartialCreateOrderOptions = None

        meta = position.get("trade_meta") or {}
        token_id = (
            meta.get("yes_token_id")
            if position["side"] == "YES"
            else meta.get("no_token_id")
        )
        if not token_id:
            logger.error(f"No token id available to exit {position['market_id']}")
            return False

        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=round(shares, 4),
            side="SELL",
        )

        tick_size = meta.get("tick_size")
        logger.info(
            f"[LIVE] Closing {position['market_id']} {position['side']} "
            f"shares={shares:.4f} @ {price:.3f}"
        )

        if PartialCreateOrderOptions is not None and tick_size is not None:
            options = PartialCreateOrderOptions(
                tick_size=str(tick_size),
                neg_risk=True,
            )
            self.clob_client.create_and_post_order(order_args, options)
        else:
            self.clob_client.create_and_post_order(order_args)

        return True

    def get_open_positions(self) -> list[dict]:
        """Query all active positions with parsed metadata."""
        statuses_sql = ", ".join(f"'{status}'" for status in ACTIVE_TRADE_STATUSES)
        try:
            cursor = self.db.execute(
                "SELECT id, market_id, side, amount, price as entry_price, "
                "shares, entry_fair_prob, trade_meta, executed_at, status "
                f"FROM trades WHERE status IN ({statuses_sql})"
            )
            results = []
            for row in cursor.fetchall():
                item = dict(row)
                raw_meta = item.get("trade_meta")
                item["trade_meta"] = json.loads(raw_meta) if raw_meta else {}
                results.append(item)
            return results
        except Exception as e:
            logger.error(f"Failed to query positions: {e}")
            return []

    def get_current_exposure(
        self,
        market_id: str | None = None,
        side: str | None = None,
        city: str | None = None,
    ) -> float:
        positions = self.get_open_positions()
        total = 0.0
        for pos in positions:
            if market_id and pos["market_id"] != market_id:
                continue
            if side and pos["side"] != side:
                continue
            if city:
                pos_city = (pos.get("trade_meta") or {}).get("city")
                if pos_city != city:
                    continue
            total += pos["amount"]
        return total

    def get_open_position(self, market_id: str, side: str) -> dict | None:
        matches = [
            pos
            for pos in self.get_open_positions()
            if pos["market_id"] == market_id and pos["side"] == side
        ]
        if not matches:
            return None

        total_amount = sum(pos["amount"] for pos in matches)
        total_shares = sum(pos.get("shares") or 0 for pos in matches)
        avg_price = (
            sum(pos["entry_price"] * pos["amount"] for pos in matches) / total_amount
            if total_amount > 0
            else 0.0
        )
        latest_meta = matches[-1].get("trade_meta") or {}
        avg_fair = None
        fair_values = [pos.get("entry_fair_prob") for pos in matches if pos.get("entry_fair_prob") is not None]
        if fair_values:
            avg_fair = sum(fair_values) / len(fair_values)

        return {
            "market_id": market_id,
            "side": side,
            "amount": total_amount,
            "shares": total_shares,
            "entry_price": avg_price,
            "entry_fair_prob": avg_fair,
            "trade_meta": latest_meta,
        }

    def close_position(self, position: dict, current_price: float | None = None) -> bool:
        """Close one specific position, submitting a live offset order when possible."""
        now = datetime.now(timezone.utc).isoformat()

        if (
            not self.dry_run
            and self.clob_client
            and current_price is not None
            and (position.get("shares") or 0) > 0
        ):
            ok = self._submit_exit_order(
                position=position,
                price=current_price,
                shares=position["shares"],
            )
            if not ok:
                return False

        try:
            self.db.execute(
                "UPDATE trades SET status = 'CLOSED', closed_at = ?, exit_price = ? "
                "WHERE id = ?",
                (now, current_price, position["id"]),
            )
            self.db.commit()
            logger.info(f"Closed position for {position['market_id']}")
            return True
        except Exception as e:
            logger.error(f"Failed to close position: {e}")
            return False

    def reduce_position(
        self,
        position: dict,
        reduce_pct: float,
        current_price: float | None = None,
    ) -> bool:
        """Reduce one position by percentage, selling inventory in live mode."""
        reduce_pct = max(0.0, min(1.0, reduce_pct))
        if reduce_pct <= 0:
            return True

        remaining_amount = round(position["amount"] * (1 - reduce_pct), 4)
        remaining_shares = round((position.get("shares") or 0) * (1 - reduce_pct), 4)
        sell_shares = round((position.get("shares") or 0) * reduce_pct, 4)

        logger.info(
            f"Reducing {position['market_id']} by {reduce_pct*100:.0f}%: "
            f"${position['amount']:.2f} -> ${remaining_amount:.2f}"
        )

        if (
            not self.dry_run
            and self.clob_client
            and current_price is not None
            and sell_shares > 0
        ):
            ok = self._submit_exit_order(
                position=position,
                price=current_price,
                shares=sell_shares,
            )
            if not ok:
                return False

        try:
            if remaining_amount <= 0 or remaining_shares <= 0:
                self.db.execute(
                    "UPDATE trades SET status = 'CLOSED', closed_at = ?, exit_price = ? "
                    "WHERE id = ?",
                    (datetime.now(timezone.utc).isoformat(), current_price, position["id"]),
                )
                self.db.commit()
                return True

            self.db.execute(
                "UPDATE trades SET amount = ?, shares = ?, exit_price = ? "
                "WHERE id = ?",
                (remaining_amount, remaining_shares, current_price, position["id"]),
            )
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to reduce position: {e}")
            return False
