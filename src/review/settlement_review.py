import json
import os
from datetime import date, datetime, timezone

from src.utils.logger import logger


class SettlementReview:
    """L9: Post-settlement review and model calibration."""

    def __init__(self, db):
        self.db = db
        os.makedirs("data/reviews", exist_ok=True)

    def run(self, market_id: str, actual_result: bool):
        """
        Run settlement review for a market.

        1. Get all signals for this market
        2. Get all trades for this market
        3. Calculate PnL
        4. Save review to DB and JSON
        """
        signals = self._get_signals(market_id)
        trades = self._get_trades(market_id)

        if not signals:
            logger.warning(f"No signals found for {market_id}")
            return

        # Calculate PnL
        total_pnl = 0.0
        for trade in trades:
            pnl = self._calc_pnl(trade, actual_result)
            total_pnl += pnl

        # Use last signal's fair_prob as our prediction
        last_signal = signals[-1]
        our_prediction = last_signal["fair_prob"]
        market_price_entry = last_signal["market_price"]
        model_error = abs(our_prediction - (1.0 if actual_result else 0.0))

        review = {
            "market_id": market_id,
            "event_date": date.today().isoformat(),
            "our_prediction": our_prediction,
            "market_price_entry": market_price_entry,
            "actual_result": actual_result,
            "total_pnl": round(total_pnl, 2),
            "model_error": round(model_error, 4),
            "num_signals": len(signals),
            "num_trades": len(trades),
            "signals": signals,
            "trades": trades,
        }

        # Save to settlements table
        try:
            self.db.execute(
                """INSERT INTO settlements
                   (market_id, event_date, our_prediction, market_price_entry,
                    actual_result, pnl, model_error, review_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    market_id,
                    date.today().isoformat(),
                    our_prediction,
                    market_price_entry,
                    1 if actual_result else 0,
                    total_pnl,
                    model_error,
                    json.dumps(review),
                ),
            )
            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to save settlement: {e}")

        # Save detailed JSON
        json_path = f"data/reviews/{market_id}_{date.today().isoformat()}.json"
        with open(json_path, "w") as f:
            json.dump(review, f, indent=2)
        logger.info(f"Review saved to {json_path}")

        return review

    def _get_signals(self, market_id: str) -> list[dict]:
        cursor = self.db.execute(
            "SELECT * FROM signals WHERE market_id = ? ORDER BY signal_time",
            (market_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _get_trades(self, market_id: str) -> list[dict]:
        cursor = self.db.execute(
            "SELECT * FROM trades WHERE market_id = ? ORDER BY executed_at",
            (market_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _calc_pnl(self, trade: dict, actual_result: bool) -> float:
        """Calculate PnL for a single trade."""
        side = trade["side"]
        price = trade["price"]
        shares = trade.get("shares")
        if shares in (None, 0):
            shares = (trade["amount"] / price) if price else 0.0

        exit_price = trade.get("exit_price")
        if exit_price is not None and trade.get("status") == "CLOSED":
            return shares * (exit_price - price)

        settlement_value = 0.0
        if side == "YES":
            settlement_value = 1.0 if actual_result else 0.0
        else:
            settlement_value = 0.0 if actual_result else 1.0

        return shares * (settlement_value - price)

    def get_cumulative_stats(self) -> dict:
        """Return cumulative trading statistics."""
        try:
            cursor = self.db.execute("SELECT * FROM settlements")
            rows = [dict(row) for row in cursor.fetchall()]

            if not rows:
                return {"total_trades": 0, "message": "No settlements yet"}

            total_trades = len(rows)
            wins = sum(1 for r in rows if (r["pnl"] or 0) > 0)
            total_pnl = sum(r["pnl"] or 0 for r in rows)

            # Model calibration by probability bucket
            calibration = {}
            for row in rows:
                pred = row["our_prediction"]
                bucket = f"{int(pred * 10) / 10:.1f}-{int(pred * 10) / 10 + 0.1:.1f}"
                if bucket not in calibration:
                    calibration[bucket] = {"count": 0, "wins": 0}
                calibration[bucket]["count"] += 1
                if row["actual_result"]:
                    calibration[bucket]["wins"] += 1

            for bucket in calibration:
                c = calibration[bucket]
                c["win_rate"] = round(c["wins"] / c["count"], 2) if c["count"] > 0 else 0

            return {
                "total_trades": total_trades,
                "win_rate": round(wins / total_trades, 2) if total_trades > 0 else 0,
                "total_pnl": round(total_pnl, 2),
                "model_calibration": calibration,
            }
        except Exception as e:
            logger.error(f"Failed to compute stats: {e}")
            return {"error": str(e)}
