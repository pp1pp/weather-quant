"""GET /api/stats — Cumulative performance statistics."""

import json

from fastapi import APIRouter, Request

from src.utils.db import get_connection

router = APIRouter()


@router.get("/stats")
def get_stats(request: Request):
    reviewer = request.app.state.modules["reviewer"]
    cumulative = reviewer.get_cumulative_stats()

    # Recent settlements from DB
    conn = get_connection()
    try:
        cursor = conn.execute(
            """SELECT market_id, event_date, our_prediction, market_price_entry,
                      actual_result, pnl, model_error, review_json
               FROM settlements
               ORDER BY event_date DESC
               LIMIT 30"""
        )
        rows = cursor.fetchall()
    finally:
        conn.close()

    settlements = []
    cumulative_pnl = 0.0
    pnl_series = []
    for row in reversed(rows):
        pnl = row["pnl"] or 0
        cumulative_pnl += pnl
        settlements.append({
            "market_id": row["market_id"],
            "event_date": row["event_date"],
            "prediction": row["our_prediction"],
            "actual": row["actual_result"],
            "pnl": round(pnl, 2),
            "model_error": row["model_error"],
        })
        pnl_series.append({
            "date": row["event_date"],
            "pnl": round(pnl, 2),
            "cumulative_pnl": round(cumulative_pnl, 2),
        })

    # Trade stats from trades table
    conn2 = get_connection()
    try:
        cursor2 = conn2.execute(
            "SELECT COUNT(*) as cnt, status FROM trades GROUP BY status"
        )
        status_counts = {row["status"]: row["cnt"] for row in cursor2.fetchall()}
    finally:
        conn2.close()

    return {
        "cumulative": cumulative,
        "settlements": settlements,
        "pnl_series": pnl_series,
        "trade_counts": status_counts,
    }
