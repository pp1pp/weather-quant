"""GET /api/positions — Open positions with P&L."""

import json

from fastapi import APIRouter, Request

from src.utils.db import get_connection
from src.web.request_context import get_event_context, get_selected_date

router = APIRouter()


@router.get("/positions")
def get_positions(request: Request):
    selected_date = get_selected_date(request)
    ctx = get_event_context(request) if selected_date else None
    selected_event_slug = ctx["event"].get("slug", "") if ctx else None

    conn = get_connection()
    try:
        cursor = conn.execute(
            """SELECT id, market_id, side, amount, price, shares,
                      entry_fair_prob, trade_meta, executed_at, status
               FROM trades
               WHERE status IN ('OPEN', 'DRY_RUN', 'SUBMITTED')
               ORDER BY executed_at DESC"""
        )
        rows = cursor.fetchall()
    finally:
        conn.close()

    positions = []
    total_exposure = 0.0
    for row in rows:
        meta = json.loads(row["trade_meta"]) if row["trade_meta"] else {}
        if selected_event_slug and meta.get("event_slug") != selected_event_slug:
            continue
        positions.append({
            "id": row["id"],
            "market_id": row["market_id"],
            "label": meta.get("label", ""),
            "side": row["side"],
            "amount": row["amount"],
            "entry_price": row["price"],
            "shares": row["shares"] or 0,
            "entry_fair_prob": row["entry_fair_prob"],
            "status": row["status"],
            "executed_at": row["executed_at"],
            "event_slug": meta.get("event_slug", ""),
        })
        total_exposure += row["amount"] or 0

    # Also get recently closed trades (last 20)
    conn2 = get_connection()
    try:
        cursor2 = conn2.execute(
            """SELECT id, market_id, side, amount, price, shares, exit_price,
                      trade_meta, executed_at, closed_at, status
               FROM trades
               WHERE status = 'CLOSED'
               ORDER BY closed_at DESC
               LIMIT 20"""
        )
        closed_rows = cursor2.fetchall()
    finally:
        conn2.close()

    closed = []
    for row in closed_rows:
        meta = json.loads(row["trade_meta"]) if row["trade_meta"] else {}
        if selected_event_slug and meta.get("event_slug") != selected_event_slug:
            continue
        entry_p = row["price"] or 0
        exit_p = row["exit_price"] or 0
        shares = row["shares"] if "shares" in row.keys() and row["shares"] not in (None, 0) else 0
        if shares == 0 and entry_p > 0:
            shares = row["amount"] / entry_p
        pnl = shares * (exit_p - entry_p) if shares > 0 else 0
        closed.append({
            "id": row["id"],
            "market_id": row["market_id"],
            "label": meta.get("label", ""),
            "side": row["side"],
            "amount": row["amount"],
            "shares": round(shares, 4) if shares else 0,
            "entry_price": entry_p,
            "exit_price": exit_p,
            "pnl": round(pnl, 2),
            "executed_at": row["executed_at"],
            "closed_at": row["closed_at"],
        })

    return {
        "open_positions": positions,
        "total_exposure": round(total_exposure, 2),
        "closed_trades": closed,
        "selected_event_slug": selected_event_slug,
        "selected_date": selected_date,
        "is_filtered": bool(selected_event_slug),
    }
