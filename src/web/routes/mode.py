"""POST /api/mode — Switch between LIVE and DRY_RUN trading modes."""

import os
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from pydantic import BaseModel

from src.web.cache import cache

router = APIRouter()


class ModeRequest(BaseModel):
    mode: str  # "LIVE" or "DRY_RUN"


def build_mode_response(request: Request) -> dict:
    """Build a user-facing mode status payload."""
    live = getattr(request.app.state, "live_trading", None)
    source = "app_state"
    if live is None:
        live = os.getenv("LIVE_TRADING", "false").lower() == "true"
        source = "env"

    current_mode = "LIVE" if live else "DRY_RUN"
    target_mode = "DRY_RUN" if live else "LIVE"

    return {
        "mode": current_mode,
        "live_trading": live,
        "mode_label": "实盘交易" if live else "模拟运行",
        "mode_badge": "实盘" if live else "模拟",
        "mode_description": (
            "将发送真实订单并使用真实资金"
            if live
            else "仅生成信号和模拟持仓，不会发送真实订单"
        ),
        "target_mode": target_mode,
        "target_mode_label": "切换到模拟" if live else "切换到实盘",
        "source": source,
    }


@router.get("/mode")
def get_mode(request: Request):
    return build_mode_response(request)


@router.post("/mode")
def set_mode(body: ModeRequest, request: Request):
    if body.mode not in ("LIVE", "DRY_RUN"):
        return {"error": "mode must be LIVE or DRY_RUN"}

    previous = build_mode_response(request)
    is_live = body.mode == "LIVE"
    request.app.state.live_trading = is_live
    os.environ["LIVE_TRADING"] = "true" if is_live else "false"

    # Clear all caches so data refreshes under new mode
    cache.clear()

    response = build_mode_response(request)
    response.update(
        {
            "previous_mode": previous["mode"],
            "switched": previous["mode"] != response["mode"],
            "switched_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    return response
