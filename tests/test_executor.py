import json
from datetime import datetime, timezone

import yaml

from src.data.schemas import Order
from src.trading.executor import Executor
from src.trading.multi_outcome_trade import (
    build_bucket_market_id,
    build_bucket_trade_meta,
)
from src.utils.db import init_db


def test_executor_records_multi_outcome_trade_metadata():
    db = init_db()
    db.execute("DELETE FROM trades")
    db.commit()

    with open("config/markets.yaml", "r") as f:
        markets = yaml.safe_load(f)

    bucket = markets["buckets"][0]
    market_id = build_bucket_market_id(markets["event_slug"], bucket["label"])
    meta = build_bucket_trade_meta(
        city="shanghai",
        event_slug=markets["event_slug"],
        settlement_time_utc=datetime.fromisoformat(
            markets["event_end_date"].replace("Z", "+00:00")
        ),
        bucket=bucket,
        fair_prob=0.42,
        yes_price=0.31,
    )

    executor = Executor({"multi_outcome": {}}, db, dry_run=True)
    result = executor.execute(
        Order(market_id=market_id, side="NO", amount=12.0, price=0.69),
        metadata=meta,
    )

    assert result["status"] == "DRY_RUN"
    row = db.execute(
        "SELECT side, amount, price, shares, entry_fair_prob, trade_meta "
        "FROM trades ORDER BY id DESC LIMIT 1"
    ).fetchone()

    assert row["side"] == "NO"
    assert row["shares"] == round(12.0 / 0.69, 4)
    assert row["entry_fair_prob"] == 0.42
    trade_meta = json.loads(row["trade_meta"])
    assert trade_meta["label"] == bucket["label"]
    assert trade_meta["no_token_id"] == bucket["no_token_id"]


def test_executor_city_exposure_counts_submitted_positions():
    db = init_db()
    db.execute("DELETE FROM trades")
    db.commit()

    executor = Executor({"multi_outcome": {}}, db, dry_run=True)
    db.execute(
        """INSERT INTO trades
           (market_id, side, amount, price, executed_at, status, shares, entry_fair_prob, trade_meta)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "shanghai::demo::18C",
            "YES",
            15.0,
            0.40,
            datetime.now(timezone.utc).isoformat(),
            "SUBMITTED",
            37.5,
            0.55,
            json.dumps({"city": "shanghai", "market_type": "multi_outcome_bucket"}),
        ),
    )
    db.commit()

    assert executor.get_current_exposure(city="shanghai") == 15.0
