from src.review.settlement_review import SettlementReview


def test_settlement_review_uses_share_count_for_yes_pnl():
    review = SettlementReview(db=None)
    trade = {
        "side": "YES",
        "amount": 10.0,
        "price": 0.40,
        "shares": None,
        "status": "DRY_RUN",
        "exit_price": None,
    }

    pnl = review._calc_pnl(trade, actual_result=True)
    assert pnl == 15.0


def test_settlement_review_uses_side_specific_price_for_no_pnl():
    review = SettlementReview(db=None)
    trade = {
        "side": "NO",
        "amount": 10.0,
        "price": 0.60,
        "shares": None,
        "status": "DRY_RUN",
        "exit_price": None,
    }

    pnl = review._calc_pnl(trade, actual_result=False)
    assert round(pnl, 4) == round((10.0 / 0.60) * 0.40, 4)
