"""
Standalone web server entry point.

Usage:
    cd weather-quant
    python -m src.web.server

Initializes the trading system modules and starts the FastAPI server on port 8000.
"""

import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import yaml
from dotenv import load_dotenv

load_dotenv("config/.env")

from src.utils.db import init_db
from src.engine.bias_calibrator import ensure_calibration_table
from src.data.fetcher_openmeteo import OpenMeteoFetcher
from src.data.fetcher_wunderground import WundergroundFetcher
from src.data.normalizer import Normalizer
from src.engine.multi_outcome_prob import MultiOutcomeProbEngine
from src.trading.series_tracker import SeriesTracker
from src.trading.executor import Executor
from src.review.settlement_review import SettlementReview
from src.trading.risk_control import RiskControl
from src.utils.logger import logger


def create_modules(config: dict) -> dict:
    """Lightweight module init for the web server (no CLOB client, no scheduler)."""
    db = init_db()
    ensure_calibration_table()

    return {
        "db": db,
        "fetcher": OpenMeteoFetcher(config["data_sources"]),
        "wu_fetcher": WundergroundFetcher(),
        "normalizer": Normalizer(),
        "multi_prob_engine": MultiOutcomeProbEngine(config),
        "series_tracker": SeriesTracker(),
        "executor": Executor(config, db, dry_run=True, clob_client=None),
        "reviewer": SettlementReview(db),
        "risk_control": RiskControl(config, db),
    }


def main():
    with open("config/settings.yaml", "r") as f:
        config = yaml.safe_load(f)

    logger.info("Initializing web server modules...")
    modules = create_modules(config)

    from src.web.app import create_app
    app = create_app(modules, config)

    import uvicorn
    logger.info("Starting web server on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
