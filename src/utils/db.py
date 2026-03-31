import sqlite3
import os
from src.utils.logger import logger

DB_PATH = "data/weather.db"


def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> sqlite3.Connection:
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_forecasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            event_date DATE NOT NULL,
            source TEXT NOT NULL,
            model_name TEXT NOT NULL,
            hourly_temps JSON NOT NULL,
            fetched_at DATETIME NOT NULL,
            raw_response JSON,
            UNIQUE(city, event_date, source, model_name, fetched_at)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS normalized_forecasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            event_date DATE NOT NULL,
            model_forecasts JSON NOT NULL,
            latest_observation REAL,
            hours_to_settlement REAL,
            updated_at DATETIME NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT NOT NULL,
            signal_time DATETIME NOT NULL,
            fair_prob REAL NOT NULL,
            market_price REAL NOT NULL,
            edge REAL NOT NULL,
            signal_level TEXT NOT NULL,
            direction TEXT,
            filters_passed JSON
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT NOT NULL,
            signal_id INTEGER REFERENCES signals(id),
            side TEXT NOT NULL,
            amount REAL NOT NULL,
            price REAL NOT NULL,
            executed_at DATETIME NOT NULL,
            status TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS settlements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT NOT NULL,
            event_date DATE NOT NULL,
            our_prediction REAL NOT NULL,
            market_price_entry REAL,
            actual_result INTEGER NOT NULL,
            pnl REAL,
            model_error REAL,
            review_json JSON
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_factors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            event_date DATE NOT NULL,
            model_name TEXT NOT NULL,
            mean_cloud_cover REAL,
            max_wind_speed REAL,
            dominant_wind_dir REAL,
            is_sea_breeze INTEGER,
            total_precipitation REAL,
            mean_humidity REAL,
            mean_pressure REAL,
            pressure_trend REAL,
            diurnal_range REAL,
            fetched_at DATETIME NOT NULL,
            UNIQUE(city, event_date, model_name, fetched_at)
        )
    """)

    conn.commit()
    _ensure_trade_columns(conn)
    logger.info("Database initialized with all tables")
    return conn


def _ensure_trade_columns(conn: sqlite3.Connection):
    """Backfill new trade columns for older local databases."""
    existing = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(trades)")
    }
    needed = {
        "shares": "REAL",
        "entry_fair_prob": "REAL",
        "trade_meta": "JSON",
        "closed_at": "DATETIME",
        "exit_price": "REAL",
    }
    for name, kind in needed.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE trades ADD COLUMN {name} {kind}")
    conn.commit()
