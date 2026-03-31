from datetime import date, datetime, timezone

from fastapi.testclient import TestClient

import src.utils.db as db_module
from src.data.normalizer import Normalizer
from src.data.schemas import RawWeatherData
from src.utils.db import init_db
from src.web.app import create_app
from src.web.cache import cache
from src.engine.bias_calibrator import ensure_calibration_table


class DummyTracker:
    def __init__(self):
        self.events = {
            "2026-03-25": {
                "slug": "highest-temperature-in-shanghai-on-march-25-2026",
                "endDate": "2026-03-25T16:00:00Z",
                "markets": [],
            },
            "2026-03-26": {
                "slug": "highest-temperature-in-shanghai-on-march-26-2026",
                "endDate": "2026-03-26T16:00:00Z",
                "markets": [],
            },
        }

    def find_event_by_date(self, target_date, city="shanghai"):
        return self.events.get(str(target_date))

    def find_latest_event(self, city="shanghai"):
        return self.events["2026-03-26"]

    def list_available_dates(self, city="shanghai"):
        return sorted(self.events.keys(), reverse=True)

    def extract_buckets(self, event):
        return []


class DummyFetcher:
    def fetch_forecast(self, city, event_date, coordinates=None):
        base = 10 if event_date.isoformat() == "2026-03-25" else 20
        now = datetime(2026, 3, 24, 0, 0, tzinfo=timezone.utc)
        return [
            RawWeatherData(
                city=city,
                event_date=event_date,
                source="open-meteo",
                model_name="ecmwf",
                hourly_temps=[float(base)] * 24,
                fetched_at=now,
                raw_response={},
            ),
            RawWeatherData(
                city=city,
                event_date=event_date,
                source="open-meteo",
                model_name="gfs",
                hourly_temps=[float(base + 1)] * 24,
                fetched_at=now,
                raw_response={},
            ),
            RawWeatherData(
                city=city,
                event_date=event_date,
                source="open-meteo",
                model_name="icon",
                hourly_temps=[float(base + 2)] * 24,
                fetched_at=now,
                raw_response={},
            ),
        ]

    def fetch_ensemble(self, city, event_date, coordinates):
        return []


class CountingFetcher:
    def __init__(self):
        self.calls = 0

    def fetch_forecast(self, city, event_date, coordinates=None):
        self.calls += 1
        base = 10 + self.calls
        now = datetime(2026, 3, 24, 0, 0, tzinfo=timezone.utc)
        return [
            RawWeatherData(
                city=city,
                event_date=event_date,
                source="open-meteo",
                model_name="ecmwf",
                hourly_temps=[float(base)] * 24,
                fetched_at=now,
                raw_response={},
            ),
            RawWeatherData(
                city=city,
                event_date=event_date,
                source="open-meteo",
                model_name="gfs",
                hourly_temps=[float(base + 1)] * 24,
                fetched_at=now,
                raw_response={},
            ),
            RawWeatherData(
                city=city,
                event_date=event_date,
                source="open-meteo",
                model_name="icon",
                hourly_temps=[float(base + 2)] * 24,
                fetched_at=now,
                raw_response={},
            ),
        ]

    def fetch_ensemble(self, city, event_date, coordinates):
        return []


class DummyReviewer:
    def get_cumulative_stats(self):
        return {"total_trades": 0}


def test_forecast_route_cache_is_scoped_by_selected_date(monkeypatch, tmp_path):
    db_path = tmp_path / "weather.db"
    monkeypatch.setattr(db_module, "DB_PATH", str(db_path))
    cache.clear()

    app = create_app(
        {
            "series_tracker": DummyTracker(),
            "fetcher": DummyFetcher(),
            "normalizer": Normalizer(),
            "reviewer": DummyReviewer(),
        },
        {
            "data_sources": {},
            "model_weights": {"ecmwf": 0.45, "gfs": 0.30, "icon": 0.25},
            "multi_outcome": {"bias_correction": {"shanghai": 0.0}},
        },
    )
    client = TestClient(app)

    first = client.get("/api/forecast?date=2026-03-25")
    second = client.get("/api/forecast?date=2026-03-26")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["event_date"] == "2026-03-25"
    assert second.json()["event_date"] == "2026-03-26"
    assert first.json()["weighted_mean"] == 10.8
    assert second.json()["weighted_mean"] == 20.8


def test_forecast_route_refresh_param_bypasses_cache(monkeypatch, tmp_path):
    db_path = tmp_path / "weather.db"
    monkeypatch.setattr(db_module, "DB_PATH", str(db_path))
    cache.clear()

    fetcher = CountingFetcher()
    app = create_app(
        {
            "series_tracker": DummyTracker(),
            "fetcher": fetcher,
            "normalizer": Normalizer(),
            "reviewer": DummyReviewer(),
        },
        {
            "data_sources": {},
            "model_weights": {"ecmwf": 0.45, "gfs": 0.30, "icon": 0.25},
            "multi_outcome": {"bias_correction": {"shanghai": 0.0}},
        },
    )
    client = TestClient(app)

    first = client.get("/api/forecast?date=2026-03-25")
    second = client.get("/api/forecast?date=2026-03-25")
    refreshed = client.get("/api/forecast?date=2026-03-25&refresh=1")

    assert first.status_code == 200
    assert second.status_code == 200
    assert refreshed.status_code == 200
    assert first.json()["weighted_mean"] == second.json()["weighted_mean"]
    assert refreshed.json()["weighted_mean"] > second.json()["weighted_mean"]
    assert fetcher.calls == 2


def test_positions_route_uses_saved_shares_for_closed_trade_pnl(monkeypatch, tmp_path):
    db_path = tmp_path / "weather.db"
    monkeypatch.setattr(db_module, "DB_PATH", str(db_path))
    db = init_db()
    db.execute(
        """INSERT INTO trades
           (market_id, side, amount, price, executed_at, status, shares, exit_price, closed_at, trade_meta)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "shanghai::demo::16C",
            "YES",
            10.0,
            0.50,
            datetime.now(timezone.utc).isoformat(),
            "CLOSED",
            5.0,
            0.70,
            datetime.now(timezone.utc).isoformat(),
            '{"label":"16°C","event_slug":"demo"}',
        ),
    )
    db.commit()

    cache.clear()
    app = create_app({"reviewer": DummyReviewer()}, {"multi_outcome": {"bias_correction": {"shanghai": 0.0}}})
    client = TestClient(app)

    response = client.get("/api/positions")
    assert response.status_code == 200
    closed = response.json()["closed_trades"]
    assert len(closed) == 1
    assert closed[0]["shares"] == 5.0
    assert closed[0]["pnl"] == 1.0


def test_positions_route_filters_by_selected_event(monkeypatch, tmp_path):
    db_path = tmp_path / "weather.db"
    monkeypatch.setattr(db_module, "DB_PATH", str(db_path))
    db = init_db()
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        """INSERT INTO trades
           (market_id, side, amount, price, executed_at, status, shares, trade_meta)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "shanghai::highest-temperature-in-shanghai-on-march-25-2026::16C",
            "YES",
            12.0,
            0.40,
            now,
            "DRY_RUN",
            30.0,
            '{"label":"16°C","event_slug":"highest-temperature-in-shanghai-on-march-25-2026"}',
        ),
    )
    db.execute(
        """INSERT INTO trades
           (market_id, side, amount, price, executed_at, status, shares, trade_meta)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "shanghai::highest-temperature-in-shanghai-on-march-26-2026::17C",
            "NO",
            8.0,
            0.55,
            now,
            "DRY_RUN",
            14.5,
            '{"label":"17°C","event_slug":"highest-temperature-in-shanghai-on-march-26-2026"}',
        ),
    )
    db.commit()

    cache.clear()
    app = create_app({"series_tracker": DummyTracker()}, {"multi_outcome": {"bias_correction": {"shanghai": 0.0}}})
    client = TestClient(app)

    response = client.get("/api/positions?date=2026-03-25")
    assert response.status_code == 200
    payload = response.json()
    assert payload["is_filtered"] is True
    assert payload["selected_date"] == "2026-03-25"
    assert len(payload["open_positions"]) == 1
    assert payload["open_positions"][0]["event_slug"] == "highest-temperature-in-shanghai-on-march-25-2026"


def test_backtest_route_returns_selected_entry(monkeypatch, tmp_path):
    db_path = tmp_path / "weather.db"
    monkeypatch.setattr(db_module, "DB_PATH", str(db_path))
    ensure_calibration_table()
    db = init_db()
    db.execute(
        """INSERT INTO bias_calibration
           (city, settle_date, wu_temp, forecast_mean, residual, recorded_at, source, is_reference)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "shanghai",
            "2026-03-25",
            16.0,
            15.6,
            0.4,
            datetime.now(timezone.utc).isoformat(),
            "verified_market",
            1,
        ),
    )
    db.commit()

    cache.clear()
    app = create_app({"series_tracker": DummyTracker()}, {"model_weights": {"ecmwf": 0.45, "gfs": 0.30, "icon": 0.25}, "multi_outcome": {"bias_correction": {"shanghai": 0.0}}})
    client = TestClient(app)

    response = client.get("/api/backtest?date=2026-03-25")
    assert response.status_code == 200
    payload = response.json()
    assert payload["selected_date"] == "2026-03-25"
    assert payload["selected_entry"]["date"] == "2026-03-25"
    assert payload["selected_entry"]["actual"] == 16.0


def test_dashboard_route_returns_mode_and_weather_metadata(monkeypatch, tmp_path):
    db_path = tmp_path / "weather.db"
    monkeypatch.setattr(db_module, "DB_PATH", str(db_path))
    monkeypatch.setenv("LIVE_TRADING", "false")
    cache.clear()

    app = create_app(
        {
            "series_tracker": DummyTracker(),
            "fetcher": DummyFetcher(),
            "normalizer": Normalizer(),
            "reviewer": DummyReviewer(),
        },
        {
            "data_sources": {},
            "model_weights": {"ecmwf": 0.45, "gfs": 0.30, "icon": 0.25},
            "multi_outcome": {"bias_correction": {"shanghai": 0.0}},
        },
    )
    client = TestClient(app)

    response = client.get("/api/dashboard?date=2026-03-25&refresh=1")
    assert response.status_code == 200

    system = response.json()["system"]
    assert system["mode"] == "DRY_RUN"
    assert system["mode_label"] == "模拟运行"
    assert system["target_mode"] == "LIVE"
    assert system["target_mode_label"] == "切换到实盘"
    assert system["view_mode"] == "HISTORICAL"
    assert system["weather_data_source"] == "live"
    assert system["weather_data_label"] == "实时抓取"
    assert system["market_data_source"] == "gamma"
    assert system["market_data_label"] == "Gamma 实时盘口"
    assert system["refresh_bypassed_cache"] is True


def test_mode_route_returns_clear_switch_metadata(monkeypatch):
    monkeypatch.setenv("LIVE_TRADING", "false")
    cache.clear()

    app = create_app({}, {})
    client = TestClient(app)

    current = client.get("/api/mode")
    assert current.status_code == 200
    assert current.json()["mode"] == "DRY_RUN"
    assert current.json()["mode_label"] == "模拟运行"
    assert current.json()["target_mode_label"] == "切换到实盘"

    switched = client.post("/api/mode", json={"mode": "LIVE"})
    assert switched.status_code == 200
    payload = switched.json()
    assert payload["previous_mode"] == "DRY_RUN"
    assert payload["mode"] == "LIVE"
    assert payload["mode_label"] == "实盘交易"
    assert payload["target_mode_label"] == "切换到模拟"
    assert payload["switched"] is True
