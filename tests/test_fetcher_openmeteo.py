from datetime import date

import pytest

import src.data.fetcher_openmeteo as fetcher_module
from src.data.fetcher_openmeteo import DataFetchError, OpenMeteoFetcher


class FailingClient:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, params=None):
        raise OSError("[Errno 8] nodename nor servname provided, or not known")


def test_fetch_forecast_does_not_sleep_after_final_failure(monkeypatch):
    fetcher = OpenMeteoFetcher({"retry_count": 3, "retry_delays": [2, 5, 10]})
    sleep_calls = []

    monkeypatch.setattr(fetcher_module.httpx, "Client", FailingClient)
    monkeypatch.setattr(fetcher_module.city_registry, "get_timezone", lambda city: "UTC")
    monkeypatch.setattr(
        fetcher_module.city_registry,
        "get_models",
        lambda city: "best_match,gfs_seamless,icon_seamless",
    )
    monkeypatch.setattr(fetcher_module.time, "sleep", lambda delay: sleep_calls.append(delay))

    with pytest.raises(DataFetchError) as exc_info:
        fetcher.fetch_forecast("shanghai", date(2026, 4, 1), (31.1434, 121.8052))

    assert sleep_calls == [2, 5]
    assert isinstance(exc_info.value.__cause__, OSError)
