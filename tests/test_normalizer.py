from datetime import date, datetime, timezone
from unittest.mock import patch

from src.data.normalizer import Normalizer
from src.data.schemas import RawWeatherData


class FixedUtcNow(datetime):
    @classmethod
    def now(cls, tz=None):
        base = cls(2026, 3, 26, 6, 0, 0, tzinfo=timezone.utc)
        if tz is None:
            return base.replace(tzinfo=None)
        return base.astimezone(tz)


def _raw(model_name: str, event_date: date) -> RawWeatherData:
    return RawWeatherData(
        city="shanghai",
        event_date=event_date,
        source="open-meteo",
        model_name=model_name,
        hourly_temps=[float(i) for i in range(24)],
        fetched_at=datetime(2026, 3, 26, 0, 0, tzinfo=timezone.utc),
        raw_response={},
    )


def test_best_match_uses_local_hour_for_same_day_observation():
    normalizer = Normalizer()
    event_date = date(2026, 3, 26)
    settlement = datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)

    with patch("src.data.normalizer.datetime", FixedUtcNow):
        forecast = normalizer.normalize(
            [_raw("ecmwf", event_date), _raw("best_match", event_date)],
            event_date,
            settlement,
        )

    # 06:00 UTC == 14:00 Asia/Shanghai, so we should pick index 14.
    assert forecast.latest_observation == 14.0


def test_best_match_is_not_treated_as_observation_for_future_local_day():
    normalizer = Normalizer()
    event_date = date(2026, 3, 27)
    settlement = datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc)

    with patch("src.data.normalizer.datetime", FixedUtcNow):
        forecast = normalizer.normalize(
            [_raw("ecmwf", event_date), _raw("best_match", event_date)],
            event_date,
            settlement,
        )

    assert forecast.latest_observation is None
