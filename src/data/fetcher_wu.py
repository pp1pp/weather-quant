from datetime import date

from src.data.schemas import RawWeatherData
from src.utils.logger import logger


class WUFetcher:
    """Weather Underground fallback data source. Phase 1: stub only."""

    def fetch_forecast(
        self, city: str, event_date: date, coordinates: tuple[float, float]
    ) -> list[RawWeatherData]:
        logger.warning("WU fetcher not implemented, returning empty list")
        return []
