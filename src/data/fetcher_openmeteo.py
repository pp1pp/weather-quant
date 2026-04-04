import json
import time
from datetime import date, datetime, timezone

import httpx

from src.data import city_registry
from src.data.schemas import RawWeatherData
from src.utils.db import get_connection
from src.utils.logger import logger


class DataFetchError(Exception):
    pass


class OpenMeteoFetcher:
    """Fetch multi-variable weather forecast data from Open-Meteo API.

    Enhanced to fetch temperature + humidity + wind + cloud + precipitation + pressure
    for conditional bias correction and improved probability estimation.
    """

    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"

    # Model keys in Open-Meteo response → our model names
    MODEL_MAP = {
        "temperature_2m_best_match": "best_match",
        "temperature_2m_gfs_seamless": "gfs",
        "temperature_2m_icon_seamless": "icon",
        "temperature_2m_ecmwf_ifs04": "ecmwf",
    }

    # Multi-variable keys to fetch per model
    WEATHER_VARS = [
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "wind_direction_10m",
        "cloud_cover",
        "precipitation",
        "surface_pressure",
        "shortwave_radiation",  # Solar radiation (W/m²) - key predictor of daytime warming
    ]

    def __init__(self, config: dict):
        self.retry_count = config.get("retry_count", 3)
        self.retry_delays = config.get("retry_delays", [5, 15, 30])

    def fetch_forecast(
        self, city: str, event_date: date, coordinates: tuple[float, float] | None = None
    ) -> list[RawWeatherData]:
        """Fetch multi-model, multi-variable forecasts from Open-Meteo.

        If coordinates not provided, looks up from city_registry.
        """
        if coordinates is None:
            coordinates = city_registry.get_coordinates(city)
        lat, lon = coordinates
        tz = city_registry.get_timezone(city)
        models = city_registry.get_models(city)

        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(self.WEATHER_VARS),
            "temperature_unit": "celsius",
            "timezone": tz,
            "forecast_days": 3,
            "models": models,
        }

        url = self.BASE_URL
        logger.info(f"Requesting Open-Meteo: {url} params={params}")

        last_error = None
        for attempt in range(self.retry_count):
            try:
                with httpx.Client(timeout=30) as client:
                    resp = client.get(url, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                    logger.info(f"Open-Meteo response status: {resp.status_code}")
                    return self._parse_response(data, city, event_date)
            except Exception as e:
                last_error = e
                if attempt == self.retry_count - 1:
                    logger.warning(
                        f"Open-Meteo fetch attempt {attempt + 1} failed: {e}. "
                        "No retries left."
                    )
                    break

                delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                logger.warning(
                    f"Open-Meteo fetch attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)

        raise DataFetchError(
            f"All {self.retry_count} retries failed for {city}: {last_error}"
        ) from last_error

    def fetch_ensemble(
        self, city: str, event_date: date, coordinates: tuple[float, float]
    ) -> list[float]:
        """Fetch ensemble forecast members for probabilistic estimation.

        Returns list of daily max temperatures from all ensemble members
        (GFS 30 members + ICON 39 members = ~69 samples).
        """
        lat, lon = coordinates
        tz = city_registry.get_timezone(city)

        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m",
            "timezone": tz,
            "forecast_days": 3,
            "models": "gfs_seamless,icon_seamless",
        }

        data = None
        for attempt in range(3):
            try:
                with httpx.Client(timeout=60) as client:
                    resp = client.get(self.ENSEMBLE_URL, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                break
            except Exception as e:
                if attempt < 2:
                    import time as _time
                    delay = [5, 15][attempt]
                    logger.warning(f"Ensemble fetch attempt {attempt+1} failed: {e}, retrying in {delay}s")
                    _time.sleep(delay)
                else:
                    logger.warning(f"Ensemble fetch failed after 3 attempts: {e}")
                    return []

        try:

            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            event_date_str = event_date.isoformat()
            day_indices = [
                i for i, t in enumerate(times) if t.startswith(event_date_str)
            ]
            if not day_indices:
                day_indices = list(range(min(24, len(times))))

            ensemble_maxes = []
            for key, vals in hourly.items():
                if key == "time":
                    continue
                # Only process member data (contains "member" in key)
                if "member" not in key:
                    continue
                if not vals:
                    continue

                day_temps = [vals[i] for i in day_indices if i < len(vals)]
                day_temps = [t for t in day_temps if t is not None]
                if day_temps:
                    ensemble_maxes.append(max(day_temps))

            logger.info(
                f"Ensemble forecast: {len(ensemble_maxes)} members for {city}/{event_date_str}"
            )
            if ensemble_maxes:
                import statistics
                mean = statistics.mean(ensemble_maxes)
                std = statistics.stdev(ensemble_maxes) if len(ensemble_maxes) > 1 else 1.5
                logger.info(
                    f"Ensemble stats: mean={mean:.2f}, std={std:.2f}, "
                    f"range=[{min(ensemble_maxes):.1f}, {max(ensemble_maxes):.1f}]"
                )

            return ensemble_maxes

        except Exception as e:
            logger.warning(f"Ensemble fetch failed: {e}")
            return []

    def _parse_response(
        self, data: dict, city: str, event_date: date
    ) -> list[RawWeatherData]:
        """Parse Open-Meteo JSON response into list of RawWeatherData with multi-variable data."""
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        event_date_str = event_date.isoformat()
        day_indices = [
            i for i, t in enumerate(times) if t.startswith(event_date_str)
        ]

        if not day_indices:
            logger.warning(
                f"No data for {event_date_str} in response. "
                f"Available times: {times[:3]}...{times[-3:]}"
            )
            day_indices = list(range(min(24, len(times))))

        results = []
        now = datetime.now(timezone.utc)

        # Determine which models returned data
        available_models = {}
        for api_key, model_name in self.MODEL_MAP.items():
            temps = hourly.get(api_key, [])
            non_none = [t for t in temps if t is not None]
            if non_none:
                available_models[api_key] = model_name

        if not available_models:
            logger.error("No models returned valid temperature data!")
            return results

        for api_key, model_name in available_models.items():
            temps = hourly.get(api_key, [])
            day_temps = [temps[i] for i in day_indices if i < len(temps)]
            day_temps = [t for t in day_temps if t is not None]

            if not day_temps:
                continue

            # Extract multi-variable data for this model
            model_suffix = api_key.replace("temperature_2m_", "")
            humidity = self._extract_var(hourly, f"relative_humidity_2m_{model_suffix}", day_indices)
            wind_speed = self._extract_var(hourly, f"wind_speed_10m_{model_suffix}", day_indices)
            wind_dir = self._extract_var(hourly, f"wind_direction_10m_{model_suffix}", day_indices)
            cloud = self._extract_var(hourly, f"cloud_cover_{model_suffix}", day_indices)
            precip = self._extract_var(hourly, f"precipitation_{model_suffix}", day_indices)
            pressure = self._extract_var(hourly, f"surface_pressure_{model_suffix}", day_indices)
            radiation = self._extract_var(hourly, f"shortwave_radiation_{model_suffix}", day_indices)

            raw = RawWeatherData(
                city=city,
                event_date=event_date,
                source="open-meteo",
                model_name=model_name,
                hourly_temps=day_temps,
                fetched_at=now,
                raw_response={},
                hourly_humidity=humidity,
                hourly_wind_speed=wind_speed,
                hourly_wind_direction=wind_dir,
                hourly_cloud_cover=cloud,
                hourly_precipitation=precip,
                hourly_pressure=pressure,
                hourly_radiation=radiation,
            )
            results.append(raw)
            logger.info(
                f"Model '{model_name}': {len(day_temps)} hourly temps, "
                f"range [{min(day_temps):.1f}, {max(day_temps):.1f}], "
                f"vars: hum={'Y' if humidity else 'N'} wind={'Y' if wind_speed else 'N'} "
                f"cloud={'Y' if cloud else 'N'} precip={'Y' if precip else 'N'} "
                f"press={'Y' if pressure else 'N'}"
            )

        # Save to database
        self._save_to_db(results)

        logger.info(f"Parsed {len(results)} models for {city}/{event_date_str}")
        return results

    @staticmethod
    def _extract_var(hourly: dict, key: str, day_indices: list[int]) -> list[float] | None:
        """Extract a weather variable for the event date indices."""
        vals = hourly.get(key, [])
        if not vals:
            return None
        day_vals = [vals[i] for i in day_indices if i < len(vals)]
        day_vals = [v for v in day_vals if v is not None]
        return day_vals if day_vals else None

    def _save_to_db(self, raw_data_list: list[RawWeatherData]):
        """Save raw forecast data to SQLite."""
        conn = get_connection()
        try:
            for raw in raw_data_list:
                conn.execute(
                    """INSERT OR IGNORE INTO raw_forecasts
                       (city, event_date, source, model_name, hourly_temps, fetched_at, raw_response)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        raw.city,
                        raw.event_date.isoformat(),
                        raw.source,
                        raw.model_name,
                        json.dumps(raw.hourly_temps),
                        raw.fetched_at.isoformat(),
                        json.dumps(raw.raw_response),
                    ),
                )
            conn.commit()
            logger.info(f"Saved {len(raw_data_list)} raw forecasts to DB")
        finally:
            conn.close()
