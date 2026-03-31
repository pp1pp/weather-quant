import statistics
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from src.data import city_registry
from src.data.schemas import NormalizedForecast, RawWeatherData, WeatherFactors
from src.utils.db import get_connection
from src.utils.logger import logger
from src.utils.time_utils import hours_until


class Normalizer:
    """L2: Clean and normalize multi-source raw weather data into a single NormalizedForecast.

    Enhanced to extract multi-variable weather factors for conditional bias correction.
    Supports all cities in config/cities.yaml.
    """

    # Named models to keep (exclude best_match)
    NAMED_MODELS = {"ecmwf", "gfs", "icon"}

    # Sea breeze direction range — per-city from city_registry
    # Default: east wind (45-135°) = coastal cooling for Shanghai ZSPD

    def normalize(
        self,
        raw_data_list: list[RawWeatherData],
        event_date: date,
        settlement_time_utc: datetime,
        ensemble_maxes: list[float] | None = None,
    ) -> NormalizedForecast:
        """
        Normalize raw data into a single forecast with weather condition factors.

        Steps:
        1. Extract daily max temp per model from hourly_temps
        2. Extract weather condition factors (wind, cloud, humidity, etc.)
        3. Extract latest_observation from best_match if available
        4. Calculate hours_to_settlement
        """
        model_forecasts: dict[str, float] = {}
        weather_factors: dict[str, WeatherFactors] = {}
        latest_observation: float | None = None
        observation_time: datetime | None = None

        city = raw_data_list[0].city if raw_data_list else "unknown"

        for raw in raw_data_list:
            if not raw.hourly_temps:
                continue

            daily_max = max(raw.hourly_temps)
            daily_min = min(raw.hourly_temps)

            if raw.model_name in self.NAMED_MODELS:
                model_forecasts[raw.model_name] = daily_max
                logger.debug(f"Model {raw.model_name}: daily max = {daily_max:.1f}C")

                # Extract weather condition factors
                factors = self._extract_factors(raw, daily_max, daily_min)
                weather_factors[raw.model_name] = factors

            elif raw.model_name == "best_match":
                tz_name = city_registry.get_timezone(city)
                if tz_name:
                    now_local = datetime.now(ZoneInfo(tz_name))
                    if now_local.date() == event_date:
                        idx = min(now_local.hour, len(raw.hourly_temps) - 1)
                        latest_observation = raw.hourly_temps[idx]
                        observation_time = now_local.astimezone(timezone.utc)
                        logger.debug(
                            "best_match observation proxy: "
                            f"{latest_observation:.1f}C at local hour {now_local.hour}"
                        )

        if not model_forecasts:
            raise ValueError(
                f"At least 1 named model required, got: {list(model_forecasts.keys())}"
            )

        # Add ensemble consensus as a pseudo-model if ensemble data is available
        # Ensemble mean is an independent 3rd forecast source (69-member mean)
        if ensemble_maxes and len(ensemble_maxes) >= 10:
            ensemble_mean = statistics.mean(ensemble_maxes)
            model_forecasts["ensemble"] = round(ensemble_mean, 1)
            logger.debug(f"Ensemble consensus: {ensemble_mean:.1f}°C from {len(ensemble_maxes)} members")

        h_to_settle = hours_until(settlement_time_utc)
        now = datetime.now(timezone.utc)

        assert now.tzinfo is not None
        if settlement_time_utc.tzinfo is None:
            settlement_time_utc = settlement_time_utc.replace(tzinfo=timezone.utc)

        result = NormalizedForecast(
            city=city,
            event_date=event_date,
            model_forecasts=model_forecasts,
            latest_observation=latest_observation,
            observation_time=observation_time,
            hours_to_settlement=h_to_settle,
            updated_at=now,
            weather_factors=weather_factors if weather_factors else None,
            ensemble_maxes=ensemble_maxes,
        )

        # Persist weather factors to DB for historical analysis
        if weather_factors:
            self._save_weather_factors(city, event_date, weather_factors, now)

        logger.info(
            f"Normalized forecast for {city}/{event_date}: "
            f"models={list(model_forecasts.keys())}, "
            f"factors={'Y' if weather_factors else 'N'}, "
            f"ensemble={'Y' if ensemble_maxes else 'N'}, "
            f"hours_to_settle={h_to_settle:.1f}"
        )

        return result

    def _extract_factors(
        self, raw: RawWeatherData, daily_max: float, daily_min: float
    ) -> WeatherFactors:
        """Extract weather condition factors from multi-variable data."""
        # Peak heating hours (indices 10-17 in 24h array = 10am-5pm local)
        # FIXED: 8am-6pm was too wide; 10am-5pm captures the core heating window
        # and avoids early morning land circulation contaminating sea breeze detection
        daytime_start = 10
        daytime_end = 17

        def daytime_slice(arr: list[float] | None) -> list[float]:
            if not arr:
                return []
            return arr[daytime_start:min(daytime_end, len(arr))]

        def safe_mean(arr: list[float], default: float = 0.0) -> float:
            return statistics.mean(arr) if arr else default

        # Cloud cover
        daytime_cloud = daytime_slice(raw.hourly_cloud_cover)
        mean_cloud = safe_mean(daytime_cloud, 50.0)

        # Wind speed and direction
        daytime_wind = daytime_slice(raw.hourly_wind_speed)
        max_wind = max(daytime_wind) if daytime_wind else 10.0

        daytime_wind_dir = daytime_slice(raw.hourly_wind_direction)
        # Dominant wind direction: circular mean
        dominant_dir = self._circular_mean(daytime_wind_dir) if daytime_wind_dir else 180.0

        # Sea breeze detection — per-city direction range from registry
        sb_range = city_registry.get_sea_breeze_dir(raw.city)
        is_sea_breeze = (sb_range[0] <= dominant_dir <= sb_range[1])

        # Precipitation
        total_precip = sum(raw.hourly_precipitation) if raw.hourly_precipitation else 0.0

        # Humidity
        daytime_humidity = daytime_slice(raw.hourly_humidity)
        mean_humidity = safe_mean(daytime_humidity, 60.0)

        # Pressure and trend
        pressure_vals = raw.hourly_pressure or []
        mean_pressure = safe_mean(pressure_vals, 1013.0)
        pressure_trend = (pressure_vals[-1] - pressure_vals[0]) if len(pressure_vals) >= 2 else 0.0

        # Diurnal range
        diurnal_range = daily_max - daily_min

        # Solar radiation (W/m²) - direct measure of daytime heating potential
        daytime_radiation = daytime_slice(raw.hourly_radiation)
        mean_radiation = safe_mean(daytime_radiation, 200.0)

        factors = WeatherFactors(
            mean_cloud_cover=round(mean_cloud, 1),
            max_wind_speed=round(max_wind, 1),
            dominant_wind_dir=round(dominant_dir, 0),
            is_sea_breeze=is_sea_breeze,
            total_precipitation=round(total_precip, 2),
            mean_humidity=round(mean_humidity, 1),
            mean_pressure=round(mean_pressure, 1),
            pressure_trend=round(pressure_trend, 2),
            diurnal_range=round(diurnal_range, 1),
            mean_solar_radiation=round(mean_radiation, 1),
        )

        logger.debug(
            f"Weather factors: cloud={mean_cloud:.0f}%, wind={max_wind:.1f}km/h "
            f"dir={dominant_dir:.0f}° sea_breeze={is_sea_breeze} "
            f"precip={total_precip:.1f}mm humidity={mean_humidity:.0f}% "
            f"pressure={mean_pressure:.0f}hPa trend={pressure_trend:+.1f}"
        )

        return factors

    @staticmethod
    def _save_weather_factors(
        city: str,
        event_date: date,
        weather_factors: dict[str, WeatherFactors],
        fetched_at: datetime,
    ):
        """Persist weather factors to DB for conditional bias learning."""
        try:
            conn = get_connection()
            for model_name, factors in weather_factors.items():
                conn.execute(
                    """INSERT OR IGNORE INTO weather_factors
                       (city, event_date, model_name, mean_cloud_cover, max_wind_speed,
                        dominant_wind_dir, is_sea_breeze, total_precipitation,
                        mean_humidity, mean_pressure, pressure_trend, diurnal_range, fetched_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        city,
                        event_date.isoformat(),
                        model_name,
                        factors.mean_cloud_cover,
                        factors.max_wind_speed,
                        factors.dominant_wind_dir,
                        int(factors.is_sea_breeze),
                        factors.total_precipitation,
                        factors.mean_humidity,
                        factors.mean_pressure,
                        factors.pressure_trend,
                        factors.diurnal_range,
                        fetched_at.isoformat(),
                    ),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"Failed to save weather factors: {e}")

    @staticmethod
    def _circular_mean(angles: list[float]) -> float:
        """Compute circular mean of wind directions (in degrees)."""
        import math
        sin_sum = sum(math.sin(math.radians(a)) for a in angles)
        cos_sum = sum(math.cos(math.radians(a)) for a in angles)
        mean_rad = math.atan2(sin_sum / len(angles), cos_sum / len(angles))
        mean_deg = math.degrees(mean_rad) % 360
        return mean_deg
