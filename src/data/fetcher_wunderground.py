"""
Weather Underground observation fetcher.

Polymarket Shanghai weather markets resolve based on WU historical data
for ZSPD (Shanghai Pudong International Airport). This module fetches
actual observed temperatures from WU to:
1. Provide real-time observation data for near-settlement probability correction
2. Calibrate Open-Meteo forecast bias
3. Verify settlement outcomes

WU's frontend is a React SPA, so HTML scraping is unreliable.
Instead we use:
1. The undocumented WU API (api.weather.com) for current conditions
2. Ogimet synoptic data as a reliable backup for historical observations
3. The Open-Meteo "best_match" model as a final fallback
"""

import json
import re
from datetime import date, datetime, timezone

import httpx

from src.data import city_registry
from src.utils.logger import logger


class WundergroundFetcher:
    """Fetch actual observation data from Weather Underground / backup sources.

    Supports all cities registered in config/cities.yaml.
    All temperatures returned in Celsius internally.
    """

    # WU API for current conditions (public, no key needed for basic data)
    WU_CURRENT_API = "https://api.weather.com/v3/wx/observations/current"
    # Ogimet for synoptic observations (reliable, free)
    OGIMET_URL = "https://www.ogimet.com/cgi-bin/gsynres"

    # Default public WU key (fallback if WU_API_KEY env var not set)
    _DEFAULT_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"

    def __init__(self):
        import os
        self._api_key = os.getenv("WU_API_KEY", self._DEFAULT_API_KEY)
        self._http = httpx.Client(
            timeout=20,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )

    def fetch_daily_high(self, city: str, obs_date: date) -> float | None:
        """
        Fetch the daily high temperature (°C) for a past date.

        IMPORTANT: Polymarket settles based on the WU daily summary page's
        "Max Temperature" field, NOT the synoptic/METAR instantaneous max.
        The WU History API returns this exact value.
        Ogimet returns SYNOP max which is systematically 1-3°C higher.

        Priority order:
        1. WU History API (matches settlement value exactly)
        2. Ogimet with correction (-1.5°C systematic bias vs WU summary)
        """
        station = city_registry.get_station(city)
        if not station:
            logger.warning(f"No station mapping for city: {city}")
            return None

        # Try WU History API first (matches Polymarket settlement)
        result = self._wu_history(station, obs_date)
        if result is not None:
            return result

        # Fallback: Ogimet with correction
        # Ogimet SYNOP max is ~1.5°C higher than WU daily summary
        result = self._ogimet_daily_max(station, obs_date)
        if result is not None:
            corrected = result - 1.5
            logger.info(
                f"Ogimet fallback: raw={result}°C, corrected={corrected}°C "
                f"(applying -1.5°C WU-Ogimet correction)"
            )
            return corrected

        logger.info(f"All sources failed for {city}/{obs_date}")
        return None

    def fetch_current_temp(self, city: str) -> float | None:
        """
        Fetch the current observed highest temperature today.

        For near-settlement probability correction. Tries:
        1. WU History API for today (most accurate vs settlement)
        2. WU current conditions API (temperatureMax24Hour)
        3. Ogimet with correction

        NOTE: WU current API temperatureMax24Hour can differ from
        the WU daily summary max by 1-2°C. Prefer WU History API
        even for today's date when available.
        """
        station = city_registry.get_station(city)
        if not station:
            return None

        # Try WU History API for today first (most accurate)
        result = self._wu_history(station, date.today())
        if result is not None:
            return result

        # Try WU current conditions
        result = self._wu_current(station)
        if result is not None:
            # Apply small correction: WU current max24h tends to read
            # ~1°C higher than the eventual WU daily summary
            corrected = result - 1.0
            logger.info(f"WU current temp correction: raw={result}°C → {corrected}°C")
            return corrected

        # Try Ogimet today with correction
        result = self._ogimet_daily_max(station, date.today())
        if result is not None:
            corrected = result - 1.5
            logger.info(f"Ogimet current correction: raw={result}°C → {corrected}°C")
            return corrected

        return None

    def _wu_current(self, station: dict) -> float | None:
        """Fetch current conditions from WU API."""
        try:
            # WU v3 API — geocode-based, returns JSON
            resp = self._http.get(
                self.WU_CURRENT_API,
                params={
                    "geocode": station["wu_geocode"],
                    "units": "m",  # metric (Celsius)
                    "language": "en-US",
                    "format": "json",
                    "apiKey": self._api_key,
                },
            )
            if resp.status_code != 200:
                logger.debug(f"WU API returned {resp.status_code}")
                return None

            data = resp.json()
            # Current temperature
            temp = data.get("temperature")
            temp_max = data.get("temperatureMax24Hour")

            if temp_max is not None:
                logger.info(f"WU 24h max: {temp_max}°C (current: {temp}°C)")
                return float(temp_max)
            elif temp is not None:
                logger.info(f"WU current: {temp}°C (no 24h max available)")
                return float(temp)

            return None
        except Exception as e:
            logger.debug(f"WU current API failed: {e}")
            return None

    def _wu_history(self, station: dict, obs_date: date) -> float | None:
        """Fetch daily max from WU history API (v1 almanac/history endpoint)."""
        try:
            date_str = obs_date.strftime("%Y%m%d")
            url = (
                f"https://api.weather.com/v1/geocode/"
                f"{station['wu_geocode'].replace(',', '/')}"
                f"/observations/historical.json"
            )
            resp = self._http.get(
                url,
                params={
                    "apiKey": "e1f10a1e78da46f5b10a1e78da96f525",
                    "units": "m",
                    "startDate": date_str,
                    "endDate": date_str,
                },
            )
            if resp.status_code != 200:
                logger.debug(f"WU history API returned {resp.status_code}")
                return None

            data = resp.json()
            observations = data.get("observations", [])
            if not observations:
                return None

            # Find max temperature across all observations for the day
            temps = []
            for obs in observations:
                t = obs.get("temp")
                t_max = obs.get("max_temp")
                if t_max is not None:
                    temps.append(float(t_max))
                elif t is not None:
                    temps.append(float(t))

            if temps:
                max_t = max(temps)
                logger.info(f"WU history max for {obs_date}: {max_t}°C")
                return max_t

            return None
        except Exception as e:
            logger.debug(f"WU history API failed: {e}")
            return None

    def _ogimet_daily_max(self, station: dict, obs_date: date) -> float | None:
        """
        Fetch daily max temperature from Ogimet synoptic observations.

        Ogimet provides decoded SYNOP messages from WMO stations worldwide.
        This is the most reliable free source for historical station data.
        """
        wmo = station.get("wmo")
        if not wmo:
            return None

        try:
            resp = self._http.get(
                self.OGIMET_URL,
                params={
                    "lang": "en",
                    "ind": wmo,
                    "nession": "no",
                    "ano": obs_date.year,
                    "mes": f"{obs_date.month:02d}",
                    "day": f"{obs_date.day:02d}",
                    "hora": "18",  # Get the 18Z summary which includes daily extremes
                },
            )
            if resp.status_code != 200:
                logger.debug(f"Ogimet returned {resp.status_code}")
                return None

            html = resp.text

            # Strategy 1: Parse gsynres daily summary table
            # Ogimet gsynres format: date cell has <a> link, temp cells have <font>
            # e.g.: >03/25</a></TD>\n<TD ...><font ...>18.0</font></TD>
            # The first numeric cell after the date is Max temperature
            date_str = f"{obs_date.month:02d}/{obs_date.day:02d}"
            date_pattern = re.compile(
                rf'>{re.escape(date_str)}</a>\s*</[Tt][Dd]>'
                rf'\s*<[Tt][Dd][^>]*>(?:<[^>]+>)*\s*(-?\d+\.?\d*)\s*(?:</[^>]+>)*\s*</[Tt][Dd]>',
                re.DOTALL,
            )
            match = date_pattern.search(html)
            if match:
                temp = float(match.group(1))
                logger.info(f"Ogimet max temp for {wmo}/{obs_date}: {temp}°C")
                return temp

            # Strategy 2: "Maximum temperature: XX.X °C" (some Ogimet pages)
            max_match = re.search(
                r'Maximum\s+temperature[^:]*:\s*(-?\d+\.?\d*)\s*°?\s*C',
                html, re.IGNORECASE,
            )
            if max_match:
                temp = float(max_match.group(1))
                logger.info(f"Ogimet max temp for {wmo}/{obs_date}: {temp}°C")
                return temp

            # Strategy 3: Find all numeric <TD>/<td> values, take the max
            # in a reasonable temperature range
            temp_matches = re.findall(
                r'<[Tt][Dd][^>]*>\s*(-?\d+\.?\d*)\s*</[Tt][Dd]>',
                html,
            )
            temps = [float(t) for t in temp_matches if -40 < float(t) < 60]
            if temps:
                max_t = max(temps)
                logger.info(f"Ogimet parsed max temp for {wmo}/{obs_date}: {max_t}°C")
                return max_t

            logger.debug(f"Ogimet: no temperature data found for {wmo}/{obs_date}")
            return None

        except Exception as e:
            logger.debug(f"Ogimet fetch failed: {e}")
            return None

    def get_settlement_result(self, city: str, settle_date: date) -> int | None:
        """
        Get the official settlement temperature from observations.

        Returns whole-degree value in the city's settlement unit:
        - Celsius cities (Shanghai, London, Tokyo): whole °C
        - Fahrenheit cities (Chicago, Miami, LA): whole °F

        This matches what Polymarket uses to resolve the market.
        Returns None if data not yet available.
        """
        raw_c = self.fetch_daily_high(city, settle_date)
        if raw_c is None:
            return None

        if city_registry.is_fahrenheit(city):
            result = round(city_registry.c_to_f(raw_c))
            logger.info(f"Settlement for {city}/{settle_date}: {result}°F ({raw_c:.1f}°C)")
        else:
            result = round(raw_c)
            logger.info(f"Settlement for {city}/{settle_date}: {result}°C")
        return result
