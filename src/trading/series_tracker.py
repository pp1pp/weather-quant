"""
Multi-City Daily Weather Series Tracker.

Polymarket has recurring daily temperature markets for multiple cities.
This module auto-discovers the latest active market for any city, extracts
bucket condition_ids/token_ids, and updates markets.yaml.

Supported cities (from config/cities.yaml):
- Shanghai (°C), Chicago (°F), Miami (°F), Los Angeles (°F), London (°C), Tokyo (°C)

Key facts:
- Slug pattern: highest-temperature-in-{city}-on-{month}-{day}-{year}
- Each event has 11 sub-markets (temperature buckets)
- Settlement source: Weather Underground station for each city
- US cities settle in °F (2°F-wide buckets), international in °C (1°C-wide)
"""

import json
import re
from datetime import datetime, timezone, timedelta

import httpx
import yaml

from src.data import city_registry
from src.utils.logger import logger

GAMMA_URL = "https://gamma-api.polymarket.com"


class SeriesTracker:
    """Track and auto-discover the latest daily temperature market for any city."""

    def __init__(self):
        self._http = httpx.Client(timeout=20)

    def find_latest_event(self, city: str = "shanghai") -> dict | None:
        """
        Find the latest active temperature event for a city via Gamma API.

        Args:
            city: City key from city_registry (e.g., 'shanghai', 'chicago')

        Returns event dict with all sub-markets, or None if no active event.
        """
        # Strategy 1: Search by tag
        event = self._search_by_tag(city)
        if event:
            return event

        # Strategy 2: Search by slug pattern for upcoming dates
        event = self._search_by_slug(city)
        if event:
            return event

        logger.warning(f"No active temperature event found for {city}")
        return None

    def find_event_by_date(self, target_date, city: str = "shanghai") -> dict | None:
        """
        Find a temperature event for a specific date and city (active or settled).

        Args:
            target_date: date object or ISO string for the target day.
            city: City key from city_registry.

        Returns event dict or None.
        """
        from datetime import date as date_type
        if isinstance(target_date, str):
            target_date = date_type.fromisoformat(target_date)

        slug_city = city_registry.get_slug_city(city)
        month_name = target_date.strftime("%B").lower()
        day = target_date.day
        year = target_date.year
        slug = f"highest-temperature-in-{slug_city}-on-{month_name}-{day}-{year}"

        try:
            resp = self._http.get(
                f"{GAMMA_URL}/events",
                params={"slug": slug},
            )
            events = resp.json()
            if events:
                event = events[0] if isinstance(events, list) else events
                logger.info(f"Found {city} event for {target_date}: {slug}")
                return event
        except Exception as e:
            logger.warning(f"Failed to find event for {city}/{target_date}: {e}")

        return None

    def list_available_dates(self, city: str = "shanghai", days_back: int = 14, days_forward: int = 3) -> list[str]:
        """
        List dates with known events (from DB bias_calibration + upcoming).

        Returns list of date strings in ISO format.
        """
        from src.utils.db import get_connection
        dates = set()

        conn = get_connection()
        try:
            rows = conn.execute(
                """SELECT DISTINCT settle_date FROM bias_calibration
                   WHERE city = ?
                   ORDER BY settle_date DESC LIMIT ?""",
                (city, days_back),
            ).fetchall()
            for row in rows:
                dates.add(row["settle_date"])
        finally:
            conn.close()

        today = datetime.now(timezone.utc).date()
        for delta in range(days_forward + 1):
            d = today + timedelta(days=delta)
            dates.add(d.isoformat())

        return sorted(dates, reverse=True)

    def _search_by_tag(self, city: str = "shanghai") -> dict | None:
        """Search for active weather events by tag for a city."""
        city_cfg = city_registry.get_city(city)
        tag = city_cfg.get("name", city.capitalize()) if city_cfg else city.capitalize()
        slug_city = city_registry.get_slug_city(city)

        try:
            resp = self._http.get(
                f"{GAMMA_URL}/events",
                params={
                    "tag": tag,
                    "closed": "false",
                    "limit": 10,
                },
            )
            events = resp.json()
            if not events:
                return None

            # Match: temperature/temp/high and city name in slug
            valid = []
            for e in events:
                slug = e.get("slug", "").lower()
                title = e.get("title", "").lower()
                combined = slug + " " + title
                has_temp = any(w in combined for w in ("temperature", "temp", "high"))
                has_city = slug_city.replace("-", " ") in combined or slug_city in combined
                if has_temp and has_city:
                    valid.append(e)

            if not valid:
                return None

            valid.sort(key=lambda x: x.get("endDate", ""), reverse=True)
            return valid[0]

        except Exception as e:
            logger.warning(f"Tag search failed for {city}: {e}")
            return None

    def _search_by_slug(self, city: str = "shanghai") -> dict | None:
        """Search for event by constructing expected slug for upcoming dates.

        Prefers unsettled events: tries today first, then tomorrow, etc.
        """
        slug_city = city_registry.get_slug_city(city)
        now = datetime.now(timezone.utc)
        today = now.date()

        found_events = []
        for delta in range(4):
            target = today + timedelta(days=delta)
            month_name = target.strftime("%B").lower()
            day = target.day
            year = target.year
            slug = f"highest-temperature-in-{slug_city}-on-{month_name}-{day}-{year}"

            try:
                resp = self._http.get(
                    f"{GAMMA_URL}/events",
                    params={"slug": slug},
                )
                events = resp.json()
                if events:
                    event = events[0] if isinstance(events, list) else events
                    found_events.append((target, event))
            except Exception:
                continue

        if not found_events:
            return None

        for target, event in found_events:
            end_str = event.get("endDate", "")
            if end_str:
                try:
                    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    if end_dt > now:
                        logger.info(f"Found {city} event for {target}: {event.get('slug', '')} (unsettled)")
                        return event
                except Exception:
                    pass

        target, event = found_events[0]
        logger.info(f"Found {city} event for {target}: {event.get('slug', '')} (fallback)")
        return event

    def _markets_to_event(self, markets: list) -> dict | None:
        """Convert a list of related markets into an event-like dict."""
        if not markets:
            return None
        # Group by groupItemTitle or similar
        return {
            "title": markets[0].get("groupItemTitle", "Shanghai Temperature"),
            "slug": markets[0].get("slug", ""),
            "endDate": markets[0].get("endDate", ""),
            "markets": markets,
        }

    def extract_buckets(self, event: dict) -> list[dict]:
        """
        Extract all temperature bucket sub-markets from an event.

        Returns list of bucket dicts with label, condition_id, token_ids, prices.
        """
        markets = event.get("markets", [])

        # If event dict doesn't have markets, fetch them
        if not markets:
            slug = event.get("slug", "")
            if slug:
                markets = self._fetch_event_markets(slug)

        buckets = []
        for m in markets:
            question = m.get("question", "") + " " + m.get("groupItemTitle", "")

            # Extract the temperature value from the question/title
            temp_label = self._extract_temp_label(question)
            if not temp_label:
                continue

            tokens = m.get("clobTokenIds", [])
            if isinstance(tokens, str):
                tokens = json.loads(tokens)

            prices = m.get("outcomePrices", [])
            if isinstance(prices, str):
                prices = json.loads(prices)

            if not tokens or len(tokens) < 2:
                continue

            bucket = {
                "label": temp_label,
                "condition_id": m.get("conditionId", ""),
                "yes_token_id": tokens[0],
                "no_token_id": tokens[1],
                "yes_price": float(prices[0]) if prices else 0,
                "no_price": float(prices[1]) if len(prices) > 1 else 0,
                "volume": float(m.get("volumeNum", 0) or 0),
                "liquidity": float(m.get("liquidity", 0) or 0),
                "tick_size": float(m.get("orderPriceMinTickSize", 0.01) or 0.01),
                "min_order_size": float(m.get("orderMinSize", 5) or 5),
                "market_id": m.get("id", ""),
                "spread": float(m.get("spread", 0) or 0),
            }

            # Parse temp range (handles both °C single values and °F ranges like "74-75°F")
            temp_range = self._parse_temp_range(temp_label)
            if temp_range is not None:
                bucket["temp_low"] = temp_range[0]
                bucket["temp_high"] = temp_range[1]

            buckets.append(bucket)

        # Sort by temperature
        buckets.sort(key=lambda b: b.get("temp_low", 0))

        logger.info(f"Extracted {len(buckets)} temperature buckets from event")
        return buckets

    def _fetch_event_markets(self, slug: str) -> list:
        """Fetch all sub-markets for an event slug."""
        try:
            resp = self._http.get(
                f"{GAMMA_URL}/events",
                params={"slug": slug},
            )
            data = resp.json()
            if isinstance(data, list) and data:
                return data[0].get("markets", [])
            elif isinstance(data, dict):
                return data.get("markets", [])
        except Exception as e:
            logger.warning(f"Failed to fetch event markets: {e}")
        return []

    def _extract_temp_label(self, text: str) -> str:
        """Extract temperature bucket label from question text.

        Handles both °C and °F formats:
        - °C: "11°C or below", "16°C", "21°C or higher"
        - °F: "38-39°F", "73°F or below", "92°F or higher"
        - °F range: "74-75°F" (2°F wide buckets)
        """
        text_lower = text.lower()

        # Pattern 1: Fahrenheit range "38-39°F"
        m = re.search(r'(\d+)\s*[-–]\s*(\d+)\s*°?\s*f\b', text_lower)
        if m:
            low = m.group(1)
            high = m.group(2)
            return f"{low}-{high}°F"

        # Pattern 2: Fahrenheit single with qualifier "73°F or below"
        m = re.search(r'(\d+)\s*°?\s*f\s*(or\s+(?:below|lower|higher|above|more|less))?', text_lower)
        if m:
            temp = m.group(1)
            qualifier = m.group(2) or ""
            if any(w in qualifier for w in ("below", "lower", "less")):
                return f"{temp}°F or below"
            elif any(w in qualifier for w in ("higher", "above", "more")):
                return f"{temp}°F or higher"
            else:
                return f"{temp}°F"

        # Pattern 3: Celsius "11°C or below", "16°C", "21°C or higher"
        m = re.search(r'(\d+)\s*°?\s*c\s*(or\s+(?:below|lower|higher|above|more|less))?', text_lower)
        if m:
            temp = m.group(1)
            qualifier = m.group(2) or ""
            if any(w in qualifier for w in ("below", "lower", "less")):
                return f"{temp}°C or below"
            elif any(w in qualifier for w in ("higher", "above", "more")):
                return f"{temp}°C or higher"
            else:
                return f"{temp}°C"

        # Pattern 4: "16 degrees celsius/fahrenheit"
        m = re.search(r'(\d+)\s*(?:degrees?\s*)?(?:celsius|c\b)', text_lower)
        if m:
            return f"{m.group(1)}°C"
        m = re.search(r'(\d+)\s*(?:degrees?\s*)?(?:fahrenheit|f\b)', text_lower)
        if m:
            return f"{m.group(1)}°F"

        # Pattern 5: standalone number in temperature context
        if any(w in text_lower for w in ("temperature", "temp", "high", "degrees")):
            m = re.search(r'\b(\d{1,3})\b', text_lower)
            if m and 0 <= int(m.group(1)) <= 120:
                return f"{m.group(1)}°C"

        return ""

    def _parse_temp_value(self, label: str) -> int | None:
        """Extract the first integer temperature from label."""
        m = re.search(r'(\d+)', label)
        return int(m.group(1)) if m else None

    def _parse_temp_range(self, label: str) -> tuple[int, int] | None:
        """Parse temperature range from label, handling both °C and °F.

        Returns (low, high) in the label's native unit.
        Examples:
            "16°C" → (16, 16)
            "38-39°F" → (38, 39)
            "73°F or below" → (-999, 73)
            "92°F or higher" → (92, 999)
        """
        # Range: "38-39°F"
        m = re.search(r'(\d+)\s*[-–]\s*(\d+)', label)
        if m:
            return (int(m.group(1)), int(m.group(2)))

        # Single with qualifier
        val = self._parse_temp_value(label)
        if val is None:
            return None

        if "or below" in label or "or lower" in label or "or less" in label:
            return (-999, val)
        elif "or higher" in label or "or above" in label or "or more" in label:
            return (val, 999)
        else:
            return (val, val)

    def update_markets_yaml(
        self, event: dict, buckets: list[dict], city: str = "shanghai",
        path: str = "config/markets.yaml"
    ) -> bool:
        """
        Update markets.yaml with the latest event data for a city.

        Replaces the existing config with fresh bucket data from the new event.
        Supports all cities via city_registry.
        """
        if not buckets:
            logger.warning("No buckets to write")
            return False

        slug = event.get("slug", "unknown")
        end_date = event.get("endDate", "")

        # Extract negRiskMarketID if available
        neg_risk_id = ""
        markets = event.get("markets", [])
        if markets:
            neg_risk_id = markets[0].get("negRiskMarketId", "")

        # Get city metadata from registry
        city_cfg = city_registry.get_city(city)
        station_name = city_cfg.get("station", city.capitalize()) if city_cfg else city.capitalize()
        coordinates = list(city_registry.get_coordinates(city))
        tz = city_registry.get_timezone(city)
        resolution_url = city_registry.get_resolution_url(city)
        unit = city_registry.get_unit(city)
        icao = city_cfg.get("icao", "") if city_cfg else ""

        config = {
            "event_slug": slug,
            "event_end_date": end_date,
            "neg_risk_market_id": neg_risk_id,
            "city": city,
            "station": station_name,
            "coordinates": coordinates,
            "timezone": tz,
            "unit": unit,
            "resolution_source": resolution_url,
            "buckets": [],
        }

        for b in buckets:
            entry = {
                "label": b["label"],
                "temp_low": b.get("temp_low", 0),
                "temp_high": b.get("temp_high", 0),
                "condition_id": b["condition_id"],
                "yes_token_id": b["yes_token_id"],
                "no_token_id": b["no_token_id"],
                "tick_size": b.get("tick_size", 0.01),
                "min_order_size": b.get("min_order_size", 5),
            }
            config["buckets"].append(entry)

        # Write header comment + YAML
        unit_symbol = "°F" if unit == "fahrenheit" else "°C"
        header = (
            f"# Polymarket {station_name} Daily Temperature Market\n"
            f"# Auto-updated by SeriesTracker at {datetime.now(timezone.utc).isoformat()}\n"
            f"# Event: {slug}\n"
            f"# Resolution: Weather Underground {icao} station, whole {unit_symbol}\n"
            f"#\n"
        )

        with open(path, "w") as f:
            f.write(header)
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        logger.info(f"Updated {path} with {len(buckets)} buckets for {city}/{slug}")
        return True

    def get_live_prices(self, buckets: list[dict]) -> dict[str, float]:
        """Get current market prices for all buckets. Returns {label: yes_price}."""
        prices = {}
        for b in buckets:
            prices[b["label"]] = b.get("yes_price", 0)
        return prices
