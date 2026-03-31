"""
Auto-discovery module: automatically scan Polymarket for weather markets,
extract condition_id/token_id, parse settlement rules from descriptions,
and update markets.yaml.

Weather market patterns observed on Polymarket:
- "Will the high temperature in New York's Central Park be 60°F or higher on November 2?"
- "Will it snow in New York's Central Park on Christmas Eve?"
- "Will it be sunny in Washington DC at noon on November 3rd?"
- "Will August 2021 average global temperature be the highest?"
- "Will a Category 5 Hurricane Make Landfall before November 1?"
"""

import json
import os
import re
from datetime import datetime, timezone
from dataclasses import dataclass, field

import httpx
import yaml

from src.utils.logger import logger

GAMMA_URL = "https://gamma-api.polymarket.com"

# City name → coordinates and station mapping
CITY_DB = {
    "new york": {"coords": (40.7828, -73.9653), "station": "Central Park", "tz": "America/New_York", "key": "new_york"},
    "central park": {"coords": (40.7828, -73.9653), "station": "Central Park", "tz": "America/New_York", "key": "new_york"},
    "nyc": {"coords": (40.7828, -73.9653), "station": "Central Park", "tz": "America/New_York", "key": "new_york"},
    "chicago": {"coords": (41.8781, -87.6298), "station": "O'Hare Airport", "tz": "America/Chicago", "key": "chicago"},
    "los angeles": {"coords": (34.0522, -118.2437), "station": "LAX", "tz": "America/Los_Angeles", "key": "los_angeles"},
    "la": {"coords": (34.0522, -118.2437), "station": "LAX", "tz": "America/Los_Angeles", "key": "los_angeles"},
    "miami": {"coords": (25.7617, -80.1918), "station": "Miami International", "tz": "America/New_York", "key": "miami"},
    "houston": {"coords": (29.7604, -95.3698), "station": "Bush Intercontinental", "tz": "America/Chicago", "key": "houston"},
    "phoenix": {"coords": (33.4484, -112.0740), "station": "Sky Harbor", "tz": "America/Phoenix", "key": "phoenix"},
    "dallas": {"coords": (32.7767, -96.7970), "station": "DFW Airport", "tz": "America/Chicago", "key": "dallas"},
    "washington": {"coords": (38.9072, -77.0369), "station": "Reagan National", "tz": "America/New_York", "key": "washington_dc"},
    "washington dc": {"coords": (38.9072, -77.0369), "station": "Reagan National", "tz": "America/New_York", "key": "washington_dc"},
    "dc": {"coords": (38.9072, -77.0369), "station": "Reagan National", "tz": "America/New_York", "key": "washington_dc"},
    "boston": {"coords": (42.3601, -71.0589), "station": "Logan Airport", "tz": "America/New_York", "key": "boston"},
    "san francisco": {"coords": (37.7749, -122.4194), "station": "SFO", "tz": "America/Los_Angeles", "key": "san_francisco"},
    "denver": {"coords": (39.7392, -104.9903), "station": "Denver International", "tz": "America/Denver", "key": "denver"},
    "atlanta": {"coords": (33.7490, -84.3880), "station": "Hartsfield-Jackson", "tz": "America/New_York", "key": "atlanta"},
    "seattle": {"coords": (47.6062, -122.3321), "station": "Sea-Tac", "tz": "America/Los_Angeles", "key": "seattle"},
    "london": {"coords": (51.5074, -0.1278), "station": "Heathrow", "tz": "Europe/London", "key": "london"},
    "paris": {"coords": (48.8566, 2.3522), "station": "Orly", "tz": "Europe/Paris", "key": "paris"},
    "tokyo": {"coords": (35.6762, 139.6503), "station": "Tokyo", "tz": "Asia/Tokyo", "key": "tokyo"},
    # Shanghai: ZSPD coords; bias correction applied in probability engine
    "shanghai": {"coords": (31.1434, 121.8052), "station": "Pudong Airport (ZSPD)", "tz": "Asia/Shanghai", "key": "shanghai"},
}


@dataclass
class DiscoveredMarket:
    """A weather market automatically discovered from Polymarket."""
    question: str
    description: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    yes_price: float
    no_price: float
    volume: float
    liquidity: float
    slug: str
    end_date: str
    active: bool
    # Parsed settlement info
    city_key: str = ""
    station: str = ""
    coordinates: tuple = (0.0, 0.0)
    event_type: str = ""  # daily_high_temp, daily_low_temp, snow, sunny, etc.
    metric: str = ""
    comparator: str = ">"
    threshold: float = 0.0
    threshold_unit: str = "F"  # F or C
    threshold_celsius: float = 0.0
    settlement_date: str = ""
    parse_confidence: float = 0.0  # 0-1, how confident we are in the parse


class AutoDiscovery:
    """
    Automatically discover and parse weather markets from Polymarket.

    Scans all active (and optionally closed) markets, identifies weather-related
    ones by pattern matching, and extracts settlement rules from descriptions.
    """

    # Regex patterns for weather market questions
    WEATHER_PATTERNS = [
        # Temperature markets: "high temperature in X be Y°F or higher on DATE"
        r'(?:high|max|maximum)\s+temperature.*?(?:in|at)\s+(.+?)\s+be\s+(\d+)\s*°?\s*([FCfc])',
        r'temperature.*?(?:in|at)\s+(.+?)\s+(?:exceed|reach|hit|above)\s+(\d+)\s*°?\s*([FCfc])',
        r'(?:in|at)\s+(.+?)\s+(?:be|reach|exceed|hit)\s+(\d+)\s*°?\s*([FCfc])',
        # Snow markets
        r'(?:will it\s+)?snow\s+(?:in|at)\s+(.+?)(?:\s+on|\s+before|\s+by)',
        # Rain/sunny markets
        r'(?:will it\s+)?(?:be\s+sunny|rain)\s+(?:in|at)\s+(.+?)(?:\s+at|\s+on)',
        # Record temperature
        r'(?:record|highest|hottest|coldest|warmest)\s+.*?temperature',
        # Hurricane
        r'hurricane.*?(?:landfall|make landfall|category)',
    ]

    # False positive exclusion patterns
    FALSE_POSITIVE_PATTERNS = [
        r'nba|nhl|nfl|mlb|fifa|premier league|la liga|serie a|bundesliga',
        r'bitcoin|crypto|stock|etf|s&p|nasdaq',
        r'election|president|congress|senate|governor|vote',
        r'ukraine|russia|ceasefire|nato|iran|israel|hamas|war|military',
        r'mayweather|boxing|ufc|fight',
        r'album|movie|tv\s+show|game|gta|release',
        r'coldplay|drake|taylor|kanye',
        r'auction|art|phillips|sotheby|christie',
        r'covid|vaccine|pandemic',
        r'earthquake|richter',
        r'ipo|acquisition|merger|company',
    ]

    def __init__(self):
        self._http = httpx.Client(timeout=20)

    def scan(self, include_closed: bool = False, max_pages: int = 30) -> list[DiscoveredMarket]:
        """
        Scan Polymarket for weather markets.

        Returns list of DiscoveredMarket with parsed settlement info.
        """
        logger.info(f"Scanning Polymarket for weather markets (max {max_pages * 100} markets)...")

        all_markets = []
        for offset in range(0, max_pages * 100, 100):
            try:
                params = {"limit": 100, "offset": offset, "active": "true"}
                if not include_closed:
                    params["closed"] = "false"
                resp = self._http.get(f"{GAMMA_URL}/markets", params=params)
                data = resp.json()
                if not data:
                    break
                all_markets.extend(data)
            except Exception as e:
                logger.warning(f"Page {offset} failed: {e}")
                break

        logger.info(f"Scanned {len(all_markets)} markets")

        # Filter and parse
        discovered = []
        for m in all_markets:
            dm = self._try_parse(m)
            if dm is not None:
                discovered.append(dm)

        # Sort by confidence then volume
        discovered.sort(key=lambda x: (-x.parse_confidence, -x.volume))

        logger.info(f"Found {len(discovered)} weather markets")
        return discovered

    def scan_with_history(self, max_pages: int = 50) -> list[DiscoveredMarket]:
        """Scan including closed markets (to learn patterns for future markets)."""
        return self.scan(include_closed=True, max_pages=max_pages)

    def _try_parse(self, raw: dict) -> DiscoveredMarket | None:
        """Try to identify and parse a weather market from raw API data."""
        question = raw.get("question", "")
        description = raw.get("description", "")
        full_text = (question + " " + description).lower()

        # Exclude false positives first
        for fp_pat in self.FALSE_POSITIVE_PATTERNS:
            if re.search(fp_pat, full_text):
                return None

        # Try to match weather patterns
        matched = False
        for pat in self.WEATHER_PATTERNS:
            if re.search(pat, full_text, re.IGNORECASE):
                matched = True
                break

        if not matched:
            return None

        # Parse token IDs
        tokens = raw.get("clobTokenIds", [])
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        if not tokens or len(tokens) < 2:
            return None

        prices = raw.get("outcomePrices", [])
        if isinstance(prices, str):
            prices = json.loads(prices)
        yes_price = float(prices[0]) if prices else 0
        no_price = float(prices[1]) if len(prices) > 1 else 0

        dm = DiscoveredMarket(
            question=question,
            description=description[:500],
            condition_id=raw.get("conditionId", ""),
            yes_token_id=tokens[0],
            no_token_id=tokens[1],
            yes_price=yes_price,
            no_price=no_price,
            volume=float(raw.get("volumeNum", 0) or 0),
            liquidity=float(raw.get("liquidity", 0) or 0),
            slug=raw.get("slug", ""),
            end_date=raw.get("endDate", ""),
            active=raw.get("active", False) and not raw.get("closed", True),
        )

        # Parse settlement details
        self._parse_settlement(dm, question, description)

        return dm

    def _parse_settlement(self, dm: DiscoveredMarket, question: str, description: str):
        """Extract settlement rules from question and description text."""
        q_lower = question.lower()
        d_lower = description.lower()
        full = q_lower + " " + d_lower
        confidence = 0.0

        # --- Extract city ---
        for city_name, info in CITY_DB.items():
            if city_name in full:
                dm.city_key = info["key"]
                dm.station = info["station"]
                dm.coordinates = info["coords"]
                confidence += 0.3
                break

        # --- Extract event type and threshold ---

        # Pattern: "high temperature ... be 60°F or higher"
        temp_match = re.search(
            r'(?:high|max|maximum)\s+(?:air\s+)?temperature.*?'
            r'(?:be|exceed|reach|hit|above)\s+(\d+)\s*°?\s*([FCfc])\s*(?:or\s+higher|or\s+above)?',
            full, re.IGNORECASE
        )
        if temp_match:
            dm.event_type = "daily_high_temp"
            dm.metric = "max_temperature"
            dm.threshold = float(temp_match.group(1))
            dm.threshold_unit = temp_match.group(2).upper()
            dm.comparator = ">="  # "or higher" = >=
            confidence += 0.3

        # Pattern: "low temperature ... be X or lower"
        if not temp_match:
            low_match = re.search(
                r'(?:low|min|minimum)\s+temperature.*?(?:be|drop|fall)\s+(\d+)\s*°?\s*([FCfc])',
                full, re.IGNORECASE
            )
            if low_match:
                dm.event_type = "daily_low_temp"
                dm.metric = "min_temperature"
                dm.threshold = float(low_match.group(1))
                dm.threshold_unit = low_match.group(2).upper()
                dm.comparator = "<="
                confidence += 0.3

        # Pattern: snow
        if "snow" in q_lower:
            dm.event_type = "snow"
            dm.metric = "snowfall"
            dm.comparator = ">"
            dm.threshold = 0.0  # any snow
            dm.threshold_unit = "inch"
            confidence += 0.2

        # Pattern: sunny
        if "sunny" in q_lower:
            dm.event_type = "sunny"
            dm.metric = "solar_radiation"
            dm.comparator = ">"
            dm.threshold = 0.0
            confidence += 0.2

        # --- Extract threshold in Celsius ---
        if dm.threshold_unit == "F" and dm.threshold > 0:
            dm.threshold_celsius = round((dm.threshold - 32) * 5 / 9, 1)
        elif dm.threshold_unit == "C" and dm.threshold > 0:
            dm.threshold_celsius = dm.threshold
        else:
            dm.threshold_celsius = dm.threshold

        # --- Extract comparator from description ---
        if "or higher" in full or "or above" in full or "at least" in full:
            dm.comparator = ">="
        elif "or lower" in full or "or below" in full:
            dm.comparator = "<="
        elif "exceed" in full or "more than" in full or "greater than" in full:
            dm.comparator = ">"
        elif "less than" in full or "below" in full or "under" in full:
            dm.comparator = "<"

        # --- Extract settlement date ---
        # From end_date
        if dm.end_date:
            try:
                dm.settlement_date = dm.end_date[:10]
                confidence += 0.1
            except Exception:
                pass

        # From question: "on November 2, 2021"
        date_match = re.search(
            r'on\s+(\w+\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4})',
            question, re.IGNORECASE
        )
        if date_match:
            dm.settlement_date = date_match.group(1)
            confidence += 0.1

        dm.parse_confidence = min(confidence, 1.0)

    def export_to_yaml(self, markets: list[DiscoveredMarket], path: str = "config/markets.yaml"):
        """Write discovered markets to markets.yaml."""
        entries = []
        for dm in markets:
            if dm.parse_confidence < 0.3:
                logger.warning(
                    f"Skipping low-confidence market: {dm.question[:60]} "
                    f"(confidence={dm.parse_confidence:.1f})"
                )
                continue

            # Build settlement time from end_date
            settle_utc = dm.end_date if dm.end_date else "2026-12-31T05:00:00Z"
            if "T" not in settle_utc:
                settle_utc += "T05:00:00Z"

            entry = {
                "id": dm.slug or dm.condition_id[:20],
                "polymarket_slug": dm.slug,
                "city": dm.city_key or "unknown",
                "event_type": dm.event_type or "unknown",
                "condition_id": dm.condition_id,
                "token_id": dm.yes_token_id,
                "no_token_id": dm.no_token_id,
                "settlement_rules": {
                    "station": dm.station or "Unknown",
                    "coordinates": list(dm.coordinates),
                    "metric": dm.metric or "max_temperature",
                    "comparator": dm.comparator,
                    "threshold_celsius": dm.threshold_celsius,
                    "threshold_original": f"{dm.threshold}°{dm.threshold_unit}",
                    "time_window_local": ["00:00", "23:59"],
                    "settlement_source": "NWS official",
                },
                "settlement_time_utc": settle_utc,
                "auto_discovered": True,
                "parse_confidence": dm.parse_confidence,
                "question": dm.question,
            }
            entries.append(entry)

        config = {"markets": entries}

        # Merge with existing config if present
        if os.path.exists(path):
            with open(path, "r") as f:
                existing = yaml.safe_load(f) or {}
            existing_ids = {m["id"] for m in existing.get("markets", [])}

            # Only add new markets
            new_entries = [e for e in entries if e["id"] not in existing_ids]
            if new_entries:
                existing.setdefault("markets", []).extend(new_entries)
                config = existing
                logger.info(f"Adding {len(new_entries)} new markets to existing config")
            else:
                logger.info("No new markets to add")
                return 0
        else:
            logger.info(f"Creating new config with {len(entries)} markets")

        with open(path, "w") as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        logger.info(f"Saved {len(entries)} markets to {path}")
        return len(entries)

    def print_report(self, markets: list[DiscoveredMarket]):
        """Print a human-readable report of discovered markets."""
        active = [m for m in markets if m.active]
        closed = [m for m in markets if not m.active]

        print(f"\n{'='*80}")
        print(f" POLYMARKET WEATHER MARKET AUTO-DISCOVERY")
        print(f" Scanned at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"{'='*80}")

        if active:
            print(f"\n ACTIVE MARKETS ({len(active)}):")
            print(f" {'─'*76}")
            for i, m in enumerate(active, 1):
                self._print_market(i, m)
        else:
            print(f"\n NO ACTIVE WEATHER MARKETS FOUND")
            print(f" Weather markets are seasonal - they appear during:")
            print(f"   - Summer heat waves (June-August)")
            print(f"   - Winter storms/snow (November-February)")
            print(f"   - Hurricane season (June-November)")
            print(f"   - Extreme weather events")
            print(f"\n The system will auto-scan every 6 hours and start trading")
            print(f" as soon as new weather markets appear.")

        if closed:
            print(f"\n HISTORICAL WEATHER MARKETS ({len(closed)}):")
            print(f" {'─'*76}")
            for i, m in enumerate(closed, 1):
                self._print_market(i, m, show_ids=False)

        print(f"\n{'='*80}")

    def _print_market(self, idx: int, m: DiscoveredMarket, show_ids: bool = True):
        status = "ACTIVE" if m.active else "CLOSED"
        conf_bar = "█" * int(m.parse_confidence * 10) + "░" * (10 - int(m.parse_confidence * 10))

        print(f"\n  {idx}. [{status}] {m.question}")
        print(f"     Parse confidence: [{conf_bar}] {m.parse_confidence:.0%}")
        print(f"     Type: {m.event_type or '?'}  |  City: {m.city_key or '?'}  |  "
              f"Threshold: {m.threshold}°{m.threshold_unit} ({m.threshold_celsius}°C)  |  "
              f"Comparator: {m.comparator}")
        print(f"     YES={m.yes_price:.3f}  NO={m.no_price:.3f}  |  "
              f"Vol=${m.volume:,.0f}  |  Liq=${m.liquidity:,.0f}")

        if show_ids:
            print(f"     condition_id: {m.condition_id}")
            print(f"     YES token:    {m.yes_token_id}")
            print(f"     NO  token:    {m.no_token_id}")
