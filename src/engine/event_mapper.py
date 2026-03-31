from datetime import time, datetime, timezone

import yaml

from src.data.schemas import EventSpec, NormalizedForecast
from src.utils.logger import logger


class ConfigError(Exception):
    pass


class MarketNotFoundError(Exception):
    pass


class EventMapper:
    """L3: Map Polymarket settlement rules to executable EventSpec objects."""

    REQUIRED_FIELDS = [
        "id", "city", "event_type",
    ]
    REQUIRED_SETTLEMENT_FIELDS = [
        "station", "coordinates", "metric",
        "comparator", "threshold_celsius", "time_window_local",
    ]
    REQUIRED_TOP_LEVEL = ["settlement_time_utc"]

    def __init__(
        self,
        markets_config_path: str = "config/markets.yaml",
        model_weights: dict[str, float] | None = None,
    ):
        with open(markets_config_path, "r") as f:
            raw = yaml.safe_load(f)

        self._events: dict[str, EventSpec] = {}
        self._model_weights = model_weights or {"ecmwf": 0.45, "gfs": 0.30, "icon": 0.25}

        for market in raw.get("markets", []):
            self._validate_and_load(market)

        logger.info(f"EventMapper loaded {len(self._events)} markets")

    def _validate_and_load(self, market: dict):
        # Check top-level fields
        for field in self.REQUIRED_FIELDS:
            if field not in market:
                raise ConfigError(f"Market missing required field: '{field}'")

        if "settlement_time_utc" not in market:
            raise ConfigError(
                f"Market '{market.get('id', '?')}' missing 'settlement_time_utc'"
            )

        rules = market.get("settlement_rules")
        if not rules:
            raise ConfigError(
                f"Market '{market['id']}' missing 'settlement_rules'"
            )

        for field in self.REQUIRED_SETTLEMENT_FIELDS:
            if field not in rules:
                raise ConfigError(
                    f"Market '{market['id']}' settlement_rules missing '{field}'"
                )

        # Validate comparator
        valid_comparators = {">", ">=", "<", "<="}
        if rules["comparator"] not in valid_comparators:
            raise ConfigError(
                f"Invalid comparator '{rules['comparator']}'. "
                f"Must be one of {valid_comparators}"
            )

        # Parse time window
        tw = rules["time_window_local"]
        start = time.fromisoformat(tw[0])
        end = time.fromisoformat(tw[1])

        # Parse settlement time
        settle_str = market["settlement_time_utc"]
        if isinstance(settle_str, str):
            settle_dt = datetime.fromisoformat(settle_str.replace("Z", "+00:00"))
        elif isinstance(settle_str, datetime):
            settle_dt = settle_str
            if settle_dt.tzinfo is None:
                settle_dt = settle_dt.replace(tzinfo=timezone.utc)
        else:
            raise ConfigError(f"Invalid settlement_time_utc: {settle_str}")

        coords = rules["coordinates"]
        event_spec = EventSpec(
            market_id=market["id"],
            city=market["city"],
            event_type=market["event_type"],
            station=rules["station"],
            coordinates=(coords[0], coords[1]),
            metric=rules["metric"],
            comparator=rules["comparator"],
            threshold=rules["threshold_celsius"],
            time_window_start=start,
            time_window_end=end,
            settlement_time_utc=settle_dt,
        )

        self._events[market["id"]] = event_spec

    def get_all_events(self) -> list[EventSpec]:
        return list(self._events.values())

    def get_event(self, market_id: str) -> EventSpec:
        if market_id not in self._events:
            raise MarketNotFoundError(f"Market '{market_id}' not found")
        return self._events[market_id]

    def evaluate(self, forecast: NormalizedForecast, event_spec: EventSpec) -> bool:
        """Evaluate whether the event triggers based on weighted forecast average."""
        weighted_temp = self._weighted_average(forecast.model_forecasts)
        threshold = event_spec.threshold
        comp = event_spec.comparator

        if comp == ">":
            return weighted_temp > threshold
        elif comp == ">=":
            return weighted_temp >= threshold
        elif comp == "<":
            return weighted_temp < threshold
        elif comp == "<=":
            return weighted_temp <= threshold
        else:
            raise ValueError(f"Unknown comparator: {comp}")

    def _weighted_average(self, model_forecasts: dict[str, float]) -> float:
        """Compute weighted average temperature, re-normalizing if models are missing."""
        total_weight = 0.0
        weighted_sum = 0.0

        for model, temp in model_forecasts.items():
            w = self._model_weights.get(model, 0)
            weighted_sum += w * temp
            total_weight += w

        if total_weight == 0:
            raise ValueError("No valid model weights found")

        return weighted_sum / total_weight
