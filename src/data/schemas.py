from datetime import date, datetime, time
from pydantic import BaseModel


class RawWeatherData(BaseModel):
    city: str
    event_date: date
    source: str  # "open-meteo" | "wu"
    model_name: str  # "ecmwf" | "gfs" | "icon" | "best_match"
    hourly_temps: list[float]
    fetched_at: datetime
    raw_response: dict = {}
    # Multi-variable weather factors (hourly arrays, same length as hourly_temps)
    hourly_humidity: list[float] | None = None  # relative_humidity_2m (%)
    hourly_wind_speed: list[float] | None = None  # wind_speed_10m (km/h)
    hourly_wind_direction: list[float] | None = None  # wind_direction_10m (degrees)
    hourly_cloud_cover: list[float] | None = None  # cloud_cover (%)
    hourly_precipitation: list[float] | None = None  # precipitation (mm)
    hourly_pressure: list[float] | None = None  # surface_pressure (hPa)
    hourly_radiation: list[float] | None = None  # shortwave_radiation (W/m²)


class WeatherFactors(BaseModel):
    """Extracted weather condition factors for conditional bias correction."""
    mean_cloud_cover: float = 50.0  # average daytime cloud cover (%)
    max_wind_speed: float = 10.0  # max wind speed (km/h)
    dominant_wind_dir: float = 180.0  # dominant wind direction (degrees)
    is_sea_breeze: bool = False  # east wind at ZSPD = sea breeze cooling
    total_precipitation: float = 0.0  # total precip (mm)
    mean_humidity: float = 60.0  # mean relative humidity (%)
    mean_pressure: float = 1013.0  # mean surface pressure (hPa)
    pressure_trend: float = 0.0  # pressure change over day (hPa)
    diurnal_range: float = 8.0  # max - min temperature (°C)
    mean_solar_radiation: float = 200.0  # mean daytime shortwave radiation (W/m²)


class NormalizedForecast(BaseModel):
    city: str
    event_date: date
    model_forecasts: dict[str, float]  # {"gfs": 29.8, "icon": 28.5}
    latest_observation: float | None = None
    observation_time: datetime | None = None
    hours_to_settlement: float
    updated_at: datetime
    # Multi-variable factors per model
    weather_factors: dict[str, WeatherFactors] | None = None  # {model_name: factors}
    # Ensemble data for probability estimation
    ensemble_maxes: list[float] | None = None  # daily max from each ensemble member


class EventSpec(BaseModel):
    market_id: str
    city: str
    event_type: str
    station: str
    coordinates: tuple[float, float]
    metric: str
    comparator: str  # ">" | ">=" | "<" | "<="
    threshold: float
    time_window_start: time
    time_window_end: time
    settlement_time_utc: datetime


class FairProbResult(BaseModel):
    fair_prob: float
    confidence: float
    breakdown: dict


class EdgeResult(BaseModel):
    edge: float
    direction: str  # "BUY_YES" | "BUY_NO" | "NO_TRADE"
    fair_prob: float
    market_price: float


class Signal(BaseModel):
    market_id: str
    level: str  # "STRONG" | "LEAN" | "NO_TRADE"
    direction: str | None
    edge: float
    filters_result: dict


class Order(BaseModel):
    market_id: str
    side: str  # "YES" | "NO"
    amount: float
    price: float


class RiskAction(BaseModel):
    action: str  # "HOLD" | "REDUCE" | "CLOSE" | "PAUSE"
    reason: str
    reduce_pct: float = 0.0


# --- Multi-Outcome Market Models ---

class BucketSpec(BaseModel):
    """One temperature bucket in a multi-outcome market."""
    label: str  # e.g. "17°C", "11°C or below"
    temp_low: float  # lower bound (inclusive), -999 for "or below"
    temp_high: float  # upper bound (inclusive), 999 for "or higher"
    condition_id: str
    yes_token_id: str
    no_token_id: str
    tick_size: float = 0.001
    min_order_size: float = 5.0


class BucketPrice(BaseModel):
    """Live market price for one bucket."""
    label: str
    yes_price: float
    no_price: float
    best_bid: float
    best_ask: float
    spread: float
    volume: float
    liquidity: float


class MultiMarketSnapshot(BaseModel):
    """Snapshot of all bucket prices for one event."""
    event_slug: str
    buckets: list[BucketPrice]
    total_price_sum: float  # sum of all YES prices (should be ~1.0)
    fetched_at: datetime


class MultiOutcomeFairResult(BaseModel):
    """Fair probabilities for all temperature buckets."""
    bucket_probs: dict[str, float]  # {"17°C": 0.35, "18°C": 0.30, ...}
    weighted_mean_temp: float
    uncertainty_std: float
    model_forecasts: dict[str, float]  # raw per-model max temps
    confidence: float


class BucketEdge(BaseModel):
    """Edge for a single bucket."""
    label: str
    fair_prob: float
    market_price: float
    edge: float  # fair_prob - market_price
    direction: str  # "BUY_YES" | "BUY_NO" | "NO_TRADE"
    kelly_fraction: float = 0.0
    # Limit order pricing
    limit_price: float = 0.0  # optimal limit order price
    expected_fill_prob: float = 0.0  # probability of limit being filled
    risk_reward_ratio: float = 0.0  # potential_profit / potential_loss


class MultiOutcomeEdgeResult(BaseModel):
    """Edge detection result across all buckets."""
    bucket_edges: list[BucketEdge]
    sum_to_one_gap: float  # total_market_price_sum - 1.0
    best_buys: list[BucketEdge]  # sorted by edge descending
    best_sells: list[BucketEdge]  # sorted by negative edge descending
    arb_opportunity: bool  # True if actionable arbitrage exists


class BucketSignal(BaseModel):
    """Trading signal for one bucket."""
    label: str
    direction: str  # "BUY_YES" | "BUY_NO"
    edge: float
    fair_prob: float
    market_price: float
    amount: float  # USD amount
    confidence: float

