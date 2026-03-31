from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def get_local_now(timezone_str: str) -> datetime:
    """Return current time in the specified timezone (timezone-aware)."""
    tz = ZoneInfo(timezone_str)
    return datetime.now(tz)


def to_utc(dt: datetime, timezone_str: str) -> datetime:
    """Convert a datetime to UTC. If naive, assume it's in the given timezone."""
    if dt.tzinfo is None:
        tz = ZoneInfo(timezone_str)
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(timezone.utc)


def hours_until(target_utc: datetime) -> float:
    """Return hours from now until target_utc."""
    now = datetime.now(timezone.utc)
    if target_utc.tzinfo is None:
        target_utc = target_utc.replace(tzinfo=timezone.utc)
    delta = target_utc - now
    return delta.total_seconds() / 3600.0
