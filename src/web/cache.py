"""Simple in-memory TTL cache for API responses."""

import time
from typing import Any


class TTLCache:
    """Thread-safe TTL cache backed by a plain dict."""

    def __init__(self):
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.time() > expires_at:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any, ttl: int = 30):
        self._store[key] = (time.time() + ttl, value)

    def clear(self):
        self._store.clear()


cache = TTLCache()
