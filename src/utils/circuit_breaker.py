"""Circuit breaker for external API calls with exponential backoff."""

import time
from src.utils.logger import logger


class CircuitBreaker:
    """Tracks consecutive failures and applies exponential backoff.

    States:
    - CLOSED: normal operation
    - OPEN: too many failures, reject calls until cooldown expires
    - HALF_OPEN: cooldown expired, allow one test call
    """

    def __init__(
        self,
        name: str,
        max_failures: int = 5,
        base_delay: float = 5.0,
        max_delay: float = 300.0,
    ):
        self.name = name
        self.max_failures = max_failures
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._failures = 0
        self._last_failure_time = 0.0
        self._state = "CLOSED"

    @property
    def state(self) -> str:
        if self._state == "OPEN":
            cooldown = self._current_delay()
            if time.time() - self._last_failure_time > cooldown:
                self._state = "HALF_OPEN"
        return self._state

    def allow_request(self) -> bool:
        """Check if a request should be allowed."""
        s = self.state
        if s == "CLOSED":
            return True
        if s == "HALF_OPEN":
            return True  # Allow one test request
        # OPEN
        remaining = self._current_delay() - (time.time() - self._last_failure_time)
        logger.info(
            f"CircuitBreaker[{self.name}]: OPEN, {remaining:.0f}s until half-open "
            f"(failures={self._failures})"
        )
        return False

    def record_success(self):
        """Record a successful call — reset the breaker."""
        if self._failures > 0:
            logger.info(f"CircuitBreaker[{self.name}]: recovered after {self._failures} failures")
        self._failures = 0
        self._state = "CLOSED"

    def record_failure(self):
        """Record a failed call — increment failure count."""
        self._failures += 1
        self._last_failure_time = time.time()
        if self._failures >= self.max_failures:
            self._state = "OPEN"
            logger.warning(
                f"CircuitBreaker[{self.name}]: OPEN after {self._failures} failures, "
                f"cooldown={self._current_delay():.0f}s"
            )
        else:
            logger.info(
                f"CircuitBreaker[{self.name}]: failure {self._failures}/{self.max_failures}"
            )

    def _current_delay(self) -> float:
        """Exponential backoff delay based on failure count."""
        delay = self.base_delay * (2 ** min(self._failures - self.max_failures, 6))
        return min(delay, self.max_delay)

    def reset(self):
        """Manually reset the breaker."""
        self._failures = 0
        self._state = "CLOSED"
