"""Exponential backoff controller with jitter."""

import asyncio
import random
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    pass


class BackoffController:
    """Manages exponential backoff with jitter for retry logic."""

    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
    RATE_LIMITED_CODES = {429}

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter: float = 0.5,
    ):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter

    def get_delay(self, attempt: int, is_rate_limited: bool = False) -> float:
        """Calculate delay for given attempt number.

        Args:
            attempt: Attempt number (0-indexed)
            is_rate_limited: If True, use 2x multiplier for rate limit responses
        """
        mult = self.multiplier * 2 if is_rate_limited else self.multiplier
        delay = self.base_delay * (mult**attempt)
        delay = min(delay, self.max_delay)

        jitter_amount = delay * self.jitter
        delay = delay + random.uniform(-jitter_amount, jitter_amount)

        return max(0.1, delay)

    async def wait(self, attempt: int, is_rate_limited: bool = False) -> float:
        """Sleep with exponential backoff + jitter, return actual delay.

        Args:
            attempt: Attempt number (0-indexed)
            is_rate_limited: If True, use 2x multiplier for rate limit responses
        """
        delay = self.get_delay(attempt, is_rate_limited)
        await asyncio.sleep(delay)
        return delay

    def is_retryable_status(self, status_code: int) -> bool:
        """Check if HTTP status code is retryable."""
        return status_code in self.RETRYABLE_STATUS_CODES

    def is_rate_limited(self, status_code: int) -> bool:
        """Check if HTTP status code indicates rate limiting."""
        return status_code in self.RATE_LIMITED_CODES

    def is_retryable_error(self, error: Exception) -> bool:
        """Check if exception is retryable."""
        if isinstance(error, httpx.TimeoutException):
            return True
        if isinstance(error, httpx.ConnectError):
            return True
        if isinstance(error, httpx.ReadError):
            return True
        if isinstance(error, httpx.HTTPStatusError):
            return self.is_retryable_status(error.response.status_code)
        return False

    def should_retry(
        self, attempt: int, max_attempts: int, error: Exception | None = None
    ) -> bool:
        """Check if request should be retried.

        Args:
            attempt: Current attempt number (0-indexed)
            max_attempts: Maximum number of attempts allowed
            error: The exception that occurred, if any
        """
        if attempt >= max_attempts - 1:
            return False

        if error is None:
            return True

        return self.is_retryable_error(error)
