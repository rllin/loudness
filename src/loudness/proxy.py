"""Proxy rotator with health tracking and multiple rotation strategies."""

import random
from datetime import datetime, timedelta
from typing import Literal

from .models import ProxyHealth, ProxyStatus


class ProxyRotator:
    """Manages a pool of proxies with health tracking and rotation strategies."""

    def __init__(
        self,
        proxies: list[str],
        strategy: Literal["round_robin", "weighted", "cooldown"] = "weighted",
        cooldown_seconds: float = 5.0,
        max_consecutive_failures: int = 3,
    ):
        """Initialize proxy rotator.

        Args:
            proxies: List of proxy URLs
            strategy: Rotation strategy to use
            cooldown_seconds: Minimum delay between uses of same proxy
            max_consecutive_failures: Mark proxy dead after this many failures
        """
        self.strategy = strategy
        self.cooldown_seconds = cooldown_seconds
        self.max_consecutive_failures = max_consecutive_failures

        self._proxies: dict[str, ProxyHealth] = {}
        self._round_robin_index = 0

        for proxy in proxies:
            self._proxies[proxy] = ProxyHealth(url=proxy)

    @property
    def proxies(self) -> list[ProxyHealth]:
        """Get all proxy health records."""
        return list(self._proxies.values())

    def get_proxy(self) -> str | None:
        """Get next proxy according to configured strategy.

        Returns:
            Proxy URL or None if no healthy proxies available
        """
        available = [p for p in self._proxies.values() if p.is_available()]

        if not available:
            return None

        if self.strategy == "round_robin":
            return self._get_round_robin(available)
        elif self.strategy == "weighted":
            return self._get_weighted(available)
        elif self.strategy == "cooldown":
            return self._get_cooldown(available)
        else:
            return random.choice(available).url

    def _get_round_robin(self, available: list[ProxyHealth]) -> str:
        """Simple round-robin rotation."""
        self._round_robin_index = self._round_robin_index % len(available)
        proxy = available[self._round_robin_index]
        self._round_robin_index += 1
        return proxy.url

    def _get_weighted(self, available: list[ProxyHealth]) -> str:
        """Weighted selection favoring proxies with better success rates."""
        weights = []
        for p in available:
            rate = p.success_rate()
            latency_penalty = min(1.0, p.avg_latency_ms / 5000.0) if p.avg_latency_ms > 0 else 0
            weight = rate * (1 - latency_penalty * 0.3)
            weight = max(0.1, weight)
            weights.append(weight)

        total = sum(weights)
        weights = [w / total for w in weights]

        r = random.random()
        cumulative = 0.0
        for proxy, weight in zip(available, weights, strict=True):
            cumulative += weight
            if r <= cumulative:
                return proxy.url

        return available[-1].url

    def _get_cooldown(self, available: list[ProxyHealth]) -> str:
        """Select proxy with longest time since last use."""
        now = datetime.utcnow()
        best_proxy = None
        best_idle_time = -1.0

        for p in available:
            if p.last_used is None:
                return p.url

            idle_time = (now - p.last_used).total_seconds()
            if idle_time > best_idle_time:
                best_idle_time = idle_time
                best_proxy = p

        if best_proxy:
            return best_proxy.url

        return available[0].url

    def report_success(self, proxy: str, latency_ms: float) -> None:
        """Report successful use of a proxy.

        Args:
            proxy: Proxy URL
            latency_ms: Request latency in milliseconds
        """
        if proxy not in self._proxies:
            return

        p = self._proxies[proxy]
        p.success_count += 1
        p.consecutive_failures = 0
        p.last_used = datetime.utcnow()

        total = p.success_count + p.fail_count
        p.avg_latency_ms = (p.avg_latency_ms * (total - 1) + latency_ms) / total

        if p.status in (ProxyStatus.SLOW, ProxyStatus.BLOCKED):
            p.status = ProxyStatus.HEALTHY

        if self.strategy == "cooldown":
            p.cooldown_until = datetime.utcnow() + timedelta(seconds=self.cooldown_seconds)

    def report_failure(self, proxy: str, error: Exception | None = None) -> None:
        """Report failed use of a proxy.

        Args:
            proxy: Proxy URL
            error: The exception that occurred
        """
        if proxy not in self._proxies:
            return

        p = self._proxies[proxy]
        p.fail_count += 1
        p.consecutive_failures += 1
        p.last_used = datetime.utcnow()

        if p.consecutive_failures >= self.max_consecutive_failures:
            p.status = ProxyStatus.DEAD
        elif self._is_blocked_error(error):
            p.status = ProxyStatus.BLOCKED
            p.cooldown_until = datetime.utcnow() + timedelta(seconds=self.cooldown_seconds * 4)
        elif self._is_slow_error(error):
            p.status = ProxyStatus.SLOW
            p.cooldown_until = datetime.utcnow() + timedelta(seconds=self.cooldown_seconds * 2)

    def _is_blocked_error(self, error: Exception | None) -> bool:
        """Check if error indicates proxy is blocked."""
        if error is None:
            return False

        import httpx

        if isinstance(error, httpx.HTTPStatusError):
            return error.response.status_code == 403

        error_str = str(error).lower()
        return "403" in error_str or "forbidden" in error_str or "blocked" in error_str

    def _is_slow_error(self, error: Exception | None) -> bool:
        """Check if error indicates proxy is slow."""
        if error is None:
            return False

        import httpx

        if isinstance(error, httpx.TimeoutException):
            return True
        if isinstance(error, httpx.HTTPStatusError):
            return error.response.status_code == 429

        return False

    def get_healthy_count(self) -> int:
        """Get count of healthy proxies."""
        return sum(1 for p in self._proxies.values() if p.status == ProxyStatus.HEALTHY)

    def get_available_count(self) -> int:
        """Get count of currently available proxies."""
        return sum(1 for p in self._proxies.values() if p.is_available())

    def get_stats(self) -> dict[str, int]:
        """Get proxy pool statistics."""
        stats: dict[str, int] = {
            "total": len(self._proxies),
            "healthy": 0,
            "slow": 0,
            "blocked": 0,
            "dead": 0,
            "available": 0,
        }

        for p in self._proxies.values():
            stats[p.status.value] = stats.get(p.status.value, 0) + 1
            if p.is_available():
                stats["available"] += 1

        return stats

    def reset_proxy(self, proxy: str) -> None:
        """Reset a proxy's health status."""
        if proxy in self._proxies:
            self._proxies[proxy] = ProxyHealth(url=proxy)

    def reset_all(self) -> None:
        """Reset all proxy health statuses."""
        for proxy in list(self._proxies.keys()):
            self._proxies[proxy] = ProxyHealth(url=proxy)
