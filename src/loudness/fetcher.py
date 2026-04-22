"""Async HTTP client with proxy rotation and retry logic."""

import asyncio
import random
import time
from typing import Any
from urllib.parse import quote

import httpx

from .backoff import BackoffController
from .models import FetchResult
from .proxy import ProxyRotator

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

YELP_GQL_BATCH_URL = "https://www.yelp.com/gql/batch"
SCRAPFLY_API_URL = "https://api.scrapfly.io/scrape"


class AsyncFetcher:
    """Async HTTP client with proxy rotation, backoff, and header rotation."""

    def __init__(
        self,
        proxy_rotator: ProxyRotator | None = None,
        backoff: BackoffController | None = None,
        max_retries: int = 3,
        timeout: float = 15.0,
        user_agents: list[str] | None = None,
        request_delay: float = 1.0,
        scrapfly_key: str | None = None,
    ):
        self.proxy_rotator = proxy_rotator
        self.backoff = backoff or BackoffController()
        self.max_retries = max_retries
        self.timeout = timeout
        self.user_agents = user_agents or USER_AGENTS
        self.request_delay = request_delay
        self.scrapfly_key = scrapfly_key

        self._client: httpx.AsyncClient | None = None
        self._last_request_time: float = 0

    async def __aenter__(self) -> "AsyncFetcher":
        await self.open()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def open(self) -> None:
        """Open HTTP client."""
        limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
        self._client = httpx.AsyncClient(
            limits=limits,
            timeout=httpx.Timeout(self.timeout),
            follow_redirects=True,
        )

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        """Get current client, raising if not opened."""
        if not self._client:
            raise RuntimeError("Fetcher not opened")
        return self._client

    def _get_headers(self, referer: str | None = None) -> dict[str, str]:
        """Get randomized headers for request."""
        headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
        if referer:
            headers["Referer"] = referer
        else:
            headers["Referer"] = "https://www.google.com/"
        return headers

    def _get_gql_headers(self, business_url: str) -> dict[str, str]:
        """Get headers for GraphQL batch request."""
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": "https://www.yelp.com",
            "Referer": business_url,
            "x-apollo-operation-name": "GetBusinessReviewFeed",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

    async def _wait_for_rate_limit(self) -> None:
        """Wait to respect rate limiting between requests."""
        if self.request_delay <= 0:
            return

        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self.request_delay:
            await asyncio.sleep(self.request_delay - elapsed)
        self._last_request_time = time.monotonic()

    async def get(self, url: str, referer: str | None = None) -> FetchResult:
        """Fetch a URL with retry logic and proxy rotation.

        Args:
            url: URL to fetch
            referer: Optional referer header

        Returns:
            FetchResult with response data or error
        """
        # Use ScrapFly if configured
        if self.scrapfly_key:
            return await self._get_via_scrapfly(url)

        return await self._get_direct(url, referer)

    async def _get_via_scrapfly(self, url: str) -> FetchResult:
        """Fetch URL via ScrapFly API for anti-bot bypass."""
        start_time = time.monotonic()

        for attempt in range(self.max_retries):
            try:
                await self._wait_for_rate_limit()

                scrapfly_url = (
                    f"{SCRAPFLY_API_URL}?"
                    f"key={self.scrapfly_key}&url={quote(url, safe='')}&asp=true"
                )

                response = await self.client.get(scrapfly_url)

                if response.status_code == 200:
                    data = response.json()
                    result = data.get("result", {})

                    if result.get("error"):
                        error_msg = result["error"].get("message", "ScrapFly error")
                        if attempt < self.max_retries - 1:
                            await self.backoff.wait(attempt)
                            continue
                        return FetchResult(
                            success=False,
                            error=error_msg,
                            attempts=attempt + 1,
                            total_time_ms=(time.monotonic() - start_time) * 1000,
                        )

                    content = result.get("content", "")
                    return FetchResult(
                        success=True,
                        status_code=result.get("status_code", 200),
                        body=content,
                        attempts=attempt + 1,
                        total_time_ms=(time.monotonic() - start_time) * 1000,
                    )

                if self.backoff.is_retryable_status(response.status_code):
                    await self.backoff.wait(attempt)
                    continue

                return FetchResult(
                    success=False,
                    status_code=response.status_code,
                    error=f"ScrapFly HTTP {response.status_code}",
                    attempts=attempt + 1,
                    total_time_ms=(time.monotonic() - start_time) * 1000,
                )

            except Exception as e:
                if self.backoff.should_retry(attempt, self.max_retries, e):
                    await self.backoff.wait(attempt)
                    continue

                return FetchResult(
                    success=False,
                    error=str(e),
                    attempts=attempt + 1,
                    total_time_ms=(time.monotonic() - start_time) * 1000,
                )

        return FetchResult(
            success=False,
            error="Max retries exceeded",
            attempts=self.max_retries,
            total_time_ms=(time.monotonic() - start_time) * 1000,
        )

    async def _get_direct(self, url: str, referer: str | None = None) -> FetchResult:
        """Fetch URL directly with proxy rotation."""
        start_time = time.monotonic()
        last_error: Exception | None = None
        proxy_used: str | None = None

        for attempt in range(self.max_retries):
            try:
                await self._wait_for_rate_limit()

                proxy = None
                if self.proxy_rotator:
                    proxy = self.proxy_rotator.get_proxy()
                    proxy_used = proxy

                request_start = time.monotonic()

                response = await self.client.get(
                    url,
                    headers=self._get_headers(referer),
                    extensions={"proxy": proxy} if proxy else {},
                )

                latency_ms = (time.monotonic() - request_start) * 1000

                if self.proxy_rotator and proxy:
                    if response.status_code == 200:
                        self.proxy_rotator.report_success(proxy, latency_ms)
                    else:
                        self.proxy_rotator.report_failure(proxy)

                if response.status_code == 200:
                    return FetchResult(
                        success=True,
                        status_code=response.status_code,
                        body=response.text,
                        attempts=attempt + 1,
                        total_time_ms=(time.monotonic() - start_time) * 1000,
                        proxy_used=proxy_used,
                    )

                if self.backoff.is_retryable_status(response.status_code):
                    is_rate_limited = self.backoff.is_rate_limited(response.status_code)
                    await self.backoff.wait(attempt, is_rate_limited)
                    continue

                return FetchResult(
                    success=False,
                    status_code=response.status_code,
                    error=f"HTTP {response.status_code}",
                    attempts=attempt + 1,
                    total_time_ms=(time.monotonic() - start_time) * 1000,
                    proxy_used=proxy_used,
                )

            except Exception as e:
                last_error = e

                if self.proxy_rotator and proxy_used:
                    self.proxy_rotator.report_failure(proxy_used, e)

                if self.backoff.should_retry(attempt, self.max_retries, e):
                    await self.backoff.wait(attempt)
                    continue

                break

        return FetchResult(
            success=False,
            error=str(last_error) if last_error else "Max retries exceeded",
            attempts=self.max_retries,
            total_time_ms=(time.monotonic() - start_time) * 1000,
            proxy_used=proxy_used,
        )

    async def post_graphql(
        self, payload: dict | list, business_url: str
    ) -> FetchResult:
        """Post GraphQL request to Yelp batch endpoint.

        Args:
            payload: GraphQL query payload
            business_url: The business URL for referer

        Returns:
            FetchResult with response data or error
        """
        start_time = time.monotonic()
        last_error: Exception | None = None
        proxy_used: str | None = None

        for attempt in range(self.max_retries):
            try:
                await self._wait_for_rate_limit()

                proxy = None
                if self.proxy_rotator:
                    proxy = self.proxy_rotator.get_proxy()
                    proxy_used = proxy

                request_start = time.monotonic()

                response = await self.client.post(
                    YELP_GQL_BATCH_URL,
                    json=payload,
                    headers=self._get_gql_headers(business_url),
                    extensions={"proxy": proxy} if proxy else {},
                )

                latency_ms = (time.monotonic() - request_start) * 1000

                if self.proxy_rotator and proxy:
                    if response.status_code == 200:
                        self.proxy_rotator.report_success(proxy, latency_ms)
                    else:
                        self.proxy_rotator.report_failure(proxy)

                if response.status_code == 200:
                    return FetchResult(
                        success=True,
                        status_code=response.status_code,
                        body=response.text,
                        attempts=attempt + 1,
                        total_time_ms=(time.monotonic() - start_time) * 1000,
                        proxy_used=proxy_used,
                    )

                if self.backoff.is_retryable_status(response.status_code):
                    is_rate_limited = self.backoff.is_rate_limited(response.status_code)
                    await self.backoff.wait(attempt, is_rate_limited)
                    continue

                return FetchResult(
                    success=False,
                    status_code=response.status_code,
                    error=f"HTTP {response.status_code}",
                    attempts=attempt + 1,
                    total_time_ms=(time.monotonic() - start_time) * 1000,
                    proxy_used=proxy_used,
                )

            except Exception as e:
                last_error = e

                if self.proxy_rotator and proxy_used:
                    self.proxy_rotator.report_failure(proxy_used, e)

                if self.backoff.should_retry(attempt, self.max_retries, e):
                    await self.backoff.wait(attempt)
                    continue

                break

        return FetchResult(
            success=False,
            error=str(last_error) if last_error else "Max retries exceeded",
            attempts=self.max_retries,
            total_time_ms=(time.monotonic() - start_time) * 1000,
            proxy_used=proxy_used,
        )
