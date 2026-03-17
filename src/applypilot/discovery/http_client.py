"""Shared async HTTP client with rate limiting, retry, and a Playwright wrapper.

All scrapers receive an ``AsyncHTTPClient`` instance injected by the
orchestrator — they never construct one themselves.  This keeps connection
pooling, rate-limit budgets, and proxy config centralised.

Classes
-------
RateLimiter        Token-bucket limiter; shared across all requests from one client.
AsyncHTTPClient    httpx-backed async client with retry + rate limiting.
PlaywrightWrapper  Chromium browser for JS-rendered pages.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from contextlib import asynccontextmanager
from typing import Optional

import httpx

log = logging.getLogger("applypilot.http_client")

# ---------------------------------------------------------------------------
# Browser-like default headers
# ---------------------------------------------------------------------------

_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}

# HTTP status codes worth retrying (server-side transient errors)
_RETRYABLE_STATUS: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------


class RateLimiter:
    """Token-bucket rate limiter for async code.

    Args:
        rate:  Sustained requests per second (tokens refilled per second).
        burst: Maximum requests allowed in a burst (bucket capacity).

    Usage::

        limiter = RateLimiter(rate=2.0, burst=5)
        await limiter.acquire()
    """

    def __init__(self, rate: float = 2.0, burst: int = 5) -> None:
        self._rate = rate
        self._burst = burst
        self._tokens: float = float(burst)
        self._last: float = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(
                float(self._burst),
                self._tokens + (now - self._last) * self._rate,
            )
            self._last = now

            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


# ---------------------------------------------------------------------------
# Async HTTP client
# ---------------------------------------------------------------------------


class AsyncHTTPClient:
    """Shared async HTTP client with per-host rate limiting and retry logic.

    Designed to be used as an async context manager::

        async with AsyncHTTPClient() as client:
            resp = await client.get("https://example.com")

    Retry strategy: exponential back-off with jitter on transient HTTP errors
    (408, 429, 5xx) and network-level exceptions (timeout, connection reset).

    Args:
        rate:          Sustained request rate (req/s).
        burst:         Burst capacity for the rate limiter.
        max_retries:   Maximum retry attempts per request.
        timeout:       Per-request timeout in seconds.
        proxy:         HTTP/HTTPS proxy URL.
        extra_headers: Merged into the default browser-like headers.
    """

    def __init__(
        self,
        *,
        rate: float = 2.0,
        burst: int = 5,
        max_retries: int = 3,
        timeout: float = 30.0,
        proxy: Optional[str] = None,
        extra_headers: Optional[dict[str, str]] = None,
    ) -> None:
        self._rate_limiter = RateLimiter(rate=rate, burst=burst)
        self._max_retries = max_retries
        self._timeout = httpx.Timeout(timeout)
        self._proxy = proxy
        self._extra_headers = extra_headers or {}
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> AsyncHTTPClient:
        headers = {**_BROWSER_HEADERS, **self._extra_headers}
        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=self._timeout,
            proxy=self._proxy,
            follow_redirects=True,
            http2=False,
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Public request methods
    # ------------------------------------------------------------------

    async def get(self, url: str, **kwargs: object) -> httpx.Response:
        return await self._request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: object) -> httpx.Response:
        return await self._request("POST", url, **kwargs)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _request(
        self, method: str, url: str, **kwargs: object
    ) -> httpx.Response:
        if self._client is None:
            raise RuntimeError(
                "AsyncHTTPClient must be used as an async context manager"
            )

        await self._rate_limiter.acquire()

        last_exc: Optional[Exception] = None

        for attempt in range(self._max_retries):
            try:
                resp: httpx.Response = await self._client.request(
                    method, url, **kwargs  # type: ignore[arg-type]
                )

                if resp.status_code in _RETRYABLE_STATUS:
                    wait = _backoff(attempt)
                    log.warning(
                        "%s %s → HTTP %d (attempt %d/%d), retry in %.1fs",
                        method,
                        url,
                        resp.status_code,
                        attempt + 1,
                        self._max_retries,
                        wait,
                    )
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp

            except (
                httpx.TimeoutException,
                httpx.NetworkError,
                httpx.RemoteProtocolError,
            ) as exc:
                last_exc = exc
                wait = _backoff(attempt)
                log.warning(
                    "%s %s → %s (attempt %d/%d), retry in %.1fs",
                    method,
                    url,
                    type(exc).__name__,
                    attempt + 1,
                    self._max_retries,
                    wait,
                )
                await asyncio.sleep(wait)

        raise last_exc or RuntimeError(
            f"All {self._max_retries} attempts failed: {url}"
        )


def _backoff(attempt: int, base: float = 2.0, jitter: float = 1.0) -> float:
    """Exponential back-off with random jitter: base^attempt ± jitter."""
    return base**attempt + random.uniform(0, jitter)


# ---------------------------------------------------------------------------
# Playwright wrapper
# ---------------------------------------------------------------------------


class PlaywrightWrapper:
    """Async Playwright wrapper for JS-rendered pages.

    Use when ``AsyncHTTPClient`` alone isn't enough — bot challenges,
    lazy-loaded content, or login flows.

    Shares a single Chromium process across multiple page fetches to keep
    launch overhead low.

    Usage::

        async with PlaywrightWrapper() as pw:
            html = await pw.fetch_page("https://example.com/jobs")
    """

    def __init__(
        self,
        *,
        proxy: Optional[str] = None,
        headless: bool = True,
    ) -> None:
        self._proxy = proxy
        self._headless = headless
        self._playwright = None
        self._browser = None

    async def __aenter__(self) -> PlaywrightWrapper:
        from playwright.async_api import async_playwright  # lazy import

        self._playwright = await async_playwright().start()
        launch_opts: dict = {"headless": self._headless}
        if self._proxy:
            launch_opts["proxy"] = {"server": self._proxy}
        self._browser = await self._playwright.chromium.launch(**launch_opts)
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    @asynccontextmanager
    async def new_context(self, **kwargs: object):
        """Yield a fresh browser context that is automatically closed on exit."""
        ctx = await self._browser.new_context(
            user_agent=_BROWSER_HEADERS["User-Agent"],
            locale="en-US",
            **kwargs,
        )
        try:
            yield ctx
        finally:
            await ctx.close()

    async def fetch_page(
        self,
        url: str,
        *,
        wait_selector: Optional[str] = None,
        wait_ms: int = 1_000,
    ) -> str:
        """Navigate to ``url`` and return the fully-rendered HTML.

        Args:
            url:            Target URL.
            wait_selector:  Optional CSS selector to wait for before returning.
            wait_ms:        Additional milliseconds to wait after DOM load.
        """
        async with self.new_context() as ctx:
            page = await ctx.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            if wait_selector:
                await page.wait_for_selector(wait_selector, timeout=10_000)
            if wait_ms:
                await page.wait_for_timeout(wait_ms)
            return await page.content()
