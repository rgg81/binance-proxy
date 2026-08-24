"""Thin async HTTP wrapper over one Binance market's klines endpoint, wired
to that market's RateLimiter. No caching or coalescing logic lives here —
this module only knows how to make one rate-limit-aware call to Binance.
"""

from __future__ import annotations

from typing import Any

import httpx

from binance_proxy.upstream.rate_limiter import RateLimiter


class BinanceApiError(Exception):
    """A non-2xx, non-rate-limit response from Binance (e.g. bad symbol).

    Carries the original status code and JSON body so the route layer can
    pass them through to the caller verbatim.
    """

    def __init__(self, status_code: int, body: object) -> None:
        self.status_code = status_code
        self.body = body
        super().__init__(f"Binance responded {status_code}: {body!r}")


class RateLimitedError(Exception):
    """Binance is actively rate-limiting or banning us (429/418), or our own
    circuit breaker is already open from a prior response.
    """

    def __init__(self, status_code: int, retry_after: float) -> None:
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"rate limited (status {status_code}), retry after {retry_after}s")


def estimate_klines_weight(limit: int) -> int:
    """Conservative local weight estimate, used only before a response
    arrives. Reconciled against Binance's own X-MBX-USED-WEIGHT-* headers
    immediately after (see RateLimiter.on_response), which are the ground
    truth this proxy actually relies on for safety.
    """
    if limit <= 100:
        return 2
    if limit <= 500:
        return 5
    return 10


class UpstreamClient:
    def __init__(
        self, http_client: httpx.AsyncClient, rate_limiter: RateLimiter, path: str
    ) -> None:
        self._http = http_client
        self._rate_limiter = rate_limiter
        self._path = path
        # How many times we actually hit Binance (as opposed to how many
        # requests the cache/coalescing layer satisfied without one) — the
        # single most direct measure of whether caching is doing its job.
        self.calls_made = 0

    async def fetch_klines(self, params: dict[str, Any]) -> list[list[Any]]:
        if self._rate_limiter.is_banned():
            raise RateLimitedError(418, self._rate_limiter.seconds_until_unbanned())

        weight = estimate_klines_weight(int(params.get("limit", 500)))
        await self._rate_limiter.acquire(weight)

        self.calls_made += 1
        response = await self._http.get(self._path, params=params)
        self._rate_limiter.on_response(response.status_code, response.headers)

        if response.status_code in (429, 418):
            raise RateLimitedError(
                response.status_code, self._rate_limiter.seconds_until_unbanned()
            )
        if response.status_code != 200:
            raise BinanceApiError(response.status_code, response.json())
        result: list[list[Any]] = response.json()
        return result
