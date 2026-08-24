"""Thin async HTTP wrapper over one Binance market, wired to that market's
RateLimiter. No caching, no coalescing, no response parsing — it returns
exactly what Binance sent back (status code + body), except:

- 429/418, which raise RateLimitedError so the route layer can respond
  with 503 + Retry-After instead of passing a ban straight through.
- A transport-level failure (timeout, connection error) or a response body
  that isn't valid JSON, which raise UpstreamUnavailableError so the route
  layer can respond with a clean 503 instead of crashing with a raw 500.
"""

from __future__ import annotations

from typing import Any

import httpx

from binance_proxy.upstream.rate_limiter import RateLimiter


class RateLimitedError(Exception):
    """Binance is actively rate-limiting or banning us (429/418), or our own
    circuit breaker is already open from a prior response.
    """

    def __init__(self, status_code: int, retry_after: float) -> None:
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"rate limited (status {status_code}), retry after {retry_after}s")


class UpstreamUnavailableError(Exception):
    """The round trip to Binance itself failed — a transport-level error
    (timeout, connection reset, DNS failure) or a response body that
    couldn't be parsed as JSON. Distinct from RateLimitedError: this means
    we couldn't complete the request at all, not that Binance rejected us.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"upstream request failed: {reason}")


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


def _parse_limit(params: dict[str, Any]) -> int:
    """Binance is the source of truth for request validity (see
    CLAUDE.md invariant #5) — a malformed `limit` must still be forwarded
    to Binance for its own proper rejection, not crash the proxy before
    the call is ever made. If it can't be parsed, assume the highest
    weight tier so the rate limiter never under-reserves.
    """
    raw = params.get("limit", 500)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 1000


class UpstreamClient:
    def __init__(self, http_client: httpx.AsyncClient, rate_limiter: RateLimiter) -> None:
        self._http = http_client
        self._rate_limiter = rate_limiter
        # How many times we actually hit Binance (as opposed to how many
        # requests the cache/coalescing layer satisfied without one) — the
        # single most direct measure of whether caching is doing its job.
        self.calls_made = 0

    async def fetch(self, path: str, params: dict[str, Any]) -> tuple[int, object]:
        if self._rate_limiter.is_banned():
            raise RateLimitedError(418, self._rate_limiter.seconds_until_unbanned())

        weight = estimate_klines_weight(_parse_limit(params))
        await self._rate_limiter.acquire(weight)

        self.calls_made += 1
        try:
            response = await self._http.get(path, params=params)
        except httpx.HTTPError as exc:
            raise UpstreamUnavailableError(f"transport error: {exc}") from exc

        self._rate_limiter.on_response(response.status_code, response.headers)

        if response.status_code in (429, 418):
            raise RateLimitedError(
                response.status_code, self._rate_limiter.seconds_until_unbanned()
            )

        try:
            body = response.json()
        except ValueError as exc:  # json.JSONDecodeError subclasses ValueError
            raise UpstreamUnavailableError(f"non-JSON response body: {exc}") from exc

        return response.status_code, body

    async def close(self) -> None:
        await self._http.aclose()
