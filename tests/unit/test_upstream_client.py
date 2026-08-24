"""Unit tests for UpstreamClient: a thin, rate-limit-aware HTTP wrapper.

No parsing, no caching — it returns exactly (status_code, body) for
whatever Binance sent back, except 429/418 which raise RateLimitedError so
the route layer can respond with 503 + Retry-After instead of passing a
ban straight through to the caller, and transport-level failures (timeout,
connection error, unparseable body) which raise UpstreamUnavailableError
so the route layer can respond with a clean 503 instead of a raw crash.
"""

import asyncio
import time

import httpx
import pytest
import respx

from binance_proxy.upstream.client import (
    RateLimitedError,
    UpstreamClient,
    UpstreamUnavailableError,
)
from binance_proxy.upstream.rate_limiter import RateLimiter

BASE = "https://api.binance.com"


def make_client() -> UpstreamClient:
    http_client = httpx.AsyncClient(base_url=BASE)
    rate_limiter = RateLimiter(
        budget_per_window=6000,
        window_seconds=60.0,
        safety_margin=0.8,
        now_fn=time.monotonic,
        sleep_fn=asyncio.sleep,
    )
    return UpstreamClient(http_client, rate_limiter)


class TestFetchReturnsStatusAndBodyVerbatim:
    @respx.mock(base_url=BASE)
    async def test_200_response(self, respx_mock):
        respx_mock.get("/api/v3/klines").mock(
            return_value=httpx.Response(200, json=[[1, "2"]])
        )
        client = make_client()

        status_code, body = await client.fetch(
            "/api/v3/klines", {"symbol": "BTCUSDT", "interval": "1m", "limit": 1}
        )

        assert status_code == 200
        assert body == [[1, "2"]]

    @respx.mock(base_url=BASE)
    async def test_client_error_response_is_returned_not_raised(self, respx_mock):
        error_body = {"code": -1121, "msg": "Invalid symbol."}
        respx_mock.get("/api/v3/klines").mock(
            return_value=httpx.Response(400, json=error_body)
        )
        client = make_client()

        status_code, body = await client.fetch(
            "/api/v3/klines", {"symbol": "NOTREAL", "interval": "1m", "limit": 1}
        )

        assert status_code == 400
        assert body == error_body


class TestCallsMadeCounter:
    async def test_starts_at_zero(self):
        assert make_client().calls_made == 0

    @respx.mock(base_url=BASE)
    async def test_increments_on_each_real_request(self, respx_mock):
        respx_mock.get("/api/v3/klines").mock(return_value=httpx.Response(200, json=[]))
        client = make_client()

        await client.fetch("/api/v3/klines", {"symbol": "BTCUSDT", "interval": "1m", "limit": 1})
        await client.fetch("/api/v3/klines", {"symbol": "BTCUSDT", "interval": "1m", "limit": 1})

        assert client.calls_made == 2

    async def test_does_not_increment_when_short_circuited_by_an_open_breaker(self):
        client = make_client()
        client._rate_limiter._trip(60.0)  # simulate an already-open breaker

        with pytest.raises(RateLimitedError):
            await client.fetch("/api/v3/klines", {"symbol": "BTCUSDT"})

        assert client.calls_made == 0


class TestRateLimitHandling:
    @respx.mock(base_url=BASE)
    async def test_429_raises_rate_limited_error(self, respx_mock):
        respx_mock.get("/api/v3/klines").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "5"})
        )
        client = make_client()

        with pytest.raises(RateLimitedError) as exc_info:
            await client.fetch("/api/v3/klines", {"symbol": "BTCUSDT"})
        assert exc_info.value.status_code == 429

    @respx.mock(base_url=BASE)
    async def test_418_raises_rate_limited_error(self, respx_mock):
        respx_mock.get("/api/v3/klines").mock(
            return_value=httpx.Response(418, headers={"Retry-After": "60"})
        )
        client = make_client()

        with pytest.raises(RateLimitedError) as exc_info:
            await client.fetch("/api/v3/klines", {"symbol": "BTCUSDT"})
        assert exc_info.value.status_code == 418


class TestMalformedLimitDoesNotCrash:
    """Binance is the source of truth for request validity (CLAUDE.md
    invariant #5) — a malformed `limit` must still reach Binance, not crash
    the proxy before ever making the call.
    """

    @respx.mock(base_url=BASE)
    async def test_non_numeric_limit_still_reaches_binance(self, respx_mock):
        route = respx_mock.get("/api/v3/klines").mock(
            return_value=httpx.Response(
                400, json={"code": -1130, "msg": "Data sent for parameter 'limit' is not valid."}
            )
        )
        client = make_client()

        status_code, body = await client.fetch(
            "/api/v3/klines", {"symbol": "BTCUSDT", "interval": "1m", "limit": "abc"}
        )

        assert route.call_count == 1
        assert status_code == 400

    @respx.mock(base_url=BASE)
    async def test_missing_limit_uses_the_default(self, respx_mock):
        respx_mock.get("/api/v3/klines").mock(return_value=httpx.Response(200, json=[]))
        client = make_client()

        status_code, _body = await client.fetch(
            "/api/v3/klines", {"symbol": "BTCUSDT", "interval": "1m"}
        )

        assert status_code == 200


class TestClose:
    async def test_close_closes_the_underlying_http_client(self):
        http_client = httpx.AsyncClient(base_url=BASE)
        rate_limiter = RateLimiter(
            budget_per_window=6000,
            window_seconds=60.0,
            safety_margin=0.8,
            now_fn=time.monotonic,
            sleep_fn=asyncio.sleep,
        )
        client = UpstreamClient(http_client, rate_limiter)

        await client.close()

        assert http_client.is_closed


class TestUpstreamUnavailable:
    @respx.mock(base_url=BASE)
    async def test_transport_error_raises_upstream_unavailable_not_a_crash(self, respx_mock):
        respx_mock.get("/api/v3/klines").mock(side_effect=httpx.ConnectError("boom"))
        client = make_client()

        with pytest.raises(UpstreamUnavailableError):
            await client.fetch("/api/v3/klines", {"symbol": "BTCUSDT"})

    @respx.mock(base_url=BASE)
    async def test_non_json_body_raises_upstream_unavailable_not_a_crash(self, respx_mock):
        respx_mock.get("/api/v3/klines").mock(
            return_value=httpx.Response(200, content=b"<html>not json</html>")
        )
        client = make_client()

        with pytest.raises(UpstreamUnavailableError):
            await client.fetch("/api/v3/klines", {"symbol": "BTCUSDT"})
