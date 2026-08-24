"""Unit tests for UpstreamClient's own bookkeeping: the calls_made counter
that /stats uses to show how many times we actually hit Binance (as opposed
to how many times the cache/coalescing layer avoided doing so).
"""

import asyncio
import time

import httpx
import pytest
import respx

from binance_proxy.upstream.client import UpstreamClient
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
    return UpstreamClient(http_client, rate_limiter, "/api/v3/klines")


class TestCallsMadeCounter:
    async def test_starts_at_zero(self):
        assert make_client().calls_made == 0

    @respx.mock(base_url=BASE)
    async def test_increments_on_each_real_request(self, respx_mock):
        respx_mock.get("/api/v3/klines").mock(
            return_value=httpx.Response(200, json=[])
        )
        client = make_client()

        await client.fetch_klines({"symbol": "BTCUSDT", "interval": "1m", "limit": 1})
        await client.fetch_klines({"symbol": "BTCUSDT", "interval": "1m", "limit": 1})

        assert client.calls_made == 2

    async def test_does_not_increment_when_short_circuited_by_an_open_breaker(self):
        client = make_client()
        client._rate_limiter._trip(60.0)  # simulate an already-open breaker

        with pytest.raises(Exception):  # noqa: B017 - RateLimitedError, imported indirectly
            await client.fetch_klines({"symbol": "BTCUSDT", "interval": "1m", "limit": 1})

        assert client.calls_made == 0
