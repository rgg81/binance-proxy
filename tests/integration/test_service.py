"""Integration tests for ProxyService: cache check -> coalesce -> fetch ->
cache -> respond, against a respx-mocked Binance backend.
"""

import asyncio
import time

import httpx

from binance_proxy.cache import TTLCache
from binance_proxy.coalescing import Coalescer
from binance_proxy.models import Market
from binance_proxy.service import ProxyService
from binance_proxy.upstream.client import UpstreamClient
from binance_proxy.upstream.rate_limiter import RateLimiter

SPOT_BASE = "https://api.binance.com"
KLINES_URL = f"{SPOT_BASE}/api/v3/klines"


def make_service(ttl_seconds: float = 60.0) -> ProxyService:
    store = TTLCache(ttl_seconds=ttl_seconds)
    coalescer = Coalescer()
    http_client = httpx.AsyncClient(base_url=SPOT_BASE)
    rate_limiter = RateLimiter(
        budget_per_window=6000,
        window_seconds=60.0,
        safety_margin=0.8,
        now_fn=time.monotonic,
        sleep_fn=asyncio.sleep,
    )
    upstream = UpstreamClient(http_client, rate_limiter)
    return ProxyService(cache=store, coalescer=coalescer, clients={Market.SPOT: upstream})


class TestCacheHit:
    async def test_repeat_identical_request_within_ttl_makes_zero_upstream_calls(
        self, respx_mock
    ):
        service = make_service()
        route = respx_mock.get(KLINES_URL).mock(
            return_value=httpx.Response(200, json=[[1, "2"]])
        )
        params = {"symbol": "BTCUSDT", "interval": "1m", "limit": "5"}

        r1 = await service.get(Market.SPOT, "/api/v3/klines", params)
        r2 = await service.get(Market.SPOT, "/api/v3/klines", params)

        assert route.call_count == 1
        assert r1 == r2 == (200, [[1, "2"]])

    async def test_different_params_are_independent_cache_entries(self, respx_mock):
        service = make_service()
        route = respx_mock.get(KLINES_URL).mock(
            side_effect=[
                httpx.Response(200, json=[["a"]]),
                httpx.Response(200, json=[["b"]]),
            ]
        )

        r1 = await service.get(Market.SPOT, "/api/v3/klines", {"symbol": "BTCUSDT"})
        r2 = await service.get(Market.SPOT, "/api/v3/klines", {"symbol": "ETHUSDT"})

        assert route.call_count == 2
        assert r1 == (200, [["a"]])
        assert r2 == (200, [["b"]])

    async def test_expired_entry_triggers_a_fresh_fetch(self, respx_mock):
        service = make_service(ttl_seconds=0.05)
        route = respx_mock.get(KLINES_URL).mock(
            side_effect=[
                httpx.Response(200, json=[["old"]]),
                httpx.Response(200, json=[["new"]]),
            ]
        )
        params = {"symbol": "BTCUSDT"}

        r1 = await service.get(Market.SPOT, "/api/v3/klines", params)
        await asyncio.sleep(0.06)
        r2 = await service.get(Market.SPOT, "/api/v3/klines", params)

        assert route.call_count == 2
        assert r1 == (200, [["old"]])
        assert r2 == (200, [["new"]])


class TestClientErrorsAreCachedToo:
    """A permanently-invalid request (e.g. a symbol that doesn't exist on a
    given market) gets the exact same 4xx from Binance every time — caching
    it, same as a 200, stops a caller that keeps retrying a bad request from
    burning a fresh upstream call on every single attempt. Confirmed as a
    real, measurable contributor to a production ban: ~10% of all upstream
    calls were wasted retries of ~546 permanently-invalid futures symbols
    (see docs/superpowers/specs/2026-08-24-simple-memory-cache-redesign.md's
    follow-up incident notes). The tradeoff — a genuinely transient 4xx
    could be replayed from cache for up to CACHE_TTL_SECONDS — is accepted;
    see CLAUDE.md invariant #2. 5xx is NOT cached — see
    TestServerErrorsAreNotCached below for why 4xx and 5xx are treated
    differently.
    """

    async def test_client_error_is_cached_and_not_retried_within_ttl(self, respx_mock):
        service = make_service()
        route = respx_mock.get(KLINES_URL).mock(
            return_value=httpx.Response(400, json={"code": -1121, "msg": "Invalid symbol."})
        )
        params = {"symbol": "BADSYMBOL"}

        r1 = await service.get(Market.SPOT, "/api/v3/klines", params)
        r2 = await service.get(Market.SPOT, "/api/v3/klines", params)

        assert route.call_count == 1  # cached -> not asked again
        assert r1 == r2 == (400, {"code": -1121, "msg": "Invalid symbol."})


class TestServerErrorsAreNotCached:
    """A 4xx means the *request itself* is invalid — deterministic, and
    (per the incident this reverses) worth caching so a caller retrying a
    permanently-bad request doesn't keep burning upstream calls. A 5xx
    means *Binance's own state* at that moment — not deterministic, and
    could genuinely resolve moments later. Caching it would silently mask
    a real Binance-side outage from every caller hitting that exact key
    for up to CACHE_TTL_SECONDS, which the 4xx case's reasoning doesn't
    apply to and doesn't justify.
    """

    async def test_server_error_is_not_cached_and_is_retried_next_time(self, respx_mock):
        service = make_service()
        route = respx_mock.get(KLINES_URL).mock(
            side_effect=[
                httpx.Response(500, json={"code": -1000, "msg": "An unknown error occurred."}),
                httpx.Response(200, json=[["recovered"]]),
            ]
        )
        params = {"symbol": "BTCUSDT"}

        r1 = await service.get(Market.SPOT, "/api/v3/klines", params)
        r2 = await service.get(Market.SPOT, "/api/v3/klines", params)

        assert route.call_count == 2  # not cached -> asked again
        assert r1 == (500, {"code": -1000, "msg": "An unknown error occurred."})
        assert r2 == (200, [["recovered"]])


class TestSingleFlightCoalescing:
    async def test_concurrent_identical_requests_result_in_one_upstream_call(self, respx_mock):
        service = make_service()
        route = respx_mock.get(KLINES_URL).mock(
            return_value=httpx.Response(200, json=[["x"]])
        )
        params = {"symbol": "BTCUSDT", "limit": "5"}

        results = await asyncio.gather(
            *[service.get(Market.SPOT, "/api/v3/klines", params) for _ in range(20)]
        )

        assert route.call_count == 1
        assert all(r == results[0] for r in results)
