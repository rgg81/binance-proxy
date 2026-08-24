"""Integration tests for KlineService: the wired-together behavior of
cache-hit short-circuiting, gap-fill, and request coalescing, against a
respx-mocked Binance backend and a real (tmp_path) SQLite store.
"""

import asyncio
import time

import httpx

from binance_proxy.cache.store import KlineStore
from binance_proxy.coalescing import Coalescer
from binance_proxy.models import Kline, Market, SeriesKey
from binance_proxy.service import KlineService
from binance_proxy.upstream.client import UpstreamClient
from binance_proxy.upstream.rate_limiter import RateLimiter

SPOT_BASE = "https://api.binance.com"
KLINES_URL = f"{SPOT_BASE}/api/v3/klines"
KEY = SeriesKey(market=Market.SPOT, symbol="BTCUSDT", interval="1m")


def make_service(tmp_path):
    store = KlineStore(tmp_path / "klines.db")
    coalescer = Coalescer()
    http_client = httpx.AsyncClient(base_url=SPOT_BASE)
    rate_limiter = RateLimiter(
        budget_per_window=6000,
        window_seconds=60.0,
        safety_margin=0.8,
        now_fn=time.monotonic,
        sleep_fn=asyncio.sleep,
    )
    upstream = UpstreamClient(http_client, rate_limiter, "/api/v3/klines")
    service = KlineService(store=store, coalescer=coalescer, clients={Market.SPOT: upstream})
    return service, store


def binance_row(open_time: int, interval_ms: int = 60_000) -> list:
    close_time = open_time + interval_ms - 1
    return [open_time, "1", "2", "0.5", "1.5", "10", close_time, "15", 3, "1", "1", "0"]


class TestCacheHitShortCircuit:
    async def test_fully_cached_range_makes_zero_upstream_calls(self, respx_mock, tmp_path):
        service, store = make_service(tmp_path)
        rows = [Kline.from_binance_row(binance_row(t)) for t in range(0, 600_000, 60_000)]
        store.upsert_klines(KEY, rows)
        store.add_coverage(KEY, (0, 600_000))
        route = respx_mock.get(KLINES_URL).mock(return_value=httpx.Response(200, json=[]))

        result = await service.get_klines(
            KEY, start_time=0, end_time=None, limit=10, now_ms=10_000_000
        )

        assert route.call_count == 0
        assert len(result) == 10
        assert result[0][0] == 0


class TestGapFill:
    async def test_only_the_missing_gap_is_fetched(self, respx_mock, tmp_path):
        service, store = make_service(tmp_path)
        store.upsert_klines(
            KEY, [Kline.from_binance_row(binance_row(t)) for t in range(0, 300_000, 60_000)]
        )
        store.add_coverage(KEY, (0, 300_000))
        route = respx_mock.get(KLINES_URL).mock(
            return_value=httpx.Response(
                200, json=[binance_row(t) for t in range(300_000, 600_000, 60_000)]
            )
        )

        result = await service.get_klines(
            KEY, start_time=0, end_time=None, limit=10, now_ms=10_000_000
        )

        assert route.call_count == 1
        called_params = route.calls[0].request.url.params
        assert called_params["startTime"] == "300000"
        assert len(result) == 10
        assert [row[0] for row in result] == list(range(0, 600_000, 60_000))

    async def test_gap_fill_result_is_persisted_for_future_requests(self, respx_mock, tmp_path):
        service, store = make_service(tmp_path)
        respx_mock.get(KLINES_URL).mock(return_value=httpx.Response(200, json=[binance_row(0)]))

        await service.get_klines(KEY, start_time=0, end_time=None, limit=1, now_ms=10_000_000)

        assert store.get_coverage(KEY) == [(0, 60_000)]
        assert len(store.get_klines(KEY, 0, 60_000)) == 1


class TestSingleFlightCoalescing:
    async def test_concurrent_identical_requests_result_in_one_upstream_call(
        self, respx_mock, tmp_path
    ):
        service, _store = make_service(tmp_path)
        route = respx_mock.get(KLINES_URL).mock(
            return_value=httpx.Response(200, json=[binance_row(0)])
        )

        results = await asyncio.gather(
            *[
                service.get_klines(KEY, start_time=0, end_time=None, limit=1, now_ms=10_000_000)
                for _ in range(20)
            ]
        )

        assert route.call_count == 1
        assert all(r == results[0] for r in results)


class TestEndTimeIsInclusiveLikeBinance:
    """Binance's `endTime` query param is inclusive: a candle whose open_time
    equals `endTime` exactly IS included in the response. Confirmed live
    against the real API. The internal cache/gap-fill arithmetic is
    half-open [start, end), so the client's raw (inclusive) end_time must be
    converted once at the service boundary — this regression-tests that
    conversion rather than the pure half-open arithmetic itself (which is
    already covered by test_fetch_plan.py under its own, correct contract).
    """

    async def test_candle_landing_exactly_on_end_time_is_included(self, respx_mock, tmp_path):
        service, _store = make_service(tmp_path)
        # Candles at 0 and 60_000; end_time == 60_000 exactly (candle-aligned).
        respx_mock.get(KLINES_URL).mock(
            return_value=httpx.Response(200, json=[binance_row(0), binance_row(60_000)])
        )

        result = await service.get_klines(
            KEY, start_time=0, end_time=60_000, limit=10, now_ms=10_000_000
        )

        assert [row[0] for row in result] == [0, 60_000]

    async def test_cached_range_still_includes_the_end_time_aligned_candle(
        self, respx_mock, tmp_path
    ):
        service, store = make_service(tmp_path)
        rows = [Kline.from_binance_row(binance_row(t)) for t in (0, 60_000)]
        store.upsert_klines(KEY, rows)
        store.add_coverage(KEY, (0, 120_000))  # half-open, correctly covers both
        route = respx_mock.get(KLINES_URL).mock(return_value=httpx.Response(200, json=[]))

        result = await service.get_klines(
            KEY, start_time=0, end_time=60_000, limit=10, now_ms=10_000_000
        )

        assert route.call_count == 0
        assert [row[0] for row in result] == [0, 60_000]


class TestLiveTail:
    async def test_open_candle_is_returned_but_never_persisted(self, respx_mock, tmp_path):
        service, store = make_service(tmp_path)
        now_ms = 120_030_000  # 30s into the candle opening at 120_000_000
        closed_boundary = 120_000_000
        open_row = binance_row(closed_boundary)  # close_time = closed_boundary + 59_999 > now
        respx_mock.get(KLINES_URL).mock(return_value=httpx.Response(200, json=[open_row]))

        result = await service.get_klines(
            KEY, start_time=closed_boundary, end_time=None, limit=1, now_ms=now_ms
        )

        assert result == [open_row]
        assert store.get_coverage(KEY) == []  # the open candle must never be cached
        assert store.get_klines(KEY, 0, 200_000_000) == []
