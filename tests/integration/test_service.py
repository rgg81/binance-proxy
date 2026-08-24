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


class TestFillGapNeverTrustsAnUnclosedCandle:
    """Defense in depth: _fill_gap must derive what it marks covered from
    what Binance actually returned as closed (close_time < now_ms), the same
    way _fetch_live_tail and _fetch_passthrough already do — rather than
    unconditionally trusting the requested [start, end) shape. This protects
    against any future/unknown way closed_boundary could be computed wrong,
    not just the specific timezone case fixed separately.
    """

    async def test_all_rows_closed_covers_the_full_requested_range(self, respx_mock, tmp_path):
        service, store = make_service(tmp_path)
        now_ms = 300_000
        respx_mock.get(KLINES_URL).mock(
            return_value=httpx.Response(200, json=[binance_row(0), binance_row(60_000)])
        )

        await service._fill_gap(KEY, 0, 120_000, 60_000, now_ms)

        assert store.get_coverage(KEY) == [(0, 120_000)]
        assert [k.open_time for k in store.get_klines(KEY, 0, 999_999)] == [0, 60_000]

    async def test_trailing_unclosed_row_is_not_persisted_or_covered_past_it(
        self, respx_mock, tmp_path
    ):
        service, store = make_service(tmp_path)
        now_ms = 300_000
        closed_row = binance_row(0)  # close_time 59_999 < now_ms: genuinely closed
        unclosed_row = [
            60_000, "1", "2", "0.5", "1.5", "10", 500_000, "15", 3, "1", "1", "0",
        ]  # close_time 500_000 >= now_ms: NOT actually closed
        respx_mock.get(KLINES_URL).mock(
            return_value=httpx.Response(200, json=[closed_row, unclosed_row])
        )

        await service._fill_gap(KEY, 0, 120_000, 60_000, now_ms)

        assert store.get_coverage(KEY) == [(0, 60_000)]
        assert [k.open_time for k in store.get_klines(KEY, 0, 999_999)] == [0]

    async def test_no_rows_closed_persists_nothing_and_covers_nothing(self, respx_mock, tmp_path):
        service, store = make_service(tmp_path)
        now_ms = 10
        unclosed_row = [0, "1", "2", "0.5", "1.5", "10", 500_000, "15", 3, "1", "1", "0"]
        respx_mock.get(KLINES_URL).mock(return_value=httpx.Response(200, json=[unclosed_row]))

        await service._fill_gap(KEY, 0, 60_000, 60_000, now_ms)

        assert store.get_coverage(KEY) == []
        assert store.get_klines(KEY, 0, 999_999) == []

    async def test_verified_empty_range_is_still_covered(self, respx_mock, tmp_path):
        service, store = make_service(tmp_path)
        respx_mock.get(KLINES_URL).mock(return_value=httpx.Response(200, json=[]))

        await service._fill_gap(KEY, 0, 60_000, 60_000, 300_000)

        assert store.get_coverage(KEY) == [(0, 60_000)]


class TestNonUtcTimezoneBypassesTheCache:
    """closed_boundary (and the whole coverage/gap-fill scheme) assumes UTC
    candle boundaries. Binance's `timeZone` param shifts candle boundaries
    for intervals >= 1d, which the coverage arithmetic does not account for
    at all — so a non-"0" timezone must never use the coverage cache
    (matching the existing "1M" bypass), or a still-forming shifted candle
    could be misclassified as closed and permanently cached mid-formation.
    """

    async def test_non_zero_timezone_never_short_circuits_via_coverage(self, respx_mock, tmp_path):
        # The real guarantee: a non-"0" timezone series never uses the
        # coverage-READ / gap-fill machinery to decide what's already cached
        # (that's the UTC-only-boundary-based logic that's unsafe here) — so
        # even an identical repeated request always calls Binance again,
        # unlike a "0" timezone range query which would short-circuit to
        # zero calls once covered. It MAY still opportunistically persist
        # verified-closed rows as a side effect (via Binance's own
        # per-row close_time, which is safe — see the test below) — that's
        # a bonus, not a violation of this guarantee.
        service, _store = make_service(tmp_path)
        key = SeriesKey(market=Market.SPOT, symbol="BTCUSDT", interval="1d", timezone="-08:00")
        route = respx_mock.get(KLINES_URL).mock(
            return_value=httpx.Response(200, json=[binance_row(0, interval_ms=86_400_000)])
        )

        await service.get_klines(key, start_time=0, end_time=None, limit=1, now_ms=10_000_000)
        await service.get_klines(key, start_time=0, end_time=None, limit=1, now_ms=10_000_000)

        assert route.call_count == 2  # never short-circuited by coverage
        assert route.calls[0].request.url.params["timeZone"] == "-08:00"

    async def test_still_forming_shifted_candle_is_never_permanently_cached(
        self, respx_mock, tmp_path
    ):
        # Concrete numbers where a naive UTC-only closed_boundary computation
        # misclassifies a genuinely still-forming (timezone-shifted) daily
        # candle as historical — see the derivation in the commit/PR notes.
        # If the coverage cache were used here (the bug), this candle would
        # be upserted and marked covered forever, even though it is still
        # actively updating on Binance's side.
        interval_ms = 86_400_000
        true_open = 144_000_000
        now_ms = 208_800_000
        true_close = true_open + interval_ms - 1  # 230_399_999, i.e. > now_ms: still open

        service, store = make_service(tmp_path)
        key = SeriesKey(market=Market.SPOT, symbol="BTCUSDT", interval="1d", timezone="-08:00")
        respx_mock.get(KLINES_URL).mock(
            return_value=httpx.Response(
                200,
                json=[
                    [
                        true_open, "1", "2", "0.5", "1.5", "10", true_close,
                        "15", 3, "1", "1", "0",
                    ]
                ],
            )
        )

        await service.get_klines(
            key, start_time=true_open, end_time=None, limit=1, now_ms=now_ms
        )

        assert store.get_coverage(key) == []
        assert store.get_klines(key, 0, 999_999_999) == []

    async def test_a_genuinely_closed_shifted_candle_is_still_safely_cached(
        self, respx_mock, tmp_path
    ):
        # The bypass disables the (unsafe) boundary-based gap-fill READ path,
        # not caching altogether: passthrough may still persist a row using
        # Binance's own close_time as ground truth, which correctly reflects
        # the real timezone-shifted boundary since Binance computed it.
        interval_ms = 86_400_000
        open_time = 0
        close_time = interval_ms - 1
        now_ms = close_time + 1  # exactly closed by the time of the call

        service, store = make_service(tmp_path)
        key = SeriesKey(market=Market.SPOT, symbol="BTCUSDT", interval="1d", timezone="-08:00")
        respx_mock.get(KLINES_URL).mock(
            return_value=httpx.Response(
                200,
                json=[[open_time, "1", "2", "0.5", "1.5", "10", close_time,
                       "15", 3, "1", "1", "0"]],
            )
        )

        await service.get_klines(key, start_time=0, end_time=None, limit=1, now_ms=now_ms)

        assert [k.open_time for k in store.get_klines(key, 0, 999_999_999)] == [open_time]


class TestPassthroughSharesTheSeriesLock:
    """A passthrough (tail/1M) fetch and a range-path gap-fill for the SAME
    series must not run concurrently — otherwise KlineStore.add_coverage's
    non-atomic read-merge-write can race and silently lose a previously
    written coverage range. Reproduced directly against KlineStore:
    concurrent add_coverage((0,100)) and add_coverage((200,300)) can leave
    only one of the two ranges behind (a classic lost update).
    """

    async def test_range_path_waits_for_an_in_flight_passthrough_on_same_series(
        self, respx_mock, tmp_path
    ):
        service, store = make_service(tmp_path)
        order: list[str] = []
        release_passthrough = asyncio.Event()
        entered_http_call = asyncio.Event()

        async def slow_passthrough_response(request):
            order.append("passthrough-http-called")
            entered_http_call.set()
            await release_passthrough.wait()
            return httpx.Response(200, json=[binance_row(0)])

        respx_mock.get(KLINES_URL).mock(side_effect=slow_passthrough_response)

        real_get_coverage = store.get_coverage

        def spying_get_coverage(key):
            order.append("range-path-entered-critical-section")
            return real_get_coverage(key)

        store.get_coverage = spying_get_coverage  # type: ignore[method-assign]

        passthrough_task = asyncio.create_task(
            service.get_klines(KEY, start_time=None, end_time=None, limit=1, now_ms=10_000_000)
        )
        await entered_http_call.wait()  # passthrough is now blocked mid-flight

        range_task = asyncio.create_task(
            service.get_klines(KEY, start_time=0, end_time=None, limit=1, now_ms=10_000_000)
        )
        # Give the range-path task every opportunity to run ahead if nothing
        # is actually blocking it (this is the failure mode being tested).
        for _ in range(5):
            await asyncio.sleep(0)

        released_at = len(order)
        release_passthrough.set()
        await asyncio.gather(passthrough_task, range_task)

        # If passthrough correctly holds series_lock for its whole
        # do_work (fix applied), the range path's get_coverage call — the
        # first statement inside its own series_lock acquisition — cannot
        # have run before release_passthrough.set() was called.
        range_path_index = order.index("range-path-entered-critical-section")
        assert range_path_index >= released_at, (
            "range-path critical section ran while an unrelated passthrough "
            "fetch for the same series was still in flight — series_lock is "
            "not actually serializing them"
        )


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
