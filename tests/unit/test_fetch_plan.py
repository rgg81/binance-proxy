"""Unit tests for plan_fetch: the pure function deciding, for a range query,
which parts can come straight from cache, which historical gaps need
fetching, and whether a live call is needed for the still-forming candle.

No I/O here — coverage is passed in as plain data, "now" is injected.
"""

from binance_proxy.service import plan_fetch

INTERVAL_MS = 60_000  # 1m


class TestPlanFetch:
    def test_fully_historical_fully_cached_range_needs_no_fetch_at_all(self):
        # start=0, limit=10 -> theoretical end = 600_000, well before "now".
        plan = plan_fetch(
            start=0,
            end=None,
            limit=10,
            interval_ms=INTERVAL_MS,
            coverage=[(0, 600_000)],
            now_ms=10_000_000,
        )
        assert plan.historical_gaps == []
        assert plan.needs_live_tail is False
        assert plan.cache_read_range == (0, 600_000)

    def test_fully_historical_uncached_range_is_a_single_gap(self):
        plan = plan_fetch(
            start=0,
            end=None,
            limit=10,
            interval_ms=INTERVAL_MS,
            coverage=[],
            now_ms=10_000_000,
        )
        assert plan.historical_gaps == [(0, 600_000)]
        assert plan.needs_live_tail is False

    def test_partially_cached_range_only_fetches_the_gap(self):
        plan = plan_fetch(
            start=0,
            end=None,
            limit=10,
            interval_ms=INTERVAL_MS,
            coverage=[(0, 300_000)],
            now_ms=10_000_000,
        )
        assert plan.historical_gaps == [(300_000, 600_000)]

    def test_end_time_further_clips_the_window(self):
        # limit would allow up to 600_000, but explicit end=200_000 wins.
        plan = plan_fetch(
            start=0,
            end=200_000,
            limit=10,
            interval_ms=INTERVAL_MS,
            coverage=[],
            now_ms=10_000_000,
        )
        assert plan.historical_gaps == [(0, 200_000)]
        assert plan.cache_read_range == (0, 200_000)

    def test_range_reaching_the_open_candle_needs_a_live_tail(self):
        now = 1_000_090_000  # currently-open candle opened at 1_000_080_000
        closed_boundary = 1_000_080_000
        plan = plan_fetch(
            start=1_000_000_000,
            end=None,
            limit=10,  # theoretical end = start + 600_000 > closed_boundary
            interval_ms=INTERVAL_MS,
            coverage=[],
            now_ms=now,
        )
        assert plan.needs_live_tail is True
        assert plan.live_tail_range == (closed_boundary, 1_000_000_000 + 600_000)
        assert plan.cache_read_range == (1_000_000_000, closed_boundary)
        assert plan.historical_gaps == [(1_000_000_000, closed_boundary)]

    def test_start_time_in_the_future_does_not_fetch_the_current_candle(self):
        # Real Binance returns [] for a startTime beyond "now" — it must
        # never be answered with the currently-forming candle instead.
        now = 1_000_000_030_000
        future_start = 1_000_000_100_000  # well past closed_boundary (1_000_000_020_000)
        plan = plan_fetch(
            start=future_start,
            end=None,
            limit=5,
            interval_ms=INTERVAL_MS,
            coverage=[],
            now_ms=now,
        )
        assert plan.needs_live_tail is True
        assert plan.live_tail_range == (future_start, future_start + 5 * INTERVAL_MS)
        assert plan.historical_gaps == []

    def test_range_entirely_within_the_open_candle_has_no_historical_part(self):
        now = 1_000_000_030_000
        closed_boundary = 1_000_000_020_000
        # both start and theoretical end fall inside the still-open candle
        plan = plan_fetch(
            start=1_000_000_025_000,
            end=None,
            limit=1,
            interval_ms=INTERVAL_MS,
            coverage=[],
            now_ms=now,
        )
        assert plan.needs_live_tail is True
        assert plan.cache_read_range == (1_000_000_025_000, closed_boundary)
        assert plan.historical_gaps == []
        # live_tail_range starts at `start` itself (not closed_boundary),
        # since start already falls after closed_boundary here.
        assert plan.live_tail_range == (1_000_000_025_000, 1_000_000_025_000 + INTERVAL_MS)
