"""Unit tests for RateLimiter: proactive weight throttling + circuit breaker.

Clock and sleep are injected so tests run instantly and deterministically —
no real waiting, no flakiness.
"""

import pytest

from binance_proxy.upstream.rate_limiter import RateLimiter


class FakeClock:
    """A controllable monotonic clock: advances only when told to."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeSleeper:
    """Records sleep durations and advances the fake clock instead of waiting.

    Guards against a genuine infinite retry loop in the code under test: since
    this never actually blocks in real time, a runaway `while True` loop would
    otherwise spin as fast as the interpreter allows rather than hang
    predictably — which can starve the whole event loop badly enough that
    even `asyncio.wait_for`'s own timeout struggles to interrupt it. Failing
    fast with a clear error after a generous call count is far safer than
    relying on a wall-clock timeout to catch this class of bug.
    """

    def __init__(self, clock: FakeClock, max_calls: int = 1000) -> None:
        self.clock = clock
        self.calls: list[float] = []
        self._max_calls = max_calls

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        if len(self.calls) > self._max_calls:
            raise RuntimeError(
                f"sleep_fn called {len(self.calls)} times — likely an infinite retry loop"
            )
        self.clock.advance(seconds)


def make_limiter(*, budget=100, window_seconds=60.0, safety_margin=0.8, clock=None):
    clock = clock or FakeClock()
    sleeper = FakeSleeper(clock)
    limiter = RateLimiter(
        budget_per_window=budget,
        window_seconds=window_seconds,
        safety_margin=safety_margin,
        now_fn=clock,
        sleep_fn=sleeper,
    )
    return limiter, clock, sleeper


class TestProactiveWeightThrottling:
    async def test_acquire_succeeds_immediately_when_under_budget(self):
        limiter, _clock, sleeper = make_limiter(budget=100, safety_margin=0.8)
        await limiter.acquire(10)
        assert sleeper.calls == []

    async def test_acquire_waits_when_it_would_cross_the_safety_margin(self):
        # budget=100, safety_margin=0.8 -> usable = 80.
        limiter, clock, sleeper = make_limiter(
            budget=100, window_seconds=60.0, safety_margin=0.8
        )
        await limiter.acquire(75)  # used=75, under 80
        await limiter.acquire(10)  # 75+10=85 > 80 -> must wait for window reset
        assert sleeper.calls  # slept at least once
        assert clock.now >= 60.0  # window rolled over

    async def test_weight_resets_after_the_window_elapses(self):
        limiter, clock, sleeper = make_limiter(budget=100, window_seconds=60.0)
        await limiter.acquire(70)
        clock.advance(61.0)
        await limiter.acquire(70)  # should not need to wait; new window
        assert sleeper.calls == []

    async def test_used_weight_from_header_is_reconciled_upward(self):
        limiter, _clock, sleeper = make_limiter(budget=100, safety_margin=0.8)
        await limiter.acquire(5)  # local estimate: used=5
        limiter.on_response(200, {"X-MBX-USED-WEIGHT-1M": "79"})
        # Now our tracked usage should reflect the server's higher truth (79),
        # so a further request of 5 (79+5=84 > 80) must wait.
        await limiter.acquire(5)
        assert sleeper.calls


class TestCircuitBreaker:
    async def test_429_response_opens_the_breaker_for_retry_after_duration(self):
        limiter, clock, sleeper = make_limiter()
        limiter.on_response(429, {"Retry-After": "30"})
        assert limiter.is_banned() is True
        await limiter.acquire(1)
        assert sleeper.calls[0] == pytest.approx(30.0)
        assert limiter.is_banned() is False

    async def test_418_response_opens_the_breaker_for_retry_after_duration(self):
        limiter, clock, sleeper = make_limiter()
        limiter.on_response(418, {"Retry-After": "120"})
        assert limiter.is_banned() is True
        await limiter.acquire(1)
        assert sleeper.calls[0] == pytest.approx(120.0)

    async def test_418_without_retry_after_header_uses_a_conservative_default(self):
        limiter, _clock, _sleeper = make_limiter()
        limiter.on_response(418, {})
        assert limiter.is_banned() is True
        assert limiter.seconds_until_unbanned() > 0

    async def test_successful_response_does_not_trip_the_breaker(self):
        limiter, _clock, _sleeper = make_limiter()
        limiter.on_response(200, {"X-MBX-USED-WEIGHT-1M": "5"})
        assert limiter.is_banned() is False


class TestAcquireNeverDeadlocksOnAnUnsatisfiableWeight:
    """If a single request's weight exceeds the entire usable budget, no
    amount of waiting for a window reset ever makes `_used_weight + weight
    <= usable_budget` true — the naive retry loop would spin forever. This
    is reachable via ordinary env-var configuration (a low safety_margin),
    not just a contrived unit test setup.
    """

    async def test_acquire_proceeds_rather_than_looping_forever(self):
        # usable_budget = 100 * 0.05 = 5, but the request itself weighs 10 —
        # unsatisfiable by construction, even in a freshly-reset window.
        # FakeSleeper's own call-count guard (see its docstring) is what
        # actually catches a regression here, not a wall-clock timeout.
        limiter, _clock, sleeper = make_limiter(budget=100, safety_margin=0.05)

        await limiter.acquire(10)

        assert len(sleeper.calls) <= 1

    async def test_still_throttles_normally_when_the_weight_alone_fits(self):
        # Sanity check the fix doesn't disable normal throttling: budget=100,
        # safety_margin=0.8 -> usable=80, single weight of 10 fits fine, but
        # 75+10 doesn't, so this must still wait for the window to roll over.
        limiter, clock, sleeper = make_limiter(budget=100, safety_margin=0.8)
        await limiter.acquire(75)
        await limiter.acquire(10)
        assert sleeper.calls
        assert clock.now >= 60.0


class TestUsedWeightVisibility:
    async def test_used_weight_starts_at_zero(self):
        limiter, _clock, _sleeper = make_limiter()
        assert limiter.used_weight() == 0

    async def test_used_weight_reflects_local_reservations(self):
        limiter, _clock, _sleeper = make_limiter()
        await limiter.acquire(7)
        assert limiter.used_weight() == 7

    async def test_used_weight_reflects_header_reconciliation(self):
        limiter, _clock, _sleeper = make_limiter()
        limiter.on_response(200, {"X-MBX-USED-WEIGHT-1M": "42"})
        assert limiter.used_weight() == 42
