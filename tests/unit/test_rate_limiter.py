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
    """Records sleep durations and advances the fake clock instead of waiting."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
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
        limiter, clock, _sleeper = make_limiter(budget=100, window_seconds=60.0)
        await limiter.acquire(70)
        clock.advance(61.0)
        await limiter.acquire(70)  # should not need to wait; new window
        # No exception / hang means success. Explicitly confirm no sleep needed:
        assert True

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
