"""Unit tests for Coalescer: exact-request single-flight (Layer A) and
per-series locking (Layer B). These are the mechanisms that directly satisfy
"only one request reaches Binance when many identical ones arrive at once".
"""

import asyncio

import pytest

from binance_proxy.coalescing import Coalescer


class TestSingleFlightCoalescing:
    async def test_concurrent_identical_calls_run_work_only_once(self):
        coalescer = Coalescer()
        call_count = 0
        started = asyncio.Event()
        release = asyncio.Event()

        async def work():
            nonlocal call_count
            call_count += 1
            started.set()
            await release.wait()
            return "result"

        task_a = asyncio.create_task(coalescer.coalesce("key", work))
        await started.wait()
        task_b = asyncio.create_task(coalescer.coalesce("key", work))
        await asyncio.sleep(0)  # let task_b reach the coalescing dict lookup

        release.set()
        result_a, result_b = await asyncio.gather(task_a, task_b)

        assert call_count == 1
        assert result_a == "result"
        assert result_b == "result"

    async def test_different_keys_each_run_their_own_work(self):
        coalescer = Coalescer()
        calls = []

        async def work_for(key):
            calls.append(key)
            return key

        await asyncio.gather(
            coalescer.coalesce("a", lambda: work_for("a")),
            coalescer.coalesce("b", lambda: work_for("b")),
        )
        assert sorted(calls) == ["a", "b"]

    async def test_sequential_calls_after_completion_each_run_work_again(self):
        # Coalescing only applies to *concurrent* overlap, not permanent
        # memoization — a call made after the prior one finished is fresh.
        coalescer = Coalescer()
        call_count = 0

        async def work():
            nonlocal call_count
            call_count += 1
            return call_count

        first = await coalescer.coalesce("key", work)
        second = await coalescer.coalesce("key", work)
        assert (first, second) == (1, 2)

    async def test_concurrent_callers_all_receive_the_same_exception(self):
        coalescer = Coalescer()
        call_count = 0
        started = asyncio.Event()
        release = asyncio.Event()

        async def failing_work():
            nonlocal call_count
            call_count += 1
            started.set()
            await release.wait()
            raise ValueError("upstream boom")

        task_a = asyncio.create_task(coalescer.coalesce("key", failing_work))
        await started.wait()
        task_b = asyncio.create_task(coalescer.coalesce("key", failing_work))
        await asyncio.sleep(0)

        release.set()
        with pytest.raises(ValueError, match="upstream boom"):
            await task_a
        with pytest.raises(ValueError, match="upstream boom"):
            await task_b

        assert call_count == 1


class TestCoalescingStats:
    async def test_tracks_how_many_calls_started_work_vs_joined_an_inflight_call(self):
        coalescer = Coalescer()
        started = asyncio.Event()
        release = asyncio.Event()

        async def work():
            started.set()
            await release.wait()
            return "result"

        task_a = asyncio.create_task(coalescer.coalesce("key", work))
        await started.wait()
        task_b = asyncio.create_task(coalescer.coalesce("key", work))
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(task_a, task_b)

        assert coalescer.calls_started == 1
        assert coalescer.calls_joined == 1


class TestPerSeriesLock:
    async def test_same_series_key_returns_the_same_lock(self):
        coalescer = Coalescer()
        assert coalescer.series_lock("BTCUSDT:1m") is coalescer.series_lock("BTCUSDT:1m")

    async def test_different_series_keys_return_different_locks(self):
        coalescer = Coalescer()
        assert coalescer.series_lock("BTCUSDT:1m") is not coalescer.series_lock("ETHUSDT:1m")

    async def test_lock_actually_serializes_critical_sections(self):
        coalescer = Coalescer()
        order = []

        async def critical_section(name, hold_seconds):
            async with coalescer.series_lock("series"):
                order.append(f"{name}-start")
                await asyncio.sleep(hold_seconds)
                order.append(f"{name}-end")

        await asyncio.gather(
            critical_section("first", 0.02),
            critical_section("second", 0.0),
        )
        # "second" must wait for "first" to fully release the lock before
        # starting, even though it has nothing to await itself.
        assert order == ["first-start", "first-end", "second-start", "second-end"]
