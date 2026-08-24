"""Concurrency control that turns a stampede of duplicate requests into one.

Two layers, used together by the service layer:

- Layer A (`coalesce`): exact-request single-flight. Concurrent callers using
  the identical key share one execution of `work` and its result/exception.
  This is the direct fix for "N desks fire the same request at once".
- Layer B (`series_lock`): a per-series `asyncio.Lock`, used to serialize the
  gap-fill critical section even for *overlapping-but-not-identical*
  requests on the same (market, symbol, interval, timezone) series.

Known limitation: if the task currently running `work()` for a key is
cancelled (e.g. the initiating client disconnects), that cancellation
propagates to every other caller coalesced onto the same key, even ones that
were never themselves cancelled. Acceptable for this proxy's usage pattern;
would need `asyncio.shield`-based supervision to fully decouple.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Hashable
from typing import Any, TypeVar

T = TypeVar("T")


class Coalescer:
    def __init__(self) -> None:
        # Holds futures of whatever type each individual coalesce() call
        # produces; Any here (not T) because one Coalescer instance is
        # shared across calls with different result types.
        self._inflight: dict[Hashable, asyncio.Future[Any]] = {}
        self._series_locks: dict[Hashable, asyncio.Lock] = {}
        # Observability only (exposed via /stats): how often a caller had to
        # do the work itself vs. got to ride along on an in-flight call.
        self.calls_started = 0
        self.calls_joined = 0

    async def coalesce(self, key: Hashable, work: Callable[[], Awaitable[T]]) -> T:
        existing = self._inflight.get(key)
        if existing is not None:
            self.calls_joined += 1
            result: T = await existing
            return result

        self.calls_started += 1
        future: asyncio.Future[T] = asyncio.get_running_loop().create_future()
        self._inflight[key] = future
        try:
            result = await work()
        except BaseException as exc:  # noqa: BLE001 - deliberately propagate any failure
            future.set_exception(exc)
        else:
            future.set_result(result)
        finally:
            del self._inflight[key]

        return await future

    def series_lock(self, series_key: Hashable) -> asyncio.Lock:
        lock = self._series_locks.get(series_key)
        if lock is None:
            lock = asyncio.Lock()
            self._series_locks[series_key] = lock
        return lock
