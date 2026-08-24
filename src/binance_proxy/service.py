"""Request orchestration: normalize -> coalesce -> gap-fill -> merge -> respond.

`plan_fetch` is the pure decision core (no I/O, fully unit-tested). The rest
of this module wires it to the store, upstream client, and coalescing layer.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from binance_proxy.cache.coverage import Range, subtract_ranges
from binance_proxy.cache.store import KlineStore
from binance_proxy.coalescing import Coalescer
from binance_proxy.intervals import interval_to_ms
from binance_proxy.models import Kline, Market, SeriesKey
from binance_proxy.upstream.client import UpstreamClient


@dataclass(frozen=True, slots=True)
class FetchPlan:
    # Sub-ranges of the historical (closed-candle) portion of the request
    # that are missing from the cache and must be fetched from Binance.
    historical_gaps: list[Range]

    # Whether the request's window reaches into or past the currently-open
    # candle, requiring a live (always-fetch, never cache-satisfied) call.
    needs_live_tail: bool

    # The [closed_boundary, effective_end) portion to fetch live, if any.
    live_tail_range: Range | None

    # The range to read back from the cache once historical_gaps are filled.
    cache_read_range: Range


def plan_fetch(
    *,
    start: int,
    end: int | None,
    limit: int,
    interval_ms: int,
    coverage: list[Range],
    now_ms: int,
) -> FetchPlan:
    """Decide what needs fetching for a range query starting at `start`.

    Only called for requests that have an explicit `start` — open-ended
    "tail" requests (no startTime) always take the live passthrough path
    and never call this function.
    """
    theoretical_end = start + limit * interval_ms
    effective_end = min(end, theoretical_end) if end is not None else theoretical_end

    closed_boundary = (now_ms // interval_ms) * interval_ms
    historical_end = min(effective_end, closed_boundary)

    historical_gaps = subtract_ranges((start, historical_end), coverage)
    needs_live_tail = effective_end > closed_boundary
    live_tail_range = (closed_boundary, effective_end) if needs_live_tail else None

    return FetchPlan(
        historical_gaps=historical_gaps,
        needs_live_tail=needs_live_tail,
        live_tail_range=live_tail_range,
        cache_read_range=(start, historical_end),
    )


@dataclass
class KlineService:
    """Orchestrates a klines request: coalesce -> gap-fill -> merge -> respond.

    `clients` maps each supported Market to the UpstreamClient that talks to
    that market's Binance base URL.
    """

    store: KlineStore
    coalescer: Coalescer
    clients: dict[Market, UpstreamClient]

    async def get_klines(
        self,
        key: SeriesKey,
        *,
        start_time: int | None,
        end_time: int | None,
        limit: int,
        now_ms: int,
    ) -> list[list[int | str]]:
        # Open-ended "tail" requests and the variable-length "1M" interval
        # can't use the fixed-duration coverage/gap-fill machinery — they
        # always take the always-live (but still coalesced) passthrough path.
        if start_time is None or key.interval == "1M":
            return await self._fetch_passthrough(
                key, start_time=start_time, end_time=end_time, limit=limit, now_ms=now_ms
            )

        interval_ms = interval_to_ms(key.interval)
        coalesce_key = (key, start_time, end_time, limit, "range")

        async def do_work() -> list[list[int | str]]:
            async with self.coalescer.series_lock(key):
                coverage = await asyncio.to_thread(self.store.get_coverage, key)
                plan = plan_fetch(
                    start=start_time,
                    end=end_time,
                    limit=limit,
                    interval_ms=interval_ms,
                    coverage=coverage,
                    now_ms=now_ms,
                )

                for gap_start, gap_end in plan.historical_gaps:
                    await self._fill_gap(key, gap_start, gap_end, interval_ms)

                read_start, read_end = plan.cache_read_range
                historical = (
                    await asyncio.to_thread(self.store.get_klines, key, read_start, read_end)
                    if read_end > read_start
                    else []
                )

                live_rows: list[Kline] = []
                if plan.needs_live_tail:
                    assert plan.live_tail_range is not None
                    live_rows = await self._fetch_live_tail(
                        key, plan.live_tail_range, now_ms
                    )

                merged = _merge_by_open_time(historical, live_rows)
                return [k.to_binance_row() for k in merged[:limit]]

        return await self.coalescer.coalesce(coalesce_key, do_work)

    async def _fill_gap(self, key: SeriesKey, start: int, end: int, interval_ms: int) -> None:
        rows = await self._call_binance(key, start, end)
        if rows:
            await asyncio.to_thread(self.store.upsert_klines, key, rows)
        await asyncio.to_thread(self.store.add_coverage, key, (start, end))

    async def _fetch_live_tail(
        self, key: SeriesKey, live_range: Range, now_ms: int
    ) -> list[Kline]:
        start, end = live_range
        rows = await self._call_binance(key, start, end)
        closed = [r for r in rows if r.close_time < now_ms]
        if closed:
            await asyncio.to_thread(self.store.upsert_klines, key, closed)
            await asyncio.to_thread(
                self.store.add_coverage, key, (start, closed[-1].close_time + 1)
            )
        return rows

    async def _call_binance(self, key: SeriesKey, start: int, end: int) -> list[Kline]:
        interval_ms = interval_to_ms(key.interval)
        count = max(1, min(1000, (end - start + interval_ms - 1) // interval_ms))
        params = {
            "symbol": key.symbol,
            "interval": key.interval,
            "timeZone": key.timezone,
            "startTime": start,
            "endTime": end - 1,
            "limit": count,
        }
        raw_rows = await self.clients[key.market].fetch_klines(params)
        return [Kline.from_binance_row(row) for row in raw_rows]

    async def _fetch_passthrough(
        self,
        key: SeriesKey,
        *,
        start_time: int | None,
        end_time: int | None,
        limit: int,
        now_ms: int,
    ) -> list[list[int | str]]:
        params = {
            "symbol": key.symbol,
            "interval": key.interval,
            "timeZone": key.timezone,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        coalesce_key = (key, start_time, end_time, limit, "passthrough")

        async def do_work() -> list[list[int | str]]:
            raw_rows = await self.clients[key.market].fetch_klines(params)
            rows = [Kline.from_binance_row(row) for row in raw_rows]

            if key.interval != "1M":
                closed = [r for r in rows if r.close_time < now_ms]
                if closed:
                    await asyncio.to_thread(self.store.upsert_klines, key, closed)
                    await asyncio.to_thread(
                        self.store.add_coverage,
                        key,
                        (closed[0].open_time, closed[-1].close_time + 1),
                    )

            return [r.to_binance_row() for r in rows]

        return await self.coalescer.coalesce(coalesce_key, do_work)


def _merge_by_open_time(*groups: list[Kline]) -> list[Kline]:
    by_open_time: dict[int, Kline] = {}
    for group in groups:
        for kline in group:
            by_open_time[kline.open_time] = kline
    return [by_open_time[t] for t in sorted(by_open_time)]
