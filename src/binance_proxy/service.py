"""Request orchestration: cache check -> coalesce -> fetch -> cache -> respond.

Deliberately simple: no historical range logic, no persistence. A cache
entry is just "the exact response Binance gave us for this exact request,
less than TTL seconds ago." A cache miss means asking Binance again, same
as if there were no cache at all — just less often.
"""

from __future__ import annotations

from dataclasses import dataclass

from binance_proxy.cache import TTLCache
from binance_proxy.coalescing import Coalescer
from binance_proxy.models import Market
from binance_proxy.upstream.client import UpstreamClient


@dataclass
class ProxyService:
    cache: TTLCache
    coalescer: Coalescer
    clients: dict[Market, UpstreamClient]

    async def get(
        self, market: Market, path: str, params: dict[str, str]
    ) -> tuple[int, object]:
        key = (market, path, tuple(sorted(params.items())))

        cached = self.cache.get(key)
        if cached is not None:
            return cached.status_code, cached.body

        async def do_work() -> tuple[int, object]:
            status_code, body = await self.clients[market].fetch(path, params)
            if status_code == 200:
                self.cache.set(key, status_code, body)
            return status_code, body

        return await self.coalescer.coalesce(key, do_work)
