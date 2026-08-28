"""Request orchestration: cache check -> coalesce -> fetch -> cache -> respond.

Deliberately simple: no historical range logic, no persistence. A cache
entry is just "the exact response Binance gave us for this exact request,
less than TTL seconds ago." A cache miss means asking Binance again, same
as if there were no cache at all — just less often.

Every response Binance gives us for a request that completes (i.e. that
UpstreamClient.fetch() returns rather than raises — see its docstring: it
raises rather than returning for 429/418 and for transport/parse failures)
is cached, including 4xx/5xx errors — not just 200s. This is deliberate: a
caller retrying a permanently-invalid request (e.g. a symbol that doesn't
exist on a given market) would otherwise burn a fresh upstream call on
every single attempt forever, since the error never changes. Confirmed as
a real, measurable contributor to a production ban — see CLAUDE.md
invariant #2 for the incident this traces back to. The accepted tradeoff:
a genuinely transient error can be replayed from cache for up to
CACHE_TTL_SECONDS.
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
            self.cache.set(key, status_code, body)
            return status_code, body

        return await self.coalescer.coalesce(key, do_work)
