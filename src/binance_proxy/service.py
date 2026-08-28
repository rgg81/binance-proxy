"""Request orchestration: cache check -> coalesce -> fetch -> cache -> respond.

Deliberately simple: no historical range logic, no persistence. A cache
entry is just "the exact response Binance gave us for this exact request,
less than TTL seconds ago." A cache miss means asking Binance again, same
as if there were no cache at all — just less often.

A 200 or 4xx response is cached; a 5xx is not. UpstreamClient.fetch()
already raises rather than returning for 429/418 and for transport/parse
failures (see its docstring), so anything reaching do_work() is a
completed round trip. Within that: a 4xx means the *request itself* is
invalid — deterministic, since the same bad params always get the same
answer — so caching it stops a caller retrying a permanently-invalid
request (e.g. a symbol that doesn't exist on a given market) from burning
a fresh upstream call on every single attempt forever. Confirmed as a
real, measurable contributor to a production ban — see CLAUDE.md
invariant #2. A 5xx means *Binance's own state* at that moment, which
isn't deterministic and could resolve moments later — caching it would
silently mask a real Binance-side outage from every caller hitting that
key for up to CACHE_TTL_SECONDS, a cost the 4xx case's reasoning doesn't
apply to and doesn't justify. The accepted tradeoff for the 4xx case: a
genuinely transient 4xx can be replayed from cache for up to
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
            if status_code < 500:
                self.cache.set(key, status_code, body)
            return status_code, body

        return await self.coalescer.coalesce(key, do_work)
