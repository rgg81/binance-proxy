"""FastAPI application factory: wires config -> cache -> coalescer ->
upstream clients/rate limiters -> service -> routes.

Intentionally a single process (see CLAUDE.md) — the cache and request
coalescing both depend on shared in-memory state, so run this with a
single uvicorn worker.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from binance_proxy.cache import TTLCache
from binance_proxy.coalescing import Coalescer
from binance_proxy.config import Settings
from binance_proxy.config import settings as default_settings
from binance_proxy.models import Market
from binance_proxy.routes import health, klines
from binance_proxy.service import ProxyService
from binance_proxy.upstream.client import UpstreamClient
from binance_proxy.upstream.rate_limiter import RateLimiter


def create_app(settings: Settings = default_settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        # Close the upstream httpx clients on shutdown so connection pools
        # don't leak sockets/file descriptors across restarts/reloads.
        await asyncio.gather(*(client.close() for client in app.state.service.clients.values()))

    app = FastAPI(title="binance-proxy", version="0.2.0", lifespan=lifespan)

    cache = TTLCache(
        ttl_seconds=settings.cache_ttl_seconds, max_entries=settings.cache_max_entries
    )
    coalescer = Coalescer()

    spot_limiter = RateLimiter(
        budget_per_window=settings.spot_weight_budget_per_minute,
        window_seconds=60.0,
        safety_margin=settings.rate_limit_safety_margin,
        now_fn=time.monotonic,
        sleep_fn=asyncio.sleep,
        market=Market.SPOT.value,
    )
    futures_limiter = RateLimiter(
        budget_per_window=settings.futures_weight_budget_per_minute,
        window_seconds=60.0,
        safety_margin=settings.rate_limit_safety_margin,
        market=Market.USDM_FUTURES.value,
        now_fn=time.monotonic,
        sleep_fn=asyncio.sleep,
    )

    clients = {
        Market.SPOT: UpstreamClient(
            httpx.AsyncClient(base_url=settings.spot_base_url), spot_limiter
        ),
        Market.USDM_FUTURES: UpstreamClient(
            httpx.AsyncClient(base_url=settings.futures_base_url), futures_limiter
        ),
    }

    service = ProxyService(cache=cache, coalescer=coalescer, clients=clients)

    app.state.service = service
    app.state.rate_limiters = {Market.SPOT: spot_limiter, Market.USDM_FUTURES: futures_limiter}

    app.include_router(klines.router)
    app.include_router(health.router)

    return app


app = create_app()
