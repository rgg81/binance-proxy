"""FastAPI application factory: wires config -> store -> coalescer ->
upstream clients/rate limiters -> service -> routes.

Intentionally a single process (see CLAUDE.md) — request coalescing depends
on shared in-memory state, so run this with a single uvicorn worker.
"""

from __future__ import annotations

import asyncio
import time

import httpx
from fastapi import FastAPI

from binance_proxy.cache.store import KlineStore
from binance_proxy.coalescing import Coalescer
from binance_proxy.config import Settings
from binance_proxy.config import settings as default_settings
from binance_proxy.models import Market
from binance_proxy.routes import health, klines
from binance_proxy.service import KlineService
from binance_proxy.upstream.client import UpstreamClient
from binance_proxy.upstream.rate_limiter import RateLimiter


def create_app(settings: Settings = default_settings) -> FastAPI:
    app = FastAPI(title="binance-proxy", version="0.1.0")

    store = KlineStore(settings.db_path)
    coalescer = Coalescer()

    spot_limiter = RateLimiter(
        budget_per_window=settings.spot_weight_budget_per_minute,
        window_seconds=60.0,
        safety_margin=settings.rate_limit_safety_margin,
        now_fn=time.monotonic,
        sleep_fn=asyncio.sleep,
    )
    futures_limiter = RateLimiter(
        budget_per_window=settings.futures_weight_budget_per_minute,
        window_seconds=60.0,
        safety_margin=settings.rate_limit_safety_margin,
        now_fn=time.monotonic,
        sleep_fn=asyncio.sleep,
    )

    clients = {
        Market.SPOT: UpstreamClient(
            httpx.AsyncClient(base_url=settings.spot_base_url),
            spot_limiter,
            "/api/v3/klines",
        ),
        Market.USDM_FUTURES: UpstreamClient(
            httpx.AsyncClient(base_url=settings.futures_base_url),
            futures_limiter,
            "/fapi/v1/klines",
        ),
    }

    service = KlineService(store=store, coalescer=coalescer, clients=clients)

    app.state.service = service
    app.state.rate_limiters = {Market.SPOT: spot_limiter, Market.USDM_FUTURES: futures_limiter}

    app.include_router(klines.router)
    app.include_router(health.router)

    return app


app = create_app()
