"""GET /healthz (liveness) and GET /stats (operational visibility).

/stats is how you confirm the cache is actually working in production:
cache hit/miss counts, coalescing counts, and per-market breaker/weight
state.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {"status": "ok"}


@router.get("/stats")
async def stats(request: Request) -> dict[str, Any]:
    service = request.app.state.service
    limiters = request.app.state.rate_limiters
    clients = service.clients

    return {
        "cache": {
            "entries": len(service.cache),
            "hits": service.cache.hits,
            "misses": service.cache.misses,
        },
        "coalescing": {
            "calls_started": service.coalescer.calls_started,
            "calls_joined": service.coalescer.calls_joined,
        },
        "markets": {
            market.value: {
                "upstream_calls_made": clients[market].calls_made,
                "used_weight": limiters[market].used_weight(),
                "banned": limiters[market].is_banned(),
                "seconds_until_unbanned": limiters[market].seconds_until_unbanned(),
            }
            for market in limiters
        },
    }
