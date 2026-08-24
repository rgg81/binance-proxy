"""GET /api/v3/klines and GET /fapi/v1/klines — same path, query params, and
response shape as Binance itself. Desks adopt this proxy by repointing their
base URL only.

No parameter parsing or validation here beyond reading the raw query
string: whatever params the caller sent are forwarded to Binance verbatim,
and whatever Binance says back — success or error — is returned verbatim.
Binance is the source of truth for what's a valid request.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from binance_proxy.models import Market
from binance_proxy.upstream.client import RateLimitedError, UpstreamUnavailableError

router = APIRouter()


async def _handle_klines(request: Request, market: Market, path: str) -> JSONResponse:
    service = request.app.state.service
    params = dict(request.query_params)

    try:
        status_code, body = await service.get(market, path, params)
    except RateLimitedError as exc:
        retry_after = max(1, int(exc.retry_after))
        return JSONResponse(
            status_code=503,
            content={
                "code": -1003,
                "msg": "binance-proxy is backing off from Binance to avoid a "
                "rate-limit ban; please retry later.",
            },
            headers={"Retry-After": str(retry_after)},
        )
    except UpstreamUnavailableError as exc:
        return JSONResponse(
            status_code=503,
            content={
                "code": -1001,
                "msg": f"binance-proxy could not reach Binance: {exc.reason}",
            },
            headers={"Retry-After": "2"},
        )

    return JSONResponse(status_code=status_code, content=body)


@router.get("/api/v3/klines")
async def spot_klines(request: Request) -> JSONResponse:
    return await _handle_klines(request, Market.SPOT, "/api/v3/klines")


@router.get("/fapi/v1/klines")
async def futures_klines(request: Request) -> JSONResponse:
    return await _handle_klines(request, Market.USDM_FUTURES, "/fapi/v1/klines")
