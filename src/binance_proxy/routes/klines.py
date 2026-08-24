"""GET /api/v3/klines and GET /fapi/v1/klines — same path, query params, and
response shape as Binance itself. Desks adopt this proxy by repointing their
base URL only.
"""

from __future__ import annotations

import time

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from binance_proxy.models import Market, SeriesKey
from binance_proxy.upstream.client import BinanceApiError, RateLimitedError

router = APIRouter()

_MIN_LIMIT = 1
_MAX_LIMIT = 1000
_DEFAULT_LIMIT = 500


def _now_ms() -> int:
    return int(time.time() * 1000)


async def _handle_klines(
    request: Request,
    market: Market,
    symbol: str,
    interval: str,
    start_time: int | None,
    end_time: int | None,
    time_zone: str,
    limit: int,
) -> JSONResponse:
    if not (_MIN_LIMIT <= limit <= _MAX_LIMIT):
        return JSONResponse(
            status_code=400,
            content={
                "code": -1130,
                "msg": f"Data sent for parameter 'limit' is not valid. "
                f"Must be between {_MIN_LIMIT} and {_MAX_LIMIT}.",
            },
        )

    service = request.app.state.service
    key = SeriesKey(market=market, symbol=symbol, interval=interval, timezone=time_zone)

    try:
        rows = await service.get_klines(
            key,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            now_ms=_now_ms(),
        )
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
    except BinanceApiError as exc:
        return JSONResponse(status_code=exc.status_code, content=exc.body)

    return JSONResponse(content=rows)


@router.get("/api/v3/klines")
async def spot_klines(
    request: Request,
    symbol: str,
    interval: str,
    startTime: int | None = None,  # noqa: N803 - mirrors Binance's own param casing
    endTime: int | None = None,  # noqa: N803
    timeZone: str = "0",  # noqa: N803
    limit: int = Query(default=_DEFAULT_LIMIT),
) -> JSONResponse:
    return await _handle_klines(
        request, Market.SPOT, symbol, interval, startTime, endTime, timeZone, limit
    )


@router.get("/fapi/v1/klines")
async def futures_klines(
    request: Request,
    symbol: str,
    interval: str,
    startTime: int | None = None,  # noqa: N803
    endTime: int | None = None,  # noqa: N803
    timeZone: str = "0",  # noqa: N803
    limit: int = Query(default=_DEFAULT_LIMIT),
) -> JSONResponse:
    return await _handle_klines(
        request, Market.USDM_FUTURES, symbol, interval, startTime, endTime, timeZone, limit
    )
