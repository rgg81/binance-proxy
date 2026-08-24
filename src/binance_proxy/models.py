"""Core data types shared across the cache, upstream client, and service layer."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Market(StrEnum):
    SPOT = "spot"
    USDM_FUTURES = "usdm_futures"


@dataclass(frozen=True, slots=True)
class SeriesKey:
    """Identifies one klines time series in the cache.

    `timezone` is part of the identity, not an afterthought: Binance's
    `timeZone` query param shifts candle boundaries for intervals >= 1d, so
    two requests that differ only in timeZone are genuinely different series.
    """

    market: Market
    symbol: str
    interval: str
    timezone: str = "0"


@dataclass(frozen=True, slots=True)
class Kline:
    """One kline row, field types matching Binance's own response exactly.

    Numeric OHLCV fields are kept as the raw strings Binance sent (not
    re-parsed to float) so a cached response can be replayed byte-identical
    to what a live Binance call would have returned.
    """

    open_time: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    close_time: int
    quote_volume: str
    num_trades: int
    taker_buy_base: str
    taker_buy_quote: str
    ignore: str

    @classmethod
    def from_binance_row(cls, row: list[int | str]) -> Kline:
        return cls(
            open_time=int(row[0]),
            open=str(row[1]),
            high=str(row[2]),
            low=str(row[3]),
            close=str(row[4]),
            volume=str(row[5]),
            close_time=int(row[6]),
            quote_volume=str(row[7]),
            num_trades=int(row[8]),
            taker_buy_base=str(row[9]),
            taker_buy_quote=str(row[10]),
            ignore=str(row[11]),
        )

    def to_binance_row(self) -> list[int | str]:
        return [
            self.open_time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.close_time,
            self.quote_volume,
            self.num_trades,
            self.taker_buy_base,
            self.taker_buy_quote,
            self.ignore,
        ]
