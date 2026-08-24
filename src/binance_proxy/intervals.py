"""Binance kline interval <-> millisecond-duration mapping.

All Binance interval strings except "1M" (calendar month) have a fixed
duration, which is what lets us do range arithmetic (coverage, gap-fill) on
them at all. "1M" is intentionally unsupported here — callers must route
those requests through an always-live passthrough instead of the cache.
"""

from __future__ import annotations

_SECOND = 1_000
_MINUTE = 60 * _SECOND
_HOUR = 60 * _MINUTE
_DAY = 24 * _HOUR

_FIXED_DURATIONS: dict[str, int] = {
    "1s": _SECOND,
    "1m": _MINUTE,
    "3m": 3 * _MINUTE,
    "5m": 5 * _MINUTE,
    "15m": 15 * _MINUTE,
    "30m": 30 * _MINUTE,
    "1h": _HOUR,
    "2h": 2 * _HOUR,
    "4h": 4 * _HOUR,
    "6h": 6 * _HOUR,
    "8h": 8 * _HOUR,
    "12h": 12 * _HOUR,
    "1d": _DAY,
    "3d": 3 * _DAY,
    "1w": 7 * _DAY,
}

_VARIABLE_LENGTH_INTERVALS = {"1M"}


def interval_to_ms(interval: str) -> int:
    if interval in _VARIABLE_LENGTH_INTERVALS:
        raise ValueError(
            f"interval {interval!r} has a variable-length duration (calendar "
            "month) and cannot be used in fixed-duration range arithmetic"
        )
    try:
        return _FIXED_DURATIONS[interval]
    except KeyError:
        raise ValueError(f"unknown interval {interval!r}") from None
