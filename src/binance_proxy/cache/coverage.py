"""Pure interval-set arithmetic over half-open [start, end) integer ranges.

Used to track which sub-ranges of a (market, symbol, interval, timezone)
kline series have already been verified-fetched from Binance ("coverage"),
and to compute the minimal set of sub-ranges still missing for a given
request. No I/O — this module knows nothing about klines, SQLite, or HTTP.
"""

from __future__ import annotations

Range = tuple[int, int]


def merge_ranges(ranges: list[Range]) -> list[Range]:
    """Sort and collapse overlapping or touching ranges into the minimal set."""
    if not ranges:
        return []

    ordered = sorted(ranges)
    merged: list[Range] = [ordered[0]]
    for start, end in ordered[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:  # overlapping or exactly adjacent
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def subtract_ranges(query: Range, covered: list[Range]) -> list[Range]:
    """Return the parts of `query` not covered by any range in `covered`.

    `covered` need not be pre-sorted or pre-merged.
    """
    query_start, query_end = query
    if query_start >= query_end:
        return []

    missing: list[Range] = []
    cursor = query_start
    for cov_start, cov_end in merge_ranges(covered):
        # Clip the covered range to the query window before comparing.
        cov_start = max(cov_start, query_start)
        cov_end = min(cov_end, query_end)
        if cov_start >= cov_end:
            continue  # no overlap with the query window
        if cov_start > cursor:
            missing.append((cursor, cov_start))
        cursor = max(cursor, cov_end)

    if cursor < query_end:
        missing.append((cursor, query_end))

    return missing
