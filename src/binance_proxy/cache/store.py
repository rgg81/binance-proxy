"""SQLite-backed persistence for kline rows and their coverage ranges.

Synchronous by design (sqlite3 is synchronous) — callers on the async path
run these methods via `asyncio.to_thread`. See `coverage.py` for the pure
interval arithmetic this module uses to keep coverage rows merged.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path

from binance_proxy.cache.coverage import Range, merge_ranges
from binance_proxy.models import Kline, SeriesKey

_SCHEMA = """
CREATE TABLE IF NOT EXISTS klines (
    market       TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    interval     TEXT NOT NULL,
    timezone     TEXT NOT NULL,
    open_time    INTEGER NOT NULL,
    open         TEXT NOT NULL,
    high         TEXT NOT NULL,
    low          TEXT NOT NULL,
    close        TEXT NOT NULL,
    volume       TEXT NOT NULL,
    close_time   INTEGER NOT NULL,
    quote_volume TEXT NOT NULL,
    num_trades   INTEGER NOT NULL,
    taker_buy_base  TEXT NOT NULL,
    taker_buy_quote TEXT NOT NULL,
    ignore       TEXT NOT NULL,
    PRIMARY KEY (market, symbol, interval, timezone, open_time)
);

CREATE TABLE IF NOT EXISTS coverage (
    market       TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    interval     TEXT NOT NULL,
    timezone     TEXT NOT NULL,
    range_start  INTEGER NOT NULL,
    range_end    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_coverage_series
    ON coverage (market, symbol, interval, timezone);
"""


class KlineStore:
    def __init__(self, db_path: Path) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # -- coverage -----------------------------------------------------

    def get_coverage(self, key: SeriesKey) -> list[Range]:
        rows = self._conn.execute(
            """
            SELECT range_start, range_end FROM coverage
            WHERE market = ? AND symbol = ? AND interval = ? AND timezone = ?
            ORDER BY range_start
            """,
            (key.market.value, key.symbol, key.interval, key.timezone),
        ).fetchall()
        return [(start, end) for start, end in rows]

    def add_coverage(self, key: SeriesKey, new_range: Range) -> None:
        existing = self.get_coverage(key)
        merged = merge_ranges([*existing, new_range])
        with self._conn:
            self._conn.execute(
                """
                DELETE FROM coverage
                WHERE market = ? AND symbol = ? AND interval = ? AND timezone = ?
                """,
                (key.market.value, key.symbol, key.interval, key.timezone),
            )
            self._conn.executemany(
                """
                INSERT INTO coverage
                    (market, symbol, interval, timezone, range_start, range_end)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (key.market.value, key.symbol, key.interval, key.timezone, s, e)
                    for s, e in merged
                ],
            )

    # -- kline rows -----------------------------------------------------

    def get_klines(self, key: SeriesKey, start: int, end: int) -> list[Kline]:
        rows = self._conn.execute(
            """
            SELECT open_time, open, high, low, close, volume, close_time,
                   quote_volume, num_trades, taker_buy_base, taker_buy_quote, ignore
            FROM klines
            WHERE market = ? AND symbol = ? AND interval = ? AND timezone = ?
              AND open_time >= ? AND open_time < ?
            ORDER BY open_time
            """,
            (key.market.value, key.symbol, key.interval, key.timezone, start, end),
        ).fetchall()
        return [Kline.from_binance_row(list(row)) for row in rows]

    def upsert_klines(self, key: SeriesKey, klines: Iterable[Kline]) -> None:
        with self._conn:
            self._conn.executemany(
                """
                INSERT OR REPLACE INTO klines
                    (market, symbol, interval, timezone, open_time, open, high, low,
                     close, volume, close_time, quote_volume, num_trades,
                     taker_buy_base, taker_buy_quote, ignore)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        key.market.value,
                        key.symbol,
                        key.interval,
                        key.timezone,
                        k.open_time,
                        k.open,
                        k.high,
                        k.low,
                        k.close,
                        k.volume,
                        k.close_time,
                        k.quote_volume,
                        k.num_trades,
                        k.taker_buy_base,
                        k.taker_buy_quote,
                        k.ignore,
                    )
                    for k in klines
                ],
            )
