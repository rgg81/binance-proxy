"""Unit tests for the SQLite-backed KlineStore: coverage tracking and kline
row persistence. Uses a real on-disk SQLite file (tmp_path) rather than
mocks, since durability across store instances is itself a requirement.
"""

from binance_proxy.cache.store import KlineStore
from binance_proxy.models import Kline, Market, SeriesKey

SPOT_1M = SeriesKey(market=Market.SPOT, symbol="BTCUSDT", interval="1m")
SPOT_5M = SeriesKey(market=Market.SPOT, symbol="BTCUSDT", interval="5m")


def make_kline(open_time: int) -> Kline:
    return Kline(
        open_time=open_time,
        open="1",
        high="2",
        low="0.5",
        close="1.5",
        volume="10",
        close_time=open_time + 59_999,
        quote_volume="15",
        num_trades=3,
        taker_buy_base="1",
        taker_buy_quote="1",
        ignore="0",
    )


class TestCoverage:
    def test_fresh_series_has_no_coverage(self, tmp_path):
        store = KlineStore(tmp_path / "klines.db")
        assert store.get_coverage(SPOT_1M) == []

    def test_added_coverage_is_readable_back(self, tmp_path):
        store = KlineStore(tmp_path / "klines.db")
        store.add_coverage(SPOT_1M, (0, 100))
        assert store.get_coverage(SPOT_1M) == [(0, 100)]

    def test_overlapping_coverage_additions_merge(self, tmp_path):
        store = KlineStore(tmp_path / "klines.db")
        store.add_coverage(SPOT_1M, (0, 100))
        store.add_coverage(SPOT_1M, (50, 150))
        assert store.get_coverage(SPOT_1M) == [(0, 150)]

    def test_coverage_is_isolated_per_series(self, tmp_path):
        store = KlineStore(tmp_path / "klines.db")
        store.add_coverage(SPOT_1M, (0, 100))
        assert store.get_coverage(SPOT_5M) == []

    def test_coverage_persists_across_store_instances(self, tmp_path):
        db_path = tmp_path / "klines.db"
        KlineStore(db_path).add_coverage(SPOT_1M, (0, 100))
        reopened = KlineStore(db_path)
        assert reopened.get_coverage(SPOT_1M) == [(0, 100)]


class TestKlineRows:
    def test_upserted_klines_are_readable_back_sorted_by_open_time(self, tmp_path):
        store = KlineStore(tmp_path / "klines.db")
        store.upsert_klines(SPOT_1M, [make_kline(120_000), make_kline(60_000)])
        rows = store.get_klines(SPOT_1M, 0, 200_000)
        assert [k.open_time for k in rows] == [60_000, 120_000]

    def test_get_klines_filters_to_the_requested_half_open_range(self, tmp_path):
        store = KlineStore(tmp_path / "klines.db")
        store.upsert_klines(
            SPOT_1M, [make_kline(0), make_kline(60_000), make_kline(120_000)]
        )
        rows = store.get_klines(SPOT_1M, 60_000, 120_000)
        assert [k.open_time for k in rows] == [60_000]

    def test_upserting_same_open_time_twice_replaces_rather_than_duplicates(
        self, tmp_path
    ):
        store = KlineStore(tmp_path / "klines.db")
        store.upsert_klines(SPOT_1M, [make_kline(60_000)])
        store.upsert_klines(SPOT_1M, [make_kline(60_000)])
        rows = store.get_klines(SPOT_1M, 0, 200_000)
        assert len(rows) == 1

    def test_klines_persist_across_store_instances(self, tmp_path):
        db_path = tmp_path / "klines.db"
        KlineStore(db_path).upsert_klines(SPOT_1M, [make_kline(60_000)])
        reopened = KlineStore(db_path)
        rows = reopened.get_klines(SPOT_1M, 0, 200_000)
        assert [k.open_time for k in rows] == [60_000]
