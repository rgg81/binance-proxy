"""Unit tests for Kline <-> raw Binance row conversion.

Correctness here matters because responses must be byte-identical to what a
live Binance call would return, including exact numeric string formatting.
"""

from binance_proxy.models import Kline

RAW_ROW = [
    1499040000000,
    "0.01634790",
    "0.80000000",
    "0.01575800",
    "0.01577100",
    "148976.11427815",
    1499644799999,
    "2434.19055334",
    308,
    "1756.87402397",
    "28.46694368",
    "0",
]


class TestKlineFromBinanceRow:
    def test_round_trips_back_to_the_exact_same_row(self):
        kline = Kline.from_binance_row(RAW_ROW)
        assert kline.to_binance_row() == RAW_ROW

    def test_preserves_numeric_strings_exactly_rather_than_reparsing_as_float(self):
        # A naive float round-trip would turn "0.01634790" into "0.0163479",
        # silently dropping the trailing zero Binance actually sent.
        kline = Kline.from_binance_row(RAW_ROW)
        assert kline.open == "0.01634790"

    def test_maps_integer_fields_to_int(self):
        kline = Kline.from_binance_row(RAW_ROW)
        assert kline.open_time == 1499040000000
        assert kline.close_time == 1499644799999
        assert kline.num_trades == 308
