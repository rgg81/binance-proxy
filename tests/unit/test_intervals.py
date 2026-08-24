import pytest

from binance_proxy.intervals import interval_to_ms


class TestIntervalToMs:
    @pytest.mark.parametrize(
        ("interval", "expected_ms"),
        [
            ("1s", 1_000),
            ("1m", 60_000),
            ("3m", 3 * 60_000),
            ("5m", 5 * 60_000),
            ("15m", 15 * 60_000),
            ("30m", 30 * 60_000),
            ("1h", 3_600_000),
            ("2h", 2 * 3_600_000),
            ("4h", 4 * 3_600_000),
            ("6h", 6 * 3_600_000),
            ("8h", 8 * 3_600_000),
            ("12h", 12 * 3_600_000),
            ("1d", 86_400_000),
            ("3d", 3 * 86_400_000),
            ("1w", 7 * 86_400_000),
        ],
    )
    def test_fixed_duration_intervals(self, interval, expected_ms):
        assert interval_to_ms(interval) == expected_ms

    def test_month_interval_is_rejected_as_variable_length(self):
        # A calendar month has no fixed millisecond duration, so it can't be
        # used in the gap-fill arithmetic. Callers must route "1M" requests
        # through the always-live passthrough path instead.
        with pytest.raises(ValueError, match="variable"):
            interval_to_ms("1M")

    def test_unknown_interval_is_rejected(self):
        with pytest.raises(ValueError, match="unknown"):
            interval_to_ms("7x")
