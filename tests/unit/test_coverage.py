"""Unit tests for pure interval-set arithmetic used by the coverage cache.

Ranges are half-open [start, end) integer intervals, e.g. millisecond epoch
timestamps. No I/O, no network — this is the crux logic that decides which
sub-ranges of a request are already cached vs. need to be fetched.
"""

from binance_proxy.cache.coverage import merge_ranges, subtract_ranges


class TestMergeRanges:
    def test_empty_input_returns_empty(self):
        assert merge_ranges([]) == []

    def test_single_range_unchanged(self):
        assert merge_ranges([(10, 20)]) == [(10, 20)]

    def test_non_overlapping_non_adjacent_ranges_stay_separate(self):
        assert merge_ranges([(0, 10), (20, 30)]) == [(0, 10), (20, 30)]

    def test_overlapping_ranges_are_merged(self):
        assert merge_ranges([(0, 15), (10, 30)]) == [(0, 30)]

    def test_adjacent_ranges_are_merged(self):
        # end of one == start of next: touching ranges represent one
        # contiguous verified-fetched span and must collapse into one row.
        assert merge_ranges([(0, 10), (10, 20)]) == [(0, 20)]

    def test_unsorted_input_is_sorted_and_merged(self):
        assert merge_ranges([(20, 30), (0, 10)]) == [(0, 10), (20, 30)]

    def test_chain_of_overlaps_collapses_to_one_range(self):
        assert merge_ranges([(0, 5), (4, 9), (8, 12)]) == [(0, 12)]

    def test_duplicate_ranges_collapse(self):
        assert merge_ranges([(0, 10), (0, 10)]) == [(0, 10)]


class TestSubtractRanges:
    def test_no_coverage_means_entire_query_is_missing(self):
        assert subtract_ranges((0, 100), []) == [(0, 100)]

    def test_coverage_fully_containing_query_leaves_nothing_missing(self):
        assert subtract_ranges((10, 20), [(0, 100)]) == []

    def test_coverage_overlapping_start_leaves_tail_missing(self):
        assert subtract_ranges((10, 30), [(0, 20)]) == [(20, 30)]

    def test_coverage_overlapping_end_leaves_head_missing(self):
        assert subtract_ranges((10, 30), [(20, 40)]) == [(10, 20)]

    def test_coverage_in_middle_leaves_two_pieces_missing(self):
        assert subtract_ranges((0, 100), [(40, 60)]) == [(0, 40), (60, 100)]

    def test_multiple_covered_ranges_leave_gaps_missing(self):
        assert subtract_ranges((0, 100), [(10, 20), (50, 60)]) == [
            (0, 10),
            (20, 50),
            (60, 100),
        ]

    def test_coverage_outside_query_has_no_effect(self):
        assert subtract_ranges((50, 60), [(0, 10), (200, 300)]) == [(50, 60)]

    def test_unsorted_and_overlapping_covered_input_is_handled(self):
        # Defensive: callers may pass raw, un-merged rows straight from the DB.
        assert subtract_ranges((0, 100), [(60, 70), (10, 20), (15, 25)]) == [
            (0, 10),
            (25, 60),
            (70, 100),
        ]

    def test_exact_match_leaves_nothing_missing(self):
        assert subtract_ranges((10, 20), [(10, 20)]) == []
