"""Unit tests for TTLCache: a simple in-memory, exact-signature-keyed cache
with no history and no persistence — just "did we already answer this exact
question recently?"
"""

from binance_proxy.cache import TTLCache


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestGetSet:
    def test_miss_on_empty_cache(self):
        cache = TTLCache(ttl_seconds=60)
        assert cache.get(("key",)) is None

    def test_hit_returns_what_was_set(self):
        cache = TTLCache(ttl_seconds=60)
        cache.set(("key",), 200, {"a": 1})
        entry = cache.get(("key",))
        assert entry is not None
        assert entry.status_code == 200
        assert entry.body == {"a": 1}

    def test_different_keys_are_independent(self):
        cache = TTLCache(ttl_seconds=60)
        cache.set(("a",), 200, "A")
        assert cache.get(("b",)) is None


class TestExpiry:
    def test_entry_within_ttl_is_a_hit(self):
        clock = FakeClock()
        cache = TTLCache(ttl_seconds=60, now_fn=clock)
        cache.set(("key",), 200, "body")
        clock.advance(59)
        assert cache.get(("key",)) is not None

    def test_entry_past_ttl_is_a_miss(self):
        clock = FakeClock()
        cache = TTLCache(ttl_seconds=60, now_fn=clock)
        cache.set(("key",), 200, "body")
        clock.advance(60)
        assert cache.get(("key",)) is None

    def test_expired_entry_is_evicted_not_just_hidden(self):
        clock = FakeClock()
        cache = TTLCache(ttl_seconds=60, now_fn=clock)
        cache.set(("key",), 200, "body")
        clock.advance(60)
        cache.get(("key",))  # triggers eviction
        assert len(cache) == 0

    def test_setting_again_after_expiry_is_a_fresh_hit(self):
        clock = FakeClock()
        cache = TTLCache(ttl_seconds=60, now_fn=clock)
        cache.set(("key",), 200, "old")
        clock.advance(60)
        cache.set(("key",), 200, "new")
        entry = cache.get(("key",))
        assert entry is not None
        assert entry.body == "new"


class TestHitMissCounters:
    def test_counters_track_hits_and_misses(self):
        cache = TTLCache(ttl_seconds=60)
        cache.get(("key",))  # miss
        cache.set(("key",), 200, "body")
        cache.get(("key",))  # hit
        cache.get(("key",))  # hit
        assert cache.misses == 1
        assert cache.hits == 2


class TestLiveEntryCount:
    """`__len__` is surfaced via /stats as the cache size an operator uses
    to judge cache health — it must reflect entries that are actually still
    usable, not ones sitting dead past their TTL waiting for a `get()` or
    an eviction sweep to notice. Real traffic often writes many near-unique
    keys that are rarely re-requested, so lazy get()-triggered eviction
    alone would let /stats' reported size drift far from reality.
    """

    def test_len_excludes_entries_past_their_ttl_even_without_a_get(self):
        clock = FakeClock()
        cache = TTLCache(ttl_seconds=60, now_fn=clock)
        cache.set(("key",), 200, "body")
        clock.advance(60)
        assert len(cache) == 0

    def test_len_still_counts_entries_within_ttl(self):
        cache = TTLCache(ttl_seconds=60)
        cache.set(("a",), 200, "A")
        cache.set(("b",), 200, "B")
        assert len(cache) == 2


class TestMaxEntriesEviction:
    def test_oldest_entry_is_evicted_when_max_entries_exceeded(self):
        cache = TTLCache(ttl_seconds=60, max_entries=2)
        cache.set(("a",), 200, "A")
        cache.set(("b",), 200, "B")
        cache.set(("c",), 200, "C")
        assert cache.get(("a",)) is None  # evicted, was oldest
        assert cache.get(("b",)) is not None
        assert cache.get(("c",)) is not None
        assert len(cache) == 2

    def test_reading_an_entry_protects_it_from_eviction(self):
        cache = TTLCache(ttl_seconds=60, max_entries=2)
        cache.set(("a",), 200, "A")
        cache.set(("b",), 200, "B")
        cache.get(("a",))  # touch "a" so "b" becomes the oldest
        cache.set(("c",), 200, "C")
        assert cache.get(("a",)) is not None
        assert cache.get(("b",)) is None  # evicted, was now the oldest
