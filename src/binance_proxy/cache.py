"""A simple in-memory, TTL-based cache keyed by exact request signature.

No history, no persistence, no partial-range logic — just "did we already
answer this exact question recently?" An entry is only ever exactly what
Binance returned for one exact set of request parameters; there is no
concept of merging or partially satisfying a request from it. Entries
expire after `ttl_seconds`; an expired entry is treated as a miss and
overwritten on the next fetch. Bounded by `max_entries` with oldest-first
eviction so a long-running process doesn't grow unbounded even though the
TTL alone mostly self-limits it.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable, Hashable
from dataclasses import dataclass

NowFn = Callable[[], float]


@dataclass(frozen=True, slots=True)
class CachedResponse:
    status_code: int
    body: object  # parsed JSON — whatever Binance returned, verbatim
    cached_at: float


class TTLCache:
    def __init__(
        self,
        ttl_seconds: float,
        max_entries: int = 5000,
        now_fn: NowFn = time.monotonic,
    ) -> None:
        self._ttl = ttl_seconds
        self._max_entries = max_entries
        self._now = now_fn
        self._store: OrderedDict[Hashable, CachedResponse] = OrderedDict()
        self.hits = 0
        self.misses = 0

    def get(self, key: Hashable) -> CachedResponse | None:
        entry = self._store.get(key)
        if entry is None:
            self.misses += 1
            return None
        if self._now() - entry.cached_at >= self._ttl:
            del self._store[key]
            self.misses += 1
            return None
        self.hits += 1
        self._store.move_to_end(key)
        return entry

    def set(self, key: Hashable, status_code: int, body: object) -> None:
        self._store[key] = CachedResponse(status_code, body, self._now())
        self._store.move_to_end(key)
        while len(self._store) > self._max_entries:
            self._store.popitem(last=False)

    def __len__(self) -> int:
        # Counts only still-usable entries, not ones sitting dead past their
        # TTL waiting for a get() or an eviction sweep to notice — this is
        # surfaced via /stats as the number an operator uses to judge cache
        # health, so it needs to reflect reality, not just dict size. Cheap:
        # bounded by max_entries (a few thousand at most).
        now = self._now()
        return sum(1 for entry in self._store.values() if now - entry.cached_at < self._ttl)
