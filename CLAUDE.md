# CLAUDE.md

Guidance for Claude Code (or any future contributor) working in this repo.

## What this is

A caching reverse proxy for Binance's public klines REST API. Full design
rationale: `docs/superpowers/specs/2026-08-24-binance-klines-proxy-design.md`.
Read it before changing anything in `service.py`, `cache/`, or
`upstream/rate_limiter.py` — those modules implement a specific, carefully
reasoned-through algorithm, not an obvious one.

## Commands

```bash
source .venv/bin/activate    # venv already created at ./.venv

pytest                        # full suite
pytest tests/unit             # pure-logic tests only, no network
pytest tests/integration       # respx-mocked Binance + real SQLite

ruff check .                    # lint
mypy                              # strict type check — must stay clean

uvicorn binance_proxy.app:app --reload   # run locally
```

All four (pytest, ruff, mypy, and a manual smoke test against the real
Binance API) passed clean as of the initial implementation. Keep them that
way — this is a small, deliberately over-engineered-for-correctness
codebase; regressions here mean real Binance bans in production.

## Invariants — do not break these without re-reading the design doc

1. **The currently-forming candle is never persisted or counted as
   covered.** `plan_fetch` in `service.py` computes `closed_boundary =
   (now_ms // interval_ms) * interval_ms` and never lets cache coverage
   extend past it. If you touch gap-fill logic, re-run
   `tests/integration/test_service.py::TestLiveTail` and make sure it still
   asserts zero persisted rows for the open candle. Relatedly,
   `live_tail_range`'s start is `max(start, closed_boundary)`, not
   `closed_boundary` alone — a request whose own `start` is already past
   `closed_boundary` (including a genuinely future `startTime`) must fetch
   from its own `start`, or the proxy fabricates the live candle in place
   of the empty result Binance itself returns for a future range. This was
   a real, confirmed bug (verified live: proxy returned a fake current
   candle for a future startTime before the fix, `[]` after).
2. **`timezone` is part of a series' identity**, not an afterthought —
   Binance's `timeZone` param shifts candle boundaries for intervals ≥ 1d.
   `SeriesKey` includes it; don't collapse it out for convenience. Further:
   **any non-`"0"` timezone bypasses the coverage-READ/gap-fill path
   entirely** (`KlineService.get_klines`'s dispatch condition) — the
   `closed_boundary` math is UTC-only and cannot be trusted to shift
   correctly for other offsets. This was a real, confirmed bug (a
   still-forming shifted daily candle got permanently mis-cached); see
   `TestNonUtcTimezoneBypassesTheCache` for the numeric reproduction. The
   bypass only disables the boundary-based *read*; passthrough may still
   safely cache a row via its own `close_time` from Binance, which is
   ground truth regardless of timezone.
3. **This proxy is single-process by design.** Coalescing (`coalescing.py`)
   and rate limiting (`upstream/rate_limiter.py`) hold in-memory state with
   no cross-process coordination. Do not add `uvicorn --workers N` or run
   multiple instances behind a load balancer without redesigning both — see
   the design doc's "Deployment model" discussion for why this was a
   deliberate choice, not an oversight.
4. **A single gap is always ≤ 1000 candles wide**, by construction (`plan_fetch`
   bounds the window to `start + limit * interval_ms`, and `limit` is capped
   at 1000 — the same cap Binance itself applies). `_call_binance` in
   `service.py` relies on this to make exactly one Binance call per gap with
   no pagination. If you ever let a gap exceed 1000 candles, you'll silently
   under-fetch and create a phantom coverage gap.
5. **`RateLimiter.on_response`'s header reconciliation only ever increases**
   the tracked used-weight (`max(self._used_weight, header_weight)`), never
   decreases it. This is deliberate — a stale/lower header must never make
   the proxy think it has more headroom than it actually does.
6. **The `1M` interval is intentionally excluded from the coverage cache**
   (`interval_to_ms` raises `ValueError` for it; `KlineService.get_klines`
   checks for it and routes to `_fetch_passthrough`). Don't try to "fix"
   this by approximating a month's duration — that reintroduces silent
   incorrectness at month/DST boundaries. If real demand for cached `1M`
   data appears, it needs its own calendar-aware coverage representation,
   not a fudge factor on the existing one.
7. **Binance's `endTime` is inclusive** (open_time ≤ endTime), confirmed
   live against the real API. `plan_fetch`'s `end` is a half-open exclusive
   boundary; `KlineService.get_klines` converts once (`end_time + 1`) at
   the seam where a client's raw endTime enters the internal arithmetic.
   Do not pass a client's raw `end_time` into `plan_fetch` un-converted —
   this was a real, confirmed bug (silently dropped the last candle of any
   candle-aligned range query).
8. **Every code path that writes to `coverage` for a series must hold
   `coalescer.series_lock(key)` for the duration of the write** —
   `KlineStore.add_coverage` is a non-atomic read-merge-write and two
   concurrent unlocked callers can lose one of their ranges (confirmed by
   direct reproduction). This includes `_fetch_passthrough`, not just the
   range path — it was a real, confirmed bug that it didn't.
9. **`_fill_gap` must never trust the requested `[start, end)` shape** —
   it derives what to persist/cover from which returned rows are actually
   closed (`close_time < now_ms`), the same way `_fetch_live_tail` and
   `_fetch_passthrough` do. This is deliberate defense-in-depth against
   any future way `closed_boundary` might be computed wrong, not just the
   timezone case invariant #2 already closes off.

## Architecture map

```
config.py           Settings (env vars / .env)
models.py            Market, SeriesKey, Kline (raw-string-preserving row type)
intervals.py           interval string -> fixed millisecond duration
cache/
  coverage.py           pure interval-set arithmetic (merge/subtract) — no I/O
  store.py                SQLite: klines + coverage tables
service.py                 plan_fetch (pure) + KlineService (async orchestration)
coalescing.py                 Coalescer: single-flight (Layer A) + per-series lock (Layer B)
upstream/
  rate_limiter.py              weight throttle + circuit breaker per market
  client.py                      thin httpx wrapper, wired to one RateLimiter
routes/
  klines.py                       GET /api/v3/klines, GET /fapi/v1/klines
  health.py                         GET /healthz, GET /stats
app.py                                 wires everything together (FastAPI factory)
```

Read order for understanding the request path: `service.py` (`plan_fetch`
then `KlineService.get_klines`) → `coalescing.py` → `upstream/client.py` →
`cache/store.py`.

## Cache on disk

SQLite file at `${DATA_DIR}/klines.db` (default `./data/klines.db`). To
inspect:

```bash
sqlite3 data/klines.db "select market, symbol, interval, count(*) from klines group by 1,2,3;"
sqlite3 data/klines.db "select * from coverage where symbol = 'BTCUSDT';"
```

To reset the cache, just delete the file (or the whole `data/` dir) while
the proxy isn't running — it's fully rebuildable from Binance, by design.

## Testing conventions

TDD throughout (`superpowers:test-driven-development`): every module here
was built test-first — write the test, watch it fail for the right reason,
then write the minimal code to pass. `cache/coverage.py`, `service.py`'s
`plan_fetch`, `upstream/rate_limiter.py`, and `coalescing.py` are pure or
near-pure logic with exhaustive unit tests and no network; they're the
correctness core. `tests/integration/` wires the real pieces together
against a respx-mocked Binance and asserts on call counts, not just
response shape — that's how the "only one call reaches Binance" guarantee
is actually verified. Keep new behavior covered the same way: pure-logic
unit test first if the logic can be isolated, integration test for the
wiring.

## Known limitations (by design, not oversights)

- **Coalescer cancellation propagation**: if the task running the shared
  `work()` for a coalesced key is cancelled (e.g. a client disconnects),
  that cancellation propagates to every other caller coalesced onto the
  same key. Documented in `coalescing.py`'s module docstring. Acceptable
  for this proxy's usage pattern; would need `asyncio.shield`-based
  supervision to fully decouple if it ever becomes a real problem.
- **Weight budget defaults are estimates**, not guarantees — see the note
  in `README.md`'s Configuration section. The real safety mechanism is
  header reconciliation (invariant #5 above), not the configured budget.
- **No pre-warming/backfill tooling.** Cache fills purely from live desk
  traffic. Was explicitly descoped for v1 (see design doc); would be a
  reasonably small addition (a CLI driving `KlineService.get_klines`
  directly) if needed later.
