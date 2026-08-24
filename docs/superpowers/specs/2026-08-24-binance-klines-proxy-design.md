# Binance Klines Caching Proxy — Design

Status: implemented (v1). Date: 2026-08-24.

## Context

Multiple independent "desks" (processes/scripts) each call Binance's public
market-data REST API directly. Because the desks don't coordinate with each
other, they frequently issue the same or overlapping requests (e.g. "last
500 1m candles for BTCUSDT") in parallel, and collectively exceed Binance's
IP-based rate limits — causing `429`/`418` responses and, at worst, IP bans.

The goal: a standalone HTTP proxy, written in Python, that:

- Exposes **exactly Binance's own REST signature** for klines, so desks can
  adopt it by changing only their base URL.
- Caches klines to disk so that repeated/overlapping requests are served
  from cache instead of Binance.
- Guarantees that **when N requests for the same data arrive concurrently,
  at most one of them reaches Binance** — the rest wait on that one and are
  served its result.
- Minimizes actual outbound Binance calls (not disk usage) — closed
  candles are immutable, so once fetched they never need to be re-fetched.

## Scope decisions (v1)

These were made explicitly, with trade-offs, rather than defaulted into:

- **Klines only** (`/api/v3/klines` spot, `/fapi/v1/klines` USD-M futures).
  Other public endpoints (depth, trades, tickers) have fundamentally
  different caching profiles (highly volatile, point-in-time) and are out
  of scope — this proxy solves the specific, high-value case of historical/
  immutable time-series data first.
- **Single-process deployment.** Coalescing and rate limiting depend on
  shared in-memory state. A multi-process or multi-instance deployment
  would need cross-process coordination (e.g. file locks, a shared lock
  table) — real complexity for a use case (a handful of internal desks)
  that doesn't need the throughput multiple workers would provide.
- **Lazy caching only.** No pre-warming/backfill CLI in v1 — the cache
  fills from real traffic. Straightforward to add later without touching
  the core design (it would just call `KlineService.get_klines` directly).
- **Fail fast on ban, don't hang.** On a Binance rate-limit/ban, the proxy
  returns `503` + `Retry-After` rather than blocking the client connection
  server-side until the ban lifts.
- **SQLite over Parquet.** Klines arrive as a trickle of individual rows
  (one new candle per interval tick); SQLite's upsert-and-index model fits
  that far better than Parquet's rewrite-on-append model. See "Storage"
  below.

## Architecture

```
Desk A ─┐
Desk B ─┼─► Proxy ─► [1] Single-flight dedupe ─► [2] Coverage/gap check ─► SQLite cache
Desk C ─┘                                              │ (miss/partial)
                                                        ▼
                                              [3] Rate limiter + circuit breaker ─► Binance
```

### Request flow

1. **Normalize** the request into `(market, symbol, interval, timeZone,
   startTime, endTime, limit)`, with Binance's own documented defaults
   filled in (`limit` default 500, max 1000; `timeZone` default `"0"`).
2. **Layer A — exact-request single-flight** (`coalescing.py::Coalescer.coalesce`).
   The normalized tuple is the coalescing key into a
   `dict[key, asyncio.Future]`. The first caller creates the future and
   does the work; concurrent callers with an *identical* normalized request
   `await` that same future. This directly solves the described failure
   mode: many desks firing the literal same request at the same moment
   collapses to exactly one upstream call.
3. **Layer B — per-series fetch lock** (`coalescing.py::Coalescer.series_lock`).
   An `asyncio.Lock` keyed on `(market, symbol, interval, timezone)`
   serializes the "figure out what's missing and fetch it" critical section
   even across *non-identical but overlapping* requests on the same series,
   so they queue rather than stampede Binance in parallel, and each
   benefits from what the previous one just cached.
4. **Gap check** (`service.py::plan_fetch`, pure function). A `coverage`
   table tracks verified-fetched `[start, end)` ranges per series.
   Verified-*empty* sub-ranges (e.g. before a symbol's listing date) count
   as covered too, so Binance is never re-asked about them.
   `requested range − covered union = minimal missing sub-ranges`. If
   nothing is missing, the response is built **100% from disk, zero
   Binance calls**.
5. **Fetch only the gaps** through the rate limiter, merge fetched rows
   into the cache (upsert), extend `coverage`, then slice/truncate the
   merged result to `limit` exactly as Binance would.
6. **The current, still-forming candle is never persisted or counted as
   covered** — a candle only enters the cache/coverage frontier once its
   `close_time` has passed relative to wall-clock "now". Requests whose
   range reaches "now" always take a live (coalesced, rate-limited) trip
   for that trailing candle only.

### Why a single gap never exceeds 1000 candles

`plan_fetch` bounds the query window to
`start + limit * interval_ms` before ever consulting coverage, and `limit`
is capped at 1000 — the same cap Binance itself enforces per call. So any
sub-range `subtract_ranges` produces is a subset of an already-≤1000-candle
window, meaning **one Binance call per gap always suffices**. No pagination
logic was needed in `_call_binance`.

### Rate limiting & ban resilience

Two independent limiter/breaker instances — spot and USD-M futures have
separate weight budgets and separate bans (`upstream/rate_limiter.py`,
one `RateLimiter` per market, wired in `app.py`).

- A weight-aware gate treats Binance's own `X-MBX-USED-WEIGHT-*` response
  headers as ground truth (scanned case-insensitively, since exact
  casing/window varies by market), backed by a configurable default weight
  table, and throttles once usage crosses a safety margin (default 80%) of
  the budget — proactively, not just after a rejection.
- On `429`: back off per `Retry-After`.
- On `418` (IP ban): open a circuit breaker for that market for the ban
  duration (default 120s if Binance omits `Retry-After`). No outbound
  calls at all while open; cache-only serving continues.
- If a request needs data that isn't cached while the breaker is open, the
  route layer returns `503` with a `Retry-After` header. The client
  connection is never held open waiting.

### Storage schema (SQLite, WAL mode, stdlib `sqlite3` via `asyncio.to_thread`)

```sql
CREATE TABLE klines (
    market       TEXT NOT NULL,   -- 'spot' | 'usdm_futures'
    symbol       TEXT NOT NULL,
    interval     TEXT NOT NULL,
    timezone     TEXT NOT NULL,   -- part of the key: shifts candle
                                   -- boundaries for intervals >= 1d
    open_time    INTEGER NOT NULL,
    open         TEXT NOT NULL,   -- exact string Binance sent, never
    high         TEXT NOT NULL,   -- re-parsed to float, so a replayed
    low          TEXT NOT NULL,   -- response is byte-identical to a
    close        TEXT NOT NULL,   -- live Binance call
    volume       TEXT NOT NULL,
    close_time   INTEGER NOT NULL,
    quote_volume TEXT NOT NULL,
    num_trades   INTEGER NOT NULL,
    taker_buy_base  TEXT NOT NULL,
    taker_buy_quote TEXT NOT NULL,
    ignore       TEXT NOT NULL,
    PRIMARY KEY (market, symbol, interval, timezone, open_time)
);

CREATE TABLE coverage (
    market       TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    interval     TEXT NOT NULL,
    timezone     TEXT NOT NULL,
    range_start  INTEGER NOT NULL,  -- inclusive, ms epoch
    range_end    INTEGER NOT NULL   -- exclusive, ms epoch
);
CREATE INDEX idx_coverage_series ON coverage (market, symbol, interval, timezone);
```

`coverage` rows are merged (adjacent/overlapping ranges collapsed) every
time new data is persisted, so the set stays small per series regardless of
how fragmented the fetch history was.

### API surface & fidelity

- `GET /api/v3/klines` (spot), `GET /fapi/v1/klines` (USD-M futures) —
  identical path, query params, JSON array-of-arrays shape, and status
  codes to Binance itself.
- Binance client errors (bad symbol, bad interval, etc.) pass through
  transparently with the same status code and JSON body — never cached,
  never retried.
- `GET /healthz` — liveness.
- `GET /stats` — per-market `upstream_calls_made`, `used_weight`,
  `banned`/`seconds_until_unbanned`, plus global coalescing
  `calls_started`/`calls_joined`. This is the operational way to confirm
  the anti-rate-limit mechanism is actually working: compare
  `upstream_calls_made` to the number of proxy requests served.

### Known correctness caveat

Binance's exact truncation semantics (which end of a range gets clipped
when it implies more than `limit` candles) are not fully unambiguous from
the docs alone. The implementation was validated by direct comparison
against live Binance responses during development (see manual smoke test
in the repo's test history), but if Binance's actual behavior at some edge
case differs from what's implemented in `plan_fetch`/`_fetch_passthrough`,
recorded real-response fixtures should be the tiebreaker over the docs.

### The `1M` interval

Calendar months don't have a fixed millisecond duration, so they can't
participate in the fixed-duration coverage/gap-fill arithmetic. `1M`
requests are intentionally routed through an always-live passthrough path
(`intervals.py::interval_to_ms` raises for `"1M"`;
`service.py::KlineService.get_klines` checks for it explicitly). This is a
deliberate scope limitation, not a bug — see `CLAUDE.md` invariant #6 before
"fixing" it with an approximation.

## Testing strategy

- Pure-function unit tests for the interval-set arithmetic
  (`cache/coverage.py`) and fetch planning (`service.py::plan_fetch`) — the
  trickiest correctness surface, fully testable without any I/O.
- Unit tests for the rate limiter/circuit breaker and the coalescer, with
  clock/sleep injected so concurrency and backoff timing are deterministic.
- Integration tests (`tests/integration/`) against a
  [respx](https://lundberg.github.io/respx/)-mocked Binance and a real
  (tmp-path) SQLite store, asserting on **call counts** — not just response
  shape — for: full cache hit (zero calls), gap-fill (only the missing
  sub-range fetched), N-concurrent-identical (exactly one call), the open
  candle never being persisted, and ban handling (breaker opens, `503` +
  `Retry-After`).
- Manually verified end-to-end against the real Binance API during
  development: response byte-parity, single-call-on-cache-hit, and
  15-concurrent-identical-requests-collapse-to-1-upstream-call, all
  confirmed live (not just mocked).

## Project layout

```
binance-proxy/
  src/binance_proxy/
    app.py                    FastAPI app factory, wiring
    config.py                  pydantic-settings
    models.py                    Market, SeriesKey, Kline
    intervals.py                   interval -> fixed ms duration
    cache/
      store.py                       SQLite: klines + coverage
      coverage.py                      pure interval-set arithmetic
    upstream/
      client.py                         httpx wrapper per market
      rate_limiter.py                     weight gate + circuit breaker
    coalescing.py                          single-flight + per-series lock
    service.py                              plan_fetch + KlineService
    routes/
      klines.py                              /api/v3/klines, /fapi/v1/klines
      health.py                                /healthz, /stats
  tests/{unit,integration}/
  README.md  CLAUDE.md  pyproject.toml  .env.example
```

Dependencies: `fastapi`, `uvicorn`, `httpx`, `pydantic-settings`. Storage is
stdlib `sqlite3` only — no ORM. Dev dependencies: `pytest`,
`pytest-asyncio`, `respx`, `ruff`, `mypy` (strict).
