# CLAUDE.md

Guidance for Claude Code (or any future contributor) working in this repo.

## What this is

A minimal caching reverse proxy for Binance's public klines REST API. Its
only job is to avoid the calling IP getting 418-banned by Binance —
nothing more. Full design rationale, including why this replaced an
earlier, much more elaborate SQLite-backed historical cache:
`docs/superpowers/specs/2026-08-24-simple-memory-cache-redesign.md` (and,
for archaeology, the original design at
`docs/superpowers/specs/2026-08-24-binance-klines-proxy-design.md`).

## Commands

```bash
source .venv/bin/activate    # venv already created at ./.venv

pytest                        # full suite
pytest tests/unit             # pure-logic tests only, no network
pytest tests/integration       # respx-mocked Binance

ruff check .                    # lint
mypy                              # strict type check — must stay clean

uvicorn binance_proxy.app:app --reload   # run locally
```

## The whole mechanism, in one paragraph

A request's cache key is `(market, path, sorted(query_params))` — the
*exact* request, nothing normalized or parsed. A hit within
`CACHE_TTL_SECONDS` (default 60s) is served from memory, zero Binance
calls. A miss goes through `Coalescer.coalesce()`: if an identical request
is already in flight, this caller awaits that result instead of starting
its own fetch. Only the caller that actually starts the fetch talks to
Binance, through `RateLimiter`/`UpstreamClient` (weight throttle +
circuit breaker, unchanged from the original design). A successful (200)
response gets cached; anything else does not. That's the entire system —
`service.py` is ~40 lines.

## Invariants — do not break these

1. **The cache key must be the exact request, not a normalized or
   partially-matched one.** `ProxyService.get()` builds the key from
   `sorted(params.items())` specifically so param order never matters, but
   it must never start interpreting *what* the params mean (no parsing
   `startTime`, no understanding intervals). The moment this cache tries
   to be clever about overlapping ranges, it has become the old design
   again — that complexity was deliberately removed.
2. **Every completed response is cached, including errors — not just
   200s.** `ProxyService.get()`'s `do_work()` caches whatever
   `UpstreamClient.fetch()` returns unconditionally (that method already
   raises rather than returning for 429/418 and for transport/parse
   failures — see invariant on `UpstreamUnavailableError` below — so
   anything reaching `do_work()`'s cache.set() is a completed round trip).
   **This reverses the original design** (v0.2.0 only cached 200s) after a
   real production incident on 2026-08-28: a caller repeatedly querying
   ~546 symbols invalid on USD-M futures burned a fresh, wasted upstream
   call on every single retry (~10% of all upstream calls), a measurable
   contributor to a 418 ban. See
   `docs/superpowers/specs/2026-08-24-simple-memory-cache-redesign.md`'s
   incident notes. The accepted tradeoff: a genuinely transient error can
   now be replayed from cache for up to `CACHE_TTL_SECONDS`. Do not
   special-case 200 back in here without a similarly deliberate,
   evidence-based reason — see `TestErrorsAreCachedToo` in
   `tests/integration/test_service.py`.
3. **This proxy is single-process by design.** Both the cache
   (`cache.py::TTLCache`) and coalescing (`coalescing.py::Coalescer`) are
   plain in-memory state with no cross-process coordination. Do not add
   `uvicorn --workers N` or run multiple instances behind a load balancer.
4. **`RateLimiter.on_response`'s header reconciliation only ever
   increases** the tracked used-weight (`max(self._used_weight,
   header_weight)`), never decreases it. A stale/lower header must never
   make the proxy think it has more headroom than it actually does.
5. **No parameter validation happens locally.** `routes/klines.py` reads
   `request.query_params` and forwards them verbatim — Binance is the
   source of truth for what's a valid request. Do not reintroduce
   klines-specific param parsing/validation; that's exactly the complexity
   this redesign removed. If Binance's behavior for some param combination
   ever needs special-casing again, that's a sign the simple design has
   met its limit — treat it as a new design decision, not a quick patch.
   This still means "don't crash" — see invariant 6.
6. **Nothing in the request path may raise an exception the route layer
   doesn't catch.** `_handle_klines` only handles `RateLimitedError` and
   `UpstreamUnavailableError`; anything else becomes a raw 500. A
   malformed `limit` must reach Binance (parsed defensively, see
   `_parse_limit` in `upstream/client.py`, not `int()`ed directly), and a
   transport failure or unparseable Binance response must raise
   `UpstreamUnavailableError`, not propagate a bare `httpx`/`json`
   exception. This was a real, confirmed bug (found by independent code
   review, fixed same day): a client sending `limit=abc` crashed the
   proxy with a 500 before ever reaching Binance, which directly violated
   invariant 5 above.
7. **`RateLimiter.acquire()` must never be able to loop forever.** If a
   single request's `weight` exceeds the entire usable budget — reachable
   via ordinary env-var configuration (a low `RATE_LIMIT_SAFETY_MARGIN`),
   not just a contrived setup — no amount of waiting for a window reset
   ever satisfies `_used_weight + weight <= usable_budget`. This was a
   real, confirmed bug: the naive retry loop span so tightly (each
   iteration only awaiting a near-instant fake/real sleep) that it could
   starve the event loop badly enough to interfere with unrelated
   timers in the same process, not just hang the one call. The fix
   (`acquire()` proceeds best-effort when `weight` alone exceeds the
   usable budget) must be preserved — see
   `TestAcquireNeverDeadlocksOnAnUnsatisfiableWeight` in
   `tests/unit/test_rate_limiter.py`, and note its `FakeSleeper` guards
   against exactly this class of bug with a hard call-count cap rather
   than a wall-clock timeout, because a wall-clock timeout is not
   reliably able to interrupt this kind of tight loop.
8. **A 429/418 from Binance must always log a warning** (`RateLimiter.
   on_response`, via stdlib `logging`, module logger in
   `upstream/rate_limiter.py`) identifying the market, the status code,
   the backoff duration, and the tracked used-weight at the time. Root-
   causing the 2026-08-28 ban (see invariant #2) required reconstructing
   what happened after the fact from access logs and `/stats` snapshots
   alone, because nothing logged the ban event itself when it occurred —
   don't let that gap reopen. `RateLimiter` takes a `market` label
   (`app.py` passes `Market.SPOT.value`/`Market.USDM_FUTURES.value`)
   purely so these log lines are attributable.

## Architecture map

```
config.py           Settings (env vars / .env)
models.py             Market enum — that's the entire file now
cache.py                TTLCache: exact-signature-keyed, TTL-expiring, size-capped
coalescing.py              Coalescer: single-flight only (no more per-series lock —
                              there's no gap-fill critical section to serialize anymore)
upstream/
  rate_limiter.py              weight throttle + circuit breaker per market (unchanged)
  client.py                      fetch(path, params) -> (status_code, body), raw passthrough
service.py                         ProxyService.get(): cache check -> coalesce -> fetch -> cache
routes/
  klines.py                          GET /api/v3/klines, GET /fapi/v1/klines — thin forwarding
  health.py                            GET /healthz, GET /stats
app.py                                 wires everything together (FastAPI factory)
```

Read order for understanding the request path: `service.py` (the whole
thing) → `coalescing.py` → `upstream/client.py`.

## Testing conventions

TDD throughout. `cache.py`, `coalescing.py`, and `upstream/rate_limiter.py`
are pure or near-pure logic with exhaustive unit tests and no network —
the correctness core. `tests/integration/` wires the real pieces together
against a respx-mocked Binance and asserts on call counts, not just
response shape — that's how "only one call reaches Binance" and "a repeat
within TTL costs zero calls" are actually verified, not just assumed.

## Production monitoring

`scripts/monitor.py` + the `monitor-binance-proxy` skill
(`.claude/skills/monitor-binance-proxy/`) run a complete correctness check
against a live instance — see the skill file for what each check verifies
and how to interpret a failure. It's scheduled via a local cron entry
(`scripts/run_monitor_skill.sh`, every 6 hours, invoking `claude -p`
headlessly) — **not** the cloud `schedule` skill, which runs in an isolated
environment with no access to a `127.0.0.1`-only proxy. This proxy also has
a separate, pre-existing uptime-only watchdog
(`crypto-trade-claude-code-market-neutral-v4/scripts/binance_proxy.sh`,
`*/5 * * * *` + `@reboot`) that restarts the process if it's down — the
monitor skill is a complementary correctness layer on top of that, not a
replacement, and deliberately never restarts the process or modifies code
itself (detect-and-alert only).

## History worth knowing before changing anything

This project originally shipped with a SQLite-persisted cache that tracked
exact coverage ranges per series and did gap-fill arithmetic to serve
partial-range requests from a mix of cached and freshly-fetched data. It
worked, was thoroughly tested (98 tests, multiple rounds of adversarial
live verification against real Binance, two genuinely confirmed live bugs
found and fixed), and is preserved at
`docs/superpowers/specs/2026-08-24-binance-klines-proxy-design.md` for
reference. It was deliberately replaced with the current design after
analyzing real production traffic and finding that ~74% of requests use a
large `limit` with a `startTime` near "now" — a shape that structurally
can never be satisfied from historical cache, no matter how well-built the
gap-fill logic is, because the requested window always extends past the
closed-candle boundary. The historical machinery was solving a problem
most real traffic didn't have, at a real cost in complexity (an entire
SQLite schema, coverage-interval arithmetic, an open-candle exclusion
invariant, and two boundary-precision bugs that took real debugging to
find). **Do not resurrect that design from memory or convenience** — if a
future need genuinely requires historical range caching again, treat it as
a fresh, deliberate decision informed by then-current traffic data, not a
reflex to "add back what used to be there."
