# Simple In-Memory Cache Redesign

Status: implemented. Date: 2026-08-24. Supersedes:
`2026-08-24-binance-klines-proxy-design.md` (preserved for reference — see
"Why the original design was replaced" below).

## Context

The original design (see the superseded spec) built a SQLite-persisted
cache with coverage-range tracking and gap-fill arithmetic, aiming to
serve historical klines ranges from disk with minimal Binance calls. It
worked and was thoroughly verified — but after the project went to
production and accumulated real traffic, analysis of the live request log
(8,713 real klines requests) showed:

- **~74% of requests structurally could not benefit from historical
  caching at all.** The dominant traffic shape — a large `limit` (e.g.
  1000) paired with a `startTime` near "now" — implies a query window that
  always extends past the closed-candle boundary, so the proxy's own
  `plan_fetch` logic *always* had to make a live Binance call for it,
  regardless of caching. This isn't a bug; it's a mathematical consequence
  of the query shape.
- A directly comparable pattern using a smaller `limit` (same endpoint,
  same interval) showed **0%** needing a live call — proving the
  historical-caching machinery genuinely worked *when the query shape
  allowed it*, but most real traffic wasn't shaped that way.
- The system had also accumulated real complexity in service of this:
  SQLite schema and migrations, an interval-set coverage algebra, an
  open-candle exclusion invariant threaded through three separate fetch
  paths, and two genuinely confirmed live-production bugs (an `endTime`
  inclusivity boundary error, and a future-`startTime` fabrication bug)
  that required careful boundary-math reasoning to find and fix.

Given the actual goal — stated directly — was "avoid 418 bans," not "build
a historical data store," the user asked for a full redesign toward
radical simplicity: an in-memory-only cache, no persisted history, just
enough mechanism to stop concurrent/duplicate requests from hammering
Binance.

## Design

### What's cached, and how

A single in-memory dict (`cache.py::TTLCache`), keyed by the **exact**
request signature: `(market, path, tuple(sorted(query_params.items())))`.
No parsing, no understanding of what a request is asking for — a cache
entry is literally "the last response Binance gave for this exact set of
params, if less than `CACHE_TTL_SECONDS` (default 60) ago." An expired
entry is treated as a miss and overwritten on the next fetch. The dict is
also capped at `CACHE_MAX_ENTRIES` with oldest-first eviction, as a safety
net against unbounded growth (the TTL alone mostly self-limits this, since
old entries stop being useful and get replaced or would fail eviction
checks — the cap just guards the case of very high param-combination
cardinality within one TTL window).

**Updated by the 2026-08-28 incident below**: originally, only successful
(`200`) responses were cached — anything else was never cached, so a
caller fixing a bad request would see that reflected immediately. In
production this meant a caller repeatedly retrying a *permanently*
invalid request (e.g. a symbol that doesn't exist on a market) burned a
fresh upstream call on every single attempt, forever — a real, measurable
contributor to a production ban (see the Incident section). As of that
incident, a 200 **or 4xx** response is cached; only a 5xx is not. A 4xx
means the request itself is invalid — deterministic, so caching it is
free — while a 5xx means Binance's own transient state, which caching
would silently mask for up to the TTL. See CLAUDE.md invariant #2 for the
full reasoning.

### Coalescing

Unchanged in mechanism, simplified in scope: `Coalescer.coalesce()`
(single-flight) is the only layer now. The original design also had a
per-series `asyncio.Lock` ("Layer B") to serialize gap-fill fetches for
*overlapping-but-not-identical* range requests — that concept doesn't
exist anymore, because there's no more concept of "overlapping" once
caching is purely exact-signature-keyed. A cache miss for any two
different-but-similar requests just means two independent fetches; there's
nothing to serialize between them, so Layer B was removed along with the
gap-fill logic it protected.

### Rate limiting

Entirely unchanged: the weight-aware throttle and circuit breaker
(`upstream/rate_limiter.py`) were never the complicated part, and remain
the actual ban-avoidance mechanism — the cache and coalescing exist to
reduce how often that mechanism is even tested.

### API surface

No parameter validation or typing at the route layer anymore.
`routes/klines.py` reads `request.query_params` as a raw dict and forwards
it to Binance unchanged; Binance's response (success or error, verbatim)
is relayed back unchanged. This is a deliberate simplification: the
previous design's local `limit` bounds-checking, `startTime`/`endTime`
semantics, and `timeZone`-aware coverage bypass all existed to support the
historical-caching logic that's now gone. There is nothing left to
validate locally — Binance is the sole source of truth for request
validity.

### What's gone

- SQLite entirely (`cache/store.py`, `cache/coverage.py`,
  `data/klines.db`).
- `plan_fetch`, gap-fill, coverage tracking, the closed/open-candle
  distinction, `Kline`/`SeriesKey` typed parsing, `intervals.py`
  (interval-to-milliseconds math) — none of it is needed when nothing is
  parsed or partially served from a persisted range.
- The corresponding invariants and their regression tests (endTime
  inclusivity, future-startTime handling, the open-candle-never-cached
  guarantee) — the bugs those protected against literally cannot recur,
  because the code paths that could produce them no longer exist. See
  `CLAUDE.md`'s "History worth knowing" section.

### Monitoring

`scripts/monitor.py` and the `monitor-binance-proxy` skill were rewritten
to match: no more SQLite-integrity or live-candle-persistence checks
(nothing to check — there's no persistence). What remains: response
fidelity vs. real Binance (anchored safely in the past to avoid comparing
against a still-forming candle across two sequential HTTP calls, which
would be flaky through no fault of the proxy), cache-served fidelity
(prime the cache, confirm a repeat is genuinely served from it via a
zero-upstream-call signal, and confirm that cached response still matches
an independent real Binance call), and coalescing effectiveness.

## Verification

- Full test suite rewritten test-first: 10 tests for `TTLCache` (hit/miss,
  expiry, eviction), simplified `coalescing.py` tests (single-flight only),
  `upstream/client.py` tests for the new `fetch() -> (status_code, body)`
  interface, and integration tests for `ProxyService` covering cache hits,
  independent cache entries, expiry, error-never-cached, and coalescing.
- Live-verified against real Binance on an isolated test instance,
  including deliberately proving the checks can catch a real problem, not
  just pass trivially: ran the monitor against an instance with
  `CACHE_TTL_SECONDS=0` (caching effectively disabled) and confirmed
  `cache_served_fidelity` correctly failed with `was_cache_served=False`,
  while `coalescing` correctly kept passing (proving the two checks
  exercise genuinely independent mechanisms).
- An independent code review (max effort) was run against the full
  redesigned codebase before this was deployed to production. It surfaced
  15 findings; each was individually triaged rather than applied blindly.
  Seven were real bugs and fixed, test-first: a malformed `limit` query
  param crashed the proxy with a 500 instead of ever reaching Binance
  (violating this project's own "Binance is the source of truth"
  invariant); uncaught transport errors and non-JSON response bodies also
  crashed with a raw 500 instead of a clean `503`; `RateLimiter.acquire()`
  could loop forever under a reachable misconfiguration (a low
  `RATE_LIMIT_SAFETY_MARGIN`) severely enough to risk starving the whole
  event loop, not just the one call; the upstream `httpx.AsyncClient`
  instances were never closed on shutdown; `/stats`' reported cache size
  counted entries already dead past their TTL; and one test had a vacuous
  assertion plus a stale docstring referencing already-removed code. Eight
  findings were reviewed and deliberately not acted on — e.g. refunding a
  weight reservation on a failed call was rejected because it would work
  *against* the established "never underestimate Binance usage" ban-
  avoidance philosophy, and a couple of findings described the CancelledError-
  propagation behavior `coalescing.py` already documents as an accepted,
  pre-existing tradeoff rather than something this redesign introduced.
  See the commit history for the full reasoning on each.

## Incident: 2026-08-28 USD-M futures ban

Roughly 4 days after deployment, the futures market got 418-banned for the
first time (spot unaffected). The monitor's own cron schedule had silently
never actually run since being set up — a PATH issue specific to cron's
minimal environment (`claude: command not found`; fixed separately, see
`.claude/skills/monitor-binance-proxy/`) — so nobody was alerted until
asked directly. Once fixed and re-run, the monitor's own investigation
(cross-checked manually) found three contributing causes:

1. **The cache is close to a no-op for this workload at scale.** 849
   distinct symbols × tens of thousands of distinct URL combinations
   against a 60s TTL produced a 6.3% hit rate. Consistent with — and
   worse than — the original ~74%-can't-benefit finding, since the
   symbol sweep had grown since that analysis.
2. **~546 permanently-invalid futures symbols were queried repeatedly.**
   ~10% of all upstream calls were wasted 400s for symbols that don't
   exist on USD-M futures. Because errors weren't cached (the original
   design), every retry burned a fresh call for zero possible benefit —
   pure waste, and a direct, measurable contributor to the ban. **Fixed
   here**: 4xx responses are now cached too (invariant #2) — 5xx
   deliberately isn't, since unlike a permanently-invalid request, a
   server error isn't deterministic and caching it would mask a real
   Binance-side outage instead.
3. **A different project's script bypasses the proxy entirely**,
   consuming the same IP's Binance weight budget invisibly to this
   proxy's rate limiter. Fixed in that project (not this repo): the
   script now checks this proxy's own `/stats` before running and skips
   its run entirely while `usdm_futures.banned` is true, so it stops
   adding load during an active ban instead of piling on invisibly.

The proxy's own weight-tracking/reservation/reconciliation/circuit-breaker
mechanism was confirmed working exactly as designed throughout — this was
a demand problem the cache/coalescing layer couldn't fully absorb, not a
bug in the rate limiter. Changes that came out of it: caching 4xx
responses too (invariant #2), logging a warning whenever the circuit
breaker trips (invariant #8) — the previous absence of that log line made
root-causing this incident materially harder than it needed to be — and,
found by a follow-up adversarial review of the incident-response fixes
themselves, a `Retry-After: inf`/`nan` header would have permanently
bricked or silently disabled the breaker (`_parse_retry_after` now
rejects non-finite/negative values).
