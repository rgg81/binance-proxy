# binance-proxy

A caching reverse proxy for Binance's public **klines** (candlestick) REST
endpoints. It exposes the exact same request/response signature as Binance
itself, caches closed candles to disk, and guarantees that when many
callers ask for the same or overlapping data at the same time, **at most
one request reaches Binance** — everyone else is served from cache or rides
along on that one in-flight call.

Built for the situation where several independent processes ("desks") each
talk to Binance directly, don't coordinate with each other, and collectively
trip Binance's IP-based rate limits. Point them all at this proxy instead —
same URLs, same JSON — and the rate-limit problem disappears without
touching the callers' code.

## Why this works

Closed klines candles are **immutable** — a candle that has closed never
changes. That means:

- Once a range of history has been fetched, it never needs to be fetched
  again. The disk cache (SQLite) never evicts closed candles.
- The proxy tracks exactly which time ranges of each `(market, symbol,
  interval, timezone)` series have already been verified against Binance,
  and on every request computes the *minimal* missing sub-range — often
  zero — rather than re-fetching anything already known.
- Concurrent requests for the same data are collapsed into one upstream
  call via request coalescing (see [Architecture](#architecture)).
- A weight-aware throttle and circuit breaker keep the proxy itself from
  ever tripping Binance's own limits, using Binance's response headers as
  the source of truth.

The only thing that's never cached is the **currently-forming candle** —
it's still changing, so it's always fetched live (though still coalesced
and rate-limited).

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env   # defaults are sane; edit if needed

uvicorn binance_proxy.app:app --host 0.0.0.0 --port 8000
```

Point a desk's existing Binance client at the proxy instead of Binance
directly — only the base URL changes:

```diff
- BASE_URL = "https://api.binance.com"
+ BASE_URL = "http://your-proxy-host:8000"
```

```bash
curl "http://localhost:8000/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=5"
```

The response is byte-for-byte what Binance itself would return.

## Supported endpoints

| Path | Market | Upstream |
|---|---|---|
| `GET /api/v3/klines` | Spot | `api.binance.com` |
| `GET /fapi/v1/klines` | USD-M Futures | `fapi.binance.com` |
| `GET /healthz` | — | liveness probe |
| `GET /stats` | — | cache/coalescing/rate-limit visibility |

Query parameters (`symbol`, `interval`, `startTime`, `endTime`, `timeZone`,
`limit`) match Binance exactly, including its `limit` default (500) and
bounds (1–1000). Invalid/unknown-symbol errors from Binance are passed
through with the same status code and JSON body.

This proxy is scoped to **klines only** for v1 — other public market-data
endpoints (order book, trades, tickers) are out of scope but could be added
following the same pattern.

## Architecture

```
Desk A ─┐
Desk B ─┼─► Proxy ─► [1] Single-flight dedupe ─► [2] Coverage/gap check ─► SQLite cache
Desk C ─┘                                              │ (miss/partial)
                                                        ▼
                                              [3] Rate limiter + circuit breaker ─► Binance
```

1. **Single-flight coalescing** — identical concurrent requests (same
   symbol/interval/range/limit) share one execution and one result.
2. **Coverage-based gap-fill** — a per-series lock serializes fetch
   planning; only the sub-range genuinely missing from the cache is
   requested from Binance, never the whole thing.
3. **Weight-aware rate limiting + circuit breaker** — proactively throttles
   before Binance would reject a request, and opens a breaker (serving
   `503` + `Retry-After` for anything not already cached) on `429`/`418`.

The full design — including the exact gap-fill algorithm, storage schema,
and the invariants that keep it correct — is in
[`docs/superpowers/specs/2026-08-24-binance-klines-proxy-design.md`](docs/superpowers/specs/2026-08-24-binance-klines-proxy-design.md).
See also [`CLAUDE.md`](CLAUDE.md) for the invariants future changes must
preserve.

## Configuration

All configuration is via environment variables (or a `.env` file — see
[`.env.example`](.env.example)):

| Variable | Default | Meaning |
|---|---|---|
| `SPOT_BASE_URL` | `https://api.binance.com` | Spot upstream base URL |
| `FUTURES_BASE_URL` | `https://fapi.binance.com` | USD-M futures upstream base URL |
| `DATA_DIR` | `./data` | Where the SQLite cache file lives |
| `RATE_LIMIT_SAFETY_MARGIN` | `0.8` | Fraction of the weight budget used before proactively throttling |
| `SPOT_WEIGHT_BUDGET_PER_MINUTE` | `6000` | Local estimate of Binance's spot weight budget (see note below) |
| `FUTURES_WEIGHT_BUDGET_PER_MINUTE` | `2400` | Same, for futures |
| `HOST` / `PORT` | `0.0.0.0` / `8000` | Bind address |
| `LOG_LEVEL` | `INFO` | Python logging level |

> **On the weight budget defaults:** Binance changes its published rate
> limits over time, and they can also differ per account. These numbers are
> a conservative local safety net only — the proxy's actual protection
> comes from reading Binance's own `X-MBX-USED-WEIGHT-*` response headers
> on every call and reconciling against them in real time. Still, check
> Binance's current documented limits for your account and adjust if
> needed.

## Operational notes

- **Run as a single process.** Request coalescing and rate limiting rely on
  in-memory state shared across requests. Running multiple `uvicorn`
  workers or multiple instances would let each one coalesce/throttle
  independently, defeating the whole point. Scale by giving this one
  process enough resources, not by adding workers.
- **No cache eviction.** Closed candles are immutable and small (~100
  bytes/row); they're kept forever. A busy 1-minute series can grow to a
  few hundred MB over years — cheap relative to the cost of re-fetching it
  and risking a ban. Revisit if disk becomes a genuine constraint.
- **The `1M` (calendar month) interval bypasses the cache.** Binance's
  monthly candles don't have a fixed duration, so the gap-fill arithmetic
  doesn't apply to them; those requests are always a live (but still
  coalesced) passthrough call.
- **Check `/stats`** to confirm the mechanism is doing its job in
  production — it reports, per market, how many requests actually reached
  Binance (`upstream_calls_made`) versus how many were coalesced
  (`coalescing.calls_joined`), plus current weight usage and breaker state.
- **A low overall cache-hit ratio in `/stats` doesn't necessarily mean
  caching is broken.** A request shape like a large `limit` (e.g. 1000)
  combined with a `startTime` near "now" always needs to re-verify against
  Binance on every call, by construction — its implied window extends far
  past the closed-candle boundary regardless of caching. Run
  `scripts/monitor.py` (see below) for a check that isolates real cache
  failures from this expected pattern.

### Correctness monitoring

`scripts/monitor.py` runs a complete correctness/health check against a
live instance: code quality (pytest/ruff/mypy), response fidelity vs. the
real Binance API, cache-hit and coalescing effectiveness, and cache
integrity read directly from disk. Run it directly:

```bash
.venv/bin/python scripts/monitor.py
```

It's also wrapped by the `monitor-binance-proxy` Claude Code skill
(`.claude/skills/monitor-binance-proxy/`), which interprets results,
distinguishes real problems from explainable traffic patterns, and sends a
push notification only for genuine failures — detect-and-alert only, no
auto-remediation. A cron entry (`scripts/run_monitor_skill.sh`, every 6
hours) runs it unattended.

## Development

```bash
pip install -e ".[dev]"

pytest                 # full test suite
ruff check .            # lint
mypy                     # type check (strict)
```

Tests are split into `tests/unit/` (pure logic — coverage arithmetic,
fetch planning, rate limiter, coalescing — no network) and
`tests/integration/` (the wired-together system against a
[respx](https://lundberg.github.io/respx/)-mocked Binance backend).

## License

MIT
