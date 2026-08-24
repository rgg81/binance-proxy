# binance-proxy

A caching reverse proxy for Binance's public **klines** (candlestick) REST
endpoints. It exposes the exact same request/response signature as Binance
itself, and its job is narrow and deliberate: **avoid getting the calling
IP banned (HTTP 418)** by making sure that when many callers ask for the
same thing at roughly the same time, Binance only ever gets asked once.

Built for the situation where several independent processes ("desks") each
talk to Binance directly, don't coordinate with each other, and collectively
trip Binance's IP-based rate limits. Point them all at this proxy instead —
same URLs, same JSON — and that problem goes away without touching the
callers' code.

## How it works

There is no database, no persistence, and no history. The entire mechanism
is:

1. **In-memory cache, keyed by the exact request** (path + every query
   param, verbatim). A repeat of the exact same request within 60 seconds
   (configurable) is served from memory — zero calls to Binance.
2. **Single-flight coalescing.** If multiple identical requests arrive
   while one is already in flight, they all share that one fetch and its
   result — this is the direct fix for "N desks fire the same request at
   once."
3. **Weight-aware rate limiting + circuit breaker**, using Binance's own
   response headers as ground truth, so the proxy backs off before Binance
   would reject it, and opens a breaker (serving `503` + `Retry-After`) on
   an actual `429`/`418` instead of hammering through one.

That's it. A cache miss just means "ask Binance again" — same as if there
were no cache at all, just less often. Nothing is parsed, validated, or
understood about *what* a request is asking for; params are forwarded to
Binance exactly as received, and Binance's response (success or error) is
relayed back exactly as received.

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

## Supported endpoints

| Path | Market | Upstream |
|---|---|---|
| `GET /api/v3/klines` | Spot | `api.binance.com` |
| `GET /fapi/v1/klines` | USD-M Futures | `fapi.binance.com` |
| `GET /healthz` | — | liveness probe |
| `GET /stats` | — | cache/coalescing/rate-limit visibility |

Any query parameter Binance's klines endpoint accepts is forwarded as-is —
there's no hardcoded list to keep in sync. Invalid-request errors from
Binance are passed through with the same status code and JSON body, and are
never cached (so fixing a bad request is immediately reflected, not stuck
behind a stale cached error).

## Architecture

```
Desk A ─┐
Desk B ─┼─► Proxy ─► [1] TTL cache check ─► [2] Single-flight coalesce ─► [3] Rate limiter + breaker ─► Binance
Desk C ─┘        (hit: zero calls)              (miss: one fetch shared)
```

The full design is in
[`docs/superpowers/specs/2026-08-24-simple-memory-cache-redesign.md`](docs/superpowers/specs/2026-08-24-simple-memory-cache-redesign.md)
— including why this replaced an earlier, more elaborate SQLite-backed
historical cache (short version: real traffic analysis showed ~74% of
requests structurally can't benefit from historical caching at all, since
they ask for data reaching "now" on every call; the elaborate machinery
was solving a problem the actual traffic mostly didn't have). See also
[`CLAUDE.md`](CLAUDE.md) for the invariants future changes must preserve.

## Configuration

All configuration is via environment variables (or a `.env` file — see
[`.env.example`](.env.example)):

| Variable | Default | Meaning |
|---|---|---|
| `SPOT_BASE_URL` | `https://api.binance.com` | Spot upstream base URL |
| `FUTURES_BASE_URL` | `https://fapi.binance.com` | USD-M futures upstream base URL |
| `CACHE_TTL_SECONDS` | `60` | How long a cached response stays valid |
| `CACHE_MAX_ENTRIES` | `5000` | Cache size cap (oldest-first eviction) |
| `RATE_LIMIT_SAFETY_MARGIN` | `0.8` | Fraction of the weight budget used before proactively throttling |
| `SPOT_WEIGHT_BUDGET_PER_MINUTE` | `6000` | Local estimate of Binance's spot weight budget (see note below) |
| `FUTURES_WEIGHT_BUDGET_PER_MINUTE` | `2400` | Same, for futures |
| `HOST` / `PORT` | `0.0.0.0` / `8000` | Bind address |
| `LOG_LEVEL` | `INFO` | Python logging level |

> **On the weight budget defaults:** Binance changes its published rate
> limits over time, and they can also differ per account. These numbers are
> a conservative local safety net only — the proxy's actual protection
> comes from reading Binance's own `X-MBX-USED-WEIGHT-*` response headers
> on every call and reconciling against them in real time.

## Operational notes

- **Run as a single process.** Both the cache and request coalescing rely
  on in-memory state shared across requests. Running multiple `uvicorn`
  workers or multiple instances would let each one cache/throttle
  independently, defeating the whole point.
- **Nothing survives a restart, on purpose.** The cache is pure memory — a
  restart just means the next 60 seconds of requests are cache misses,
  same as normal operation. There's nothing to back up, migrate, or clean
  up.
- **Check `/stats`** to confirm the mechanism is doing its job in
  production — cache hit/miss counts, coalescing counts
  (`calls_started`/`calls_joined`), per-market upstream call counts, and
  current weight/breaker state.

### Correctness monitoring

`scripts/monitor.py` runs a complete correctness/health check against a
live instance: code quality (pytest/ruff/mypy), response fidelity vs. the
real Binance API, and cache/coalescing effectiveness. Run it directly:

```bash
.venv/bin/python scripts/monitor.py
```

It's also wrapped by the `monitor-binance-proxy` Claude Code skill
(`.claude/skills/monitor-binance-proxy/`), which interprets results and
sends a push notification only for genuine failures — detect-and-alert
only, no auto-remediation. A cron entry (`scripts/run_monitor_skill.sh`,
every 6 hours) runs it unattended.

## Development

```bash
pip install -e ".[dev]"

pytest                 # full test suite
ruff check .            # lint
mypy                     # type check (strict)
```

Tests are split into `tests/unit/` (pure logic — the TTL cache, rate
limiter, coalescing — no network) and `tests/integration/` (the wired-
together system against a [respx](https://lundberg.github.io/respx/)-
mocked Binance backend).

## License

MIT
