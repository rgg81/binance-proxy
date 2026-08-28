---
name: monitor-binance-proxy
description: Run a complete correctness/health check on the running binance-proxy instance — code correctness, response fidelity vs real Binance, cache and coalescing effectiveness. Use when asked to check on binance-proxy, verify it's healthy, or investigate whether its cache is behaving correctly. Also invoked on a recurring schedule.
---

# Monitor binance-proxy

binance-proxy is a critical, production dependency for other applications.
Its purpose is simple and deliberately narrow: cache Binance klines
responses in memory for a short TTL (default 60s) and coalesce concurrent
identical requests, so multiple callers hitting Binance in parallel don't
trip a 418 ban. There is no persistence and no historical data — a cache
miss just means "ask Binance again." This skill verifies that mechanism is
actually working, not just that the process is up.

## Running the check

```bash
cd /home/roberto/binance-proxy
.venv/bin/python scripts/monitor.py
```

The script does the real work deterministically — see its module docstring
for exactly what each check does and why. It:

- Exits `0` if everything passed, `1` if anything failed.
- Prints a `[PASS]`/`[FAIL]` line per check.
- Always writes a timestamped JSON report to `monitoring/reports/`, pass or
  fail, so there's a history to look back over.
- Skips the live/network checks if the proxy itself is unreachable — a dead
  process is its own finding, not a reason to also report every dependent
  check as failed.

Pass `--skip-code-quality` to skip the pytest/ruff/mypy pass (faster,
useful if you just want a live-behavior check). Pass `--proxy-url` if the
proxy isn't at the default `http://127.0.0.1:8000`.

## Interpreting results

**All passed:** the proxy is healthy. For a manual invocation, report this
briefly. For a scheduled/unattended run, do **not** send a push notification
for a clean pass — per PushNotification's own guidance, a notification
nobody needed is worse than no notification. Just let the report file speak
for the history.

**Something failed:** don't relay the raw failure as-is — investigate
first, the same way you would for any bug report on this project.

1. Read the failing check's `detail` in the printed output / report JSON.
2. For a `fidelity_*` or `cache_served_fidelity` failure: these compare
   directly against a live Binance call, so a mismatch is a strong signal
   of a real regression — re-run the specific comparison once to rule out
   a transient network blip, then treat it as real if it persists.
   `cache_served_fidelity` specifically proves the cache round-trip
   doesn't corrupt what it stores — it forces a cache-served response
   (proven via a zero-upstream-call signal) and compares that against real
   Binance, not just against the proxy's own earlier response.
3. For an `error_responses_are_cached` failure: this is a regression check
   for the *confirmed, actual root cause* of a real production 418 ban on
   2026-08-28 — a caller repeatedly querying ~546 symbols invalid on
   USD-M futures burned a fresh upstream call on every single retry
   (~10% of all upstream calls) because errors weren't being cached
   (CLAUDE.md invariant #2). A failure here means the proxy is back to
   that exact wasteful, ban-contributing behavior — treat it as urgent,
   not a flaky signal.
4. For a `malformed_input_does_not_crash` failure: Binance is the sole
   source of truth for request validity here (CLAUDE.md invariant #5) —
   the proxy does no local validation, so a bad param must still relay
   Binance's own error response, never crash with a raw 500. This is a
   regression check for a real, previously-fixed bug: a malformed `limit`
   once crashed the proxy before the request ever reached Binance
   (invariant #6). A failure here is a real regression, not a flaky
   network signal — treat it as such immediately.
5. For a `coalescing` failure: `N` concurrent identical requests should
   collapse to at most one upstream call. A failure here means the
   single-flight mechanism — the direct fix for "many desks call the same
   thing in parallel" — isn't working, which is the core reason this
   proxy exists.
6. For a `pytest`, `ruff`, or `mypy` failure (three separately-named
   results, not one combined check): this means the *deployed code
   itself* regressed — read the actual failing test/lint/type output
   (full output is in the report JSON if the printed tail isn't enough)
   and treat it like any other broken test in this repo.
7. **If `markets.<market>.banned` is ever true in `/stats`** (check it
   directly, not just via the fidelity checks above, which only catch a
   ban indirectly): this is the exact failure this whole project exists
   to prevent. Read `RateLimiter`'s warning-level log line (invariant #8)
   for the market/status/duration/used-weight at the moment it tripped,
   check `logs/proxy.log` for the request pattern around that point (symbol
   diversity, repeated invalid symbols, request volume), and check whether
   anything else on this machine calls Binance directly for the same
   market, bypassing this proxy's rate limiter entirely (this was a real,
   confirmed contributing factor once already — grep other projects on
   this machine for hardcoded `api.binance.com`/`fapi.binance.com`).
   Always alert on this, regardless of what caused it.

Once you've determined it's a real problem, send a push notification with
a concise, specific summary — this is exactly the kind of thing worth
pulling someone's attention for on a system other applications depend on.

## Scope

**This skill detects, investigates, and alerts. It does not modify code,
restart the process, or attempt any remediation, automatically or
otherwise** — on a system other applications depend on, a human should
decide how to respond to a real finding, not have it fixed unattended and
unreviewed. This applies fully to scheduled/cron-triggered runs. If a human
is directly asking you, interactively, to also fix a bug this check turned
up, that's a separate, explicit request they make in that conversation —
follow the repo's normal TDD workflow (see `CLAUDE.md`) for that, but it is
never something to do on your own initiative from this skill.
