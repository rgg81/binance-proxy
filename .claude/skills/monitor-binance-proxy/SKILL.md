---
name: monitor-binance-proxy
description: Run a complete correctness/health check on the running binance-proxy instance — code correctness, response fidelity vs real Binance, cache-hit and coalescing effectiveness, and cache integrity. Use when asked to check on binance-proxy, verify it's healthy, or investigate whether its cache/data is behaving correctly. Also invoked on a recurring schedule.
---

# Monitor binance-proxy

binance-proxy is a critical, production dependency for other applications. Its
entire purpose is to serve **correct** Binance klines data while minimizing
upstream calls via caching and request coalescing. This skill verifies both
properties are actually holding, not just that the process is up.

## Running the check

```bash
cd /home/roberto/binance-proxy
.venv/bin/python scripts/monitor.py
```

The script is self-contained and does the real work deterministically — see
its module docstring for exactly what each check does and why. It:

- Exits `0` if everything passed, `1` if anything failed.
- Prints a `[PASS]`/`[FAIL]` line per check.
- Always writes a timestamped JSON report to `monitoring/reports/`, pass or
  fail, so there's a history to look back over.
- Skips the live/network checks (but still runs cache integrity) if the
  proxy itself is unreachable — a dead process is its own finding, not a
  reason to also report every dependent check as failed.

Pass `--skip-code-quality` to skip the pytest/ruff/mypy pass (faster,
useful if you just want a live-behavior check). Pass `--proxy-url` if the
proxy isn't at the default `http://127.0.0.1:8000`.

## Interpreting results

**All passed:** the proxy is healthy. For a manual invocation, report this
briefly. For a scheduled/unattended run, do **not** send a push notification
for a clean pass — per PushNotification's own guidance, a notification
nobody needed is worse than no notification. Just let the report file speak
for the history.

**Something failed:** don't relay the raw failure as-is — investigate first,
the same way you would for any bug report on this project. Before deciding
something is broken:

1. Read the failing check's `detail` in the printed output / report JSON.
2. For a `fidelity_*` or `end_time_inclusive` or `future_start_time_empty`
   failure: these compare directly against a live Binance call, so a
   mismatch is a strong signal of a real regression — re-run the specific
   comparison once to rule out a transient network blip, then treat it as
   real if it persists. These three checks map directly to bugs that were
   real and confirmed in this project before; see
   `docs/superpowers/specs/2026-08-24-binance-klines-proxy-design.md` and
   `CLAUDE.md`'s invariants for the history.
3. For a `cache_hit` or `coalescing` failure: check `/stats` on the running
   proxy and consider whether the query shape used by real traffic could
   explain it before assuming a bug. **Known false alarm, already
   investigated once:** a low overall cache-hit ratio in `/stats` does NOT
   by itself mean caching is broken — if real traffic uses a large `limit`
   (e.g. 1000) with a `startTime` near "now", that query's implied window
   always extends past the closed-candle boundary, so it *must* re-verify
   against Binance on every call, by construction (see `plan_fetch` in
   `service.py`). This is not a bug. What actually indicates a *real* cache
   failure is `check_cache_hit`/`check_coalescing` in the script above
   failing, since those specifically use a firmly historical window that
   should be immune to this effect.
4. For `code_quality` (pytest/ruff/mypy) failures: this means the *deployed
   code itself* regressed — read the actual failing test/lint/type output
   (full output is in the report JSON if the printed tail isn't enough) and
   treat it like any other broken test in this repo.
5. For `cache_integrity` failures: this reads the SQLite file directly and
   found either a candle cached past its true closed_boundary (a violation
   of CLAUDE.md invariant #1/#9) or a malformed coverage range. Both are
   serious — this is the exact class of bug this project has had before.

Once you've determined whether it's a real problem:

- **Real problem:** send a push notification with a concise, specific
  summary (what failed, one likely cause if you found one) — this is
  exactly the kind of thing worth pulling someone's attention for on a
  system other applications depend on.
- **False alarm / explained by traffic pattern, not a bug:** do not send a
  push notification. Note the explanation in your response so there's a
  record, but a system behaving correctly doesn't need to interrupt anyone.

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
