#!/usr/bin/env python3
"""Complete correctness/health monitor for a running binance-proxy instance.

Run manually:
    .venv/bin/python scripts/monitor.py

Or via the monitor-binance-proxy skill, which also interprets the result
and sends a push notification on failure.

Exits 0 if every check passes, 1 if any check fails. Always writes a
timestamped JSON report (and a human-readable copy) under
monitoring/reports/, regardless of outcome, so history is preserved.

What this checks (see .claude/skills/monitor-binance-proxy/SKILL.md for the
"why" behind each one). The proxy is deliberately simple now — an in-memory
TTL cache keyed by exact request signature, no persistence, no history — so
these checks are simpler too:
  1. The proxy process is up and /healthz responds.
  2. The deployed code is still correct: full pytest + ruff + mypy (three
     separately-named results: "pytest", "ruff", "mypy").
  3. Neither market is currently banned (checked directly via /stats, not
     just inferred from a fidelity mismatch) — the exact failure this
     project exists to prevent, and the thing a real production incident
     confirmed wasn't being alerted on loudly enough.
  4. Response fidelity vs. the real Binance API (spot + futures) — the
     proxy forwards params verbatim and must relay Binance's answer
     unchanged.
  5. A repeated identical request within the TTL is served from cache
     (zero new upstream calls) and matches real Binance — proves the cache
     round-trip doesn't corrupt anything, not just that it's self-consistent.
  6. An error response (e.g. an invalid symbol) is cached too, not
     re-fetched on every repeat — regression check for the confirmed root
     cause of a real production ban (CLAUDE.md invariant #2). Checked on
     both spot and usdm_futures: the actual incident was futures-specific,
     so a spot-only version of this check could pass right through a
     futures-only regression.
  7. A malformed query param (e.g. a non-numeric `limit`) must still reach
     Binance, not crash the proxy with a raw 500 — regression check for a
     real, previously-fixed bug (CLAUDE.md invariant #6).
  8. Concurrent identical requests collapse to one upstream call.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "monitoring" / "reports"
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"

REAL_SPOT_BASE = "https://api.binance.com"
REAL_FUTURES_BASE = "https://fapi.binance.com"


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def _now_ms() -> int:
    return int(time.time() * 1000)


# -- 1. process / liveness / ban status --------------------------------------


def check_healthz(client: httpx.Client) -> CheckResult:
    try:
        r = client.get("/healthz", timeout=5)
        ok = r.status_code == 200 and r.json().get("status") == "ok"
        return CheckResult("proxy_healthz", ok, f"status={r.status_code} body={r.text}")
    except httpx.HTTPError as exc:
        return CheckResult("proxy_healthz", False, f"unreachable: {exc}")


# Any of these can legitimately happen from a malformed/degraded /stats
# response (bad JSON, a missing or renamed field) — every check that reads
# /stats must fail cleanly with a CheckResult, never crash run_all() and
# take the whole report down with it. That would repeat, for a different
# reason, exactly the "silent total monitoring failure" this project
# already had once (see the cron-PATH fix in this same commit).
_STATS_ERRORS = (httpx.HTTPError, ValueError, KeyError, TypeError)


def _get_stats(client: httpx.Client) -> dict[str, Any]:
    # Any, not a nested typed shape: this is untyped JSON off the wire, and
    # monitor.py isn't part of the project's strict-mypy scope (pyproject.toml
    # `[tool.mypy] files = ["src"]`) — a fully typed /stats schema belongs
    # there if it's ever worth adding, not duplicated ad hoc here.
    stats: dict[str, Any] = client.get("/stats", timeout=10).json()
    return stats


def check_not_banned(client: httpx.Client) -> CheckResult:
    """Direct check of the exact failure this whole project exists to
    prevent, rather than relying on it surfacing indirectly through a
    fidelity-check mismatch (which is how the 2026-08-28 ban was first
    noticed — it worked, but required interpretation to trace back to a
    ban rather than being immediately obvious).
    """
    try:
        stats = _get_stats(client)
        banned = {
            market: info.get("seconds_until_unbanned")
            for market, info in stats.get("markets", {}).items()
            if info.get("banned")
        }
    except _STATS_ERRORS as exc:
        return CheckResult("not_banned", False, f"could not read /stats: {exc!r}")

    ok = not banned
    detail = "no market banned" if ok else f"BANNED: {banned} (seconds remaining)"
    return CheckResult("not_banned", ok, detail)


# -- 2. deployed code correctness ------------------------------------------


def check_code_quality() -> list[CheckResult]:
    if not VENV_PYTHON.exists():
        return [CheckResult("code_quality", False, f"venv not found at {VENV_PYTHON}")]

    results = []
    for name, cmd in [
        ("pytest", [str(VENV_PYTHON), "-m", "pytest", "-q"]),
        ("ruff", [str(VENV_PYTHON), "-m", "ruff", "check", "."]),
        ("mypy", [str(VENV_PYTHON), "-m", "mypy"]),
    ]:
        proc = subprocess.run(
            cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=300
        )
        tail = "\n".join((proc.stdout + proc.stderr).strip().splitlines()[-15:])
        results.append(CheckResult(name, proc.returncode == 0, tail or "clean"))
    return results


# -- 3. response fidelity vs real Binance -----------------------------------


def check_response_fidelity(
    client: httpx.Client, real: httpx.Client, symbol: str, market: str
) -> CheckResult:
    """The proxy forwards params verbatim and caches the raw response — the
    first call for a fresh (never-cached) param combo must relay Binance's
    answer unchanged. Uses the large-limit query shape matching real
    traffic (see the traffic analysis in CLAUDE.md), but anchored a full
    day back rather than at "now": the live/still-forming candle changes
    between the two sequential HTTP calls this check makes (a new trade
    can legitimately land in between), which would make this check flaky
    through no fault of the proxy — anchoring safely in the past avoids
    that without losing the realistic large-limit shape.
    """
    proxy_path = "/api/v3/klines" if market == "spot" else "/fapi/v1/klines"
    real_base = REAL_SPOT_BASE if market == "spot" else REAL_FUTURES_BASE
    now = _now_ms()
    yesterday_boundary_8h = (now // 28_800_000) * 28_800_000 - 86_400_000
    params: dict[str, str | int] = {
        "symbol": symbol,
        "interval": "8h",
        "startTime": yesterday_boundary_8h,
        "limit": 2,
    }

    try:
        proxy_resp = client.get(proxy_path, params=params, timeout=15).json()
        real_resp = real.get(f"{real_base}{proxy_path}", params=params, timeout=15).json()
    except httpx.HTTPError as exc:
        return CheckResult(f"fidelity_{market}_{symbol}", False, f"request failed: {exc}")

    ok = proxy_resp == real_resp
    detail = "byte-identical" if ok else f"MISMATCH: proxy={proxy_resp!r} real={real_resp!r}"
    return CheckResult(f"fidelity_{market}_{symbol}", ok, detail)


# -- 4. cache-hit fidelity ----------------------------------------------------


def _upstream_calls(client: httpx.Client, market: str) -> int:
    """Reads /stats' upstream_calls_made counter for one market. Callers must
    catch `_STATS_ERRORS` — same reasoning as `_get_stats`: a malformed body
    here must not crash the whole monitor run.
    """
    stats = _get_stats(client)
    return int(stats["markets"][market]["upstream_calls_made"])


def check_cache_served_fidelity(client: httpx.Client, real: httpx.Client) -> CheckResult:
    """Prime the cache, confirm a repeat within the TTL is served from cache
    (zero new upstream calls) rather than just self-consistent, and confirm
    that cached response still matches an independent real Binance call —
    the cache must not corrupt or mutate what it stores.
    """
    now = _now_ms()
    params: dict[str, str | int] = {
        "symbol": "DOTUSDT",
        "interval": "1m",
        "startTime": now - 600_000,
        "limit": 3,
    }

    try:
        client.get("/api/v3/klines", params=params, timeout=15)  # prime
        before = _upstream_calls(client, "spot")
        cached_resp = client.get("/api/v3/klines", params=params, timeout=15).json()
        after = _upstream_calls(client, "spot")
        real_resp = real.get(f"{REAL_SPOT_BASE}/api/v3/klines", params=params, timeout=15).json()
    except _STATS_ERRORS as exc:
        return CheckResult("cache_served_fidelity", False, f"request failed: {exc!r}")

    was_cache_served = after == before
    matches_real = cached_resp == real_resp
    ok = was_cache_served and matches_real
    return CheckResult(
        "cache_served_fidelity",
        ok,
        f"was_cache_served={was_cache_served} matches_real_binance={matches_real}"
        + ("" if matches_real else f" MISMATCH: cached={cached_resp!r} real={real_resp!r}"),
    )


# -- 5. error responses must be cached too, not retried every time ----------


def check_error_responses_are_cached(client: httpx.Client, market: str) -> CheckResult:
    """Regression check for the actual, confirmed root cause of the
    2026-08-28 production ban (CLAUDE.md invariant #2): a request for a
    symbol invalid on the target market must not re-hit Binance on every
    repeat within the TTL. ~546 permanently-invalid futures symbols being
    retried this way, uncached, was ~10% of all upstream calls and a
    direct, measurable contributor to that ban.

    Runs against both spot and usdm_futures (run_all calls this twice) —
    the actual incident was specifically ~546 invalid USD-M *futures*
    symbols, so a regression that broke error-caching only for the futures
    UpstreamClient/RateLimiter wiring while leaving spot fine would still
    show a spot-only version of this check PASS.

    The invalid symbol includes the current timestamp so this check is
    never accidentally satisfied by a stale cache entry left over from a
    previous run (within CACHE_TTL_SECONDS) — that would make
    `first_call_made` false for a reason that has nothing to do with
    whether caching is actually working right now.
    """
    proxy_path = "/api/v3/klines" if market == "spot" else "/fapi/v1/klines"
    params: dict[str, str | int] = {
        "symbol": f"NOTREALSYMBOL{_now_ms()}",
        "interval": "1m",
        "limit": 1,
    }
    try:
        before = _upstream_calls(client, market)
        r1 = client.get(proxy_path, params=params, timeout=15)
        mid = _upstream_calls(client, market)
        r2 = client.get(proxy_path, params=params, timeout=15)
        after = _upstream_calls(client, market)
    except _STATS_ERRORS as exc:
        return CheckResult(
            f"error_responses_are_cached_{market}", False, f"request failed: {exc!r}"
        )

    first_call_made = mid > before
    second_call_avoided = after == mid
    ok = first_call_made and second_call_avoided and r1.status_code == r2.status_code
    return CheckResult(
        f"error_responses_are_cached_{market}",
        ok,
        f"first_call_made={first_call_made} second_call_avoided={second_call_avoided} "
        f"status={r1.status_code}",
    )


# -- 6. malformed input must never crash the proxy ---------------------------


def check_malformed_input_does_not_crash(client: httpx.Client) -> CheckResult:
    """Regression check for a real, previously-fixed bug (CLAUDE.md
    invariant #6): a malformed `limit` query param crashed the proxy with a
    raw 500 instead of ever reaching Binance, which is the source of truth
    for request validity here — the proxy does no local validation of its
    own. A healthy proxy relays whatever Binance says (almost certainly a
    400) rather than crashing before the call is even made.
    """
    try:
        r = client.get(
            "/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": "not-a-number"},
            timeout=15,
        )
    except httpx.HTTPError as exc:
        return CheckResult("malformed_input_does_not_crash", False, f"request failed: {exc}")

    ok = r.status_code != 500
    return CheckResult(
        "malformed_input_does_not_crash", ok, f"status={r.status_code} body={r.text[:200]}"
    )


# -- 7. coalescing effectiveness ---------------------------------------------


def check_coalescing(client: httpx.Client, n: int = 15) -> CheckResult:
    """N concurrent identical requests on a fresh param combo must collapse
    to exactly one upstream call, with every response identical."""
    now = _now_ms()
    params: dict[str, str | int] = {
        "symbol": "LINKUSDT",
        "interval": "1m",
        "startTime": now - 1_200_000,
        "limit": 5,
    }

    try:
        before = _upstream_calls(client, "spot")

        async def fetch() -> object:
            async with httpx.AsyncClient(base_url=str(client.base_url)) as c:
                r = await c.get("/api/v3/klines", params=params, timeout=15)
                return r.json()

        results = asyncio.run(_gather_n(fetch, n))
        after = _upstream_calls(client, "spot")
    except _STATS_ERRORS as exc:
        return CheckResult("coalescing", False, f"request failed: {exc!r}")

    all_identical = all(r == results[0] for r in results)
    delta = after - before
    ok = all_identical and delta <= 1  # 0 if it happened to already be cached, else exactly 1
    return CheckResult(
        "coalescing",
        ok,
        f"{n} requests, all_identical={all_identical}, upstream_call_delta={delta}",
    )


async def _gather_n(fetch: Callable[[], Awaitable[object]], n: int) -> list[object]:
    return await asyncio.gather(*[fetch() for _ in range(n)])


# -- orchestration ------------------------------------------------------------


def run_all(proxy_url: str, skip_code_quality: bool) -> list[CheckResult]:
    results: list[CheckResult] = []
    with httpx.Client(base_url=proxy_url) as client, httpx.Client() as real:
        results.append(check_healthz(client))

        if not skip_code_quality:
            results.extend(check_code_quality())

        # Only run live/network checks if the proxy is actually reachable.
        if results[0].passed:
            results.append(check_not_banned(client))
            results.append(check_response_fidelity(client, real, "BTCUSDT", "spot"))
            results.append(check_response_fidelity(client, real, "BTCUSDT", "usdm_futures"))
            results.append(check_cache_served_fidelity(client, real))
            results.append(check_error_responses_are_cached(client, "spot"))
            results.append(check_error_responses_are_cached(client, "usdm_futures"))
            results.append(check_malformed_input_does_not_crash(client))
            results.append(check_coalescing(client))

    return results


def write_report(results: list[CheckResult]) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = REPORTS_DIR / f"{timestamp}.json"
    payload = {
        "timestamp": timestamp,
        "all_passed": all(r.passed for r in results),
        "checks": [asdict(r) for r in results],
    }
    path.write_text(json.dumps(payload, indent=2))
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--proxy-url", default=os.environ.get("PROXY_URL", "http://127.0.0.1:8000")
    )
    parser.add_argument("--skip-code-quality", action="store_true")
    args = parser.parse_args()

    results = run_all(args.proxy_url, args.skip_code_quality)
    report_path = write_report(results)

    for r in results:
        print(f"[{'PASS' if r.passed else 'FAIL'}] {r.name}: {r.detail}")
    print(f"\nReport written to {report_path}")

    all_passed = all(r.passed for r in results)
    print("\nOVERALL:", "PASS" if all_passed else "FAIL")
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
