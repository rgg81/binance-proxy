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
"why" behind each one):
  1. The proxy process is up and /healthz responds.
  2. The deployed code is still correct: full pytest + ruff + mypy.
  3. Response fidelity vs. the real Binance API (spot + futures), including
     the specific boundary cases that were previously real, confirmed bugs
     in this project: endTime-inclusive, future-startTime-is-empty.
  4. The disk cache actually prevents redundant upstream calls, for a
     properly historical (non-live-tail-triggering) query.
  5. Concurrent identical requests collapse to one upstream call.
  6. Cache integrity, read directly from SQLite: no still-forming candle
     has leaked into the cache or its coverage index.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "monitoring" / "reports"
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"

sys.path.insert(0, str(REPO_ROOT / "src"))


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def _now_ms() -> int:
    return int(time.time() * 1000)


# -- 1. process / liveness -------------------------------------------------


def check_healthz(client: httpx.Client) -> CheckResult:
    try:
        r = client.get("/healthz", timeout=5)
        ok = r.status_code == 200 and r.json().get("status") == "ok"
        return CheckResult("proxy_healthz", ok, f"status={r.status_code} body={r.text}")
    except httpx.HTTPError as exc:
        return CheckResult("proxy_healthz", False, f"unreachable: {exc}")


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

REAL_SPOT_BASE = "https://api.binance.com"
REAL_FUTURES_BASE = "https://fapi.binance.com"


def check_response_fidelity(
    client: httpx.Client, real: httpx.Client, symbol: str, market: str
) -> CheckResult:
    proxy_path = "/api/v3/klines" if market == "spot" else "/fapi/v1/klines"
    real_base = REAL_SPOT_BASE if market == "spot" else REAL_FUTURES_BASE
    now = _now_ms()
    start, end = now - 3_600_000, now - 3_000_000  # firmly historical, 10 candles

    try:
        proxy_resp = client.get(
            proxy_path,
            params={"symbol": symbol, "interval": "1m", "startTime": start, "endTime": end},
            timeout=15,
        ).json()
        real_resp = real.get(
            f"{real_base}{proxy_path}",
            params={"symbol": symbol, "interval": "1m", "startTime": start, "endTime": end},
            timeout=15,
        ).json()
    except httpx.HTTPError as exc:
        return CheckResult(f"fidelity_{market}_{symbol}", False, f"request failed: {exc}")

    ok = proxy_resp == real_resp
    detail = "byte-identical" if ok else f"MISMATCH: proxy={proxy_resp!r} real={real_resp!r}"
    return CheckResult(f"fidelity_{market}_{symbol}", ok, detail)


def check_end_time_inclusive(client: httpx.Client, real: httpx.Client) -> CheckResult:
    """Regression check for a real, previously-fixed bug: endTime landing
    exactly on a candle boundary must not drop that candle."""
    now = _now_ms()
    base = (now - 7_200_000) // 60_000 * 60_000
    end = base + 1_200_000
    try:
        proxy_resp = client.get(
            "/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "startTime": base, "endTime": end},
            timeout=15,
        ).json()
        real_resp = real.get(
            f"{REAL_SPOT_BASE}/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "startTime": base, "endTime": end},
            timeout=15,
        ).json()
    except httpx.HTTPError as exc:
        return CheckResult("end_time_inclusive", False, f"request failed: {exc}")

    ok = proxy_resp == real_resp
    return CheckResult(
        "end_time_inclusive",
        ok,
        f"proxy={len(proxy_resp) if isinstance(proxy_resp, list) else '?'} candles, "
        f"real={len(real_resp) if isinstance(real_resp, list) else '?'} candles",
    )


def check_future_start_time_empty(client: httpx.Client) -> CheckResult:
    """Regression check for a real, previously-fixed bug: a future startTime
    must return [], not a fabricated current candle."""
    future = _now_ms() + 3_600_000
    try:
        resp = client.get(
            "/api/v3/klines",
            params={"symbol": "XRPUSDT", "interval": "1m", "startTime": future, "limit": 5},
            timeout=15,
        ).json()
    except httpx.HTTPError as exc:
        return CheckResult("future_start_time_empty", False, f"request failed: {exc}")

    ok = resp == []
    return CheckResult("future_start_time_empty", ok, f"got {resp!r}")


# -- 4 & 5. cache-hit and coalescing effectiveness ---------------------------


def _spot_upstream_calls(client: httpx.Client) -> int:
    return int(client.get("/stats", timeout=10).json()["markets"]["spot"]["upstream_calls_made"])


def check_cache_hit(client: httpx.Client) -> CheckResult:
    """A firmly historical, bounded window (well under 'now', small limit)
    must be a zero-upstream-call cache hit on repeat. Deliberately NOT the
    "startTime=today & limit=1000" shape real traffic often uses — that
    shape always needs a live-tail check by construction (see SKILL.md) and
    isn't a useful cache-health signal on its own.
    """
    now = _now_ms()
    start, end = now - 5_400_000, now - 5_000_000
    params: dict[str, str | int] = {
        "symbol": "ADAUSDT",
        "interval": "1m",
        "startTime": start,
        "endTime": end,
    }

    try:
        r1 = client.get("/api/v3/klines", params=params, timeout=15).json()
        before = _spot_upstream_calls(client)
        r2 = client.get("/api/v3/klines", params=params, timeout=15).json()
        after = _spot_upstream_calls(client)
    except httpx.HTTPError as exc:
        return CheckResult("cache_hit", False, f"request failed: {exc}")

    ok = r1 == r2 and after == before
    return CheckResult(
        "cache_hit", ok, f"responses_equal={r1 == r2} upstream_calls_before={before} after={after}"
    )


def check_cache_served_fidelity(client: httpx.Client, real: httpx.Client) -> CheckResult:
    """The check above (`check_cache_hit`) only proves the cache-served
    response is *self-consistent* (identical to what the proxy itself
    returned the first time) — it never compares against real Binance. Every
    other fidelity check below always hits a never-before-queried window
    (since `now` shifts every run), so they only ever exercise the fresh
    Binance-passthrough path, never the SQLite round-trip. Neither on its
    own proves what actually comes out of the cache is correct — a bug in
    Kline (de)serialization, or in how a cached read is reconstructed into
    Binance's array shape, could reproduce itself identically on every read
    and still pass both of those. This check closes that gap directly: it
    forces a cache-served response (confirmed via the same upstream-call-
    delta proof as check_cache_hit) and compares THAT, specifically,
    against an independent real Binance call for the same window.
    """
    now = _now_ms()
    start, end = now - 9_000_000, now - 8_600_000  # a window this check owns exclusively
    params: dict[str, str | int] = {
        "symbol": "DOTUSDT",
        "interval": "1m",
        "startTime": start,
        "endTime": end,
    }

    try:
        client.get("/api/v3/klines", params=params, timeout=15)  # warm the cache
        before = _spot_upstream_calls(client)
        cached_resp = client.get("/api/v3/klines", params=params, timeout=15).json()
        after = _spot_upstream_calls(client)
        real_resp = real.get(
            f"{REAL_SPOT_BASE}/api/v3/klines", params=params, timeout=15
        ).json()
    except httpx.HTTPError as exc:
        return CheckResult("cache_served_fidelity", False, f"request failed: {exc}")

    was_cache_served = after == before
    matches_real = cached_resp == real_resp
    ok = was_cache_served and matches_real
    return CheckResult(
        "cache_served_fidelity",
        ok,
        f"was_cache_served={was_cache_served} matches_real_binance={matches_real}"
        + ("" if matches_real else f" MISMATCH: cached={cached_resp!r} real={real_resp!r}"),
    )


def check_coalescing(client: httpx.Client, n: int = 15) -> CheckResult:
    """N concurrent identical requests on a fresh window must collapse to
    exactly one upstream call, with every response identical."""
    now = _now_ms()
    start = now - 1_800_000
    params: dict[str, str | int] = {
        "symbol": "LINKUSDT",
        "interval": "1m",
        "startTime": start,
        "limit": 5,
    }

    try:
        before = _spot_upstream_calls(client)

        async def fetch() -> object:
            async with httpx.AsyncClient(base_url=str(client.base_url)) as c:
                r = await c.get("/api/v3/klines", params=params, timeout=15)
                return r.json()

        results = asyncio.run(_gather_n(fetch, n))
        after = _spot_upstream_calls(client)
    except httpx.HTTPError as exc:
        return CheckResult("coalescing", False, f"request failed: {exc}")

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


def check_live_candle(client: httpx.Client, real: httpx.Client, db_path: Path) -> CheckResult:
    """Every other check above deliberately avoids the still-forming candle
    (windows are 50min-2.5h in the past) — comparing it byte-for-byte
    against a second, slightly-later Binance call would be flaky by nature:
    a new trade can legitimately land between the two calls, changing
    close/high/volume/trade-count with no bug involved. That's exactly why
    those checks use firmly historical windows instead. But that leaves the
    live-tail fetch path itself — the mechanism that serves "now" — never
    actively exercised by any live check; check_cache_integrity only proves
    nothing bad is *currently* sitting in the DB, not that this code path is
    working right now on the deployed system.

    This check exercises it directly, comparing only what's actually stable
    about an in-progress candle (which one is open, and its opening price —
    fixed the instant the candle starts, unlike close/high/volume) and
    verifying — by reading the DB immediately after — that it was not
    persisted. Uses a spot symbol deliberately: real production traffic is
    100% futures, so there's no risk of this racing against real traffic
    into the same cache rows.
    """
    from binance_proxy.intervals import interval_to_ms

    interval_ms = interval_to_ms("1m")
    symbol = "ETHUSDT"
    now_ms = _now_ms()
    closed_boundary = (now_ms // interval_ms) * interval_ms
    params: dict[str, str | int] = {
        "symbol": symbol,
        "interval": "1m",
        "startTime": closed_boundary,
        "limit": 1,
    }

    try:
        proxy_resp = client.get("/api/v3/klines", params=params, timeout=15).json()
        real_resp = real.get(
            f"{REAL_SPOT_BASE}/api/v3/klines", params=params, timeout=15
        ).json()
    except httpx.HTTPError as exc:
        return CheckResult("live_candle", False, f"request failed: {exc}")

    if not (isinstance(proxy_resp, list) and proxy_resp):
        return CheckResult("live_candle", False, f"expected the open candle, got {proxy_resp!r}")
    if not (isinstance(real_resp, list) and real_resp):
        return CheckResult("live_candle", False, f"real Binance returned {real_resp!r}")

    proxy_open_time, proxy_open_price = proxy_resp[0][0], proxy_resp[0][1]
    real_open_time, real_open_price = real_resp[0][0], real_resp[0][1]
    # Tolerate a one-interval difference: the check's own closed_boundary
    # computation can straddle a real minute rollover between here and the
    # two HTTP calls.
    agrees_with_real = (
        abs(proxy_open_time - real_open_time) <= interval_ms
        and (proxy_open_time != real_open_time or proxy_open_price == real_open_price)
    )

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    (persisted_count,) = conn.execute(
        "SELECT COUNT(*) FROM klines WHERE market='spot' AND symbol=? AND interval='1m' "
        "AND open_time=?",
        (symbol, proxy_open_time),
    ).fetchone()
    conn.close()
    not_persisted = persisted_count == 0

    ok = agrees_with_real and not_persisted
    return CheckResult(
        "live_candle",
        ok,
        f"proxy_open_time={proxy_open_time} real_open_time={real_open_time} "
        f"agrees_with_real={agrees_with_real} not_persisted={not_persisted}",
    )


# -- 6. cache integrity, read directly from disk -----------------------------


def check_cache_integrity(db_path: Path) -> CheckResult:
    from binance_proxy.intervals import interval_to_ms

    if not db_path.exists():
        return CheckResult("cache_integrity", False, f"db not found at {db_path}")

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    now_ms = _now_ms()
    problems = []

    series = conn.execute(
        "SELECT DISTINCT market, symbol, interval, timezone FROM klines"
    ).fetchall()
    for market, symbol, interval, timezone in series:
        if timezone != "0" or interval == "1M":
            continue  # not covered by the boundary invariant (bypassed by design)
        try:
            interval_ms = interval_to_ms(interval)
        except ValueError:
            continue
        closed_boundary = (now_ms // interval_ms) * interval_ms
        (bad_count,) = conn.execute(
            "SELECT COUNT(*) FROM klines WHERE market=? AND symbol=? AND interval=? "
            "AND timezone=? AND open_time >= ?",
            (market, symbol, interval, timezone, closed_boundary),
        ).fetchone()
        if bad_count:
            problems.append(
                f"{market}/{symbol}/{interval}: {bad_count} row(s) at/after closed_boundary "
                f"({closed_boundary}) — an unclosed candle may have been cached"
            )

    coverage_rows = conn.execute(
        "SELECT market, symbol, interval, timezone, range_start, range_end FROM coverage"
    ).fetchall()
    for market, symbol, interval, timezone, start, end in coverage_rows:
        if start >= end:
            problems.append(
                f"{market}/{symbol}/{interval}/{timezone}: malformed coverage range "
                f"({start}, {end})"
            )

    conn.close()
    ok = not problems
    detail = "clean" if ok else "; ".join(problems)
    return CheckResult("cache_integrity", ok, detail)


# -- orchestration ------------------------------------------------------------


def run_all(proxy_url: str, skip_code_quality: bool, db_path: Path) -> list[CheckResult]:
    results: list[CheckResult] = []
    with httpx.Client(base_url=proxy_url) as client, httpx.Client() as real:
        results.append(check_healthz(client))

        if not skip_code_quality:
            results.extend(check_code_quality())

        # Only run live/network checks if the proxy is actually reachable.
        if results[0].passed:
            results.append(check_response_fidelity(client, real, "BTCUSDT", "spot"))
            results.append(check_response_fidelity(client, real, "BTCUSDT", "usdm_futures"))
            results.append(check_end_time_inclusive(client, real))
            results.append(check_future_start_time_empty(client))
            results.append(check_cache_hit(client))
            results.append(check_cache_served_fidelity(client, real))
            results.append(check_coalescing(client))
            results.append(check_live_candle(client, real, db_path))

        results.append(check_cache_integrity(db_path))

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
    parser.add_argument(
        "--db-path", default=str(REPO_ROOT / "data" / "klines.db"), type=Path
    )
    args = parser.parse_args()

    results = run_all(args.proxy_url, args.skip_code_quality, args.db_path)
    report_path = write_report(results)

    for r in results:
        print(f"[{'PASS' if r.passed else 'FAIL'}] {r.name}: {r.detail}")
    print(f"\nReport written to {report_path}")

    all_passed = all(r.passed for r in results)
    print("\nOVERALL:", "PASS" if all_passed else "FAIL")
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
