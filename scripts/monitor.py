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
  2. The deployed code is still correct: full pytest + ruff + mypy.
  3. Response fidelity vs. the real Binance API (spot + futures) — the
     proxy forwards params verbatim and must relay Binance's answer
     unchanged.
  4. A repeated identical request within the TTL is served from cache
     (zero new upstream calls) and matches real Binance — proves the cache
     round-trip doesn't corrupt anything, not just that it's self-consistent.
  5. Concurrent identical requests collapse to one upstream call.
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


def _spot_upstream_calls(client: httpx.Client) -> int:
    return int(client.get("/stats", timeout=10).json()["markets"]["spot"]["upstream_calls_made"])


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
        before = _spot_upstream_calls(client)
        cached_resp = client.get("/api/v3/klines", params=params, timeout=15).json()
        after = _spot_upstream_calls(client)
        real_resp = real.get(f"{REAL_SPOT_BASE}/api/v3/klines", params=params, timeout=15).json()
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


# -- 5. coalescing effectiveness ---------------------------------------------


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


# -- orchestration ------------------------------------------------------------


def run_all(proxy_url: str, skip_code_quality: bool) -> list[CheckResult]:
    results: list[CheckResult] = []
    with httpx.Client(base_url=proxy_url) as client, httpx.Client() as real:
        results.append(check_healthz(client))

        if not skip_code_quality:
            results.extend(check_code_quality())

        # Only run live/network checks if the proxy is actually reachable.
        if results[0].passed:
            results.append(check_response_fidelity(client, real, "BTCUSDT", "spot"))
            results.append(check_response_fidelity(client, real, "BTCUSDT", "usdm_futures"))
            results.append(check_cache_served_fidelity(client, real))
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
