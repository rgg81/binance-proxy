"""Per-market weight-aware throttle + circuit breaker for outbound Binance calls.

Two independent instances are used in practice — one for spot, one for
USD-M futures — since they have separate weight budgets and separate bans.

Clock and sleep are injectable so the throttling/backoff logic can be tested
without real waiting.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping

NowFn = Callable[[], float]
SleepFn = Callable[[float], Awaitable[None]]

_DEFAULT_BAN_SECONDS = 120.0  # used when a 418 arrives with no Retry-After header
_DEFAULT_429_BACKOFF_SECONDS = 5.0


def _find_used_weight_header(headers: Mapping[str, str]) -> int | None:
    """Binance reports used weight as `X-MBX-USED-WEIGHT-<window>`, e.g.
    `X-MBX-USED-WEIGHT-1M`. The exact casing/window varies by market, so we
    scan case-insensitively for the family rather than one exact key.
    """
    for key, value in headers.items():
        if key.lower().startswith("x-mbx-used-weight"):
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    return None


class RateLimiter:
    def __init__(
        self,
        *,
        budget_per_window: int,
        window_seconds: float,
        safety_margin: float,
        now_fn: NowFn,
        sleep_fn: SleepFn,
    ) -> None:
        self._budget = budget_per_window
        self._window_seconds = window_seconds
        self._safety_margin = safety_margin
        self._now = now_fn
        self._sleep = sleep_fn

        self._window_start = self._now()
        self._used_weight = 0
        self._banned_until: float | None = None
        self._lock = asyncio.Lock()

    def _usable_budget(self) -> float:
        return self._budget * self._safety_margin

    def _roll_window_if_expired(self) -> None:
        if self._now() - self._window_start >= self._window_seconds:
            self._window_start = self._now()
            self._used_weight = 0

    def used_weight(self) -> int:
        """Current tracked weight usage in the active window (for /stats)."""
        self._roll_window_if_expired()
        return self._used_weight

    def is_banned(self) -> bool:
        return self._banned_until is not None and self._now() < self._banned_until

    def seconds_until_unbanned(self) -> float:
        if self._banned_until is None:
            return 0.0
        return max(0.0, self._banned_until - self._now())

    async def acquire(self, weight: int) -> None:
        """Block (via the injected sleeper) until it's safe to spend `weight`."""
        async with self._lock:
            while True:
                if self.is_banned():
                    await self._sleep(self.seconds_until_unbanned())
                    continue

                self._roll_window_if_expired()
                if self._used_weight + weight > self._usable_budget():
                    remaining = self._window_seconds - (self._now() - self._window_start)
                    await self._sleep(max(remaining, 0.0))
                    continue

                self._used_weight += weight
                return

    def on_response(self, status_code: int, headers: Mapping[str, str]) -> None:
        """Reconcile weight usage and trip the breaker based on a real response."""
        header_weight = _find_used_weight_header(headers)
        if header_weight is not None:
            self._roll_window_if_expired()
            # The header is ground truth for "how much has actually been
            # used this window" — never let a stale lower local estimate
            # under-report it.
            self._used_weight = max(self._used_weight, header_weight)

        if status_code == 429:
            retry_after = self._parse_retry_after(headers, default=_DEFAULT_429_BACKOFF_SECONDS)
            self._trip(retry_after)
        elif status_code == 418:
            retry_after = self._parse_retry_after(headers, default=_DEFAULT_BAN_SECONDS)
            self._trip(retry_after)

    def _trip(self, retry_after_seconds: float) -> None:
        candidate = self._now() + retry_after_seconds
        if self._banned_until is None or candidate > self._banned_until:
            self._banned_until = candidate

    @staticmethod
    def _parse_retry_after(headers: Mapping[str, str], *, default: float) -> float:
        for key, value in headers.items():
            if key.lower() == "retry-after":
                try:
                    return float(value)
                except (TypeError, ValueError):
                    break
        return default
