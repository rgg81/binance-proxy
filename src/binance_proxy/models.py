"""Core data types shared across the cache, upstream client, and service layer."""

from __future__ import annotations

from enum import StrEnum


class Market(StrEnum):
    SPOT = "spot"
    USDM_FUTURES = "usdm_futures"
