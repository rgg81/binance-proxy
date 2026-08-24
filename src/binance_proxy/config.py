"""Application configuration, loaded from environment variables / .env."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    spot_base_url: str = "https://api.binance.com"
    futures_base_url: str = "https://fapi.binance.com"

    # How long a cached response stays valid before being treated as stale
    # and re-fetched. There is no persistence and no history — this is the
    # entire cache policy.
    cache_ttl_seconds: float = 60.0
    cache_max_entries: int = 5000

    # Fraction of Binance's per-minute weight budget we allow ourselves to
    # use before proactively throttling outbound requests.
    rate_limit_safety_margin: float = 0.8

    # Conservative defaults for Binance's per-IP weight budgets. These are a
    # local safety net only — the actual source of truth is the
    # X-MBX-USED-WEIGHT-* header on every response (see RateLimiter). Check
    # Binance's current published limits and override via env if needed;
    # these numbers do drift over time.
    spot_weight_budget_per_minute: int = 6000
    futures_weight_budget_per_minute: int = 2400

    host: str = "0.0.0.0"
    port: int = 8000

    log_level: str = "INFO"


settings = Settings()
