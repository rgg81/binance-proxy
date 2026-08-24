"""Application configuration, loaded from environment variables / .env."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    spot_base_url: str = "https://api.binance.com"
    futures_base_url: str = "https://fapi.binance.com"

    data_dir: Path = Path("./data")

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

    @property
    def db_path(self) -> Path:
        return self.data_dir / "klines.db"


settings = Settings()
