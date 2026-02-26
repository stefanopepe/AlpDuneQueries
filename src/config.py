from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import os


@dataclass
class SignalConfig:
    lookback_days: int = 90
    ls1_hours: int = 1
    ls24_hours: int = 24
    flow_window_days: int = 7
    sample_size: int = 200
    dune_performance: str = "medium"
    chains: tuple[str, ...] = ("ethereum", "base")
    venues: tuple[str, ...] = ("morpho_blue",)
    stable_symbols: tuple[str, ...] = ("USDC", "USDT", "DAI")
    btc_symbols: tuple[str, ...] = ("WBTC", "cbBTC", "tBTC")
    bridge_addresses: dict[str, set[str]] = field(
        default_factory=lambda: {
            "ethereum": {
                "0x3154cf16ccdb4c6d922629664174b904d80f2c35",
            },
            "base": {
                "0x4200000000000000000000000000000000000010",
            },
        }
    )

    @property
    def as_of(self) -> datetime:
        return datetime.now(UTC)

    @property
    def start_time(self) -> datetime:
        return self.as_of - timedelta(days=self.lookback_days)

    @classmethod
    def from_env(cls) -> "SignalConfig":
        cfg = cls()
        cfg.lookback_days = int(os.getenv("LOOKBACK_DAYS", str(cfg.lookback_days)))
        cfg.sample_size = int(os.getenv("SAMPLE_SIZE", str(cfg.sample_size)))
        cfg.dune_performance = os.getenv("DUNE_PERFORMANCE", cfg.dune_performance)
        return cfg
