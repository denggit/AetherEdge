from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class TradeFeatureRuntimeConfig:
    enabled: bool = False
    fixed_time_trade_bars_enabled: bool | None = None
    trade_footprint_enabled: bool | None = None
    range_footprint_enabled: bool | None = None
    contract_value: str = "0.01"
    large_trade_threshold: str = "10000"
    price_bucket_size: str = "1"
    range_pct: str = "0.002"
    range_price_step: str = "1"

    def __post_init__(self) -> None:
        legacy = bool(self.enabled)
        fixed = (
            legacy
            if self.fixed_time_trade_bars_enabled is None
            else bool(self.fixed_time_trade_bars_enabled)
        )
        trade = (
            legacy
            if self.trade_footprint_enabled is None
            else bool(self.trade_footprint_enabled)
        )
        range_ = (
            legacy
            if self.range_footprint_enabled is None
            else bool(self.range_footprint_enabled)
        )
        object.__setattr__(self, "fixed_time_trade_bars_enabled", fixed)
        object.__setattr__(self, "trade_footprint_enabled", trade)
        object.__setattr__(self, "range_footprint_enabled", range_)
        object.__setattr__(self, "enabled", fixed or trade or range_)

    @classmethod
    def from_strategy(cls, strategy: object | None) -> "TradeFeatureRuntimeConfig":
        """Adapt the legacy plugin provider exactly once during composition."""

        if strategy is None:
            return cls()
        provider = getattr(strategy, "trade_feature_runtime_config", None)
        if not callable(provider):
            return cls()
        value = provider()
        if not isinstance(value, Mapping):
            return cls()
        return cls(
            enabled=bool(value.get("enabled", False)),
            fixed_time_trade_bars_enabled=(
                bool(value["fixed_time_trade_bars_enabled"])
                if "fixed_time_trade_bars_enabled" in value
                else None
            ),
            trade_footprint_enabled=(
                bool(value["trade_footprint_enabled"])
                if "trade_footprint_enabled" in value
                else None
            ),
            range_footprint_enabled=(
                bool(value["range_footprint_enabled"])
                if "range_footprint_enabled" in value
                else None
            ),
            contract_value=str(value.get("contract_value", "0.01")),
            large_trade_threshold=str(
                value.get("large_trade_threshold", "10000")
            ),
            price_bucket_size=str(value.get("price_bucket_size", "1")),
            range_pct=str(value.get("range_pct", "0.002")),
            range_price_step=str(value.get("range_price_step", "1")),
        )

__all__ = ["TradeFeatureRuntimeConfig"]
