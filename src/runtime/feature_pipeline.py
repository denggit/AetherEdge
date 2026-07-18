from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Optional, Protocol

from src.market_data.derived import (
    FixedTimeTradeBarBuilder,
    RangeFootprintBuilder,
    TradeFootprintBuilder,
)
from src.market_data.events import MarketFeatureEvent
from src.platform.data.models import MarketTrade
from src.runtime.features import (
    fixed_time_trade_bar_feature,
    range_footprint_feature,
    trade_footprint_feature,
)


class _TradeFeatureBuilder(Protocol):
    def on_trade(self, trade: MarketTrade) -> Sequence[object]: ...


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


class TradeDerivedFeaturePipeline:
    """Build and emit normalized features derived directly from trades."""

    def __init__(
        self,
        *,
        strategy: object | None = None,
        config: TradeFeatureRuntimeConfig | None = None,
        emit_feature: Callable[[MarketFeatureEvent], Awaitable[None]],
        fixed_time_trade_bar_builder: FixedTimeTradeBarBuilder | None = None,
        trade_footprint_builder: TradeFootprintBuilder | None = None,
        range_footprint_builder: RangeFootprintBuilder | None = None,
    ) -> None:
        self._config = config or TradeFeatureRuntimeConfig.from_strategy(strategy)
        self._emit_feature = emit_feature
        self.fixed_time_trade_bar_builder = fixed_time_trade_bar_builder
        self.trade_footprint_builder = trade_footprint_builder
        self.range_footprint_builder = range_footprint_builder
        if not self._config.enabled:
            return
        if (
            self._config.fixed_time_trade_bars_enabled
            and self.fixed_time_trade_bar_builder is None
        ):
            self.fixed_time_trade_bar_builder = FixedTimeTradeBarBuilder(
                contract_value=self._config.contract_value,
                large_trade_threshold_notional=(
                    self._config.large_trade_threshold
                ),
            )
        if (
            self._config.trade_footprint_enabled
            and self.trade_footprint_builder is None
        ):
            self.trade_footprint_builder = TradeFootprintBuilder(
                contract_value=self._config.contract_value,
                price_bucket_size=self._config.price_bucket_size,
            )
        if (
            self._config.range_footprint_enabled
            and self.range_footprint_builder is None
        ):
            self.range_footprint_builder = RangeFootprintBuilder(
                contract_value=self._config.contract_value,
                range_pct=self._config.range_pct,
                price_step=self._config.range_price_step,
            )

    async def process_trade(self, trade: MarketTrade) -> None:
        if not self._config.enabled:
            return

        range_features = _feed_trade(self.range_footprint_builder, trade)
        tradebars = _feed_trade(self.fixed_time_trade_bar_builder, trade)
        footprints = _feed_trade(self.trade_footprint_builder, trade)
        for feature in range_features:
            await self._emit_feature(
                range_footprint_feature(feature, exchange=trade.exchange)
            )
        for bar in tradebars:
            await self._emit_feature(
                fixed_time_trade_bar_feature(
                    bar,
                    exchange=trade.exchange,
                    next_open_price=trade.price,
                    next_open_time_ms=(
                        trade.trade_time_ms or trade.event_time_ms
                    ),
                )
            )
        for feature in footprints:
            await self._emit_feature(
                trade_footprint_feature(feature, exchange=trade.exchange)
            )


def _feed_trade(
    builder: Optional[_TradeFeatureBuilder],
    trade: MarketTrade,
) -> Sequence[object]:
    return () if builder is None else builder.on_trade(trade)


__all__ = ["TradeDerivedFeaturePipeline", "TradeFeatureRuntimeConfig"]
