from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

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


class TradeDerivedFeaturePipeline:
    """Build and emit normalized features derived directly from trades."""

    def __init__(
        self,
        *,
        strategy: object,
        emit_feature: Callable[[MarketFeatureEvent], Awaitable[None]],
        fixed_time_trade_bar_builder: FixedTimeTradeBarBuilder | None = None,
        trade_footprint_builder: TradeFootprintBuilder | None = None,
        range_footprint_builder: RangeFootprintBuilder | None = None,
    ) -> None:
        self._strategy = strategy
        self._emit_feature = emit_feature
        self.fixed_time_trade_bar_builder = fixed_time_trade_bar_builder
        self.trade_footprint_builder = trade_footprint_builder
        self.range_footprint_builder = range_footprint_builder

    async def process_trade(self, trade: MarketTrade) -> None:
        config_provider = getattr(
            self._strategy, "trade_feature_runtime_config", None
        )
        if not callable(config_provider):
            return
        raw_config = config_provider()
        if not isinstance(raw_config, Mapping) or not bool(
            raw_config.get("enabled", False)
        ):
            return

        contract_value = str(raw_config.get("contract_value", "0.01"))
        price_bucket_size = str(raw_config.get("price_bucket_size", "1"))
        if self.fixed_time_trade_bar_builder is None:
            self.fixed_time_trade_bar_builder = FixedTimeTradeBarBuilder(
                contract_value=contract_value,
                large_trade_threshold_notional=str(
                    raw_config.get("large_trade_threshold", "10000")
                ),
            )
        if self.trade_footprint_builder is None:
            self.trade_footprint_builder = TradeFootprintBuilder(
                contract_value=contract_value,
                price_bucket_size=price_bucket_size,
            )
        if self.range_footprint_builder is None:
            self.range_footprint_builder = RangeFootprintBuilder(
                contract_value=contract_value,
                range_pct=str(raw_config.get("range_pct", "0.002")),
                price_step=str(raw_config.get("range_price_step", "1")),
            )

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


def _feed_trade(builder: object, trade: MarketTrade):
    return getattr(builder, "on_" + "trade")(trade)


__all__ = ["TradeDerivedFeaturePipeline"]
