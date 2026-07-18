from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from src.market_data.derived import (
    FixedTimeTradeBarBuilder,
    RangeFootprintBuilder,
    TradeFootprintBuilder,
)
from src.market_data.events import MarketFeatureEvent
from src.platform.data.models import MarketTrade
from src.runtime.capabilities import (
    FEATURE_FIXED_TIME_TRADE_BARS,
    FEATURE_RANGE_FOOTPRINT,
    FEATURE_TRADE_FOOTPRINT,
    MARKET_TRADES,
)
from src.runtime.features import (
    fixed_time_trade_bar_feature,
    range_footprint_feature,
    trade_footprint_feature,
)
from src.runtime.market_data.dispatcher import BoundedOrderedEventDispatcher
from src.runtime.module import CapabilityId, ModuleHealth, ModuleState


FeaturePublisher = Callable[[MarketFeatureEvent], Awaitable[None]]


class TradeFeatureBuilder(Protocol):
    def on_trade(self, trade: MarketTrade) -> Sequence[object]: ...


@dataclass(frozen=True)
class FixedTimeTradeBarModuleConfig:
    contract_value: str = "0.01"
    large_trade_threshold_notional: str = "10000"


@dataclass(frozen=True)
class TradeFootprintModuleConfig:
    contract_value: str = "0.01"
    price_bucket_size: str = "1"


@dataclass(frozen=True)
class RangeFootprintModuleConfig:
    contract_value: str = "0.01"
    range_pct: str = "0.002"
    price_step: str = "1"


class _TradeFeatureModule:
    def __init__(
        self,
        *,
        module_id: str,
        capability: CapabilityId,
        dispatcher: BoundedOrderedEventDispatcher[MarketTrade],
    ) -> None:
        self.module_id = module_id
        self.provides = frozenset({capability})
        self.requires = frozenset({MARKET_TRADES})
        self._state = ModuleState.CREATED
        self._error: BaseException | None = None
        self.events_seen = 0
        self.features_emitted = 0
        dispatcher.subscribe(
            subscriber_id=module_id,
            handler=self._process_trade,
            on_error=self._on_dispatch_error,
        )

    async def prepare(self) -> None:
        self._state = ModuleState.PREPARED

    async def start(self) -> None:
        self._state = ModuleState.RUNNING

    async def stop(self) -> None:
        self._state = ModuleState.STOPPED

    def health(self) -> ModuleHealth:
        return ModuleHealth(
            module_id=self.module_id,
            state=self._state,
            healthy=self._error is None,
            detail=(
                None
                if self._error is None
                else f"{type(self._error).__name__}: {self._error}"
            ),
            metadata=(
                ("events_seen", str(self.events_seen)),
                ("features_emitted", str(self.features_emitted)),
            ),
        )

    async def _process_trade(self, trade: MarketTrade) -> None:
        self.events_seen += 1
        await self.process_trade(trade)

    async def process_trade(self, trade: MarketTrade) -> None:
        raise NotImplementedError

    def _on_dispatch_error(
        self,
        _subscriber_id: str,
        exc: BaseException,
    ) -> None:
        self._error = exc
        self._state = ModuleState.ERROR


class FixedTimeTradeBarModule(_TradeFeatureModule):
    def __init__(
        self,
        *,
        config: FixedTimeTradeBarModuleConfig,
        dispatcher: BoundedOrderedEventDispatcher[MarketTrade],
        publish: FeaturePublisher,
        builder: TradeFeatureBuilder | None = None,
    ) -> None:
        super().__init__(
            module_id="fixed-time-trade-bars",
            capability=FEATURE_FIXED_TIME_TRADE_BARS,
            dispatcher=dispatcher,
        )
        self.config = config
        self._publish = publish
        self._builder = builder or FixedTimeTradeBarBuilder(
            contract_value=config.contract_value,
            large_trade_threshold_notional=(
                config.large_trade_threshold_notional
            ),
        )

    async def process_trade(self, trade: MarketTrade) -> None:
        for bar in self._builder.on_trade(trade):
            await self._publish(
                fixed_time_trade_bar_feature(
                    bar,
                    exchange=trade.exchange,
                    next_open_price=trade.price,
                    next_open_time_ms=(
                        trade.trade_time_ms or trade.event_time_ms
                    ),
                )
            )
            self.features_emitted += 1


class TradeFootprintModule(_TradeFeatureModule):
    def __init__(
        self,
        *,
        config: TradeFootprintModuleConfig,
        dispatcher: BoundedOrderedEventDispatcher[MarketTrade],
        publish: FeaturePublisher,
        builder: TradeFeatureBuilder | None = None,
    ) -> None:
        super().__init__(
            module_id="trade-footprint",
            capability=FEATURE_TRADE_FOOTPRINT,
            dispatcher=dispatcher,
        )
        self.config = config
        self._publish = publish
        self._builder = builder or TradeFootprintBuilder(
            contract_value=config.contract_value,
            price_bucket_size=config.price_bucket_size,
        )

    async def process_trade(self, trade: MarketTrade) -> None:
        for feature in self._builder.on_trade(trade):
            await self._publish(
                trade_footprint_feature(feature, exchange=trade.exchange)
            )
            self.features_emitted += 1


class RangeFootprintModule(_TradeFeatureModule):
    def __init__(
        self,
        *,
        config: RangeFootprintModuleConfig,
        dispatcher: BoundedOrderedEventDispatcher[MarketTrade],
        publish: FeaturePublisher,
        builder: TradeFeatureBuilder | None = None,
    ) -> None:
        super().__init__(
            module_id="range-footprint",
            capability=FEATURE_RANGE_FOOTPRINT,
            dispatcher=dispatcher,
        )
        self.config = config
        self._publish = publish
        self._builder = builder or RangeFootprintBuilder(
            contract_value=config.contract_value,
            range_pct=config.range_pct,
            price_step=config.price_step,
        )

    async def process_trade(self, trade: MarketTrade) -> None:
        for feature in self._builder.on_trade(trade):
            await self._publish(
                range_footprint_feature(feature, exchange=trade.exchange)
            )
            self.features_emitted += 1


__all__ = [
    "FeaturePublisher",
    "FixedTimeTradeBarModule",
    "FixedTimeTradeBarModuleConfig",
    "RangeFootprintModule",
    "RangeFootprintModuleConfig",
    "TradeFootprintModule",
    "TradeFootprintModuleConfig",
]
