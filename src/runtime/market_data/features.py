from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

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
from src.runtime.market_data.integrity import TradeDataIntegrityTracker
from src.runtime.module import CapabilityId, ModuleHealth, ModuleState


FeaturePublisher = Callable[[MarketFeatureEvent], Awaitable[None]]


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
        integrity: TradeDataIntegrityTracker | None = None,
    ) -> None:
        self.module_id = module_id
        self.provides = frozenset({capability})
        self.requires = frozenset({MARKET_TRADES})
        self._state = ModuleState.CREATED
        self._integrity = integrity or TradeDataIntegrityTracker()
        self._error: BaseException | None = None
        self.events_seen = 0
        self.features_emitted = 0
        self.features_suppressed = 0
        self.last_invalid_reason: str | None = None

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
                ("features_suppressed", str(self.features_suppressed)),
                ("events_dropped", "0"),
                (
                    "data_complete",
                    str(self.last_invalid_reason is None).lower(),
                ),
                ("invalid_reason", self.last_invalid_reason or ""),
            ),
        )

    async def _process_trade(self, trade: MarketTrade) -> None:
        self.events_seen += 1
        await self.process_trade(trade)

    async def process_trade(self, trade: MarketTrade) -> None:
        raise NotImplementedError

    def mark_failed(self, exc: BaseException) -> None:
        self._error = exc
        self._state = ModuleState.ERROR

    def _window_is_complete(self, start_ms: int, end_ms: int) -> bool:
        reason = self._integrity.invalid_reason(start_ms, end_ms)
        if reason is None:
            self.last_invalid_reason = None
            return True
        self.last_invalid_reason = reason
        self.features_suppressed += 1
        return False


class FixedTimeTradeBarModule(_TradeFeatureModule):
    def __init__(
        self,
        *,
        config: FixedTimeTradeBarModuleConfig,
        publish: FeaturePublisher | None = None,
        builder: Any | None = None,
        integrity: TradeDataIntegrityTracker | None = None,
    ) -> None:
        super().__init__(
            module_id="fixed-time-trade-bars",
            capability=FEATURE_FIXED_TIME_TRADE_BARS,
            integrity=integrity,
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
            if not self._window_is_complete(
                int(getattr(bar, "open_time_ms", _trade_time_ms(trade))),
                int(getattr(bar, "close_time_ms", _trade_time_ms(trade))),
            ):
                continue
            self.features_emitted += 1
            if self._publish is not None:
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


class TradeFootprintModule(_TradeFeatureModule):
    def __init__(
        self,
        *,
        config: TradeFootprintModuleConfig,
        publish: FeaturePublisher | None = None,
        builder: Any | None = None,
        integrity: TradeDataIntegrityTracker | None = None,
    ) -> None:
        super().__init__(
            module_id="trade-footprint",
            capability=FEATURE_TRADE_FOOTPRINT,
            integrity=integrity,
        )
        self.config = config
        self._publish = publish
        self._builder = builder or TradeFootprintBuilder(
            contract_value=config.contract_value,
            price_bucket_size=config.price_bucket_size,
        )

    async def process_trade(self, trade: MarketTrade) -> None:
        for feature in self._builder.on_trade(trade):
            if not self._window_is_complete(
                int(getattr(feature, "open_time_ms", _trade_time_ms(trade))),
                int(getattr(feature, "close_time_ms", _trade_time_ms(trade))),
            ):
                continue
            self.features_emitted += 1
            if self._publish is not None:
                await self._publish(
                    trade_footprint_feature(feature, exchange=trade.exchange)
                )


class RangeFootprintModule(_TradeFeatureModule):
    def __init__(
        self,
        *,
        config: RangeFootprintModuleConfig,
        publish: FeaturePublisher | None = None,
        builder: Any | None = None,
        integrity: TradeDataIntegrityTracker | None = None,
    ) -> None:
        super().__init__(
            module_id="range-footprint",
            capability=FEATURE_RANGE_FOOTPRINT,
            integrity=integrity,
        )
        self.config = config
        self._publish = publish
        self._builder = builder or RangeFootprintBuilder(
            contract_value=config.contract_value,
            range_pct=config.range_pct,
            price_step=config.price_step,
        )
        self._integrity_revision = self._integrity.revision

    async def process_trade(self, trade: MarketTrade) -> None:
        if self._integrity.revision != self._integrity_revision:
            discard = getattr(self._builder, "discard_active", None)
            has_active = bool(
                getattr(self._builder, "has_active_range", False)
            )
            if has_active and callable(discard):
                discard()
                issues = self._integrity.issues_since(
                    self._integrity_revision
                )
                self.features_suppressed += 1
                self.last_invalid_reason = (
                    issues[-1].reason if issues else "trade_data_incomplete"
                )
            self._integrity_revision = self._integrity.revision
        for feature in self._builder.on_trade(trade):
            if not self._window_is_complete(
                int(
                    getattr(feature, "range_start_ms", _trade_time_ms(trade))
                ),
                int(
                    getattr(feature, "range_end_ms", _trade_time_ms(trade))
                ),
            ):
                continue
            self.features_emitted += 1
            if self._publish is not None:
                await self._publish(
                    range_footprint_feature(feature, exchange=trade.exchange)
                )


def _trade_time_ms(trade: MarketTrade) -> int:
    value = trade.trade_time_ms or trade.event_time_ms
    return 0 if value is None else int(value)


__all__ = [
    "FeaturePublisher",
    "FixedTimeTradeBarModule",
    "FixedTimeTradeBarModuleConfig",
    "RangeFootprintModule",
    "RangeFootprintModuleConfig",
    "TradeFootprintModule",
    "TradeFootprintModuleConfig",
]
