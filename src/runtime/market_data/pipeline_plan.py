from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from src.platform.data.models import MarketKline
from src.runtime.requirements import StrategyRuntimeRequirements


@dataclass(frozen=True)
class ResolvedMarketPipelinePlan:
    trades_enabled: bool
    closed_kline_enabled: bool
    order_book_enabled: bool
    enabled_module_ids: tuple[str, ...]

    @property
    def ordered_trade_module_ids(self) -> tuple[str, ...]:
        return tuple(
            module_id
            for module_id in self.enabled_module_ids
            if module_id != "trade-stream"
        )


@dataclass
class ClosedBarControlEvent:
    open_time_ms: int
    kline: MarketKline
    started: bool = False
    result: object | None = None
    _completion: asyncio.Future | None = field(default=None, repr=False)

    @property
    def completion(self) -> asyncio.Future:
        if self._completion is None:
            self._completion = asyncio.get_running_loop().create_future()
        return self._completion


_DEFAULT_ORDER = (
    "range-footprint",
    "fixed-time-trade-bars",
    "trade-footprint",
    "range-bars",
    "raw-trade-callback",
)


def resolve_market_pipeline(
    requirements: StrategyRuntimeRequirements,
    *,
    feature_config: object | None = None,
) -> ResolvedMarketPipelinePlan:
    raw_trades = requirements.trades.enabled and requirements.trades.stream_enabled
    features = []
    for attribute, module_id in (
        ("range_footprint_enabled", "range-footprint"),
        ("fixed_time_trade_bars_enabled", "fixed-time-trade-bars"),
        ("trade_footprint_enabled", "trade-footprint"),
    ):
        if feature_config is not None and getattr(feature_config, attribute, False):
            features.append(module_id)

    trades_enabled = bool(raw_trades or requirements.range_bars.enabled or features)
    enabled = (["trade-stream"] if trades_enabled else []) + features
    if requirements.range_bars.enabled:
        enabled.append("range-bars")
    if raw_trades:
        enabled.append("raw-trade-callback")
    enabled_set = frozenset(enabled)
    ordered = tuple(
        module_id for module_id in _DEFAULT_ORDER if module_id in enabled_set
    )
    return ResolvedMarketPipelinePlan(
        trades_enabled=trades_enabled,
        closed_kline_enabled=requirements.closed_kline.enabled,
        order_book_enabled=(
            requirements.order_book.enabled
            and requirements.order_book.stream_enabled
        ),
        enabled_module_ids=(
            (("trade-stream",) if trades_enabled else ()) + ordered
        ),
    )


__all__ = [
    "ClosedBarControlEvent",
    "ResolvedMarketPipelinePlan",
    "resolve_market_pipeline",
]
