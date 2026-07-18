from __future__ import annotations

from dataclasses import dataclass

from src.runtime.feature_pipeline import TradeFeatureRuntimeConfig
from src.runtime.module import CapabilityId
from src.runtime.requirements import StrategyRuntimeRequirements


MARKET_TRADES = CapabilityId("market.trades")
MARKET_ORDER_BOOK = CapabilityId("market.order_book")
MARKET_CLOSED_KLINES = CapabilityId("market.closed_klines")
ACCOUNT_PRIVATE_EVENTS = CapabilityId("account.private_events")
ACCOUNT_SNAPSHOT = CapabilityId("account.snapshot")
ACCOUNT_POLL = CapabilityId("account.poll")
ORDER_POLL = CapabilityId("orders.poll")
FEATURE_RANGE_BARS = CapabilityId("feature.range_bars")
FEATURE_FIXED_TIME_TRADE_BARS = CapabilityId(
    "feature.fixed_time_trade_bars"
)
FEATURE_TRADE_FOOTPRINT = CapabilityId("feature.trade_footprint")
FEATURE_RANGE_FOOTPRINT = CapabilityId("feature.range_footprint")


@dataclass(frozen=True)
class CapabilityRequest:
    capabilities: frozenset[CapabilityId]
    trade_features: TradeFeatureRuntimeConfig


def capability_request_from_requirements(
    requirements: StrategyRuntimeRequirements,
    *,
    trade_features: TradeFeatureRuntimeConfig | None = None,
) -> CapabilityRequest:
    """Translate the public strategy manifest into architecture capabilities."""

    requested: set[CapabilityId] = set()
    if requirements.trades.enabled and requirements.trades.stream_enabled:
        requested.add(MARKET_TRADES)
    if (
        requirements.order_book.enabled
        and requirements.order_book.stream_enabled
    ):
        requested.add(MARKET_ORDER_BOOK)
    if requirements.closed_kline.enabled:
        requested.add(MARKET_CLOSED_KLINES)
    if requirements.range_bars.enabled:
        requested.add(FEATURE_RANGE_BARS)
    if requirements.private_account_stream.enabled:
        requested.add(ACCOUNT_PRIVATE_EVENTS)
    if requirements.account_state.startup_snapshot_enabled:
        requested.add(ACCOUNT_SNAPSHOT)
    if requirements.account_state.poll_enabled:
        requested.add(ACCOUNT_POLL)
    if requirements.order_state.poll_when_position_enabled:
        requested.add(ORDER_POLL)

    resolved_trade_features = trade_features or TradeFeatureRuntimeConfig()
    if resolved_trade_features.fixed_time_trade_bars_enabled:
        requested.add(FEATURE_FIXED_TIME_TRADE_BARS)
    if resolved_trade_features.trade_footprint_enabled:
        requested.add(FEATURE_TRADE_FOOTPRINT)
    if resolved_trade_features.range_footprint_enabled:
        requested.add(FEATURE_RANGE_FOOTPRINT)

    return CapabilityRequest(
        capabilities=frozenset(requested),
        trade_features=resolved_trade_features,
    )


__all__ = [
    "ACCOUNT_POLL",
    "ACCOUNT_PRIVATE_EVENTS",
    "ACCOUNT_SNAPSHOT",
    "CapabilityRequest",
    "FEATURE_FIXED_TIME_TRADE_BARS",
    "FEATURE_RANGE_BARS",
    "FEATURE_RANGE_FOOTPRINT",
    "FEATURE_TRADE_FOOTPRINT",
    "MARKET_CLOSED_KLINES",
    "MARKET_ORDER_BOOK",
    "MARKET_TRADES",
    "ORDER_POLL",
    "capability_request_from_requirements",
]
