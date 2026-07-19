from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from src.platform.data.models import MarketOrderBook, MarketTrade
from src.platform.data.websocket.ports import OrderBookStream, TradeStream
from src.runtime.capabilities import (
    FEATURE_FIXED_TIME_TRADE_BARS,
    FEATURE_RANGE_BARS,
    FEATURE_RANGE_FOOTPRINT,
    FEATURE_TRADE_FOOTPRINT,
    MARKET_ORDER_BOOK,
    MARKET_TRADES,
)
from src.runtime.market_data.dispatcher import (
    BoundedEventDispatcher,
    BoundedOrderedEventDispatcher,
)
from src.runtime.market_data.features import (
    FeaturePublisher,
    FixedTimeTradeBarModule,
    FixedTimeTradeBarModuleConfig,
    RangeFootprintModule,
    RangeFootprintModuleConfig,
    TradeFootprintModule,
    TradeFootprintModuleConfig,
)
from src.runtime.market_data.integrity import (
    OrderBookDataIntegrityTracker,
    TradeDataIntegrityTracker,
)
from src.runtime.market_data.sources import (
    OrderBookStreamModule,
    TradeStreamModule,
)
from src.runtime.registry import ModuleDefinition, ModuleRegistry
from src.runtime.module import RuntimeModule


TradeStreamFactory = Callable[[], TradeStream]
OrderBookStreamFactory = Callable[[], OrderBookStream]
RangeModuleFactory = Callable[
    [BoundedOrderedEventDispatcher[MarketTrade]], RuntimeModule
]
TradeConsumer = Callable[[MarketTrade], Awaitable[None] | None]
OrderBookConsumer = Callable[[MarketOrderBook], Awaitable[None] | None]
DroppedTradeConsumer = Callable[[MarketTrade], Awaitable[None] | None]


@dataclass(frozen=True)
class MarketDataModuleConfig:
    trade_queue_maxsize: int = 1_000
    order_book_queue_maxsize: int = 100
    fixed_time_trade_bars: FixedTimeTradeBarModuleConfig = field(
        default_factory=FixedTimeTradeBarModuleConfig
    )
    trade_footprint: TradeFootprintModuleConfig = field(
        default_factory=TradeFootprintModuleConfig
    )
    range_footprint: RangeFootprintModuleConfig = field(
        default_factory=RangeFootprintModuleConfig
    )


def build_market_data_registry(
    *,
    create_trade_stream: TradeStreamFactory,
    create_order_book_stream: OrderBookStreamFactory,
    publish_feature: FeaturePublisher,
    config: MarketDataModuleConfig = MarketDataModuleConfig(),
    create_range_module: RangeModuleFactory | None = None,
    trade_dispatcher: BoundedOrderedEventDispatcher[MarketTrade] | None = None,
    order_book_dispatcher: BoundedEventDispatcher[MarketOrderBook] | None = None,
    consume_trade: TradeConsumer | None = None,
    consume_dropped_trade: DroppedTradeConsumer | None = None,
    consume_order_book: OrderBookConsumer | None = None,
    trade_integrity: TradeDataIntegrityTracker | None = None,
    order_book_integrity: OrderBookDataIntegrityTracker | None = None,
) -> ModuleRegistry:
    """Build lazy definitions; no stream, task or store is created here."""

    trade_dispatcher = trade_dispatcher or BoundedOrderedEventDispatcher[MarketTrade](
        maxsize=config.trade_queue_maxsize,
    )
    order_book_dispatcher = order_book_dispatcher or BoundedEventDispatcher[MarketOrderBook]()
    trade_integrity = trade_integrity or TradeDataIntegrityTracker()
    order_book_integrity = order_book_integrity or OrderBookDataIntegrityTracker()
    if consume_trade is not None:
        trade_dispatcher.subscribe(
            subscriber_id="runtime-trade-consumer",
            handler=consume_trade,
            order=500,
        )
    if consume_order_book is not None:
        order_book_dispatcher.subscribe(
            subscriber_id="runtime-order-book-consumer",
            handler=consume_order_book,
            maxsize=config.order_book_queue_maxsize,
        )
    registry = ModuleRegistry()
    registry.register(
        ModuleDefinition(
            module_id="trade-stream",
            provides=frozenset({MARKET_TRADES}),
            requires=frozenset(),
            factory=lambda: TradeStreamModule(
                stream=create_trade_stream(),
                dispatcher=trade_dispatcher,
                on_dropped=consume_dropped_trade,
                integrity=trade_integrity,
            ),
        )
    )
    if create_range_module is not None:
        registry.register(
            ModuleDefinition(
                module_id="range-bars",
                provides=frozenset({FEATURE_RANGE_BARS}),
                requires=frozenset({MARKET_TRADES}),
                factory=lambda: create_range_module(trade_dispatcher),
            )
        )
    registry.register(
        ModuleDefinition(
            module_id="order-book-stream",
            provides=frozenset({MARKET_ORDER_BOOK}),
            requires=frozenset(),
            factory=lambda: OrderBookStreamModule(
                stream=create_order_book_stream(),
                dispatcher=order_book_dispatcher,
                integrity=order_book_integrity,
            ),
        )
    )
    registry.register(
        ModuleDefinition(
            module_id="fixed-time-trade-bars",
            provides=frozenset({FEATURE_FIXED_TIME_TRADE_BARS}),
            requires=frozenset({MARKET_TRADES}),
            factory=lambda: FixedTimeTradeBarModule(
                config=config.fixed_time_trade_bars,
                dispatcher=trade_dispatcher,
                publish=publish_feature,
                integrity=trade_integrity,
            ),
        )
    )
    registry.register(
        ModuleDefinition(
            module_id="trade-footprint",
            provides=frozenset({FEATURE_TRADE_FOOTPRINT}),
            requires=frozenset({MARKET_TRADES}),
            factory=lambda: TradeFootprintModule(
                config=config.trade_footprint,
                dispatcher=trade_dispatcher,
                publish=publish_feature,
                integrity=trade_integrity,
            ),
        )
    )
    registry.register(
        ModuleDefinition(
            module_id="range-footprint",
            provides=frozenset({FEATURE_RANGE_FOOTPRINT}),
            requires=frozenset({MARKET_TRADES}),
            factory=lambda: RangeFootprintModule(
                config=config.range_footprint,
                dispatcher=trade_dispatcher,
                publish=publish_feature,
                integrity=trade_integrity,
            ),
        )
    )
    return registry


__all__ = [
    "MarketDataModuleConfig",
    "OrderBookStreamFactory",
    "RangeModuleFactory",
    "TradeStreamFactory",
    "build_market_data_registry",
]
