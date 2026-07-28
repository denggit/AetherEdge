from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from src.platform.data.models import (
    MarketFullOrderBook,
    MarketOpenInterest,
    MarketOrderBook,
    MarketOrderBookL2,
    MarketTrade,
)
from src.platform.data.polling import FullOrderBookStream
from src.platform.data.websocket.ports import (
    OpenInterestStream,
    OrderBookL2Stream,
    OrderBookStream,
    TradeStream,
)
from src.runtime.capabilities import (
    FEATURE_FIXED_TIME_TRADE_BARS,
    FEATURE_RANGE_BARS,
    FEATURE_RANGE_FOOTPRINT,
    FEATURE_TRADE_FOOTPRINT,
    MARKET_FULL_ORDER_BOOK,
    MARKET_OPEN_INTEREST,
    MARKET_ORDER_BOOK,
    MARKET_ORDER_BOOK_L2,
    MARKET_TRADES,
)
from src.runtime.market_data.dispatcher import (
    BackpressurePolicy,
    BoundedEventDispatcher,
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
    FullOrderBookPollingModule,
    OpenInterestStreamModule,
    OrderBookL2StreamModule,
    OrderBookStreamModule,
    TradeStreamModule,
)
from src.runtime.market_data.processor import MarketEventProcessor
from src.runtime.registry import ModuleDefinition, ModuleRegistry
from src.runtime.module import RuntimeModule


TradeStreamFactory = Callable[[], TradeStream]
OrderBookStreamFactory = Callable[[], OrderBookStream]
OrderBookL2StreamFactory = Callable[[], OrderBookL2Stream]
FullOrderBookStreamFactory = Callable[[], FullOrderBookStream]
OpenInterestStreamFactory = Callable[[], OpenInterestStream]
RangeModuleFactory = Callable[[], RuntimeModule]
OrderBookConsumer = Callable[[MarketOrderBook], Awaitable[None] | None]
OrderBookL2Consumer = Callable[[MarketOrderBookL2], Awaitable[None] | None]
FullOrderBookConsumer = Callable[[MarketFullOrderBook], Awaitable[None] | None]
OpenInterestConsumer = Callable[[MarketOpenInterest], Awaitable[None] | None]
DroppedTradeConsumer = Callable[[MarketTrade], Awaitable[None] | None]


@dataclass(frozen=True)
class MarketDataModuleConfig:
    order_book_queue_maxsize: int = 100
    order_book_l2_queue_maxsize: int = 1
    full_order_book_queue_maxsize: int = 1
    open_interest_queue_maxsize: int = 1
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
    create_order_book_l2_stream: OrderBookL2StreamFactory | None = None,
    create_full_order_book_stream: FullOrderBookStreamFactory | None = None,
    create_open_interest_stream: OpenInterestStreamFactory | None = None,
    config: MarketDataModuleConfig = MarketDataModuleConfig(),
    create_range_module: RangeModuleFactory | None = None,
    order_book_dispatcher: BoundedEventDispatcher[MarketOrderBook] | None = None,
    order_book_l2_dispatcher: (
        BoundedEventDispatcher[MarketOrderBookL2] | None
    ) = None,
    full_order_book_dispatcher: (
        BoundedEventDispatcher[MarketFullOrderBook] | None
    ) = None,
    open_interest_dispatcher: (
        BoundedEventDispatcher[MarketOpenInterest] | None
    ) = None,
    consume_dropped_trade: DroppedTradeConsumer | None = None,
    consume_order_book: OrderBookConsumer | None = None,
    consume_order_book_l2: OrderBookL2Consumer | None = None,
    consume_full_order_book: FullOrderBookConsumer | None = None,
    consume_open_interest: OpenInterestConsumer | None = None,
    trade_integrity: TradeDataIntegrityTracker | None = None,
    order_book_integrity: OrderBookDataIntegrityTracker | None = None,
    trade_processor: MarketEventProcessor | None = None,
    on_first_live_trade: Callable[[int], None] | None = None,
) -> ModuleRegistry:
    """Build lazy module definitions without opening streams or stores."""

    order_book_dispatcher = order_book_dispatcher or BoundedEventDispatcher[MarketOrderBook]()
    trade_integrity = trade_integrity or TradeDataIntegrityTracker()
    order_book_integrity = order_book_integrity or OrderBookDataIntegrityTracker()

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
                processor=trade_processor,
                on_dropped=consume_dropped_trade,
                on_first_live_trade=on_first_live_trade,
            ),
        )
    )

    if create_order_book_l2_stream is not None:
        def build_order_book_l2_module() -> RuntimeModule:
            dispatcher = (
                order_book_l2_dispatcher
                or BoundedEventDispatcher[MarketOrderBookL2]()
            )
            if consume_order_book_l2 is not None:
                dispatcher.subscribe(
                    subscriber_id="runtime-order-book-l2-consumer",
                    handler=consume_order_book_l2,
                    maxsize=config.order_book_l2_queue_maxsize,
                    policy=BackpressurePolicy.DROP_OLDEST,
                )
            return OrderBookL2StreamModule(
                stream=create_order_book_l2_stream(),
                dispatcher=dispatcher,
            )

        registry.register(
            ModuleDefinition(
                module_id="order-book-l2-stream",
                provides=frozenset({MARKET_ORDER_BOOK_L2}),
                requires=frozenset(),
                factory=build_order_book_l2_module,
            )
        )

    if create_full_order_book_stream is not None:
        def build_full_order_book_module() -> RuntimeModule:
            dispatcher = (
                full_order_book_dispatcher
                or BoundedEventDispatcher[MarketFullOrderBook]()
            )
            if consume_full_order_book is not None:
                dispatcher.subscribe(
                    subscriber_id="runtime-full-order-book-consumer",
                    handler=consume_full_order_book,
                    maxsize=config.full_order_book_queue_maxsize,
                    policy=BackpressurePolicy.DROP_OLDEST,
                )
            return FullOrderBookPollingModule(
                stream=create_full_order_book_stream(),
                dispatcher=dispatcher,
            )

        registry.register(
            ModuleDefinition(
                module_id="full-order-book-poller",
                provides=frozenset({MARKET_FULL_ORDER_BOOK}),
                requires=frozenset(),
                factory=build_full_order_book_module,
            )
        )

    if create_open_interest_stream is not None:
        def build_open_interest_module() -> RuntimeModule:
            dispatcher = (
                open_interest_dispatcher
                or BoundedEventDispatcher[MarketOpenInterest]()
            )
            if consume_open_interest is not None:
                dispatcher.subscribe(
                    subscriber_id="runtime-open-interest-consumer",
                    handler=consume_open_interest,
                    maxsize=config.open_interest_queue_maxsize,
                    policy=BackpressurePolicy.DROP_OLDEST,
                )
            return OpenInterestStreamModule(
                stream=create_open_interest_stream(),
                dispatcher=dispatcher,
            )

        registry.register(
            ModuleDefinition(
                module_id="open-interest-stream",
                provides=frozenset({MARKET_OPEN_INTEREST}),
                requires=frozenset(),
                factory=build_open_interest_module,
            )
        )

    if create_range_module is not None:
        registry.register(
            ModuleDefinition(
                module_id="range-bars",
                provides=frozenset({FEATURE_RANGE_BARS}),
                requires=frozenset({MARKET_TRADES}),
                factory=create_range_module,
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
                publish=publish_feature,
                integrity=trade_integrity,
            ),
        )
    )

    return registry


__all__ = [
    "MarketDataModuleConfig",
    "FullOrderBookStreamFactory",
    "OpenInterestStreamFactory",
    "OrderBookL2StreamFactory",
    "OrderBookStreamFactory",
    "RangeModuleFactory",
    "TradeStreamFactory",
    "build_market_data_registry",
]
