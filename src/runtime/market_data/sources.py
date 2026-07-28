from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Generic, TypeVar

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
    MARKET_FULL_ORDER_BOOK,
    MARKET_OPEN_INTEREST,
    MARKET_ORDER_BOOK,
    MARKET_ORDER_BOOK_L2,
    MARKET_TRADES,
)
from src.runtime.market_data.dispatcher import EventDispatcher
from src.runtime.market_data.integrity import (
    OrderBookDataIntegrityTracker,
)
from src.runtime.market_data.processor import MarketEventProcessor
from src.runtime.module import (
    CapabilityId,
    ModuleHealth,
    ModuleState,
)


EventT = TypeVar("EventT")
StreamFactory = Callable[[], AsyncIterator[EventT]]
ModuleErrorHandler = Callable[[str, BaseException], None]
DroppedEventHandler = Callable[[EventT], Awaitable[None] | None]
FirstLiveTradeHandler = Callable[[int], None]


class OrderBookResyncRequired(RuntimeError):
    pass


class _StreamModule(Generic[EventT]):
    shutdown_priority = 100

    def __init__(
        self,
        *,
        module_id: str,
        capability: CapabilityId,
        stream: StreamFactory[EventT],
        dispatcher: EventDispatcher[EventT] | None = None,
        on_error: ModuleErrorHandler | None = None,
        on_dropped: DroppedEventHandler[EventT] | None = None,
    ) -> None:
        self.module_id = module_id
        self.provides = frozenset({capability})
        self.requires: frozenset[CapabilityId] = frozenset()
        self._stream = stream
        self._dispatcher = dispatcher
        self._on_error = on_error
        self._on_dropped = on_dropped
        self._task: asyncio.Task[None] | None = None
        self._state = ModuleState.CREATED
        self._error: BaseException | None = None
        self.events_seen = 0
        self.events_dropped = 0
        self._stopping = False

    async def prepare(self) -> None:
        self._state = ModuleState.PREPARED

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        if self._dispatcher is not None:
            await self._dispatcher.start()
        self._stopping = False
        self._state = ModuleState.RUNNING
        self._task = asyncio.create_task(
            self._consume(),
            name=f"market-source:{self.module_id}",
        )

    async def stop(self) -> None:
        self._stopping = True
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if self._dispatcher is not None:
            try:
                await self._dispatcher.stop()
            except BaseException as exc:
                self._error = exc
                self._state = ModuleState.ERROR
                raise
        self._state = ModuleState.STOPPED

    def health(self) -> ModuleHealth:
        dispatcher_error = (
            getattr(self._dispatcher, "error", None)
            if self._dispatcher is not None
            else None
        )
        error = self._error or dispatcher_error
        return ModuleHealth(
            module_id=self.module_id,
            state=self._state,
            healthy=error is None,
            detail=(
                None
                if error is None
                else f"{type(error).__name__}: {error}"
            ),
            background_tasks=int(
                self._task is not None and not self._task.done()
            ),
            metadata=(
                ("events_seen", str(self.events_seen)),
                ("events_dropped", str(self.events_dropped)),
            ),
        )

    async def _consume(self) -> None:
        try:
            async for event in self._stream():
                self.events_seen += 1
                result = self._dispatcher.publish(event)
                self.events_dropped += result.dropped
                for dropped in result.dropped_events:
                    await self._handle_dropped_event(dropped)
                if result.dropped and not result.dropped_events:
                    for _ in range(result.dropped):
                        await self._handle_dropped_event(event)
            if not self._stopping:
                raise RuntimeError(
                    f"market source ended unexpectedly: {self.module_id}"
                )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._error = exc
            self._state = ModuleState.ERROR
            if self._on_error is not None:
                self._on_error(self.module_id, exc)
            raise

    async def _handle_dropped_event(self, event: EventT) -> None:
        if self._on_dropped is None:
            return
        callback_result = self._on_dropped(event)
        if inspect.isawaitable(callback_result):
            await callback_result


class TradeStreamModule(_StreamModule[MarketTrade]):
    def __init__(
        self,
        *,
        stream: TradeStream,
        processor: MarketEventProcessor | None,
        on_error: ModuleErrorHandler | None = None,
        on_dropped: DroppedEventHandler[MarketTrade] | None = None,
        on_first_live_trade: FirstLiveTradeHandler | None = None,
    ) -> None:
        if processor is None:
            raise ValueError("trade stream requires MarketEventProcessor")
        self._processor = processor
        self._on_first_live_trade = on_first_live_trade
        self._first_live_trade_seen = False
        super().__init__(
            module_id="trade-stream",
            capability=MARKET_TRADES,
            stream=stream.stream_trades,
            dispatcher=None,
            on_error=on_error,
            on_dropped=on_dropped,
        )

    async def _consume(self) -> None:
        last_event_time_ms = 0
        try:
            async for event in self._stream():
                self.events_seen += 1
                last_event_time_ms = event.trade_time_ms or event.event_time_ms or 0
                try:
                    if not self._first_live_trade_seen:
                        if self._on_first_live_trade is not None:
                            self._on_first_live_trade(last_event_time_ms)
                        self._first_live_trade_seen = True
                    self._processor.submit_trade(event)
                except Exception:
                    self.events_dropped += 1
                    await super()._handle_dropped_event(event)
                    raise
            if not self._stopping:
                raise RuntimeError(
                    f"market source ended unexpectedly: {self.module_id}"
                )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if not self._stopping and self._processor.is_accepting:
                self._processor.mark_source_incomplete(
                    last_event_time_ms or int(time.time() * 1000),
                    "trade_source_disconnected",
                )
            self._error = exc
            self._state = ModuleState.ERROR
            if self._on_error is not None:
                self._on_error(self.module_id, exc)
            raise


class OrderBookStreamModule(_StreamModule[MarketOrderBook]):
    def __init__(
        self,
        *,
        stream: OrderBookStream,
        dispatcher: EventDispatcher[MarketOrderBook],
        on_error: ModuleErrorHandler | None = None,
        on_dropped: DroppedEventHandler[MarketOrderBook] | None = None,
        integrity: OrderBookDataIntegrityTracker | None = None,
    ) -> None:
        self._integrity = integrity
        super().__init__(
            module_id="order-book-stream",
            capability=MARKET_ORDER_BOOK,
            stream=stream.stream_order_book,
            dispatcher=dispatcher,
            on_error=on_error,
            on_dropped=on_dropped,
        )

    async def _handle_dropped_event(self, event: MarketOrderBook) -> None:
        if self._integrity is not None:
            self._integrity.mark_dropped("order_book_dispatcher_drop")
        await super()._handle_dropped_event(event)
        raise OrderBookResyncRequired(
            "order book delta/snapshot sequence is incomplete; reconnect/resync required"
        )

    def health(self) -> ModuleHealth:
        health = super().health()
        integrity = None if self._integrity is None else self._integrity.snapshot()
        if integrity is None:
            return health
        return ModuleHealth(
            module_id=health.module_id,
            state=health.state,
            healthy=health.healthy and not integrity.resync_required,
            detail=health.detail or integrity.reason,
            background_tasks=health.background_tasks,
            metadata=health.metadata
            + (
                ("order_book_dropped", str(integrity.dropped_count)),
                ("resync_required", str(integrity.resync_required).lower()),
            ),
        )


class _LatestStateStreamModule(_StreamModule[EventT]):
    def __init__(
        self,
        *,
        owner: object,
        module_id: str,
        capability: CapabilityId,
        stream: StreamFactory[EventT],
        dispatcher: EventDispatcher[EventT],
        on_error: ModuleErrorHandler | None = None,
        on_dropped: DroppedEventHandler[EventT] | None = None,
    ) -> None:
        self._owner = owner
        super().__init__(
            module_id=module_id,
            capability=capability,
            stream=stream,
            dispatcher=dispatcher,
            on_error=on_error,
            on_dropped=on_dropped,
        )

    async def stop(self) -> None:
        error: BaseException | None = None
        try:
            await super().stop()
        except BaseException as exc:
            error = exc
        close = getattr(self._owner, "close", None)
        if not callable(close):
            close = getattr(self._owner, "stop", None)
        if callable(close):
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except BaseException as exc:
                if error is None:
                    error = exc
        if error is not None:
            raise error


class OrderBookL2StreamModule(
    _LatestStateStreamModule[MarketOrderBookL2]
):
    def __init__(
        self,
        *,
        stream: OrderBookL2Stream,
        dispatcher: EventDispatcher[MarketOrderBookL2],
        on_error: ModuleErrorHandler | None = None,
        on_dropped: DroppedEventHandler[MarketOrderBookL2] | None = None,
    ) -> None:
        super().__init__(
            owner=stream,
            module_id="order-book-l2-stream",
            capability=MARKET_ORDER_BOOK_L2,
            stream=stream.stream_order_book_l2,
            dispatcher=dispatcher,
            on_error=on_error,
            on_dropped=on_dropped,
        )


class FullOrderBookPollingModule(
    _LatestStateStreamModule[MarketFullOrderBook]
):
    def __init__(
        self,
        *,
        stream: FullOrderBookStream,
        dispatcher: EventDispatcher[MarketFullOrderBook],
        on_error: ModuleErrorHandler | None = None,
        on_dropped: DroppedEventHandler[MarketFullOrderBook] | None = None,
    ) -> None:
        super().__init__(
            owner=stream,
            module_id="full-order-book-poller",
            capability=MARKET_FULL_ORDER_BOOK,
            stream=stream.stream_full_order_book,
            dispatcher=dispatcher,
            on_error=on_error,
            on_dropped=on_dropped,
        )


class OpenInterestStreamModule(
    _LatestStateStreamModule[MarketOpenInterest]
):
    def __init__(
        self,
        *,
        stream: OpenInterestStream,
        dispatcher: EventDispatcher[MarketOpenInterest],
        on_error: ModuleErrorHandler | None = None,
        on_dropped: DroppedEventHandler[MarketOpenInterest] | None = None,
    ) -> None:
        super().__init__(
            owner=stream,
            module_id="open-interest-stream",
            capability=MARKET_OPEN_INTEREST,
            stream=stream.stream_open_interest,
            dispatcher=dispatcher,
            on_error=on_error,
            on_dropped=on_dropped,
        )


__all__ = [
    "FullOrderBookPollingModule",
    "OpenInterestStreamModule",
    "OrderBookL2StreamModule",
    "OrderBookResyncRequired",
    "OrderBookStreamModule",
    "TradeStreamModule",
]
