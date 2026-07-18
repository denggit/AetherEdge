from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Generic, Protocol, TypeVar

from src.platform.data.models import MarketOrderBook, MarketTrade
from src.platform.data.websocket.ports import OrderBookStream, TradeStream
from src.runtime.capabilities import MARKET_ORDER_BOOK, MARKET_TRADES
from src.runtime.market_data.dispatcher import EventDispatcher
from src.runtime.module import (
    CapabilityId,
    ModuleHealth,
    ModuleState,
)


EventT = TypeVar("EventT")
StreamFactory = Callable[[], AsyncIterator[EventT]]
ModuleErrorHandler = Callable[[str, BaseException], None]


class _StreamIdentity(Protocol):
    async def stop(self) -> None: ...


class _StreamModule(Generic[EventT]):
    def __init__(
        self,
        *,
        module_id: str,
        capability: CapabilityId,
        stream: StreamFactory[EventT],
        dispatcher: EventDispatcher[EventT],
        on_error: ModuleErrorHandler | None = None,
    ) -> None:
        self.module_id = module_id
        self.provides = frozenset({capability})
        self.requires: frozenset[CapabilityId] = frozenset()
        self._stream = stream
        self._dispatcher = dispatcher
        self._on_error = on_error
        self._task: asyncio.Task[None] | None = None
        self._state = ModuleState.CREATED
        self._error: BaseException | None = None
        self.events_seen = 0
        self.events_dropped = 0

    async def prepare(self) -> None:
        self._state = ModuleState.PREPARED

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        await self._dispatcher.start()
        self._state = ModuleState.RUNNING
        self._task = asyncio.create_task(
            self._consume(),
            name=f"market-source:{self.module_id}",
        )

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._dispatcher.stop()
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
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._error = exc
            self._state = ModuleState.ERROR
            if self._on_error is not None:
                self._on_error(self.module_id, exc)
            raise


class TradeStreamModule(_StreamModule[MarketTrade]):
    def __init__(
        self,
        *,
        stream: TradeStream,
        dispatcher: EventDispatcher[MarketTrade],
        on_error: ModuleErrorHandler | None = None,
    ) -> None:
        super().__init__(
            module_id="trade-stream",
            capability=MARKET_TRADES,
            stream=stream.stream_trades,
            dispatcher=dispatcher,
            on_error=on_error,
        )


class OrderBookStreamModule(_StreamModule[MarketOrderBook]):
    def __init__(
        self,
        *,
        stream: OrderBookStream,
        dispatcher: EventDispatcher[MarketOrderBook],
        on_error: ModuleErrorHandler | None = None,
    ) -> None:
        super().__init__(
            module_id="order-book-stream",
            capability=MARKET_ORDER_BOOK,
            stream=stream.stream_order_book,
            dispatcher=dispatcher,
            on_error=on_error,
        )


__all__ = ["OrderBookStreamModule", "TradeStreamModule"]
