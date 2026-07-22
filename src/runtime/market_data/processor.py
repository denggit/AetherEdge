from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from src.platform.data.models import MarketTrade
from src.runtime.market_data.integrity import TradeDataIntegrityTracker
from src.runtime.market_data.pipeline_plan import (
    ClosedBarControlEvent,
    ResolvedMarketPipelinePlan,
)
from src.utils.log import get_logger

logger = get_logger(__name__)


class TradeProcessor(Protocol):
    module_id: str

    async def process_trade(self, trade: MarketTrade) -> None: ...


class ClosedBarProcessor(Protocol):
    async def process_closed_bar(self, event: ClosedBarControlEvent) -> None: ...


@dataclass
class ProcessorStats:
    trades_processed: int = 0
    closed_bars_processed: int = 0
    trades_dropped: int = 0
    errors: int = 0
    max_queue_depth: int = 0
    total_processing_time_ms: float = 0.0
    last_event_time_ms: int = 0
    module_timings: dict[str, float] = field(default_factory=dict)
    processing_times_ms: deque[float] = field(
        default_factory=lambda: deque(maxlen=10_000)
    )


class ProcessorOverflowError(RuntimeError):
    pass


class ProcessorFailureError(RuntimeError):
    pass


class CausalIntegrityError(RuntimeError):
    pass


class MarketEventProcessor:
    def __init__(
        self,
        *,
        plan: ResolvedMarketPipelinePlan,
        trade_modules: Sequence[TradeProcessor] = (),
        closed_bar_handler: ClosedBarProcessor | None = None,
        raw_trade_callback: Callable[[MarketTrade], Awaitable[None]] | None = None,
        integrity: TradeDataIntegrityTracker | None = None,
        maxsize: int = 4096,
        overflow_policy: str = "fail_fast",
        drain_timeout_seconds: float = 5.0,
    ) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        if overflow_policy not in {"fail_fast", "drop_oldest"}:
            raise ValueError(f"unsupported overflow_policy: {overflow_policy}")
        self._plan = plan
        self._queue: asyncio.Queue[MarketTrade | ClosedBarControlEvent] = asyncio.Queue(maxsize)
        self._modules = list(trade_modules)
        self._closed_bar_handler = closed_bar_handler
        self._raw_trade_callback = raw_trade_callback
        self._integrity = integrity
        self._overflow_policy = overflow_policy
        self._drain_timeout = float(drain_timeout_seconds)
        self._task: asyncio.Task[None] | None = None
        self._failure_event = asyncio.Event()
        self._controls: list[ClosedBarControlEvent] = []
        self._closed_through_ms = -1
        self._accepting = True
        self._error: BaseException | None = None
        self.stats = ProcessorStats()

    def set_trade_modules(self, modules: Sequence[TradeProcessor]) -> None:
        self._modules = list(modules)

    @property
    def trade_module_ids(self) -> tuple[str, ...]:
        return tuple(module.module_id for module in self._modules)

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def is_accepting(self) -> bool:
        return self._accepting

    def submit_trade(self, trade: MarketTrade) -> None:
        if not self._accepting:
            raise ProcessorFailureError("MarketEventProcessor is not accepting events")
        event_ms = trade.trade_time_ms or trade.event_time_ms or 0
        if event_ms <= self._closed_through_ms:
            error = CausalIntegrityError(
                "late trade crossed a completed closed-bar boundary | "
                f"trade_time_ms={event_ms} close_time_ms={self._closed_through_ms}"
            )
            self._mark_incomplete(event_ms, "late_trade_after_closed_bar_completed")
            self._fail(error)
            raise error
        for control in tuple(self._controls):
            if event_ms > control.kline.close_time_ms:
                continue
            if control.started or control.completion.done():
                error = CausalIntegrityError(
                    "late trade crossed an active closed-bar boundary | "
                    f"trade_time_ms={event_ms} close_time_ms={control.kline.close_time_ms}"
                )
                self._mark_incomplete(event_ms, "late_trade_after_closed_bar_started")
                self._fail(error)
                raise error
            if control.skip_reason is None:
                logger.warning(
                    "Closed bar skipped for late trade | close_time_ms=%s",
                    control.kline.close_time_ms,
                )
            control.skip_reason = "late_trade_before_closed_bar_started"
            self._mark_incomplete(event_ms, control.skip_reason)
        self._put(trade, event_ms=event_ms)

    def submit_closed_bar(self, event: ClosedBarControlEvent) -> None:
        if not self._accepting:
            event.completion.set_exception(
                ProcessorFailureError("MarketEventProcessor is not accepting events")
            )
            return
        try:
            self._put(event)
        except ProcessorOverflowError as exc:
            event.completion.set_exception(exc)
            return
        self._controls.append(event)

    def _put(
        self,
        event: MarketTrade | ClosedBarControlEvent,
        *,
        event_ms: int = 0,
    ) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self.stats.trades_dropped += 1
            self._mark_incomplete(event_ms or int(time.time() * 1000), "processor_queue_overflow")
            if self._overflow_policy == "fail_fast":
                error = ProcessorOverflowError(
                    f"MarketEventProcessor queue full (maxsize={self._queue.maxsize}); fail-fast triggered"
                )
                self._fail(error)
                raise error
            dropped = self._queue.get_nowait()
            self._queue.task_done()
            if isinstance(dropped, ClosedBarControlEvent) and not dropped.completion.done():
                dropped.completion.set_exception(ProcessorOverflowError("control event dropped"))
            self._queue.put_nowait(event)
        self.stats.max_queue_depth = max(self.stats.max_queue_depth, self._queue.qsize())

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._accepting = True
        self._error = None
        self._failure_event.clear()
        self._task = asyncio.create_task(self._worker(), name="market-event-processor")

    async def stop(self) -> None:
        self._accepting = False
        if self._error is None:
            try:
                await asyncio.wait_for(self._queue.join(), self._drain_timeout)
            except TimeoutError:
                logger.warning("MarketEventProcessor drain timed out | pending=%s", self._queue.qsize())
        else:
            self._discard_pending(self._error)
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def raise_if_failed(self) -> None:
        if self._error is not None:
            raise ProcessorFailureError(f"MarketEventProcessor failed: {self._error}") from self._error

    async def wait_failed(self) -> None:
        await self._failure_event.wait()
        self.raise_if_failed()

    async def _worker(self) -> None:
        while True:
            event = await self._queue.get()
            try:
                started = time.monotonic()
                if isinstance(event, ClosedBarControlEvent):
                    await self._process_closed_bar(event)
                    self.stats.closed_bars_processed += 1
                else:
                    await self._process_trade(event)
                    self.stats.trades_processed += 1
                elapsed_ms = (time.monotonic() - started) * 1000
                self.stats.total_processing_time_ms += elapsed_ms
                self.stats.processing_times_ms.append(elapsed_ms)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                self.stats.errors += 1
                self._fail(exc)
                self._discard_pending(exc)
                logger.exception("MarketEventProcessor worker error | %s", exc)
                raise
            finally:
                self._queue.task_done()

    async def _process_trade(self, trade: MarketTrade) -> None:
        self.stats.last_event_time_ms = trade.trade_time_ms or trade.event_time_ms or 0
        for module in self._modules:
            started = time.monotonic()
            await module.process_trade(trade)
            self.stats.module_timings[module.module_id] = (
                self.stats.module_timings.get(module.module_id, 0.0)
                + (time.monotonic() - started) * 1000
            )
        if self._raw_trade_callback is not None:
            await self._raw_trade_callback(trade)

    async def _process_closed_bar(self, event: ClosedBarControlEvent) -> None:
        event.started = True
        try:
            if self._closed_bar_handler is not None:
                await self._closed_bar_handler.process_closed_bar(event)
            if not event.completion.done():
                event.completion.set_result(None)
            self._closed_through_ms = max(
                self._closed_through_ms,
                event.kline.close_time_ms,
            )
        except BaseException as exc:
            if not event.completion.done():
                event.completion.set_exception(exc)
            raise
        finally:
            if event in self._controls:
                self._controls.remove(event)

    def _mark_incomplete(self, event_ms: int, reason: str) -> None:
        if self._integrity is not None:
            self._integrity.mark_dropped(event_ms, reason)

    def _fail(self, error: BaseException) -> None:
        self._accepting = False
        self._error = self._error or error
        self._failure_event.set()
        if self._task is not None and self._task is not asyncio.current_task():
            self._task.cancel()

    def _discard_pending(self, error: BaseException) -> None:
        while True:
            try:
                event = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(event, ClosedBarControlEvent) and not event.completion.done():
                event.completion.set_exception(error)
            self._queue.task_done()
        self._controls.clear()


__all__ = [
    "CausalIntegrityError",
    "ClosedBarProcessor",
    "MarketEventProcessor",
    "ProcessorFailureError",
    "ProcessorOverflowError",
    "ProcessorStats",
    "TradeProcessor",
]
