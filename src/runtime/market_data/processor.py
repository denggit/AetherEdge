from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from src.platform.data.models import MarketTrade
from src.runtime.market_data.integrity import TradeDataIntegrityTracker
from src.runtime.market_data.pipeline_plan import (
    ClosedBarControlEvent,
)
from src.utils.log import get_logger

logger = get_logger(__name__)


@dataclass
class ProcessorStats:
    trades_processed: int = 0
    closed_bars_processed: int = 0
    trades_dropped: int = 0
    errors: int = 0
    max_queue_depth: int = 0
    max_future_buffer_depth: int = 0
    closed_bar_pending_time_ms: float = 0.0
    rest_pending_time_ms: float = 0.0
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
        trade_modules: Sequence[Any] = (),
        closed_bar_handler: Any | None = None,
        raw_trade_callback: Callable[[MarketTrade], Awaitable[None]] | None = None,
        integrity: TradeDataIntegrityTracker | None = None,
        maxsize: int = 4096,
        future_buffer_maxsize: int | None = None,
        cutoff_timeout_seconds: float = 120.0,
        drain_timeout_seconds: float = 5.0,
    ) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        if future_buffer_maxsize is not None and future_buffer_maxsize <= 0:
            raise ValueError("future_buffer_maxsize must be positive")
        self._queue: asyncio.Queue[MarketTrade | ClosedBarControlEvent] = asyncio.Queue(maxsize)
        self._future_trades: deque[MarketTrade] = deque()
        self._pending_trades: deque[MarketTrade] = deque()
        self._pending_done = asyncio.Event()
        self._pending_done.set()
        self._future_maxsize = future_buffer_maxsize or maxsize
        self._modules = list(trade_modules)
        self._closed_bar_handler = closed_bar_handler
        self._raw_trade_callback = raw_trade_callback
        self._integrity = integrity
        self._cutoff_timeout = float(cutoff_timeout_seconds)
        self._drain_timeout = float(drain_timeout_seconds)
        self._task: asyncio.Task[None] | None = None
        self._failure_event = asyncio.Event()
        self._pending_cutoff: tuple[int, int, float] | None = None
        self._cutoff_active = False
        self._pending_control: ClosedBarControlEvent | None = None
        self._processing_trade_time_ms: int | None = None
        self._cutoff_timer: asyncio.TimerHandle | None = None
        self._closed_through_ms = -1
        self._accepting = True
        self._accepting_controls = True
        self._error: BaseException | None = None
        self.stats = ProcessorStats()

    def set_trade_modules(self, modules: Sequence[Any]) -> None:
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

    @property
    def future_buffer_size(self) -> int:
        return len(self._future_trades)

    @property
    def pending_cutoff(self) -> tuple[int, int] | None:
        pending = self._pending_cutoff
        return None if pending is None else pending[:2]

    def arm_closed_bar_cutoff(self, open_time_ms: int, close_time_ms: int) -> None:
        if not self._accepting_controls:
            raise ProcessorFailureError("MarketEventProcessor is not accepting events")
        boundary = (int(open_time_ms), int(close_time_ms))
        if boundary[1] < boundary[0]:
            raise ValueError("closed-bar cutoff ends before it starts")
        if self._pending_cutoff is not None:
            if self._pending_cutoff[:2] == boundary:
                return
            error = CausalIntegrityError(
                "another closed-bar cutoff is already pending"
            )
            self._fail(error)
            raise error
        self._pending_cutoff = (*boundary, 0.0)

    def begin_closed_bar_cutoff(self, open_time_ms: int, close_time_ms: int) -> None:
        self.arm_closed_bar_cutoff(open_time_ms, close_time_ms)

    def activate_closed_bar_cutoff(self, now_ms: int) -> None:
        pending = self._pending_cutoff
        if pending is not None and int(now_ms) > pending[1]:
            self._activate_cutoff()

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
        pending = self._pending_cutoff
        if pending is not None:
            cutoff = pending[1]
            control = self._pending_control
            if event_ms > cutoff:
                self._activate_cutoff()
                self._buffer_future_trade(trade, event_ms)
                return
            if self._cutoff_active and control is not None and (
                control.started or control.completion.done()
            ):
                error = CausalIntegrityError(
                    "late trade crossed an active closed-bar boundary | "
                    f"trade_time_ms={event_ms} close_time_ms={cutoff}"
                )
                self._mark_incomplete(event_ms, "late_trade_after_closed_bar_started")
                self._fail(error)
                raise error
        self._put(trade, event_ms=event_ms)

    def submit_closed_bar(self, event: ClosedBarControlEvent) -> None:
        if not self._accepting_controls:
            event.completion.set_exception(
                ProcessorFailureError("MarketEventProcessor is not accepting events")
            )
            return
        pending = self._pending_cutoff
        expected = (event.open_time_ms, event.kline.close_time_ms)
        if pending is None or pending[:2] != expected:
            error = CausalIntegrityError(
                "closed-bar control does not match a registered cutoff"
            )
            event.completion.set_exception(error)
            self._fail(error)
            return
        self._activate_cutoff()
        if self._pending_control is not None:
            event.completion.set_exception(CausalIntegrityError(
                "closed-bar control is already pending"
            ))
            return
        self._pending_control = event
        try:
            self._put(event)
        except ProcessorOverflowError as exc:
            event.completion.set_exception(exc)
            return

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
            error = ProcessorOverflowError(
                f"MarketEventProcessor queue full (maxsize={self._queue.maxsize}); fail-fast triggered"
            )
            self._fail(error)
            raise error
        self.stats.max_queue_depth = max(self.stats.max_queue_depth, self._queue.qsize())

    def _buffer_future_trade(self, trade: MarketTrade, event_ms: int) -> None:
        if len(self._future_trades) >= self._future_maxsize:
            self.stats.trades_dropped += 1
            self._mark_incomplete(event_ms, "closed_bar_future_buffer_overflow")
            error = ProcessorOverflowError(
                "closed-bar future Trade buffer full "
                f"(maxsize={self._future_maxsize}); fail-fast triggered"
            )
            self._fail(error)
            raise error
        self._future_trades.append(trade)
        self.stats.max_future_buffer_depth = max(
            self.stats.max_future_buffer_depth, len(self._future_trades)
        )

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._accepting = True
        self._accepting_controls = True
        self._error = None
        self._failure_event.clear()
        self._task = asyncio.create_task(self._worker(), name="market-event-processor")

    def stop_accepting(self) -> None:
        self._accepting = False

    def stop_accepting_controls(self) -> None:
        self._accepting_controls = False

    def mark_source_incomplete(self, event_time_ms: int, reason: str) -> None:
        self._mark_incomplete(event_time_ms, reason)

    async def stop(self) -> None:
        self.stop_accepting()
        self.stop_accepting_controls()
        drain_error: BaseException | None = None
        if self._error is None:
            try:
                await asyncio.wait_for(
                    asyncio.gather(self._queue.join(), self._pending_done.wait()),
                    self._drain_timeout,
                )
            except TimeoutError:
                drain_error = ProcessorFailureError(
                    "MarketEventProcessor drain timed out | "
                    f"pending={self._queue.qsize() + len(self._pending_trades)} "
                    f"future={len(self._future_trades)}"
                )
            if drain_error is None and (self._cutoff_active or self._future_trades):
                drain_error = ProcessorFailureError(
                    "MarketEventProcessor stopped with an unresolved closed-bar cutoff"
                )
            if drain_error is not None:
                event_ms = (
                    self._pending_cutoff[1] if self._pending_cutoff
                    else self.stats.last_event_time_ms
                )
                self._mark_incomplete(event_ms, "processor_drain_incomplete")
                self._fail(drain_error)
                self._discard_pending(drain_error)
            elif self._pending_cutoff is not None:
                self._finish_cutoff()
        else:
            self._discard_pending(self._error)
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if drain_error is not None:
            raise ProcessorFailureError(str(drain_error)) from drain_error

    def raise_if_failed(self) -> None:
        if self._error is not None:
            raise ProcessorFailureError(f"MarketEventProcessor failed: {self._error}") from self._error

    async def wait_failed(self) -> None:
        await self._failure_event.wait()
        self.raise_if_failed()

    async def _worker(self) -> None:
        while True:
            from_pending = bool(self._pending_trades)
            event = (
                self._pending_trades.popleft()
                if from_pending
                else await self._queue.get()
            )
            try:
                if isinstance(event, ClosedBarControlEvent):
                    await self._classify_queued_for_cutoff()
                    await self._record_processing(event)
                    self._queue_future_snapshot()
                    self._advance_cutoff(event)
                    if not event.completion.done():
                        event.completion.set_result(None)
                else:
                    await self._handle_trade(event)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                self.stats.errors += 1
                self._fail(exc)
                self._discard_pending(exc)
                logger.exception("MarketEventProcessor worker error | %s", exc)
                raise
            finally:
                if from_pending:
                    if not self._pending_trades:
                        self._pending_done.set()
                else:
                    self._queue.task_done()

    async def _record_processing(self, event: MarketTrade | ClosedBarControlEvent) -> None:
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

    async def _handle_trade(self, trade: MarketTrade) -> None:
        event_ms = trade.trade_time_ms or trade.event_time_ms or 0
        pending = self._pending_cutoff
        if pending is not None and event_ms > pending[1]:
            self._activate_cutoff()
            self._buffer_future_trade(trade, event_ms)
            return
        self._processing_trade_time_ms = event_ms
        try:
            await self._record_processing(trade)
        finally:
            self._processing_trade_time_ms = None

    async def _classify_queued_for_cutoff(self) -> None:
        while True:
            try:
                event = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                if not isinstance(event, MarketTrade):
                    raise CausalIntegrityError("multiple closed-bar controls queued")
                await self._handle_trade(event)
            finally:
                self._queue.task_done()

    def _queue_future_snapshot(self) -> None:
        if not self._future_trades:
            return
        self._pending_trades.extend(self._future_trades)
        self._future_trades.clear()
        self._pending_done.clear()

    async def _process_trade(self, trade: MarketTrade) -> None:
        self.stats.last_event_time_ms = trade.trade_time_ms or trade.event_time_ms or 0
        for module in self._modules:
            started = time.monotonic()
            try:
                await module.process_trade(trade)
            except BaseException as exc:
                mark_failed = getattr(module, "mark_failed", None)
                if callable(mark_failed):
                    mark_failed(exc)
                logger.error(
                    "Market feature module failed | module_id=%s error=%s",
                    module.module_id,
                    exc,
                )
                raise
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
            self._closed_through_ms = max(
                self._closed_through_ms,
                event.kline.close_time_ms,
            )
        except BaseException as exc:
            if not event.completion.done():
                event.completion.set_exception(exc)
            raise

    def _finish_cutoff(self) -> None:
        pending = self._pending_cutoff
        if pending is not None and pending[2] > 0:
            self.stats.closed_bar_pending_time_ms = (time.monotonic() - pending[2]) * 1000
        self._pending_cutoff = None
        self._cutoff_active = False
        self._pending_control = None
        if self._cutoff_timer is not None:
            self._cutoff_timer.cancel()
            self._cutoff_timer = None

    def _advance_cutoff(self, event: ClosedBarControlEvent) -> None:
        interval_ms = event.kline.close_time_ms - event.open_time_ms + 1
        next_open = event.open_time_ms + interval_ms
        self._finish_cutoff()
        if self._accepting_controls:
            self.arm_closed_bar_cutoff(next_open, next_open + interval_ms - 1)

    def _activate_cutoff(self) -> None:
        pending = self._pending_cutoff
        if pending is None or self._cutoff_active:
            return
        cutoff = pending[1]
        processing = self._processing_trade_time_ms
        if processing is not None and processing > cutoff:
            error = CausalIntegrityError(
                "future Trade entered a business module before cutoff activation | "
                f"trade_time_ms={processing} close_time_ms={cutoff}"
            )
            self._mark_incomplete(processing, "future_trade_processed_before_cutoff")
            self._fail(error)
            raise error
        self._cutoff_active = True
        self._pending_cutoff = (pending[0], cutoff, time.monotonic())
        if self._cutoff_timeout > 0:
            self._cutoff_timer = asyncio.get_running_loop().call_later(
                self._cutoff_timeout, self._expire_cutoff
            )

    def _expire_cutoff(self) -> None:
        if self._pending_cutoff is None:
            return
        error = CausalIntegrityError("closed-bar cutoff exceeded its safety timeout")
        self._mark_incomplete(self._pending_cutoff[1], "closed_bar_cutoff_timeout")
        self._fail(error)
        self._discard_pending(error)

    def _mark_incomplete(self, event_ms: int, reason: str) -> None:
        if self._integrity is not None:
            self._integrity.mark_dropped(event_ms, reason)
            revision = self._integrity.revision
            for module in self._modules:
                mark = getattr(module, "mark_trade_incomplete", None)
                if callable(mark):
                    mark(event_ms, reason, revision)

    def _fail(self, error: BaseException) -> None:
        self._accepting = False
        self._accepting_controls = False
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
        control = self._pending_control
        if control is not None and not control.completion.done():
            control.completion.set_exception(error)
        self._future_trades.clear()
        self._pending_trades.clear()
        self._pending_done.set()
        self._finish_cutoff()


__all__ = [
    "CausalIntegrityError",
    "MarketEventProcessor",
    "ProcessorFailureError",
    "ProcessorOverflowError",
    "ProcessorStats",
]
