from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Callable, Mapping, Sequence
from src.app.alerts import AppAlert
from src.market_data.events import MarketFeatureEvent
from src.market_data.range_repair import (
    JOURNAL_INVALID_DROPPED_TRADE,
    JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
    JOURNAL_INVALID_PRODUCER_FAILED,
    JOURNAL_INVALID_PRODUCER_STALE,
    RangeRepairJournalWriter,
)
from src.platform.data.models import (
    MarketEvent,
    MarketEventType,
    MarketKline,
    MarketOrderBook,
    MarketTicker,
    MarketTrade,
)
from src.runtime.models import RuntimePhase
from src.runtime.market_data.integrity import (
    OrderBookDataIntegrityTracker,
    TradeDataIntegrityTracker,
)
from src.signals import TradeSignal

from src.runtime.live_helpers import _event_time_ms
from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats,
    StartupPreviewState, logger,
)
from src.runtime.components.base import RuntimeComponent
from src.runtime.components.latest_state_mailbox import (
    LatestStateMarketEvent,
    is_latest_state_market_event,
)


class MarketEventsComponent(RuntimeComponent):
    async def _handle_market_data_trade_drop(
        self,
        event: MarketEvent,
    ) -> None:
        self.stats.market_events_dropped += 1
        self._mark_range_context_degraded_for_event(
            event,
            reason="market_queue_dropped_trade",
        )
        self._emit_market_queue_full_alert(event)

    async def _process_market_event(self, event: MarketEvent) -> None:
        self.stats.market_events_seen += 1
        is_trade = (
            isinstance(event, MarketTrade)
            or event.event_type is MarketEventType.TRADE
        )
        event_ms = _event_time_ms(event)
        heartbeat = getattr(self, "_heartbeat_service", None)
        if heartbeat is not None:
            heartbeat.note_market_event(event_ms)

        should_update_health = True
        if is_trade:
            now_ms = int(time.time() * 1000)
            should_update_health = (
                now_ms - self._last_trade_health_update_ms >= 1000
            )
            if should_update_health:
                self._last_trade_health_update_ms = now_ms

        if should_update_health:
            self._set_health(
                RuntimePhase.RUNNING,
                healthy=self._health.healthy,
                last_market_event_time_ms=event_ms,
                metadata={
                    **dict(self._health.metadata),
                    "last_event_type": event.event_type.value,
                },
            )

        signals = await self._call_strategy_market_event(event)
        await self._execute_signals(
            signals,
            source=event.event_type.value,
            event_time_ms=event_ms,
        )
        self._maybe_log_live_data_path_stats()

    async def _process_market_feature_event(
        self,
        event: MarketFeatureEvent,
    ) -> None:
        self.stats.feature_events_seen += 1
        if event.type_value == "fixed_time_trade_bar" and isinstance(
            event.data, dict
        ):
            open_ms = event.data.get("open_time_ms")
            if isinstance(open_ms, int):
                self._latest_fixed_time_trade_bar_open_time_ms = open_ms
        heartbeat = getattr(self, "_heartbeat_service", None)
        if heartbeat is not None and event.type_value == "closed_kline":
            open_ms = (
                event.data.get("open_time_ms")
                if isinstance(event.data, dict)
                else None
            )
            if isinstance(open_ms, int):
                heartbeat.note_closed_bar(open_ms)
        signals = await self._get_market_feature_pipeline().dispatch(event)
        await self._execute_signals(
            signals,
            source=event.type_value,
            event_time_ms=event.event_time_ms,
            metadata={"feature_type": event.type_value},
        )
        self._maybe_log_live_data_path_stats()

    async def _enqueue_market_event(self, event: MarketEvent) -> None:
        if isinstance(event, MarketTrade) or event.event_type is MarketEventType.TRADE:
            raise LiveRuntimeError("Trade events must enter MarketEventProcessor")
        if is_latest_state_market_event(event):
            replaced = self._latest_state_mailbox.publish(event)
            if replaced:
                self._record_latest_state_coalesced(event)
            return
        dropped_event: MarketEvent | None = None
        if self._market_queue.full():
            self.stats.market_events_dropped += 1
            try:
                dropped_event = self._market_queue.get_nowait()
                self._market_queue.task_done()
            except asyncio.QueueEmpty:
                pass
            dropped = dropped_event or event
            self._emit_market_queue_full_alert(dropped)
            if (
                isinstance(dropped, MarketOrderBook)
                or dropped.event_type is MarketEventType.ORDER_BOOK
            ):
                tracker = self._order_book_integrity_tracker()
                if tracker is not None:
                    tracker.mark_dropped("runtime_market_queue_drop")
                raise LiveRuntimeError(
                    "order book runtime queue overflow; snapshot/resync required"
                )
        else:
            self._maybe_log_market_queue_backlog(event=event)
        await self._market_queue.put(event)
        self._market_event_available.set()

    def _record_latest_state_coalesced(
        self,
        event: LatestStateMarketEvent,
    ) -> None:
        self.stats.latest_state_events_coalesced += 1
        if event.event_type is MarketEventType.ORDER_BOOK_L2:
            self.stats.order_book_l2_coalesced += 1
        elif event.event_type is MarketEventType.FULL_ORDER_BOOK:
            self.stats.full_order_book_coalesced += 1
        elif event.event_type is MarketEventType.OPEN_INTEREST:
            self.stats.open_interest_coalesced += 1

    def _maybe_log_market_queue_backlog(self, *, event: MarketEvent) -> None:
        qsize = self._market_queue.qsize()
        threshold = self._market_queue_backlog_warn_threshold
        if qsize < threshold:
            return

        now_ms = int(time.time() * 1000)
        if now_ms - self._last_market_queue_backlog_log_ms < 60_000:
            return

        self._last_market_queue_backlog_log_ms = now_ms
        logger.warning(
            "Market queue backlog high | incoming_event_type=%s queue_size=%s threshold=%s maxsize=%s dropped_total=%s",
            event.event_type.value,
            qsize,
            threshold,
            self._market_queue.maxsize,
            self.stats.market_events_dropped,
        )

    def _mark_range_context_degraded_for_event(self, event: MarketEvent, *, reason: str) -> None:
        if not isinstance(event, MarketTrade) and event.event_type is not MarketEventType.TRADE:
            return

        event_ms = _event_time_ms(event)
        if event_ms is None:
            event_ms = int(time.time() * 1000)

        bucket_start = (event_ms // self._closed_bar_interval_ms) * self._closed_bar_interval_ms
        self._mark_range_context_degraded_bucket(bucket_start_ms=bucket_start, reason=reason, event_time_ms=event_ms)

    def _mark_range_context_degraded_bucket(self, *, bucket_start_ms: int, reason: str, event_time_ms: int | None = None) -> None:
        journal_status = {
            "market_queue_dropped_trade": JOURNAL_INVALID_DROPPED_TRADE,
            "trade_dispatcher_drop": JOURNAL_INVALID_DROPPED_TRADE,
            "market_queue_drain_incomplete_before_closed_bar": (
                JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE
            ),
            "market_data_barrier_failed": (
                JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE
            ),
            "trade_data_incomplete_before_closed_bar": (
                JOURNAL_INVALID_DROPPED_TRADE
            ),
            "producer_stale": JOURNAL_INVALID_PRODUCER_STALE,
            "producer_failed": JOURNAL_INVALID_PRODUCER_FAILED,
        }.get(reason)
        if journal_status is not None:
            journal = self._range_repair_journal
            if journal is not None:
                journal.invalidate(
                    bucket_start_ms=bucket_start_ms,
                    status=journal_status,
                    reason=reason,
                    dropped_trades=int(reason in {
                        "market_queue_dropped_trade", "trade_dispatcher_drop"
                    }),
                )
        module = self._range_module
        if module is not None and module.degraded_reason(bucket_start_ms) is None:
            module.mark_degraded(
                bucket_start_ms=bucket_start_ms,
                reason=reason,
            )
            logger.warning(
                "Range context degraded | reason=%s bucket_start_ms=%s event_time_ms=%s dropped_total=%s",
                reason,
                bucket_start_ms,
                event_time_ms,
                self.stats.market_events_dropped,
            )

    def _emit_market_queue_full_alert(self, event: MarketEvent) -> None:
        now_ms = int(time.time() * 1000)
        # Avoid flooding email/alert sinks during a burst, but never drop market
        # data silently.  The closed-bar catch-up path can repair range bars,
        # while this alert tells operators the live stream fell behind.
        if now_ms - self._last_market_queue_full_log_ms >= 60_000:
            self._last_market_queue_full_log_ms = now_ms
            logger.warning(
                "Market queue full; dropped oldest event | incoming_event_type=%s queue_size=%s maxsize=%s dropped_total=%s",
                event.event_type.value,
                self._market_queue.qsize(),
                self._market_queue.maxsize,
                self.stats.market_events_dropped,
            )
        if now_ms - self._last_market_queue_full_alert_ms < 300_000:
            return
        self._last_market_queue_full_alert_ms = now_ms
        self.context.alerts.emit(
            AppAlert(
                subject="AetherEdge market queue full",
                content=(
                    f"Dropped oldest market event before enqueueing {event.event_type.value}; "
                    f"queue_size={self._market_queue.qsize()} maxsize={self._market_queue.maxsize}\n"
                    f"pid={os.getpid()}\n"
                    f"runtime_id={self.app_config.strategy}::{self.app_config.symbol}\n"
                    f"dropped_total={self.stats.market_events_dropped}\n"
                ),
                severity="error",
            )
        )

    async def _consume_market_events(self, *, max_market_events: int | None) -> None:
        while not self._stop_event.is_set():
            if max_market_events is not None and self.stats.market_events_seen >= max_market_events:
                break
            self._raise_on_unhealthy_market_data()
            self._raise_on_unhealthy_producer()
            if self.requirements.closed_kline.enabled:
                await self.poll_closed_bar_once(_health_prechecked=True)
            self._raise_on_unhealthy_market_data()
            remaining_capacity = self._market_queue_drain_batch_size + 1
            if max_market_events is not None:
                remaining_capacity = min(
                    remaining_capacity,
                    max(
                        0,
                        max_market_events - self.stats.market_events_seen,
                    ),
                )
            self._market_event_available.clear()
            events = self._drain_ready_market_events(
                capacity=remaining_capacity
            )
            if not events:
                if (
                    self._all_producers_done()
                    and self._market_queue.empty()
                    and self._latest_state_mailbox.empty()
                ):
                    break
                try:
                    await asyncio.wait_for(
                        self._market_event_available.wait(),
                        timeout=max(
                            min(
                                self.runtime_config.scheduler_poll_seconds,
                                0.25,
                            ),
                            0.05,
                        ),
                    )
                except asyncio.TimeoutError:
                    pass
                continue
            for event, from_fifo_queue in events:
                try:
                    await self.process_market_event(event)
                finally:
                    if from_fifo_queue:
                        self._market_queue.task_done()
            if max_market_events is not None and self.stats.market_events_seen >= max_market_events:
                break

    def _drain_ready_market_events(
        self,
        *,
        capacity: int,
    ) -> list[tuple[MarketEvent, bool]]:
        if capacity <= 0:
            return []

        events: list[tuple[MarketEvent, bool]] = []
        normal_ready = not self._market_queue.empty()
        latest_ready = not self._latest_state_mailbox.empty()

        def take_latest() -> None:
            if len(events) >= capacity:
                return
            try:
                event = self._latest_state_mailbox.get_nowait()
            except asyncio.QueueEmpty:
                return
            events.append((event, False))

        def take_normal(*, reserve_latest: bool) -> None:
            available_capacity = capacity - len(events)
            normal_capacity = min(
                self._market_queue_drain_batch_size,
                available_capacity,
            )
            if (
                reserve_latest
                and normal_capacity == available_capacity
                and normal_capacity > 0
            ):
                normal_capacity -= 1
            for _ in range(max(0, normal_capacity)):
                try:
                    event = self._market_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                events.append((event, True))

        if self._prefer_latest_state_event:
            take_latest()
            take_normal(reserve_latest=False)
        else:
            take_normal(
                reserve_latest=latest_ready and capacity > 1,
            )
            take_latest()

        if normal_ready and latest_ready:
            self._prefer_latest_state_event = (
                not self._prefer_latest_state_event
            )
        return events

    def _raise_on_unhealthy_market_data(self) -> None:
        integrity_error = self.market_state.integrity_error
        if integrity_error is not None:
            raise LiveRuntimeError(str(integrity_error)) from integrity_error
        runtime = self.market_state.runtime
        if runtime is not None:
            runtime.raise_if_failed()

    def _trade_integrity_tracker(self) -> TradeDataIntegrityTracker | None:
        value = self.service_dependencies().trade_data_integrity_tracker
        return value if isinstance(value, TradeDataIntegrityTracker) else None

    def _order_book_integrity_tracker(
        self,
    ) -> OrderBookDataIntegrityTracker | None:
        value = self.service_dependencies().order_book_data_integrity_tracker
        return (
            value
            if isinstance(value, OrderBookDataIntegrityTracker)
            else None
        )

    async def _call_strategy_market_event(self, event: MarketEvent) -> Sequence[TradeSignal]:
        return await self._strategy_host.on_market_event(event)

