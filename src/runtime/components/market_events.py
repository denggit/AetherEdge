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
from src.platform.data.models import MarketEvent, MarketEventType, MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.runtime.feature_pipeline import (
    TradeDerivedFeaturePipeline,
    TradeFeatureRuntimeConfig,
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

        if is_trade:
            await self._process_trade(event)  # type: ignore[arg-type]
            if self._trade_events_are_range_only():
                self._maybe_log_live_data_path_stats()
                return
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
                isinstance(dropped, MarketTrade)
                or dropped.event_type is MarketEventType.TRADE
            ):
                self._mark_trade_integrity_dropped(
                    dropped,
                    reason="market_queue_dropped_trade",
                )
                self._mark_range_context_degraded_for_event(
                    dropped,
                    reason="market_queue_dropped_trade",
                )
            elif (
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
            self._invalidate_range_repair_journal(
                bucket_start_ms=bucket_start_ms,
                status=journal_status,
                reason=reason,
                dropped_trades=(
                    1
                    if reason
                    in {"market_queue_dropped_trade", "trade_dispatcher_drop"}
                    else 0
                ),
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
            if self._all_producers_done() and self._market_queue.empty():
                break
            try:
                event = await asyncio.wait_for(self._market_queue.get(), timeout=max(self.runtime_config.scheduler_poll_seconds, 0.05))
            except asyncio.TimeoutError:
                continue
            events = [event]
            remaining_capacity = self._market_queue_drain_batch_size - 1
            if max_market_events is not None:
                remaining_capacity = min(remaining_capacity, max(0, max_market_events - self.stats.market_events_seen - 1))
            for _ in range(max(0, remaining_capacity)):
                try:
                    events.append(self._market_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            for event in events:
                try:
                    await self.process_market_event(event)
                finally:
                    self._market_queue.task_done()
            if max_market_events is not None and self.stats.market_events_seen >= max_market_events:
                break

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

    def _mark_trade_integrity_dropped(
        self,
        event: MarketEvent,
        *,
        reason: str,
    ) -> None:
        tracker = self._trade_integrity_tracker()
        if tracker is None:
            return
        event_ms = _event_time_ms(event)
        tracker.mark_dropped(
            int(time.time() * 1000) if event_ms is None else event_ms,
            reason,
        )

    async def _process_trade(self, trade: MarketTrade) -> None:
        if self.market_state.modules_managed:
            module = self._range_module
            if module is not None:
                self.stats.range_bars_closed = module.bars_closed
                self._append_range_repair_trade(trade)
            return
        await self._dispatch_trade_derived_features(trade)
        if not self.requirements.range_bars.enabled:
            return
        module = self._require_range_module()
        before = module.bars_closed
        await module.process_trade(trade)
        self.stats.range_bars_closed += module.bars_closed - before
        self._append_range_repair_trade(trade)

    async def _dispatch_trade_derived_features(
        self, trade: MarketTrade
    ) -> None:
        await self._trade_derived_feature_pipeline.process_trade(trade)

    @property
    def _fixed_time_trade_bar_builder(self):
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            return pipeline.fixed_time_trade_bar_builder
        return getattr(self, "_fixed_time_trade_bar_builder_compat", None)

    @_fixed_time_trade_bar_builder.setter
    def _fixed_time_trade_bar_builder(self, value) -> None:
        self._fixed_time_trade_bar_builder_compat = value
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            pipeline.fixed_time_trade_bar_builder = value

    @property
    def _trade_footprint_builder(self):
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            return pipeline.trade_footprint_builder
        return getattr(self, "_trade_footprint_builder_compat", None)

    @_trade_footprint_builder.setter
    def _trade_footprint_builder(self, value) -> None:
        self._trade_footprint_builder_compat = value
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            pipeline.trade_footprint_builder = value

    @property
    def _range_footprint_builder(self):
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            return pipeline.range_footprint_builder
        return getattr(self, "_range_footprint_builder_compat", None)

    @_range_footprint_builder.setter
    def _range_footprint_builder(self, value) -> None:
        self._range_footprint_builder_compat = value
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            pipeline.range_footprint_builder = value

    def _submit_range_checkpoint_if_due(self, trade: MarketTrade) -> bool:
        return self._require_range_module().submit_checkpoint_if_due(trade)

    def _prune_range_bars_by_bucket(self, *, current_bucket: int) -> None:
        self._require_range_module().prune(current_bucket=current_bucket)

    async def _call_strategy_market_event(self, event: MarketEvent) -> Sequence[TradeSignal]:
        return await self._strategy_host.on_market_event(event)

    def _trade_events_are_range_only(self) -> bool:
        return getattr(
            self.context.strategy,
            "raw_trade_callbacks_enabled",
            None,
        ) is False
