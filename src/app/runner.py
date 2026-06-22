from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Sequence

from src.app.alerts import AppAlert
from src.app.config import AppConfig
from src.app.context import AppContext
from src.platform.data.models import MarketEvent, MarketEventType, MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.execution import MultiExchangeExecutionClient
from src.planner import ExecutionPlan, PlannedExecutionAction
from src.signals import TradeSignal
from src.utils.log import get_logger

logger = get_logger(__name__)


@dataclass
class AppRunnerStats:
    market_events_seen: int = 0
    signals_seen: int = 0
    plans_created: int = 0
    execution_actions: int = 0
    dry_run_actions: int = 0
    dropped_market_events: int = 0
    errors: int = 0


class AppRunner:
    """Lightweight strategy application runner.

    It wires data -> strategy -> signal -> planner -> execution. It does not
    contain concrete strategy rules.
    """

    def __init__(self, *, config: AppConfig, context: AppContext) -> None:
        self.config = config
        self.context = context
        self.stats = AppRunnerStats()
        self._market_queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=config.market_queue_maxsize)
        self._signal_queue: asyncio.Queue[TradeSignal] = asyncio.Queue(maxsize=config.signal_queue_maxsize)
        self._stop_event = asyncio.Event()
        self._last_market_queue_full_log_ms = 0

    def stop(self) -> None:
        self._stop_event.set()

    async def process_market_event(self, event: MarketEvent) -> None:
        self.stats.market_events_seen += 1
        try:
            signals = await self._call_strategy(event)
            if signals:
                logger.info("Strategy emitted signals | event_type=%s count=%s", event.event_type.value, len(signals))
            await self.process_signals(signals)
        except Exception as exc:
            self.stats.errors += 1
            logger.exception("Strategy error | event_type=%s", event.event_type.value)
            self.context.alerts.emit(AppAlert(subject="AetherEdge strategy error", content=str(exc), severity="error"))

    async def process_signals(self, signals: Sequence[TradeSignal]) -> None:
        for signal in signals:
            self.stats.signals_seen += 1
            plan = self.context.planner.plan(signal)
            self.stats.plans_created += len(plan.items)
            await self.execute_plan(plan)

    async def execute_plan(self, plan: ExecutionPlan) -> None:
        for item in plan.items:
            if self.config.dry_run:
                self.stats.dry_run_actions += 1
                logger.info("Dry-run execution skipped | action=%s", item.action.value)
                continue
            try:
                logger.info("Executing planned action | action=%s", item.action.value)
                await self._execute_item(item.action, item.order_request, item.stop_market_request)
                self.stats.execution_actions += 1
            except Exception as exc:
                self.stats.errors += 1
                logger.exception("Execution error | action=%s", item.action.value)
                self.context.alerts.emit(AppAlert(subject="AetherEdge execution error", content=str(exc), severity="error"))

    async def run_streams(self, *, max_market_events: int | None = None) -> AppRunnerStats:
        logger.info(
            "App runner streams starting | symbol=%s streams=%s dry_run=%s max_market_events=%s",
            self.config.symbol,
            ",".join(self.config.data_streams),
            self.config.dry_run,
            max_market_events,
        )
        self.context.alerts.start()
        producers = self._start_producers()
        consumer = asyncio.create_task(self._consume_market_events(max_market_events=max_market_events))
        try:
            await consumer
        finally:
            for task in producers:
                task.cancel()
            await asyncio.gather(*producers, return_exceptions=True)
            await self.context.alerts.stop()
            logger.info("App runner streams stopped | stats=%s", self.stats)
        return self.stats

    def _start_producers(self) -> list[asyncio.Task]:
        tasks: list[asyncio.Task] = []
        streams = set(self.config.data_streams)
        if "trades" in streams:
            logger.info("Starting market producer | stream=trades")
            tasks.append(asyncio.create_task(self._produce_trades()))
        if "order_book" in streams or "books" in streams:
            logger.info("Starting market producer | stream=order_book")
            tasks.append(asyncio.create_task(self._produce_order_books()))
        return tasks

    async def _produce_trades(self) -> None:
        async for trade in self.context.data.stream_trades():
            if self._stop_event.is_set():
                break
            self._put_market_event_nowait(trade)

    async def _produce_order_books(self) -> None:
        async for book in self.context.data.stream_order_book():
            if self._stop_event.is_set():
                break
            self._put_market_event_nowait(book)

    async def _consume_market_events(self, *, max_market_events: int | None) -> None:
        while not self._stop_event.is_set():
            event = await self._market_queue.get()
            try:
                await self.process_market_event(event)
            finally:
                self._market_queue.task_done()
            if max_market_events is not None and self.stats.market_events_seen >= max_market_events:
                self.stop()
                break

    def _put_market_event_nowait(self, event: MarketEvent) -> None:
        try:
            self._market_queue.put_nowait(event)
        except asyncio.QueueFull:
            try:
                self._market_queue.get_nowait()
                self._market_queue.task_done()
            except asyncio.QueueEmpty:
                pass
            self.stats.dropped_market_events += 1
            self._log_market_queue_full(event)
            self._market_queue.put_nowait(event)

    def _log_market_queue_full(self, event: MarketEvent) -> None:
        now_ms = int(time.time() * 1000)
        if now_ms - self._last_market_queue_full_log_ms < 60_000:
            return
        self._last_market_queue_full_log_ms = now_ms
        logger.warning(
            "Market queue full; dropped oldest event | incoming_event_type=%s queue_size=%s maxsize=%s dropped_total=%s",
            event.event_type.value,
            self._market_queue.qsize(),
            self._market_queue.maxsize,
            self.stats.dropped_market_events,
        )

    async def _call_strategy(self, event: MarketEvent) -> Sequence[TradeSignal]:
        if isinstance(event, MarketKline) or event.event_type is MarketEventType.KLINE:
            return await self.context.strategy.on_kline(event)  # type: ignore[arg-type]
        if isinstance(event, MarketTicker) or event.event_type is MarketEventType.TICKER:
            return await self.context.strategy.on_ticker(event)  # type: ignore[arg-type]
        if isinstance(event, MarketTrade) or event.event_type is MarketEventType.TRADE:
            return await self.context.strategy.on_trade(event)  # type: ignore[arg-type]
        if isinstance(event, MarketOrderBook) or event.event_type is MarketEventType.ORDER_BOOK:
            return await self.context.strategy.on_order_book(event)  # type: ignore[arg-type]
        return []

    async def _execute_item(self, action, order_request, stop_market_request) -> None:
        execution = self.context.execution
        if isinstance(execution, MultiExchangeExecutionClient):
            if action is PlannedExecutionAction.PLACE_ORDER:
                await execution.place_order_all(order_request)
            elif action is PlannedExecutionAction.PLACE_STOP_MARKET_ORDER:
                await execution.place_stop_market_order_all(stop_market_request)
            elif action is PlannedExecutionAction.CANCEL_ALL_ORDERS:
                await execution.cancel_all_orders_all()
            elif action is PlannedExecutionAction.CANCEL_ALL_STOP_ORDERS:
                await execution.cancel_all_stop_orders_all()
            return

        if action is PlannedExecutionAction.PLACE_ORDER:
            await execution.place_order(order_request)
        elif action is PlannedExecutionAction.PLACE_STOP_MARKET_ORDER:
            await execution.place_stop_market_order(stop_market_request)
        elif action is PlannedExecutionAction.CANCEL_ALL_ORDERS:
            await execution.cancel_all_orders()
        elif action is PlannedExecutionAction.CANCEL_ALL_STOP_ORDERS:
            await execution.cancel_all_stop_orders()
