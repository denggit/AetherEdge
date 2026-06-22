from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.app import AppConfig, AppContext
from src.app.alerts import AppAlert
from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.events import MarketFeatureEvent
from src.market_data.models import MarketDataSet, RangeBar, TimeRange, WarmupRequest
from src.market_data.storage import SqliteKlineStore, SqliteRangeBarStore, SqliteTradeStore
from src.market_data.warmup.gap_detector import interval_to_ms
from src.market_data.warmup.current_rangebar import CurrentRangeBarWarmupService
from src.market_data.warmup.service import KlineWarmupService
from src.order_management import MasterFollowerExecutionPolicy, MultiExchangeOrderCoordinator, RepositoryDuplicateOrderGuard, SqliteOrderJournalStore, SqlitePositionPlanStore
from src.order_management.models import ExchangeOrderResult, OrderIntentStatus
from src.platform import create_account_client, create_execution_client
from src.platform.account.event_factory import create_account_event_stream
from src.platform.account.events import AccountEvent
from src.platform.account.ports import AccountClient
from src.platform.data.models import MarketEvent, MarketEventType, MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.execution.ports import ExecutionClient
from src.platform.snapshot import PlatformSnapshot
from src.runtime.config import LiveRuntimeConfig, live_runtime_config_from_app
from src.runtime.features import closed_kline_feature, range_aggregate_feature, range_bar_closed_feature
from src.runtime.models import RuntimeHealth, RuntimePhase
from src.runtime.requirements import StrategyRuntimeRequirements, resolve_strategy_runtime_requirements
from src.runtime.orders import LiveOrderIntentFactory
from src.runtime.recovery.service import RecoveryExchangeContext, RuntimeRecoveryService
from src.runtime.tasks import ClosedBarScheduler, ProducerHealthMonitor, ProducerSupervisor
from src.runtime.tasks.scheduler import closed_bar_open_time_ms
from src.signals import TradeSignal


@dataclass
class LiveRuntimeStats:
    market_events_seen: int = 0
    account_events_seen: int = 0
    feature_events_seen: int = 0
    signals_seen: int = 0
    dry_run_actions: int = 0
    order_intents_created: int = 0
    order_results_seen: int = 0
    submitted_intents: int = 0
    partial_failures: int = 0
    failed_intents: int = 0
    range_bars_closed: int = 0
    range_aggregates_created: int = 0
    closed_klines_seen: int = 0
    warmup_runs: int = 0
    recovery_runs: int = 0
    on_start_called: bool = False
    producer_failures: int = 0
    producer_stale: int = 0
    errors: int = 0


class LiveRuntimeError(RuntimeError):
    pass


class LiveRuntimeRunner:
    """Live runtime orchestration for strategy plugins.

    The legacy ``AppRunner`` path is intentionally left untouched. This runner
    composes existing platform, market_data, order_management and recovery
    services into the ``AETHER_RUNTIME_MODE=live_runtime`` path.
    """

    def __init__(
        self,
        *,
        app_config: AppConfig,
        app_context: AppContext,
        runtime_config: LiveRuntimeConfig | None = None,
        services: Mapping[str, Any] | None = None,
    ) -> None:
        self.app_config = app_config
        self.runtime_config = runtime_config or live_runtime_config_from_app(app_config)
        self.context = app_context
        self.services = dict(services or {})
        self.requirements: StrategyRuntimeRequirements = self.services.get("runtime_requirements") or resolve_strategy_runtime_requirements(app_context.strategy, fallback_data_streams=app_config.data_streams)
        self.stats = LiveRuntimeStats()
        self._market_queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=app_config.market_queue_maxsize)
        self._stop_event = asyncio.Event()
        self._producer_tasks: list[asyncio.Task] = []
        self._execution_clients: tuple[ExecutionClient, ...] | None = None
        self._account_clients: tuple[AccountClient, ...] | None = None
        self._order_journal = self.services.get("order_journal")
        self._position_plan_store = self.services.get("position_plan_store")
        self._order_coordinator = self.services.get("order_coordinator")
        self._recovery_service = self.services.get("recovery_service", "__default__")
        self._range_bar_store = self.services.get("range_bar_store")
        self._trade_store = self.services.get("trade_store")
        self._range_bar_builder = self.services.get("range_bar_builder")
        self._range_bar_aggregator = self.services.get("range_bar_aggregator")
        self._producer_monitor: ProducerHealthMonitor = self.services.get("producer_monitor") or ProducerHealthMonitor()
        self._producer_supervisor: ProducerSupervisor = self.services.get("producer_supervisor") or ProducerSupervisor(
            monitor=self._producer_monitor,
            stale_after_ms=self.runtime_config.producer_stale_timeout_ms,
        )
        self._closed_bar_interval = self.requirements.closed_kline.interval if self.requirements.closed_kline.enabled else self.runtime_config.closed_bar_interval
        self._closed_bar_buffer_ms = self.requirements.closed_kline.close_buffer_ms if self.requirements.closed_kline.close_buffer_ms is not None else self.runtime_config.closed_bar_buffer_ms
        self._closed_bar_interval_ms = interval_to_ms(self._closed_bar_interval)
        self._range_pct = self.requirements.range_bars.range_pct if self.requirements.range_bars.enabled else self.runtime_config.range_pct
        self._range_aggregate_interval = self.requirements.range_bars.aggregate_interval if self.requirements.range_bars.enabled else self._closed_bar_interval
        self._closed_bar_scheduler: ClosedBarScheduler = self.services.get("closed_bar_scheduler") or ClosedBarScheduler(
            interval_ms=self._closed_bar_interval_ms,
            close_buffer_ms=self._closed_bar_buffer_ms,
        )
        self._intent_factory = self.services.get("intent_factory") or LiveOrderIntentFactory(
            strategy_id=self.app_config.strategy,
            target_exchanges=self.app_config.exchanges,
        )
        self._last_snapshot: PlatformSnapshot | None = self.services.get("snapshot")
        self._health = RuntimeHealth(
            phase=RuntimePhase.CREATED,
            warmup_complete=not self.runtime_config.warmup_enabled,
            caught_up=not self.runtime_config.warmup_enabled,
            metadata={"runtime_mode": self.runtime_config.mode.value, "strategy": self.app_config.strategy},
        )

    async def run(self, *, max_market_events: int | None = None) -> LiveRuntimeStats:
        self.context.alerts.start()
        try:
            await self._startup()
            self._producer_tasks = self._start_producers()
            await self._consume_market_events(max_market_events=max_market_events)
            self._set_health(RuntimePhase.STOPPED, healthy=True)
            return self.stats
        except Exception as exc:
            self.stats.errors += 1
            self._set_health(RuntimePhase.ERROR, healthy=False, error=str(exc))
            self.context.alerts.emit(AppAlert(subject="AetherEdge live runtime error", content=str(exc), severity="error"))
            raise
        finally:
            await self._stop_producers()
            await self.context.alerts.stop()

    async def start(self) -> RuntimeHealth:
        self._set_health(RuntimePhase.RUNNING, healthy=True, warmup_complete=True, caught_up=True)
        return self._health

    async def stop(self) -> RuntimeHealth:
        self._stop_event.set()
        await self._stop_producers()
        self._set_health(RuntimePhase.STOPPED, healthy=True)
        return self._health

    async def health(self) -> RuntimeHealth:
        return self._health

    async def process_market_event(self, event: MarketEvent) -> None:
        self.stats.market_events_seen += 1
        self._set_health(
            RuntimePhase.RUNNING,
            healthy=self._health.healthy,
            last_market_event_time_ms=_event_time_ms(event),
            metadata={**dict(self._health.metadata), "last_event_type": event.event_type.value},
        )
        if isinstance(event, MarketTrade) or event.event_type is MarketEventType.TRADE:
            await self._process_trade(event)  # type: ignore[arg-type]
        signals = await self._call_strategy_market_event(event)
        await self._execute_signals(signals, source=event.event_type.value, event_time_ms=_event_time_ms(event))

    async def process_market_feature(self, event: MarketFeatureEvent) -> None:
        self.stats.feature_events_seen += 1
        handler = getattr(self.context.strategy, "on_market_feature", None)
        if not callable(handler):
            return
        signals = await handler(event)
        await self._execute_signals(signals or (), source=event.type_value, event_time_ms=event.event_time_ms, metadata={"feature_type": event.type_value})

    async def process_account_event(self, event: AccountEvent) -> None:
        await self._process_account_event(event)

    async def poll_closed_bar_once(self, *, now_ms: int | None = None) -> list[MarketFeatureEvent]:
        now = int(time.time() * 1000) if now_ms is None else now_ms
        open_time_ms = self._closed_bar_scheduler.due_closed_bar(now)
        if open_time_ms is None:
            return []
        rows = await self.context.data.fetch_klines(
            interval=self._closed_bar_interval,
            limit=10,
            use_cache=True,
            oldest_first=True,
        )
        closed_rows = [row for row in rows if row.is_closed and row.open_time_ms == open_time_ms]
        if not closed_rows:
            return []
        event = closed_kline_feature(closed_rows[-1])
        self.stats.closed_klines_seen += 1
        await self.process_market_feature(event)
        mark_emitted = getattr(self._closed_bar_scheduler, "mark_emitted", None)
        if callable(mark_emitted):
            mark_emitted(open_time_ms)
        else:
            self._closed_bar_scheduler.last_emitted_open_time_ms = open_time_ms
        features = [event]
        # Before producing the range aggregate for a just-closed 4H bar, close
        # any gap between startup warmup and the live websocket stream. This
        # avoids silently missing trades if startup/backfill took several
        # seconds or the process restarted mid-bucket.
        if self.requirements.range_bars.enabled and self.requirements.trades.enabled and self.requirements.trades.warmup_enabled:
            await self._run_rangebar_warmup_for_range(
                TimeRange(open_time_ms, open_time_ms + self._closed_bar_interval_ms - 1),
                fail_if_incomplete=True,
            )
        features.extend(await self.emit_range_aggregate_for_bucket(open_time_ms))
        return features

    async def emit_range_aggregate_for_bucket(self, bucket_start_ms: int) -> list[MarketFeatureEvent]:
        store = self._get_range_bar_store()
        rows = store.load(
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            time_range=TimeRange(bucket_start_ms, bucket_start_ms + self._closed_bar_interval_ms - 1),
        )
        if not rows:
            return []
        aggregates = self._get_range_bar_aggregator().aggregate(rows, bucket_ms=self._closed_bar_interval_ms)
        events: list[MarketFeatureEvent] = []
        for aggregate in aggregates:
            if aggregate.bucket_start_ms != bucket_start_ms:
                continue
            event = range_aggregate_feature(aggregate, exchange=self.app_config.data_exchange, timeframe=self._range_aggregate_interval)
            self.stats.range_aggregates_created += 1
            await self.process_market_feature(event)
            events.append(event)
        return events

    async def _startup(self) -> None:
        self._set_health(RuntimePhase.WARMING_UP, healthy=True)
        await self._run_warmup()
        self._set_health(RuntimePhase.CATCHING_UP, healthy=True, warmup_complete=True)
        snapshot = await self._run_recovery()
        await self._call_on_start(snapshot)
        self._set_health(RuntimePhase.RUNNING, healthy=True, warmup_complete=True, caught_up=True)

    async def _run_warmup(self) -> None:
        warmup_services = self.services.get("warmup_services") or self.services.get("warmup_service")
        if warmup_services is not None:
            if not isinstance(warmup_services, (list, tuple)):
                warmup_services = (warmup_services,)
            for service in warmup_services:
                result = service() if callable(service) and not hasattr(service, "warmup") else service
                if hasattr(result, "warmup"):
                    maybe = result.warmup()
                else:
                    maybe = result
                if asyncio.iscoroutine(maybe):
                    await maybe
                self.stats.warmup_runs += 1
        await self._run_requirement_warmup()

    async def _run_requirement_warmup(self) -> None:
        # Closed-kline warmup is generic and can be built from the platform data feed.
        # Historical-trade warmup remains an adapter-specific capability; if a
        # strategy requires it without injecting an implementation, fail fast
        # instead of silently starting with incomplete range-bar context.
        if self.requirements.closed_kline.enabled and self.requirements.closed_kline.warmup_days > 0:
            end_open = closed_bar_open_time_ms(
                int(time.time() * 1000),
                interval_ms=self._closed_bar_interval_ms,
                close_buffer_ms=self._closed_bar_buffer_ms,
            )
            if end_open >= 0:
                start_open = max(0, end_open - int(self.requirements.closed_kline.warmup_days) * 24 * 60 * 60_000)
                repository = self.services.get("kline_store") or SqliteKlineStore()
                service = KlineWarmupService(data_feed=self.context.data, repository=repository)
                result = await service.warmup(
                    WarmupRequest(
                        symbol=self.app_config.symbol,
                        dataset=MarketDataSet.KLINES,
                        interval=self._closed_bar_interval,
                        time_range=TimeRange(start_open, end_open),
                    )
                )
                self.stats.warmup_runs += 1
                if not result.caught_up:
                    raise LiveRuntimeError(f"closed-kline warmup did not catch up: {len(result.gaps_after)} gaps remain")
                await self._hydrate_strategy_closed_klines(repository, time_range=TimeRange(start_open, end_open))
        if self.requirements.range_bars.enabled and self.requirements.trades.enabled and self.requirements.trades.warmup_enabled:
            await self._run_current_rangebar_warmup()

    async def _run_current_rangebar_warmup(self) -> None:
        now_ms = int(time.time() * 1000)
        bucket_start_ms = (now_ms // self._closed_bar_interval_ms) * self._closed_bar_interval_ms
        if now_ms <= bucket_start_ms:
            return
        await self._run_rangebar_warmup_for_range(TimeRange(bucket_start_ms, now_ms), fail_if_incomplete=now_ms - bucket_start_ms > 60_000)

    async def _run_rangebar_warmup_for_range(self, time_range: TimeRange, *, fail_if_incomplete: bool) -> None:
        trade_store = self._get_trade_store()
        range_store = self._get_range_bar_store()
        profile = self.context.data.market_profile
        contract_value = profile.contract_value(self.app_config.data_exchange) or Decimal("1")
        historical_feed = self.services.get("historical_trade_feed")
        if historical_feed is None and hasattr(self.context.data, "fetch_trades"):
            historical_feed = self.context.data
        service = self.services.get("current_rangebar_warmup_service") or CurrentRangeBarWarmupService(
            trade_repository=trade_store,
            trade_coverage_repository=trade_store,
            range_bar_repository=range_store,
            historical_trade_feed=historical_feed,
            range_pct=self._range_pct,
            contract_value=contract_value,
            batch_limit=int(os.getenv("AETHER_TRADE_WARMUP_BATCH_LIMIT", "1000")),
        )
        result = await service.warmup(symbol=self.app_config.symbol, time_range=time_range)
        self.stats.warmup_runs += 1
        self.stats.range_bars_closed += result.range_bars_saved
        bars = range_store.load(
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            time_range=time_range,
        )
        seed = getattr(self._get_range_bar_builder(), "seed_from_bars", None)
        if callable(seed):
            seed(bars)
        if fail_if_incomplete and not result.caught_up:
            raise LiveRuntimeError("range-bar trade warmup did not catch up; historical trade feed or local coverage is incomplete")

    async def _hydrate_strategy_closed_klines(self, repository, *, time_range: TimeRange) -> None:
        handler = getattr(self.context.strategy, "on_market_feature", None)
        if not callable(handler):
            return
        rows = repository.load(symbol=self.app_config.symbol, interval=self._closed_bar_interval, time_range=time_range)
        for row in rows:
            if not row.is_closed:
                continue
            await self.process_market_feature(closed_kline_feature(row))

    async def _run_recovery(self) -> PlatformSnapshot:
        service = self._get_recovery_service()
        if service is None:
            if self._last_snapshot is None:
                raise LiveRuntimeError("startup snapshot is required before live trading")
            return self._last_snapshot
        report = await service.recover(strategy=self.context.strategy)
        self.stats.recovery_runs += 1
        if not report.ok:
            raise LiveRuntimeError(f"runtime recovery failed: {tuple(report.issues)}")
        if report.strategy_signals:
            await self._execute_signals(report.strategy_signals, source="recovery", event_time_ms=int(time.time() * 1000), metadata={"feature_type": "recovery"})
        if report.snapshots:
            self._last_snapshot = report.snapshots[0]
        if self._last_snapshot is None:
            raise LiveRuntimeError("recovery completed without a startup snapshot")
        return self._last_snapshot

    async def _call_on_start(self, snapshot: PlatformSnapshot) -> None:
        on_start = getattr(self.context.strategy, "on_start", None)
        if not callable(on_start):
            return
        signals = await on_start(snapshot)
        self.stats.on_start_called = True
        await self._execute_signals(signals or (), source="on_start", event_time_ms=int(time.time() * 1000))

    def _start_producers(self) -> list[asyncio.Task]:
        tasks: list[asyncio.Task] = []
        if self.requirements.trades.enabled and self.requirements.trades.stream_enabled:
            tasks.append(asyncio.create_task(self._producer_supervisor.run_stream(name="trades", stream=self.context.data.stream_trades(), on_item=self._enqueue_market_event)))
        if self.requirements.order_book.enabled and self.requirements.order_book.stream_enabled:
            tasks.append(asyncio.create_task(self._producer_supervisor.run_stream(name="order_book", stream=self.context.data.stream_order_book(), on_item=self._enqueue_market_event)))
        if self.requirements.private_account_stream.enabled:
            for stream in self._get_account_event_streams():
                tasks.append(asyncio.create_task(self._producer_supervisor.run_stream(name=f"account:{stream.exchange.value}", stream=stream.stream_events(), on_item=self._process_account_event)))
        return tasks

    async def _enqueue_market_event(self, event: MarketEvent) -> None:
        if self._market_queue.full():
            try:
                self._market_queue.get_nowait()
                self._market_queue.task_done()
            except asyncio.QueueEmpty:
                pass
        await self._market_queue.put(event)

    async def _process_account_event(self, event: AccountEvent) -> None:
        self.stats.account_events_seen += 1
        save = getattr(self.context.state_store, "save_account_event", None)
        if callable(save):
            await asyncio.to_thread(save, event)
        handler = getattr(self.context.strategy, "on_account_event", None)
        if not callable(handler):
            return
        signals = await handler(event)
        await self._execute_signals(signals or (), source=f"account:{event.exchange.value}", event_time_ms=event.event_time_ms)

    async def _consume_market_events(self, *, max_market_events: int | None) -> None:
        while not self._stop_event.is_set():
            if max_market_events is not None and self.stats.market_events_seen >= max_market_events:
                break
            if self.requirements.closed_kline.enabled:
                await self.poll_closed_bar_once()
            self._raise_on_unhealthy_producer()
            if self._all_producers_done() and self._market_queue.empty():
                break
            try:
                event = await asyncio.wait_for(self._market_queue.get(), timeout=max(self.runtime_config.scheduler_poll_seconds, 0.05))
            except asyncio.TimeoutError:
                continue
            try:
                await self.process_market_event(event)
            finally:
                self._market_queue.task_done()
            if max_market_events is not None and self.stats.market_events_seen >= max_market_events:
                break

    async def _process_trade(self, trade: MarketTrade) -> None:
        if not self.requirements.range_bars.enabled:
            return
        builder = self._get_range_bar_builder()
        closed = builder.on_trade(trade)
        if not closed:
            return
        store = self._get_range_bar_store()
        for bar in closed:
            await asyncio.to_thread(store.save, [bar])
            self.stats.range_bars_closed += 1
            await self.process_market_feature(range_bar_closed_feature(bar, exchange=trade.exchange))

    async def _call_strategy_market_event(self, event: MarketEvent) -> Sequence[TradeSignal]:
        strategy = self.context.strategy
        if isinstance(event, MarketKline) or event.event_type is MarketEventType.KLINE:
            handler = getattr(strategy, "on_kline", None)
        elif isinstance(event, MarketTicker) or event.event_type is MarketEventType.TICKER:
            handler = getattr(strategy, "on_ticker", None)
        elif isinstance(event, MarketTrade) or event.event_type is MarketEventType.TRADE:
            handler = getattr(strategy, "on_trade", None)
        elif isinstance(event, MarketOrderBook) or event.event_type is MarketEventType.ORDER_BOOK:
            handler = getattr(strategy, "on_order_book", None)
        else:
            handler = None
        if not callable(handler):
            return ()
        return await handler(event) or ()

    async def _execute_signals(
        self,
        signals: Sequence[TradeSignal],
        *,
        source: str,
        event_time_ms: int | None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        for signal in signals:
            self.stats.signals_seen += 1
            if self.app_config.dry_run:
                self.stats.dry_run_actions += 1
                continue
            intent = self._intent_factory.create(signal, source=source, event_time_ms=event_time_ms, metadata=metadata)
            results = await self._get_order_coordinator().execute(intent)
            self._record_order_results(results)

    def _record_order_results(self, results: Sequence[ExchangeOrderResult]) -> None:
        self.stats.order_intents_created += 1
        self.stats.order_results_seen += len(results)
        ok_count = sum(1 for result in results if result.ok)
        if ok_count == len(results) and results:
            self.stats.submitted_intents += 1
            return
        if ok_count > 0:
            self.stats.partial_failures += 1
            self._set_health(
                RuntimePhase.RUNNING,
                healthy=False,
                error="partial exchange execution failure",
                metadata={**dict(self._health.metadata), "partial_failures": self.stats.partial_failures},
            )
        else:
            self.stats.failed_intents += 1
            self._set_health(RuntimePhase.RUNNING, healthy=False, error="exchange execution failed")

    async def _stop_producers(self) -> None:
        for task in self._producer_tasks:
            task.cancel()
        if self._producer_tasks:
            await asyncio.gather(*self._producer_tasks, return_exceptions=True)
        self._producer_tasks = []

    def _raise_on_unhealthy_producer(self) -> None:
        unhealthy = self._producer_supervisor.check()
        if not unhealthy:
            return
        self.stats.producer_failures += sum(1 for item in unhealthy if item.status.value == "failed")
        self.stats.producer_stale += sum(1 for item in unhealthy if item.status.value == "stale")
        message = "; ".join(f"{item.name}:{item.status.value}:{item.error}" for item in unhealthy)
        raise LiveRuntimeError(f"producer unhealthy: {message}")

    def _all_producers_done(self) -> bool:
        return bool(self._producer_tasks) and all(task.done() for task in self._producer_tasks)

    def _get_account_event_streams(self):
        injected = self.services.get("account_event_streams")
        if injected is not None:
            return tuple(injected)
        return tuple(
            create_account_event_stream(exchange, symbol=self.app_config.symbol, config=ExchangeConfig.from_env(exchange))
            for exchange in self.app_config.exchanges
        )

    def _get_execution_clients(self) -> tuple[ExecutionClient, ...]:
        if self._execution_clients is None:
            injected = self.services.get("execution_clients")
            if injected is not None:
                self._execution_clients = tuple(injected)
            else:
                self._execution_clients = tuple(
                    create_execution_client(exchange, symbol=self.app_config.symbol, config=ExchangeConfig.from_env(exchange))
                    for exchange in self.app_config.exchanges
                )
        return self._execution_clients

    def _get_account_clients(self) -> tuple[AccountClient, ...]:
        if self._account_clients is None:
            injected = self.services.get("account_clients")
            if injected is not None:
                self._account_clients = tuple(injected)
            else:
                self._account_clients = tuple(
                    create_account_client(exchange, symbol=self.app_config.symbol, config=ExchangeConfig.from_env(exchange))
                    for exchange in self.app_config.exchanges
                )
        return self._account_clients

    def _get_order_journal(self):
        if self._order_journal is None:
            path = os.getenv("AETHER_ORDER_JOURNAL_DB", "data/state/aether_order_journal.sqlite3")
            self._order_journal = SqliteOrderJournalStore(path)
        return self._order_journal

    def _get_order_coordinator(self):
        if self._order_coordinator is None:
            journal = self._get_order_journal()
            self._order_coordinator = MultiExchangeOrderCoordinator(
                clients=self._get_execution_clients(),
                repository=journal,
                planner=self.context.planner,
                duplicate_guard=RepositoryDuplicateOrderGuard(journal),
                master_follower_policy=(
                    None
                    if self.runtime_config.master_follower_policy is None
                    else MasterFollowerExecutionPolicy.from_config(self.runtime_config.master_follower_policy)
                ),
                position_plan_store=self._get_position_plan_store(),
            )
        return self._order_coordinator

    def _get_recovery_service(self):
        if self._recovery_service == "__default__":
            clients = self._get_execution_clients()
            accounts = self._get_account_clients()
            contexts = [
                RecoveryExchangeContext(account=account, execution=execution, state_store=self.context.state_store)
                for account, execution in zip(accounts, clients, strict=False)
            ]
            self._recovery_service = RuntimeRecoveryService(exchange_contexts=contexts, order_journal=self._get_order_journal(), position_plan_store=self._get_position_plan_store())
        return self._recovery_service

    def _get_position_plan_store(self):
        if self._position_plan_store is None:
            path = os.getenv("AETHER_POSITION_PLAN_DB", "data/state/aether_position_plan.sqlite3")
            self._position_plan_store = SqlitePositionPlanStore(path)
        return self._position_plan_store

    def _get_range_bar_builder(self):
        if self._range_bar_builder is None:
            profile = self.context.data.market_profile
            contract_value = profile.contract_value(self.app_config.data_exchange) or Decimal("1")
            self._range_bar_builder = RangeBarBuilder(range_pct=self._range_pct, contract_value=contract_value)
        return self._range_bar_builder

    def _get_trade_store(self):
        if self._trade_store is None:
            self._trade_store = SqliteTradeStore()
        return self._trade_store

    def _get_range_bar_store(self):
        if self._range_bar_store is None:
            self._range_bar_store = SqliteRangeBarStore()
        return self._range_bar_store

    def _get_range_bar_aggregator(self):
        if self._range_bar_aggregator is None:
            self._range_bar_aggregator = RangeBarAggregator()
        return self._range_bar_aggregator

    def _set_health(
        self,
        phase: RuntimePhase,
        *,
        healthy: bool | None = None,
        warmup_complete: bool | None = None,
        caught_up: bool | None = None,
        last_market_event_time_ms: int | None = None,
        error: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self._health = RuntimeHealth(
            phase=phase,
            healthy=self._health.healthy if healthy is None else healthy,
            warmup_complete=self._health.warmup_complete if warmup_complete is None else warmup_complete,
            caught_up=self._health.caught_up if caught_up is None else caught_up,
            last_market_event_time_ms=self._health.last_market_event_time_ms if last_market_event_time_ms is None else last_market_event_time_ms,
            error=error if error is not None else self._health.error,
            metadata=dict(self._health.metadata if metadata is None else metadata),
        )


def _event_time_ms(event: MarketEvent) -> int | None:
    if isinstance(event, MarketTrade):
        return event.trade_time_ms if event.trade_time_ms is not None else event.event_time_ms
    if isinstance(event, MarketOrderBook):
        return event.event_time_ms
    if isinstance(event, MarketKline):
        return event.close_time_ms
    if isinstance(event, MarketTicker):
        return event.time_ms
    return None
