from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Callable, Iterable, Mapping

from src.app.alerts import AppAlert
from src.platform.exchanges.models import Balance, MarginMode, Order, OrderQuery, OrderStatus, StopOrderQuery
from src.platform.snapshot import PlatformSnapshot
from src.runtime.account_sync.models import SyncExchangeContext, SyncResult
from src.runtime.requirements import AccountStateRequirement, OrderStateRequirement
from src.utils.log import get_logger

logger = get_logger(__name__)


class RequestThrottle:
    """Small per-exchange throttle for request-based sync tasks."""

    def __init__(self, *, min_interval_seconds: float = 0.0, now_fn: Callable[[], float] | None = None) -> None:
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._now_fn = now_fn or time.monotonic
        self._last_by_exchange: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def wait(self, exchange: str, *, priority: bool = False) -> None:
        lock = self._locks.setdefault(exchange, asyncio.Lock())
        async with lock:
            if not priority and self.min_interval_seconds > 0:
                elapsed = self._now_fn() - self._last_by_exchange.get(exchange, 0.0)
                delay = self.min_interval_seconds - elapsed
                if delay > 0:
                    await asyncio.sleep(delay)
            self._last_by_exchange[exchange] = self._now_fn()

    def recently_synced(self, exchange: str, *, within_seconds: float) -> bool:
        last = self._last_by_exchange.get(exchange)
        return last is not None and self._now_fn() - last < within_seconds


class AccountStateSyncService:
    def __init__(
        self,
        *,
        contexts: Iterable[SyncExchangeContext],
        config: AccountStateRequirement | None = None,
        alert_sink: Any | None = None,
        throttle: RequestThrottle | None = None,
        asset: str = "USDT",
    ) -> None:
        self.contexts = tuple(contexts)
        self.config = config or AccountStateRequirement()
        self.alert_sink = alert_sink
        self.throttle = throttle or RequestThrottle()
        self.asset = asset
        self._failures: dict[str, int] = {}

    async def run_periodic(self, stop_event: asyncio.Event) -> None:
        logger.info("Account sync task started | interval_seconds=%s", self.config.poll_interval_seconds)
        while not stop_event.is_set():
            await _sleep_with_jitter(stop_event, self.config.poll_interval_seconds)
            if stop_event.is_set():
                break
            await self.sync_once(sync_type="account_periodic")

    async def sync_once(self, *, sync_type: str = "account_periodic", priority: bool = False) -> tuple[SyncResult, ...]:
        return tuple([await self._sync_context(context, sync_type=sync_type, priority=priority) for context in self.contexts])

    async def _sync_context(self, context: SyncExchangeContext, *, sync_type: str, priority: bool) -> SyncResult:
        exchange = context.account.exchange.value
        start = time.perf_counter()
        request_count = 0
        try:
            await self.throttle.wait(exchange, priority=priority)
            balance = await context.account.fetch_balance(self.asset)
            request_count += 1
            positions = await context.account.fetch_positions()
            request_count += 1
            leverage = await context.account.fetch_leverage(margin_mode=MarginMode.CROSS)
            request_count += 1
            position_mode = await context.account.fetch_position_mode()
            request_count += 1
            snapshot = PlatformSnapshot(
                symbol=context.account.symbol,
                balance=Balance(
                    exchange=balance.exchange,
                    asset=balance.asset,
                    total=balance.total,
                    available=balance.available,
                    raw={**dict(balance.raw), "snapshot_scope": "account_state_only"},
                ),
                positions=positions,
                open_orders=[],
                open_stop_orders=[],
                leverage=leverage,
                position_mode=position_mode,
            )
            context.state_store.save_snapshot(snapshot)
            self._failures[exchange] = 0
            result = _result(exchange, sync_type, request_count, start, True)
            logger.info(
                "Account state synced | exchange=%s sync_type=%s request_count=%s duration_ms=%s success=%s",
                result.exchange,
                result.sync_type,
                result.request_count,
                result.duration_ms,
                result.success,
            )
            return result
        except Exception as exc:
            failures = self._failures.get(exchange, 0) + 1
            self._failures[exchange] = failures
            result = _result(exchange, sync_type, request_count, start, False, error=str(exc), metadata={"consecutive_failures": failures})
            logger.warning(
                "Account state sync failed | exchange=%s sync_type=%s request_count=%s duration_ms=%s success=%s failures=%s error=%s",
                result.exchange,
                result.sync_type,
                result.request_count,
                result.duration_ms,
                result.success,
                failures,
                exc,
            )
            if failures >= self.config.consecutive_failure_alert_threshold:
                _emit(self.alert_sink, AppAlert(subject="AetherEdge account sync failures", content=f"{exchange} {sync_type} failed {failures} times: {exc}", severity="error"))
            return result


class OrderStateSyncService:
    def __init__(
        self,
        *,
        contexts: Iterable[SyncExchangeContext],
        config: OrderStateRequirement | None = None,
        alert_sink: Any | None = None,
        throttle: RequestThrottle | None = None,
        active_check: Callable[[], bool] | None = None,
        position_plan_store: Any | None = None,
        known_order_ids: Callable[[], Mapping[str, Mapping[str, tuple[str | None, ...]]]] | None = None,
    ) -> None:
        self.contexts = tuple(contexts)
        self.config = config or OrderStateRequirement()
        self.alert_sink = alert_sink
        self.throttle = throttle or RequestThrottle()
        self.active_check = active_check or (lambda: False)
        self.position_plan_store = position_plan_store
        self.known_order_ids = known_order_ids
        self._failures: dict[str, int] = {}

    async def run_periodic(self, stop_event: asyncio.Event) -> None:
        logger.info("Order sync task started | interval_seconds=%s active_only=True", self.config.poll_interval_seconds)
        while not stop_event.is_set():
            await _sleep_with_jitter(stop_event, self.config.poll_interval_seconds)
            if stop_event.is_set():
                break
            if not self.active_check():
                logger.info("Order state sync skipped | reason=no_active_position_no_pending_orders")
                continue
            logger.info("Order state sync tick | active_position=True interval_seconds=%s", self.config.poll_interval_seconds)
            await self.sync_once(sync_type="order_periodic")

    async def sync_once(self, *, sync_type: str = "order_periodic", priority: bool = False) -> tuple[SyncResult, ...]:
        return tuple([await self._sync_context(context, sync_type=sync_type, priority=priority) for context in self.contexts])

    async def _sync_context(self, context: SyncExchangeContext, *, sync_type: str, priority: bool) -> SyncResult:
        exchange = context.execution.exchange.value
        start = time.perf_counter()
        request_count = 0
        try:
            await self.throttle.wait(exchange, priority=priority)
            if self.config.sync_position:
                await context.account.fetch_positions()
                request_count += 1
                logger.info("Positions synced | exchange=%s sync_type=%s", exchange, sync_type)
            if self.config.sync_open_orders:
                orders = await context.execution.fetch_open_orders()
                request_count += 1
                for order in orders:
                    context.state_store.save_order(order, is_stop_order=False)
                self._mark_missing_open_orders_closed(context, orders=orders, is_stop_order=False)
                logger.info("Open orders synced | exchange=%s sync_type=%s count=%s", exchange, sync_type, len(orders))
            if self.config.sync_open_stop_orders:
                stop_orders = await context.execution.fetch_open_stop_orders()
                request_count += 1
                for order in stop_orders:
                    context.state_store.save_order(order, is_stop_order=True)
                self._mark_missing_open_orders_closed(context, orders=stop_orders, is_stop_order=True)
                logger.info("Open stop orders synced | exchange=%s sync_type=%s count=%s", exchange, sync_type, len(stop_orders))
            for order_id in self._known_ids(exchange, key="orders"):
                query = OrderQuery(symbol=context.execution.symbol, order_id=order_id)
                order = await context.execution.fetch_order_status(query)
                request_count += 1
                context.state_store.save_order(order, is_stop_order=False)
            for stop_order_id in self._known_ids(exchange, key="stop_orders"):
                query = StopOrderQuery(symbol=context.execution.symbol, stop_order_id=stop_order_id)
                order = await context.execution.fetch_stop_order_status(query)
                request_count += 1
                context.state_store.save_order(order, is_stop_order=True)
            self._failures[exchange] = 0
            result = _result(exchange, sync_type, request_count, start, True)
            logger.info(
                "Order state synced | exchange=%s sync_type=%s request_count=%s duration_ms=%s success=%s",
                result.exchange,
                result.sync_type,
                result.request_count,
                result.duration_ms,
                result.success,
            )
            logger.info("Position plan reconciled | exchange=%s sync_type=%s", exchange, sync_type)
            return result
        except Exception as exc:
            failures = self._failures.get(exchange, 0) + 1
            self._failures[exchange] = failures
            result = _result(exchange, sync_type, request_count, start, False, error=str(exc), metadata={"consecutive_failures": failures})
            logger.warning(
                "Order state sync failed | exchange=%s sync_type=%s request_count=%s duration_ms=%s success=%s failures=%s error=%s",
                result.exchange,
                result.sync_type,
                result.request_count,
                result.duration_ms,
                result.success,
                failures,
                exc,
            )
            if failures >= self.config.consecutive_failure_alert_threshold:
                _emit(self.alert_sink, AppAlert(subject="AetherEdge order sync failures", content=f"{exchange} {sync_type} failed {failures} times: {exc}", severity="error"))
            return result

    def _known_ids(self, exchange: str, *, key: str) -> tuple[str, ...]:
        if callable(self.known_order_ids):
            payload = self.known_order_ids()
            return tuple(item for item in payload.get(exchange, {}).get(key, ()) if item)
        if self.position_plan_store is None:
            return ()
        ids: list[str] = []
        for plan in getattr(self.position_plan_store, "list_active_positions", lambda: ())():
            for leg in getattr(self.position_plan_store, "get_legs", lambda _position_id: ())(plan.position_id):
                if leg.exchange.value != exchange:
                    continue
                value = leg.entry_order_id if key == "orders" else leg.stop_order_id
                if value:
                    ids.append(value)
        return tuple(dict.fromkeys(ids))

    def _mark_missing_open_orders_closed(self, context: SyncExchangeContext, *, orders: Iterable[Order], is_stop_order: bool) -> None:
        marker = getattr(context.state_store, "mark_missing_open_orders_closed", None)
        if not callable(marker):
            logger.warning(
                "State store does not support open order snapshot cleanup | exchange=%s is_stop_order=%s",
                context.execution.exchange.value,
                is_stop_order,
            )
            return
        closed = marker(
            exchange=context.execution.exchange,
            symbol=context.execution.symbol,
            live_order_keys={(order.order_id, order.client_order_id) for order in orders},
            is_stop_order=is_stop_order,
            missing_status=OrderStatus.CANCELED,
            reason="missing_from_exchange_open_orders",
        )
        if closed:
            logger.info(
                "Missing local open orders marked closed | exchange=%s is_stop_order=%s count=%s",
                context.execution.exchange.value,
                is_stop_order,
                closed,
            )


def _result(exchange: str, sync_type: str, request_count: int, start: float, success: bool, *, error: str | None = None, metadata: Mapping[str, Any] | None = None) -> SyncResult:
    return SyncResult(
        exchange=exchange,
        sync_type=sync_type,
        request_count=request_count,
        duration_ms=int((time.perf_counter() - start) * 1000),
        success=success,
        error=error,
        metadata=dict(metadata or {}),
    )


async def _sleep_with_jitter(stop_event: asyncio.Event, interval_seconds: int) -> None:
    interval = max(1, int(interval_seconds))
    jitter = random.uniform(0, min(5.0, interval * 0.1))
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=interval + jitter)
    except asyncio.TimeoutError:
        return


def _emit(alert_sink: Any | None, alert: AppAlert) -> None:
    if alert_sink is None:
        return
    emit = getattr(alert_sink, "emit", None)
    if callable(emit):
        emit(alert)
