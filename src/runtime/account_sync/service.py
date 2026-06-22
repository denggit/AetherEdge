from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Callable, Iterable, Mapping

from src.app.alerts import AppAlert
from src.order_management.reconciliation.validation import (
    is_valid_exchange_order_id,
    is_valid_client_order_id,
    resolve_query_params,
)
from src.platform.exchanges.models import Balance, ExchangeName, MarginMode, Order, OrderQuery, OrderStatus, StopOrderQuery
from src.platform.snapshot import PlatformSnapshot
from src.runtime.account_sync.models import KnownOrderRef, KnownOrderRefStatus, SyncExchangeContext, SyncResult
from src.runtime.requirements import AccountStateRequirement, OrderStateRequirement
from src.utils.log import get_logger

logger = get_logger(__name__)

_INVALID_ID_VALUES: frozenset[str] = frozenset(
    {"", "none", "null", "nan", "n/a", "na", "undefined"}
)


def _clean_order_id(value: object) -> str | None:
    """Return a trimmed, valid order-id string or ``None``.

    Sentinel values like ``"None"``, ``"null"``, ``""``, ``"N/A"`` that
    originate from stored plans or legacy callbacks are rejected so they
    never reach an exchange REST endpoint.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in _INVALID_ID_VALUES:
        return None
    return text


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
            # ── known order status fetch (with client_order_id fallback) ──
            known_failures: list[str] = []
            skipped_invalid: int = 0
            for ref in self._known_ids(exchange, key="orders"):
                if ref.status in {KnownOrderRefStatus.INVALID_FORMAT, KnownOrderRefStatus.STALE_RECONCILED}:
                    skipped_invalid += 1
                    continue
                if ref.order_id is None and ref.client_order_id is None:
                    skipped_invalid += 1
                    logger.debug("Skipped invalid known order ref | exchange=%s key=orders", exchange)
                    continue
                query = OrderQuery(
                    symbol=context.execution.symbol,
                    order_id=ref.order_id,
                    client_order_id=ref.client_order_id,
                )
                try:
                    order = await context.execution.fetch_order_status(query)
                    request_count += 1
                    context.state_store.save_order(order, is_stop_order=False)
                except Exception as exc:
                    known_failures.append(f"order:{ref.order_id or ref.client_order_id}:{exc}")
                    logger.warning(
                        "Known order status fetch failed | exchange=%s order_id=%s client_order_id=%s error=%s",
                        exchange,
                        ref.order_id,
                        ref.client_order_id,
                        exc,
                    )
            for ref in self._known_ids(exchange, key="stop_orders"):
                if ref.status in {KnownOrderRefStatus.INVALID_FORMAT, KnownOrderRefStatus.STALE_RECONCILED}:
                    skipped_invalid += 1
                    continue
                if ref.order_id is None and ref.client_order_id is None:
                    skipped_invalid += 1
                    logger.debug("Skipped invalid known stop order ref | exchange=%s key=stop_orders", exchange)
                    continue
                query = StopOrderQuery(
                    symbol=context.execution.symbol,
                    stop_order_id=ref.order_id,
                    client_order_id=ref.client_order_id,
                )
                try:
                    order = await context.execution.fetch_stop_order_status(query)
                    request_count += 1
                    context.state_store.save_order(order, is_stop_order=True)
                except Exception as exc:
                    known_failures.append(f"stop:{ref.order_id or ref.client_order_id}:{exc}")
                    logger.warning(
                        "Known stop order status fetch failed | exchange=%s stop_order_id=%s client_order_id=%s error=%s",
                        exchange,
                        ref.order_id,
                        ref.client_order_id,
                        exc,
                    )
            if known_failures:
                failures = self._failures.get(exchange, 0) + 1
                self._failures[exchange] = failures
                meta: dict[str, Any] = {"consecutive_failures": failures, "known_order_status_failures": known_failures}
                if skipped_invalid > 0:
                    meta["skipped_invalid_order_refs"] = skipped_invalid
                result = _result(exchange, sync_type, request_count, start, False, metadata=meta)
                logger.warning(
                    "Order state synced with known order failures | exchange=%s sync_type=%s request_count=%s duration_ms=%s success=%s known_failures=%s skipped_invalid=%s consecutive_failures=%s",
                    result.exchange,
                    result.sync_type,
                    result.request_count,
                    result.duration_ms,
                    result.success,
                    len(known_failures),
                    skipped_invalid,
                    failures,
                )
                if failures >= self.config.consecutive_failure_alert_threshold:
                    _emit(
                        self.alert_sink,
                        AppAlert(
                            subject="AetherEdge order sync known order status failures",
                            content=(
                                f"exchange={exchange}\n"
                                f"sync_type={sync_type}\n"
                                f"consecutive_failures={failures}\n"
                                f"known_order_status_failures={known_failures}\n"
                                f"skipped_invalid_order_refs={skipped_invalid}\n"
                                f"request_count={request_count}\n"
                            ),
                            severity="error",
                        ),
                    )
            else:
                self._failures[exchange] = 0
                meta: dict[str, Any] = {}
                if skipped_invalid > 0:
                    meta["skipped_invalid_order_refs"] = skipped_invalid
                result = _result(exchange, sync_type, request_count, start, True, metadata=meta)
                logger.info(
                    "Order state synced | exchange=%s sync_type=%s request_count=%s duration_ms=%s success=%s skipped_invalid=%s",
                    result.exchange,
                    result.sync_type,
                    result.request_count,
                    result.duration_ms,
                    result.success,
                    skipped_invalid,
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

    def _known_ids(self, exchange: str, *, key: str) -> tuple[KnownOrderRef, ...]:
        """Return cleaned order references for *exchange*.

        Supports three sources (in priority order):

        1. **known_order_ids callback** — may return either the legacy
           ``{"okx": {"orders": ("id1",)}}`` shape (plain strings) or the
           newer ``{"okx": {"orders": [{"order_id": "...",
           "client_order_id": "..."}]}}`` shape (list of dicts).
        2. **position_plan_store** — reads ``entry_order_id`` /
           ``entry_client_order_id`` (key="orders") or ``stop_order_id`` /
           ``stop_client_order_id`` (key="stop_orders") from each active leg.
        3. Falls back to an empty tuple.
        """
        if callable(self.known_order_ids):
            payload = self.known_order_ids()
            exchange_payload = payload.get(exchange, {}).get(key, ())
            if not exchange_payload:
                return ()
            raw_refs: list[KnownOrderRef] = []
            for item in exchange_payload:
                if isinstance(item, dict):
                    raw_refs.append(
                        KnownOrderRef(
                            order_id=_clean_order_id(item.get("order_id")),
                            client_order_id=_clean_order_id(item.get("client_order_id")),
                        )
                    )
                elif isinstance(item, (list, tuple)):
                    # (order_id, client_order_id) pair
                    oid = _clean_order_id(item[0]) if len(item) > 0 else None
                    cid = _clean_order_id(item[1]) if len(item) > 1 else None
                    raw_refs.append(KnownOrderRef(order_id=oid, client_order_id=cid))
                else:
                    # Legacy plain string — map to KnownOrderRef.order_id
                    cleaned = _clean_order_id(item)
                    if cleaned is not None:
                        raw_refs.append(KnownOrderRef(order_id=cleaned))
                    else:
                        logger.debug(
                            "Skipped invalid known order ref | exchange=%s key=%s raw=%r",
                            exchange,
                            key,
                            item,
                        )
            # ── Exchange-specific validation (same as position_plan_store path) ──
            refs: list[KnownOrderRef] = []
            exchange_enum = ExchangeName(exchange)
            for ref in raw_refs:
                oid_valid = is_valid_exchange_order_id(exchange_enum, ref.order_id)
                cid_valid = is_valid_client_order_id(ref.client_order_id)

                if not oid_valid and not cid_valid:
                    # Both IDs invalid — mark INVALID_FORMAT, skip querying
                    refs.append(
                        KnownOrderRef(
                            order_id=None,
                            client_order_id=None,
                            status=KnownOrderRefStatus.INVALID_FORMAT,
                        )
                    )
                    logger.debug(
                        "Known order ref (callback) marked INVALID_FORMAT | "
                        "exchange=%s key=%s order_id=%r client_order_id=%r",
                        exchange,
                        key,
                        ref.order_id,
                        ref.client_order_id,
                    )
                    continue

                if not oid_valid and cid_valid:
                    # Fake/non-numeric exchange order_id + valid client_order_id
                    # → use ONLY client_order_id, must NOT send fake exchange ID
                    resolved_oid, resolved_cid = resolve_query_params(
                        exchange_enum, ref.order_id, ref.client_order_id
                    )
                    refs.append(
                        KnownOrderRef(
                            order_id=resolved_oid,
                            client_order_id=resolved_cid,
                            status=KnownOrderRefStatus.ACTIVE,
                        )
                    )
                    logger.debug(
                        "Known order ref (callback) fallback to client_order_id | "
                        "exchange=%s key=%s resolved_cid=%s",
                        exchange,
                        key,
                        resolved_cid,
                    )
                    continue

                # Both valid — use resolved params
                resolved_oid, resolved_cid = resolve_query_params(
                    exchange_enum, ref.order_id, ref.client_order_id
                )
                refs.append(
                    KnownOrderRef(
                        order_id=resolved_oid,
                        client_order_id=resolved_cid,
                        status=KnownOrderRefStatus.ACTIVE,
                    )
                )
            # Deduplicate while preserving order
            seen: set[tuple[str | None, str | None]] = set()
            deduped: list[KnownOrderRef] = []
            for ref in refs:
                sig = (ref.order_id, ref.client_order_id)
                if sig not in seen:
                    seen.add(sig)
                    deduped.append(ref)
            return tuple(deduped)

        if self.position_plan_store is None:
            return ()
        refs: list[KnownOrderRef] = []
        for plan in getattr(self.position_plan_store, "list_active_positions", lambda: ())():
            for leg in getattr(self.position_plan_store, "get_legs", lambda _position_id: ())(plan.position_id):
                if leg.exchange.value != exchange:
                    continue
                if key == "orders":
                    oid = _clean_order_id(leg.entry_order_id)
                    cid = _clean_order_id(leg.entry_client_order_id)
                else:
                    oid = _clean_order_id(leg.stop_order_id)
                    cid = _clean_order_id(leg.stop_client_order_id)

                # ── Exchange-specific validation (Task C + D) ──
                exchange_enum = ExchangeName(leg.exchange.value)
                oid_valid = is_valid_exchange_order_id(exchange_enum, oid)
                cid_valid = is_valid_client_order_id(cid)

                if not oid_valid and not cid_valid:
                    # Both IDs are invalid — mark INVALID_FORMAT, skip querying
                    refs.append(
                        KnownOrderRef(
                            order_id=None,
                            client_order_id=None,
                            status=KnownOrderRefStatus.INVALID_FORMAT,
                        )
                    )
                    logger.debug(
                        "Known order ref marked INVALID_FORMAT | exchange=%s key=%s "
                        "position_id=%s entry_order_id=%r stop_order_id=%r",
                        exchange,
                        key,
                        plan.position_id,
                        _clean_order_id(leg.entry_order_id),
                        _clean_order_id(leg.stop_order_id),
                    )
                    continue

                if not oid_valid and cid_valid:
                    # Exchange order ID is fake/non-numeric — use ONLY client_order_id
                    resolved_oid, resolved_cid = resolve_query_params(
                        exchange_enum, oid, cid
                    )
                    refs.append(
                        KnownOrderRef(
                            order_id=resolved_oid,
                            client_order_id=resolved_cid,
                            status=KnownOrderRefStatus.ACTIVE,
                        )
                    )
                    logger.debug(
                        "Known order ref fallback to client_order_id | exchange=%s "
                        "key=%s position_id=%s resolved_cid=%s",
                        exchange,
                        key,
                        plan.position_id,
                        resolved_cid,
                    )
                    continue

                # Both valid — use resolved params
                resolved_oid, resolved_cid = resolve_query_params(
                    exchange_enum, oid, cid
                )
                refs.append(
                    KnownOrderRef(
                        order_id=resolved_oid,
                        client_order_id=resolved_cid,
                        status=KnownOrderRefStatus.ACTIVE,
                    )
                )
        # Deduplicate while preserving order
        seen: set[tuple[str | None, str | None]] = set()
        deduped: list[KnownOrderRef] = []
        for ref in refs:
            sig = (ref.order_id, ref.client_order_id)
            if sig not in seen:
                seen.add(sig)
                deduped.append(ref)
        return tuple(deduped)

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
