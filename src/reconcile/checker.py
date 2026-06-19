from __future__ import annotations

import json
import time
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping

from src.platform.account.ports import AccountClient
from src.platform.exchanges.models import ExchangeName, Order, OrderStatus, Position
from src.platform.execution.ports import ExecutionClient
from src.platform.state.models import StoredAccountSnapshot, StoredOrder
from src.platform.state.ports import StateStore
from src.reconcile.models import ReconcileCategory, ReconcileIssue, ReconcileReport, ReconcileSeverity
from src.reconcile.notifier import NoopReconcileNotifier
from src.reconcile.ports import ReconcileNotifier


class Reconciler:
    """Read-only consistency checker between local evidence and exchange state.

    The checker reports differences only. It must not place, cancel, amend, or
    repair anything.
    """

    def __init__(
        self,
        *,
        account: AccountClient,
        execution: ExecutionClient,
        state_store: StateStore,
        notifier: ReconcileNotifier | None = None,
    ) -> None:
        if account.exchange != execution.exchange:
            raise ValueError(f"account/execution exchange mismatch: {account.exchange} != {execution.exchange}")
        if account.symbol != execution.symbol:
            raise ValueError(f"account/execution symbol mismatch: {account.symbol} != {execution.symbol}")
        self.account = account
        self.execution = execution
        self.state_store = state_store
        self.notifier = notifier or NoopReconcileNotifier()

    async def check(self) -> ReconcileReport:
        exchange = self.execution.exchange
        symbol = self.execution.symbol
        local_orders = self.state_store.list_open_orders(exchange=exchange, symbol=symbol, include_stop_orders=True)
        local_normal = [order for order in local_orders if not order.is_stop_order]
        local_stop = [order for order in local_orders if order.is_stop_order]

        remote_normal = await self.execution.fetch_open_orders()
        remote_stop = await self.execution.fetch_open_stop_orders()
        remote_positions = await self.account.fetch_positions()
        snapshot = self.state_store.load_latest_account_snapshot(exchange=exchange, symbol=symbol)

        issues: list[ReconcileIssue] = []
        issues.extend(
            _compare_orders(
                exchange=exchange,
                symbol=symbol,
                local_orders=local_normal,
                remote_orders=remote_normal,
                missing_local_category=ReconcileCategory.MISSING_LOCAL_ORDER,
                missing_exchange_category=ReconcileCategory.MISSING_EXCHANGE_ORDER,
                status_mismatch_category=ReconcileCategory.ORDER_STATUS_MISMATCH,
                label="order",
            )
        )
        issues.extend(
            _compare_orders(
                exchange=exchange,
                symbol=symbol,
                local_orders=local_stop,
                remote_orders=remote_stop,
                missing_local_category=ReconcileCategory.MISSING_LOCAL_STOP_ORDER,
                missing_exchange_category=ReconcileCategory.MISSING_EXCHANGE_STOP_ORDER,
                status_mismatch_category=ReconcileCategory.STOP_ORDER_STATUS_MISMATCH,
                label="stop order",
            )
        )
        issues.extend(_compare_positions(exchange=exchange, symbol=symbol, snapshot=snapshot, remote_positions=remote_positions))
        return ReconcileReport(exchange=exchange, symbol=symbol, checked_at_ms=int(time.time() * 1000), issues=issues)

    async def check_and_notify(self) -> ReconcileReport:
        report = await self.check()
        await self.notifier.notify(report)
        return report


def _compare_orders(
    *,
    exchange: ExchangeName,
    symbol: str,
    local_orders: Iterable[StoredOrder],
    remote_orders: Iterable[Order],
    missing_local_category: ReconcileCategory,
    missing_exchange_category: ReconcileCategory,
    status_mismatch_category: ReconcileCategory,
    label: str,
) -> list[ReconcileIssue]:
    issues: list[ReconcileIssue] = []
    local_by_key = {_order_key(order.order_id, order.client_order_id): order for order in local_orders if _order_key(order.order_id, order.client_order_id)}
    remote_by_key = {_order_key(order.order_id, order.client_order_id): order for order in remote_orders if _order_key(order.order_id, order.client_order_id)}

    for key, remote in remote_by_key.items():
        local = local_by_key.get(key)
        if local is None:
            issues.append(
                ReconcileIssue(
                    exchange=exchange,
                    symbol=symbol,
                    severity=ReconcileSeverity.WARNING,
                    category=missing_local_category,
                    entity_id=key,
                    message=f"Exchange has open {label} {key}, but local state store does not.",
                    remote=_order_payload(remote),
                )
            )
            continue
        if _status_value(local.status) != _status_value(remote.status):
            issues.append(
                ReconcileIssue(
                    exchange=exchange,
                    symbol=symbol,
                    severity=ReconcileSeverity.WARNING,
                    category=status_mismatch_category,
                    entity_id=key,
                    message=f"Local {label} {key} status is {local.status.value}, exchange status is {remote.status.value}.",
                    local=_stored_order_payload(local),
                    remote=_order_payload(remote),
                )
            )

    for key, local in local_by_key.items():
        if key not in remote_by_key:
            issues.append(
                ReconcileIssue(
                    exchange=exchange,
                    symbol=symbol,
                    severity=ReconcileSeverity.WARNING,
                    category=missing_exchange_category,
                    entity_id=key,
                    message=f"Local state store has open {label} {key}, but exchange does not.",
                    local=_stored_order_payload(local),
                )
            )
    return issues


def _compare_positions(
    *,
    exchange: ExchangeName,
    symbol: str,
    snapshot: StoredAccountSnapshot | None,
    remote_positions: Iterable[Position],
) -> list[ReconcileIssue]:
    if snapshot is None:
        return [
            ReconcileIssue(
                exchange=exchange,
                symbol=symbol,
                severity=ReconcileSeverity.INFO,
                category=ReconcileCategory.MISSING_LOCAL_SNAPSHOT,
                message="No local account snapshot found; position consistency cannot be checked yet.",
            )
        ]

    local = _positions_from_snapshot(snapshot)
    remote = _positions_from_exchange(remote_positions)
    issues: list[ReconcileIssue] = []
    for key in sorted(set(local) | set(remote)):
        local_qty = local.get(key, Decimal("0"))
        remote_qty = remote.get(key, Decimal("0"))
        if local_qty != remote_qty:
            issues.append(
                ReconcileIssue(
                    exchange=exchange,
                    symbol=symbol,
                    severity=ReconcileSeverity.WARNING,
                    category=ReconcileCategory.POSITION_MISMATCH,
                    entity_id=key,
                    message=f"Position {key} local quantity is {local_qty}, exchange quantity is {remote_qty}.",
                    local={"quantity": str(local_qty)},
                    remote={"quantity": str(remote_qty)},
                )
            )
    return issues


def _positions_from_snapshot(snapshot: StoredAccountSnapshot) -> dict[str, Decimal]:
    try:
        rows = json.loads(snapshot.positions_json or "[]")
    except json.JSONDecodeError:
        rows = []
    result: dict[str, Decimal] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        key = _position_key(row, fallback_symbol=snapshot.symbol)
        result[key] = result.get(key, Decimal("0")) + _position_quantity(row)
    return result


def _positions_from_exchange(positions: Iterable[Position]) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for position in positions:
        raw = position.raw if position.raw else {}
        row: dict[str, Any] = dict(raw)
        row.setdefault("symbol", position.raw_symbol or position.symbol)
        row.setdefault("positionSide", position.side.value)
        row.setdefault("quantity", str(position.quantity))
        key = _position_key(row, fallback_symbol=position.symbol)
        result[key] = result.get(key, Decimal("0")) + _position_quantity(row)
    return result


def _position_key(row: Mapping[str, Any], *, fallback_symbol: str) -> str:
    raw_symbol = _first(row, "instId", "symbol", "raw_symbol") or fallback_symbol
    side = _first(row, "posSide", "positionSide", "side") or "both"
    return f"{raw_symbol}:{str(side).lower()}"


def _position_quantity(row: Mapping[str, Any]) -> Decimal:
    value = _first(row, "pos", "positionAmt", "quantity", "qty", "sz")
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _order_key(order_id: str | None, client_order_id: str | None) -> str:
    if order_id:
        return f"order:{order_id}"
    if client_order_id:
        return f"client:{client_order_id}"
    return ""


def _status_value(status: OrderStatus | None) -> str:
    return OrderStatus.UNKNOWN.value if status is None else status.value


def _order_payload(order: Order) -> dict[str, Any]:
    return _dataclass_payload(order)


def _stored_order_payload(order: StoredOrder) -> dict[str, Any]:
    return _dataclass_payload(order)


def _dataclass_payload(value: Any) -> dict[str, Any]:
    if is_dataclass(value):
        payload = asdict(value)
    else:
        payload = dict(value)
    return {key: _stringify(raw_value) for key, raw_value in payload.items() if key != "raw"}


def _stringify(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, dict):
        return {str(key): _stringify(raw_value) for key, raw_value in value.items()}
    if isinstance(value, list):
        return [_stringify(raw_value) for raw_value in value]
    return value


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None
