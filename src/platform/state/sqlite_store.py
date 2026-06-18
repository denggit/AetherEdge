from __future__ import annotations

import json
import sqlite3
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.exchanges.models import ExchangeName, Order, OrderSide, OrderStatus, OrderType, PositionMode
from src.platform.snapshot import PlatformSnapshot
from src.platform.state.models import StoredAccountSnapshot, StoredEvent, StoredFill, StoredOrder

_OPEN_ORDER_STATUSES = {OrderStatus.NEW.value, OrderStatus.PARTIALLY_FILLED.value, OrderStatus.UNKNOWN.value}


class SqliteStateStore:
    """SQLite store for order/account evidence and restart snapshots."""

    def __init__(self, path: str | Path = "data/state/aether_state.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def save_order(self, order: Order, *, is_stop_order: bool = False) -> None:
        stored = _stored_order_from_order(order, is_stop_order=is_stop_order)
        self._upsert_order(stored)

    def save_account_event(self, event: AccountEvent) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO events (exchange, event_type, symbol, event_time_ms, raw_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event.exchange.value,
                    event.event_type.value,
                    event.symbol,
                    event.event_time_ms,
                    _json(event.raw),
                ),
            )
        if event.event_type == AccountEventType.ORDER:
            self._upsert_order(_stored_order_from_event(event))
            fill = _stored_fill_from_event(event)
            if fill is not None:
                self._save_fill(fill)

    def save_snapshot(self, snapshot: PlatformSnapshot) -> None:
        created_time_ms = int(time.time() * 1000)
        for order in snapshot.open_orders:
            self.save_order(order, is_stop_order=False)
        for order in snapshot.open_stop_orders:
            self.save_order(order, is_stop_order=True)
        positions_json = _json([position.raw for position in snapshot.positions])
        account_snapshot = StoredAccountSnapshot(
            exchange=snapshot.balance.exchange,
            symbol=snapshot.symbol,
            asset=snapshot.balance.asset,
            total=snapshot.balance.total,
            available=snapshot.balance.available,
            positions_json=positions_json,
            leverage=snapshot.leverage.leverage,
            position_mode=snapshot.position_mode,
            created_time_ms=created_time_ms,
            raw={
                "balance": snapshot.balance.raw,
                "positions": [position.raw for position in snapshot.positions],
                "leverage": snapshot.leverage.raw,
            },
        )
        self._save_account_snapshot(account_snapshot)

    def get_order(
        self,
        *,
        exchange: ExchangeName,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> StoredOrder | None:
        if not order_id and not client_order_id:
            raise ValueError("order_id or client_order_id is required")
        where = ["exchange = ?", "symbol = ?"]
        params: list[Any] = [exchange.value, symbol]
        if order_id:
            where.append("order_id = ?")
            params.append(order_id)
        if client_order_id:
            where.append("client_order_id = ?")
            params.append(client_order_id)
        sql = f"""
            SELECT exchange, symbol, raw_symbol, order_id, client_order_id, status, side, order_type,
                   price, quantity, filled_quantity, updated_time_ms, is_stop_order, raw_json
            FROM orders
            WHERE {' AND '.join(where)}
            ORDER BY updated_time_ms DESC
            LIMIT 1
        """
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return _row_to_order(row) if row is not None else None

    def list_open_orders(self, *, exchange: ExchangeName, symbol: str, include_stop_orders: bool = True) -> list[StoredOrder]:
        where = ["exchange = ?", "symbol = ?", f"status IN ({','.join('?' for _ in _OPEN_ORDER_STATUSES)})"]
        params: list[Any] = [exchange.value, symbol, *_OPEN_ORDER_STATUSES]
        if not include_stop_orders:
            where.append("is_stop_order = 0")
        sql = f"""
            SELECT exchange, symbol, raw_symbol, order_id, client_order_id, status, side, order_type,
                   price, quantity, filled_quantity, updated_time_ms, is_stop_order, raw_json
            FROM orders
            WHERE {' AND '.join(where)}
            ORDER BY updated_time_ms DESC
        """
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_order(row) for row in rows]

    def load_recent_events(self, *, exchange: ExchangeName, symbol: str | None = None, limit: int = 100) -> list[StoredEvent]:
        where = ["exchange = ?"]
        params: list[Any] = [exchange.value]
        if symbol is not None:
            where.append("symbol = ?")
            params.append(symbol)
        params.append(limit)
        sql = f"""
            SELECT id, exchange, event_type, symbol, event_time_ms, raw_json
            FROM events
            WHERE {' AND '.join(where)}
            ORDER BY id DESC
            LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return list(reversed([_row_to_event(row) for row in rows]))

    def load_recent_fills(self, *, exchange: ExchangeName, symbol: str, limit: int = 100) -> list[StoredFill]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, raw_symbol, order_id, trade_id, side, price, quantity,
                       fee, fee_asset, event_time_ms, raw_json
                FROM fills
                WHERE exchange = ? AND symbol = ?
                ORDER BY event_time_ms DESC, id DESC
                LIMIT ?
                """,
                (exchange.value, symbol, limit),
            ).fetchall()
        return list(reversed([_row_to_fill(row) for row in rows]))

    def _upsert_order(self, order: StoredOrder) -> None:
        key_order_id = order.order_id or ""
        key_client_order_id = order.client_order_id or ""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO orders (
                    exchange, symbol, raw_symbol, order_id, client_order_id, status, side, order_type,
                    price, quantity, filled_quantity, updated_time_ms, is_stop_order, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(exchange, symbol, order_id, client_order_id) DO UPDATE SET
                    raw_symbol=excluded.raw_symbol,
                    status=excluded.status,
                    side=excluded.side,
                    order_type=excluded.order_type,
                    price=excluded.price,
                    quantity=excluded.quantity,
                    filled_quantity=excluded.filled_quantity,
                    updated_time_ms=excluded.updated_time_ms,
                    is_stop_order=excluded.is_stop_order,
                    raw_json=excluded.raw_json
                """,
                (
                    order.exchange.value,
                    order.symbol,
                    order.raw_symbol,
                    key_order_id,
                    key_client_order_id,
                    order.status.value,
                    None if order.side is None else order.side.value,
                    None if order.order_type is None else order.order_type.value,
                    _dec(order.price),
                    _dec(order.quantity),
                    _dec(order.filled_quantity),
                    order.updated_time_ms,
                    1 if order.is_stop_order else 0,
                    _json(order.raw),
                ),
            )

    def _save_fill(self, fill: StoredFill) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO fills (
                    exchange, symbol, raw_symbol, order_id, trade_id, side, price, quantity,
                    fee, fee_asset, event_time_ms, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill.exchange.value,
                    fill.symbol,
                    fill.raw_symbol,
                    fill.order_id,
                    fill.trade_id,
                    None if fill.side is None else fill.side.value,
                    _dec(fill.price),
                    _dec(fill.quantity),
                    _dec(fill.fee),
                    fill.fee_asset,
                    fill.event_time_ms,
                    _json(fill.raw),
                ),
            )

    def _save_account_snapshot(self, snapshot: StoredAccountSnapshot) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO account_snapshots (
                    exchange, symbol, asset, total, available, positions_json,
                    leverage, position_mode, created_time_ms, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.exchange.value,
                    snapshot.symbol,
                    snapshot.asset,
                    str(snapshot.total),
                    str(snapshot.available),
                    snapshot.positions_json,
                    _dec(snapshot.leverage),
                    snapshot.position_mode.value,
                    snapshot.created_time_ms,
                    _json(snapshot.raw),
                ),
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    raw_symbol TEXT,
                    order_id TEXT NOT NULL DEFAULT '',
                    client_order_id TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    side TEXT,
                    order_type TEXT,
                    price TEXT,
                    quantity TEXT,
                    filled_quantity TEXT,
                    updated_time_ms INTEGER,
                    is_stop_order INTEGER NOT NULL DEFAULT 0,
                    raw_json TEXT NOT NULL,
                    PRIMARY KEY (exchange, symbol, order_id, client_order_id)
                );
                CREATE INDEX IF NOT EXISTS idx_orders_lookup
                    ON orders (exchange, symbol, status, is_stop_order);

                CREATE TABLE IF NOT EXISTS fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    raw_symbol TEXT,
                    order_id TEXT,
                    trade_id TEXT NOT NULL,
                    side TEXT,
                    price TEXT,
                    quantity TEXT,
                    fee TEXT,
                    fee_asset TEXT,
                    event_time_ms INTEGER,
                    raw_json TEXT NOT NULL,
                    UNIQUE(exchange, symbol, trade_id)
                );
                CREATE INDEX IF NOT EXISTS idx_fills_lookup
                    ON fills (exchange, symbol, event_time_ms);

                CREATE TABLE IF NOT EXISTS account_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    asset TEXT NOT NULL,
                    total TEXT NOT NULL,
                    available TEXT NOT NULL,
                    positions_json TEXT NOT NULL,
                    leverage TEXT,
                    position_mode TEXT NOT NULL,
                    created_time_ms INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_account_snapshots_lookup
                    ON account_snapshots (exchange, symbol, created_time_ms);

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exchange TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    symbol TEXT,
                    event_time_ms INTEGER,
                    raw_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_events_lookup
                    ON events (exchange, symbol, id);
                """
            )


def _stored_order_from_order(order: Order, *, is_stop_order: bool) -> StoredOrder:
    return StoredOrder(
        exchange=order.exchange,
        symbol=order.symbol,
        raw_symbol=order.raw_symbol,
        order_id=order.order_id,
        client_order_id=order.client_order_id,
        status=order.status,
        side=order.side,
        order_type=order.order_type,
        price=order.price,
        quantity=order.quantity,
        filled_quantity=order.filled_quantity,
        updated_time_ms=_event_time_from_raw(order.raw),
        is_stop_order=is_stop_order,
        raw=order.raw,
    )


def _stored_order_from_event(event: AccountEvent) -> StoredOrder:
    return StoredOrder(
        exchange=event.exchange,
        symbol=event.symbol or "",
        raw_symbol=event.raw_symbol,
        order_id=event.order_id,
        client_order_id=event.client_order_id,
        status=event.order_status or OrderStatus.UNKNOWN,
        side=event.side,
        order_type=None,
        price=event.price,
        quantity=event.quantity,
        filled_quantity=event.filled_quantity,
        updated_time_ms=event.event_time_ms,
        is_stop_order=_is_stop_order_event(event.raw),
        raw=event.raw,
    )


def _stored_fill_from_event(event: AccountEvent) -> StoredFill | None:
    trade_id = _first_raw_value(event.raw, "tradeId", "trade_id", "t")
    if not trade_id:
        return None
    qty = _optional_decimal(_first_raw_value(event.raw, "fillSz", "l")) or event.filled_quantity
    price = _optional_decimal(_first_raw_value(event.raw, "fillPx", "L")) or event.price
    fee = _optional_decimal(_first_raw_value(event.raw, "fee", "n"))
    fee_asset = _first_raw_value(event.raw, "feeCcy", "N")
    return StoredFill(
        exchange=event.exchange,
        symbol=event.symbol or "",
        raw_symbol=event.raw_symbol,
        order_id=event.order_id,
        trade_id=str(trade_id),
        side=event.side,
        price=price,
        quantity=qty,
        fee=fee,
        fee_asset=None if fee_asset in (None, "") else str(fee_asset),
        event_time_ms=event.event_time_ms,
        raw=event.raw,
    )


def _is_stop_order_event(raw: Mapping[str, Any]) -> bool:
    text = " ".join(str(raw.get(key, "")) for key in ("ordType", "type", "algoType", "algoId"))
    return "STOP" in text.upper() or "conditional" in text.lower() or bool(raw.get("algoId"))


def _event_time_from_raw(raw: Mapping[str, Any]) -> int | None:
    return _optional_int(_first_raw_value(raw, "uTime", "T", "E", "updateTime", "time"))


def _row_to_order(row) -> StoredOrder:
    return StoredOrder(
        exchange=ExchangeName(row[0]),
        symbol=row[1],
        raw_symbol=row[2],
        order_id=row[3] or None,
        client_order_id=row[4] or None,
        status=OrderStatus(row[5]),
        side=OrderSide(row[6]) if row[6] else None,
        order_type=OrderType(row[7]) if row[7] else None,
        price=_optional_decimal(row[8]),
        quantity=_optional_decimal(row[9]),
        filled_quantity=_optional_decimal(row[10]),
        updated_time_ms=row[11],
        is_stop_order=bool(row[12]),
        raw=json.loads(row[13]),
    )


def _row_to_fill(row) -> StoredFill:
    return StoredFill(
        exchange=ExchangeName(row[0]),
        symbol=row[1],
        raw_symbol=row[2],
        order_id=row[3],
        trade_id=row[4],
        side=OrderSide(row[5]) if row[5] else None,
        price=_optional_decimal(row[6]),
        quantity=_optional_decimal(row[7]),
        fee=_optional_decimal(row[8]),
        fee_asset=row[9],
        event_time_ms=row[10],
        raw=json.loads(row[11]),
    )


def _row_to_event(row) -> StoredEvent:
    return StoredEvent(
        id=int(row[0]),
        exchange=ExchangeName(row[1]),
        event_type=AccountEventType(row[2]),
        symbol=row[3],
        event_time_ms=row[4],
        raw=json.loads(row[5]),
    )


def _first_raw_value(raw: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = raw.get(key)
        if value not in (None, ""):
            return value
    return None


def _dec(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)
