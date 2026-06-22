from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.order_management.models import ExchangeOrderResult, OrderIntent, OrderIntentStatus, OrderJournalEvent
from src.platform.exchanges.models import ExchangeName, OrderSide, OrderStatus
from src.signals.models import SignalAction, SignalOrderType, TradeSignal


class SqliteOrderJournalStore:
    """SQLite order-intent journal for restart recovery and audit."""

    def __init__(self, path: str | Path = "data/state/aether_order_journal.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def save_intent(self, intent: OrderIntent) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO order_intents (
                    intent_id, strategy_id, signal_json, target_exchanges_json,
                    status, created_time_ms, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(intent_id) DO UPDATE SET
                    status=excluded.status,
                    metadata_json=excluded.metadata_json
                """,
                (
                    intent.intent_id,
                    intent.strategy_id,
                    _signal_to_json(intent.signal),
                    json.dumps([exchange.value for exchange in intent.target_exchanges], separators=(",", ":")),
                    intent.status.value,
                    intent.created_time_ms,
                    _json(intent.metadata),
                ),
            )
            conn.execute(
                """
                INSERT INTO order_journal_events (intent_id, status, message, exchange, created_time_ms, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (intent.intent_id, intent.status.value, "intent_saved", None, intent.created_time_ms, _json({})),
            )

    def update_status(self, *, intent_id: str, status: OrderIntentStatus) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE order_intents SET status = ? WHERE intent_id = ?", (status.value, intent_id))
            conn.execute(
                """
                INSERT INTO order_journal_events (intent_id, status, message, exchange, created_time_ms, metadata_json)
                VALUES (?, ?, ?, ?, strftime('%s','now') * 1000, ?)
                """,
                (intent_id, status.value, "status_updated", None, _json({})),
            )

    def save_result(self, *, intent_id: str, result: ExchangeOrderResult) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO exchange_order_results (
                    intent_id, exchange, ok, order_id, client_order_id, status,
                    side, quantity, filled_quantity, avg_fill_price, fee, fee_asset, error, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent_id,
                    result.exchange.value,
                    1 if result.ok else 0,
                    result.order_id,
                    result.client_order_id,
                    None if result.status is None else result.status.value,
                    None if result.side is None else result.side.value,
                    None if result.quantity is None else _decimal(result.quantity),
                    None if result.filled_quantity is None else _decimal(result.filled_quantity),
                    None if result.avg_fill_price is None else _decimal(result.avg_fill_price),
                    None if result.fee is None else _decimal(result.fee),
                    result.fee_asset,
                    result.error,
                    _json(result.raw),
                ),
            )
            conn.execute(
                """
                INSERT INTO order_journal_events (intent_id, status, message, exchange, created_time_ms, metadata_json)
                VALUES (?, ?, ?, ?, strftime('%s','now') * 1000, ?)
                """,
                (
                    intent_id,
                    OrderIntentStatus.SUBMITTED.value if result.ok else OrderIntentStatus.FAILED.value,
                    "exchange_result",
                    result.exchange.value,
                    _json({"ok": result.ok, "error": result.error}),
                ),
            )

    def add_event(self, event: OrderJournalEvent) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO order_journal_events (intent_id, status, message, exchange, created_time_ms, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event.intent_id,
                    event.status.value,
                    event.message,
                    None if event.exchange is None else event.exchange.value,
                    event.created_time_ms,
                    _json(event.metadata),
                ),
            )

    def get_intent(self, intent_id: str) -> OrderIntent | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT intent_id, strategy_id, signal_json, target_exchanges_json,
                       status, created_time_ms, metadata_json
                FROM order_intents
                WHERE intent_id = ?
                """,
                (intent_id,),
            ).fetchone()
        if row is None:
            return None
        return OrderIntent(
            intent_id=str(row[0]),
            strategy_id=str(row[1]),
            signal=_signal_from_json(str(row[2])),
            target_exchanges=tuple(ExchangeName(value) for value in json.loads(str(row[3]))),
            status=OrderIntentStatus(str(row[4])),
            created_time_ms=int(row[5]),
            metadata=json.loads(str(row[6] or "{}")),
        )

    def list_results(self, *, intent_id: str) -> list[ExchangeOrderResult]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, ok, order_id, client_order_id, status, side, quantity,
                       filled_quantity, avg_fill_price, fee, fee_asset, error, raw_json
                FROM exchange_order_results
                WHERE intent_id = ?
                ORDER BY rowid ASC
                """,
                (intent_id,),
            ).fetchall()
        return [_row_to_result(row) for row in rows]

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_intents (
                    intent_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    signal_json TEXT NOT NULL,
                    target_exchanges_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_time_ms INTEGER NOT NULL,
                    metadata_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS exchange_order_results (
                    intent_id TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    ok INTEGER NOT NULL,
                    order_id TEXT,
                    client_order_id TEXT,
                    status TEXT,
                    side TEXT,
                    quantity TEXT,
                    filled_quantity TEXT,
                    avg_fill_price TEXT,
                    fee TEXT,
                    fee_asset TEXT,
                    error TEXT,
                    raw_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_journal_events (
                    intent_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT NOT NULL,
                    exchange TEXT,
                    created_time_ms INTEGER NOT NULL,
                    metadata_json TEXT NOT NULL
                )
                """
            )
            _ensure_column(conn, "exchange_order_results", "filled_quantity", "TEXT")
            _ensure_column(conn, "exchange_order_results", "avg_fill_price", "TEXT")
            _ensure_column(conn, "exchange_order_results", "fee", "TEXT")
            _ensure_column(conn, "exchange_order_results", "fee_asset", "TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_order_results_intent ON exchange_order_results(intent_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_order_events_intent ON order_journal_events(intent_id)")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn


def _signal_to_json(signal: TradeSignal) -> str:
    payload = {
        "symbol": signal.symbol,
        "action": signal.action.value,
        "quantity": None if signal.quantity is None else _decimal(signal.quantity),
        "order_type": signal.order_type.value,
        "price": None if signal.price is None else _decimal(signal.price),
        "trigger_price": None if signal.trigger_price is None else _decimal(signal.trigger_price),
        "client_order_id": signal.client_order_id,
        "reason": signal.reason,
        "metadata": dict(signal.metadata),
        "created_time_ms": signal.created_time_ms,
    }
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _signal_from_json(raw: str) -> TradeSignal:
    payload = json.loads(raw)
    return TradeSignal(
        symbol=payload["symbol"],
        action=SignalAction(payload["action"]),
        quantity=None if payload["quantity"] is None else Decimal(str(payload["quantity"])),
        order_type=SignalOrderType(payload["order_type"]),
        price=None if payload["price"] is None else Decimal(str(payload["price"])),
        trigger_price=None if payload["trigger_price"] is None else Decimal(str(payload["trigger_price"])),
        client_order_id=payload.get("client_order_id"),
        reason=payload.get("reason", ""),
        metadata=payload.get("metadata", {}),
        created_time_ms=int(payload["created_time_ms"]),
    )


def _row_to_result(row: tuple[Any, ...]) -> ExchangeOrderResult:
    return ExchangeOrderResult(
        exchange=ExchangeName(str(row[0])),
        ok=bool(row[1]),
        order_id=str(row[2]) if row[2] is not None else None,
        client_order_id=str(row[3]) if row[3] is not None else None,
        status=OrderStatus(str(row[4])) if row[4] is not None else None,
        side=OrderSide(str(row[5])) if row[5] is not None else None,
        quantity=Decimal(str(row[6])) if row[6] is not None else None,
        filled_quantity=Decimal(str(row[7])) if row[7] is not None else None,
        avg_fill_price=Decimal(str(row[8])) if row[8] is not None else None,
        fee=Decimal(str(row[9])) if row[9] is not None else None,
        fee_asset=str(row[10]) if row[10] is not None else None,
        error=str(row[11]) if row[11] is not None else None,
        raw=json.loads(str(row[12] or "{}")),
    )


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, default=str)


def _decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
