from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from src.order_management.position_plan.models import LegPlan, LegRole, LegSyncStatus, PositionPlan, PositionPlanStatus
from src.platform.exchanges.models import ExchangeName


class SqlitePositionPlanStore:
    """Durable master/follower position plan store.

    This stores intended per-exchange leg targets separately from exchange
    snapshots. Recovery compares actual follower state to these planned targets,
    never to the master account quantity.
    """

    def __init__(self, path: str | Path = "data/state/aether_position_plan.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def upsert_position(self, plan: PositionPlan) -> None:
        now = int(time.time() * 1000)
        plan = replace(plan, updated_time_ms=now)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO position_plans (
                    position_id, strategy_id, entry_engine, side, status,
                    canonical_stop_price, master_exchange, master_target_qty_base,
                    master_filled_qty_base, created_time_ms, updated_time_ms, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(position_id) DO UPDATE SET
                    strategy_id=excluded.strategy_id,
                    entry_engine=excluded.entry_engine,
                    side=excluded.side,
                    status=excluded.status,
                    canonical_stop_price=excluded.canonical_stop_price,
                    master_exchange=excluded.master_exchange,
                    master_target_qty_base=excluded.master_target_qty_base,
                    master_filled_qty_base=excluded.master_filled_qty_base,
                    updated_time_ms=excluded.updated_time_ms,
                    metadata_json=excluded.metadata_json
                """,
                _position_params(plan),
            )

    def upsert_leg(self, leg: LegPlan) -> None:
        now = int(time.time() * 1000)
        leg = replace(leg, updated_time_ms=now)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO leg_plans (
                    position_id, exchange, role, target_qty_base, filled_qty_base,
                    entry_order_id, entry_client_order_id, stop_order_id, stop_client_order_id,
                    stop_price, sync_status, created_time_ms, updated_time_ms, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(position_id, exchange) DO UPDATE SET
                    role=excluded.role,
                    target_qty_base=excluded.target_qty_base,
                    filled_qty_base=excluded.filled_qty_base,
                    entry_order_id=COALESCE(excluded.entry_order_id, leg_plans.entry_order_id),
                    entry_client_order_id=COALESCE(excluded.entry_client_order_id, leg_plans.entry_client_order_id),
                    stop_order_id=COALESCE(excluded.stop_order_id, leg_plans.stop_order_id),
                    stop_client_order_id=COALESCE(excluded.stop_client_order_id, leg_plans.stop_client_order_id),
                    stop_price=COALESCE(excluded.stop_price, leg_plans.stop_price),
                    sync_status=excluded.sync_status,
                    updated_time_ms=excluded.updated_time_ms,
                    metadata_json=excluded.metadata_json
                """,
                _leg_params(leg),
            )

    def get_position(self, position_id: str) -> PositionPlan | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT position_id, strategy_id, entry_engine, side, status,
                       canonical_stop_price, master_exchange, master_target_qty_base,
                       master_filled_qty_base, created_time_ms, updated_time_ms, metadata_json
                FROM position_plans
                WHERE position_id = ?
                """,
                (position_id,),
            ).fetchone()
        return _row_to_position(row) if row is not None else None

    def get_legs(self, position_id: str) -> tuple[LegPlan, ...]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT position_id, exchange, role, target_qty_base, filled_qty_base,
                       entry_order_id, entry_client_order_id, stop_order_id, stop_client_order_id,
                       stop_price, sync_status, created_time_ms, updated_time_ms, metadata_json
                FROM leg_plans
                WHERE position_id = ?
                ORDER BY role, exchange
                """,
                (position_id,),
            ).fetchall()
        return tuple(_row_to_leg(row) for row in rows)

    def list_active_positions(self) -> tuple[PositionPlan, ...]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT position_id, strategy_id, entry_engine, side, status,
                       canonical_stop_price, master_exchange, master_target_qty_base,
                       master_filled_qty_base, created_time_ms, updated_time_ms, metadata_json
                FROM position_plans
                WHERE status IN (?, ?, ?, ?)
                ORDER BY updated_time_ms DESC
                """,
                (PositionPlanStatus.ACTIVE.value, PositionPlanStatus.MASTER_ACTIVE_PLAN_UNKNOWN.value, PositionPlanStatus.MANUAL_REQUIRED.value, PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED.value),
            ).fetchall()
        return tuple(_row_to_position(row) for row in rows)

    def update_leg_sync_status(self, *, position_id: str, exchange: ExchangeName | str, sync_status: LegSyncStatus | str) -> None:
        exchange_name = exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).strip().lower())
        with self._connect() as conn:
            conn.execute(
                "UPDATE leg_plans SET sync_status = ?, updated_time_ms = ? WHERE position_id = ? AND exchange = ?",
                (_enum_value(sync_status), int(time.time() * 1000), position_id, exchange_name.value),
            )

    def add_to_leg_target(self, *, position_id: str, exchange: ExchangeName, delta_target_qty_base: Decimal, delta_filled_qty_base: Decimal = Decimal("0")) -> None:
        legs = {leg.exchange: leg for leg in self.get_legs(position_id)}
        leg = legs.get(exchange)
        if leg is None:
            return
        self.upsert_leg(
            replace(
                leg,
                target_qty_base=leg.target_qty_base + delta_target_qty_base,
                filled_qty_base=leg.filled_qty_base + delta_filled_qty_base,
            )
        )

    def update_stop(self, *, position_id: str, stop_price: Decimal, exchange: ExchangeName | None = None, stop_order_id: str | None = None, stop_client_order_id: str | None = None) -> None:
        plan = self.get_position(position_id)
        if plan is not None:
            self.upsert_position(replace(plan, canonical_stop_price=stop_price))
        legs = self.get_legs(position_id)
        for leg in legs:
            if exchange is not None and leg.exchange is not exchange:
                continue
            self.upsert_leg(replace(leg, stop_price=stop_price, stop_order_id=stop_order_id or leg.stop_order_id, stop_client_order_id=stop_client_order_id or leg.stop_client_order_id))

    def serialize_active_positions(self) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for plan in self.list_active_positions():
            payload.append({"position": _position_dict(plan), "legs": [_leg_dict(leg) for leg in self.get_legs(plan.position_id)]})
        return payload

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS position_plans (
                    position_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    entry_engine TEXT NOT NULL,
                    side TEXT NOT NULL,
                    status TEXT NOT NULL,
                    canonical_stop_price TEXT,
                    master_exchange TEXT NOT NULL,
                    master_target_qty_base TEXT NOT NULL,
                    master_filled_qty_base TEXT NOT NULL,
                    created_time_ms INTEGER NOT NULL,
                    updated_time_ms INTEGER NOT NULL,
                    metadata_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS leg_plans (
                    position_id TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    role TEXT NOT NULL,
                    target_qty_base TEXT NOT NULL,
                    filled_qty_base TEXT NOT NULL,
                    entry_order_id TEXT,
                    entry_client_order_id TEXT,
                    stop_order_id TEXT,
                    stop_client_order_id TEXT,
                    stop_price TEXT,
                    sync_status TEXT NOT NULL,
                    created_time_ms INTEGER NOT NULL,
                    updated_time_ms INTEGER NOT NULL,
                    metadata_json TEXT NOT NULL,
                    PRIMARY KEY(position_id, exchange)
                );
                CREATE INDEX IF NOT EXISTS idx_position_plans_status ON position_plans(status, updated_time_ms);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn


def _position_params(plan: PositionPlan) -> tuple[object, ...]:
    return (
        plan.position_id,
        plan.strategy_id,
        plan.entry_engine,
        plan.side,
        _enum_value(plan.status),
        _dec(plan.canonical_stop_price),
        plan.master_exchange.value,
        _dec(plan.master_target_qty_base),
        _dec(plan.master_filled_qty_base),
        plan.created_time_ms,
        plan.updated_time_ms,
        _json(plan.metadata),
    )


def _leg_params(leg: LegPlan) -> tuple[object, ...]:
    return (
        leg.position_id,
        leg.exchange.value,
        _enum_value(leg.role),
        _dec(leg.target_qty_base),
        _dec(leg.filled_qty_base),
        leg.entry_order_id,
        leg.entry_client_order_id,
        leg.stop_order_id,
        leg.stop_client_order_id,
        _dec(leg.stop_price),
        _enum_value(leg.sync_status),
        leg.created_time_ms,
        leg.updated_time_ms,
        _json(leg.metadata),
    )


def _row_to_position(row: Sequence[Any]) -> PositionPlan:
    return PositionPlan(
        position_id=str(row[0]),
        strategy_id=str(row[1]),
        entry_engine=str(row[2]),
        side=str(row[3]),
        status=PositionPlanStatus(str(row[4])) if str(row[4]) in {item.value for item in PositionPlanStatus} else str(row[4]),
        canonical_stop_price=_optional_decimal(row[5]),
        master_exchange=ExchangeName(str(row[6])),
        master_target_qty_base=Decimal(str(row[7])),
        master_filled_qty_base=Decimal(str(row[8])),
        created_time_ms=int(row[9]),
        updated_time_ms=int(row[10]),
        metadata=json.loads(str(row[11] or "{}")),
    )


def _row_to_leg(row: Sequence[Any]) -> LegPlan:
    role = str(row[2])
    sync_status = str(row[10])
    return LegPlan(
        position_id=str(row[0]),
        exchange=ExchangeName(str(row[1])),
        role=LegRole(role) if role in {item.value for item in LegRole} else role,
        target_qty_base=Decimal(str(row[3])),
        filled_qty_base=Decimal(str(row[4])),
        entry_order_id=None if row[5] is None else str(row[5]),
        entry_client_order_id=None if row[6] is None else str(row[6]),
        stop_order_id=None if row[7] is None else str(row[7]),
        stop_client_order_id=None if row[8] is None else str(row[8]),
        stop_price=_optional_decimal(row[9]),
        sync_status=LegSyncStatus(sync_status) if sync_status in {item.value for item in LegSyncStatus} else sync_status,
        created_time_ms=int(row[11]),
        updated_time_ms=int(row[12]),
        metadata=json.loads(str(row[13] or "{}")),
    )


def _position_dict(plan: PositionPlan) -> dict[str, Any]:
    return {
        "position_id": plan.position_id,
        "strategy_id": plan.strategy_id,
        "entry_engine": plan.entry_engine,
        "side": plan.side,
        "status": _enum_value(plan.status),
        "canonical_stop_price": _dec(plan.canonical_stop_price),
        "master_exchange": plan.master_exchange.value,
        "master_target_qty_base": _dec(plan.master_target_qty_base),
        "master_filled_qty_base": _dec(plan.master_filled_qty_base),
        "created_time_ms": plan.created_time_ms,
        "updated_time_ms": plan.updated_time_ms,
        "metadata": dict(plan.metadata),
    }


def _leg_dict(leg: LegPlan) -> dict[str, Any]:
    return {
        "position_id": leg.position_id,
        "exchange": leg.exchange.value,
        "role": _enum_value(leg.role),
        "target_qty_base": _dec(leg.target_qty_base),
        "filled_qty_base": _dec(leg.filled_qty_base),
        "entry_order_id": leg.entry_order_id,
        "entry_client_order_id": leg.entry_client_order_id,
        "stop_order_id": leg.stop_order_id,
        "stop_client_order_id": leg.stop_client_order_id,
        "stop_price": _dec(leg.stop_price),
        "sync_status": _enum_value(leg.sync_status),
        "created_time_ms": leg.created_time_ms,
        "updated_time_ms": leg.updated_time_ms,
        "metadata": dict(leg.metadata),
    }


def _enum_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _dec(value: Decimal | None) -> str | None:
    return None if value is None else format(value.normalize(), "f")


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, default=str)
