from __future__ import annotations

import json
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from strategies.eth_portfolio_v1.domain.models import Side
from strategies.eth_portfolio_v1.domain.position_state import ExchangeLegState, V8PositionState


class JsonV8StateStore:
    """Tiny durable state store for plugin-local V8 state.

    Runtime/order journals remain in ``src/order_management``. This store only
    persists strategy-local canonical state when the plugin chooses to use it.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def save(self, state: V8PositionState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(_to_jsonable(state), ensure_ascii=False, sort_keys=True), encoding="utf-8")

    def load(self) -> V8PositionState:
        if not self.path.exists():
            return V8PositionState()
        data = json.loads(self.path.read_text(encoding="utf-8"))
        state = V8PositionState(
            position_id=data.get("position_id"),
            in_pos=bool(data.get("in_pos", False)),
            side=Side(int(data.get("side", 0))),
            entry_time_ms=data.get("entry_time_ms"),
            first_entry=_d_or_none(data.get("first_entry")),
            avg_entry=_d_or_none(data.get("avg_entry")),
            initial_sl=_d_or_none(data.get("initial_sl")),
            stop_price=_d_or_none(data.get("stop_price")),
            confirmed_stop_price=_d_or_none(data.get("confirmed_stop_price", data.get("stop_price"))),
            desired_stop_price=_d_or_none(data.get("desired_stop_price")),
            pending_stop_replace=bool(data.get("pending_stop_replace", False)),
            pending_stop_reason=data.get("pending_stop_reason"),
            pending_stop_bar_close_time_ms=data.get("pending_stop_bar_close_time_ms"),
            risk_per_coin=_d_or_none(data.get("risk_per_coin")),
            qty=Decimal(str(data.get("qty", "0"))),
            units=int(data.get("units", 0)),
            max_fav=Decimal(str(data.get("max_fav", "0"))),
            max_adv=Decimal(str(data.get("max_adv", "0"))),
            entry_risk_mult=Decimal(str(data.get("entry_risk_mult", "1"))),
            entry_engine=str(data.get("entry_engine", "")),
            last_exit_time_ms=data.get("last_exit_time_ms"),
        )
        for exchange, leg_data in dict(data.get("legs", {})).items():
            state.legs[str(exchange)] = ExchangeLegState(
                exchange=str(exchange),
                is_open=bool(leg_data.get("is_open", False)),
                avg_fill_price=_d_or_none(leg_data.get("avg_fill_price")),
                base_qty=Decimal(str(leg_data.get("base_qty", "0"))),
                native_qty=_d_or_none(leg_data.get("native_qty")),
                entry_order_id=leg_data.get("entry_order_id"),
                entry_client_order_id=leg_data.get("entry_client_order_id"),
                stop_order_id=leg_data.get("stop_order_id"),
                stop_client_order_id=leg_data.get("stop_client_order_id"),
                stop_price=_d_or_none(leg_data.get("stop_price")),
                sync_status=str(leg_data.get("sync_status", "flat")),
                metadata=dict(leg_data.get("metadata", {})),
            )
        return state


def _to_jsonable(state: V8PositionState) -> dict[str, Any]:
    data = asdict(state)
    data["side"] = int(state.side.value)
    return _convert(data)


def _convert(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _convert(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_convert(item) for item in value]
    return value


def _d_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))
