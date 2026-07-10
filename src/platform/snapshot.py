from __future__ import annotations

import inspect
from dataclasses import dataclass

from src.platform.account.ports import AccountClient
from src.platform.exchanges.models import (
    Balance,
    InstrumentRule,
    LeverageInfo,
    MarginMode,
    Order,
    Position,
    PositionMode,
)
from src.platform.execution.ports import ExecutionClient


@dataclass(frozen=True)
class PlatformSnapshot:
    """Read-only startup snapshot for one exchange + one bound market."""

    symbol: str
    balance: Balance
    positions: list[Position]
    open_orders: list[Order]
    open_stop_orders: list[Order]
    leverage: LeverageInfo
    position_mode: PositionMode
    instrument_rule: InstrumentRule | None = None


async def fetch_platform_snapshot(
    *,
    account: AccountClient,
    execution: ExecutionClient,
    asset: str = "USDT",
    leverage_margin_mode: MarginMode = MarginMode.CROSS,
) -> PlatformSnapshot:
    """Collect read-only account/order state without applying recovery logic."""

    if account.exchange != execution.exchange:
        raise ValueError(f"snapshot clients must use same exchange, got {account.exchange} and {execution.exchange}")
    if account.symbol != execution.symbol:
        raise ValueError(f"snapshot clients must use same symbol, got {account.symbol} and {execution.symbol}")
    fetch_rule = getattr(execution, "fetch_instrument_rule", None)
    instrument_rule = await fetch_rule() if callable(fetch_rule) else None
    if not isinstance(instrument_rule, InstrumentRule):
        instrument_rule = None
    fetch_leverage = account.fetch_leverage
    leverage_parameters = inspect.signature(fetch_leverage).parameters
    supports_margin_mode = (
        "margin_mode" in leverage_parameters
        or any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in leverage_parameters.values()
        )
    )
    leverage = (
        await fetch_leverage(margin_mode=leverage_margin_mode)
        if supports_margin_mode
        else await fetch_leverage()
    )
    return PlatformSnapshot(
        symbol=account.symbol,
        balance=await account.fetch_balance(asset),
        positions=await account.fetch_positions(),
        open_orders=await execution.fetch_open_orders(),
        open_stop_orders=await execution.fetch_open_stop_orders(),
        leverage=leverage,
        position_mode=await account.fetch_position_mode(),
        instrument_rule=instrument_rule,
    )
