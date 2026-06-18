from __future__ import annotations

from dataclasses import dataclass

from src.platform.account.ports import AccountClient
from src.platform.exchanges.models import Balance, LeverageInfo, Order, Position, PositionMode
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


async def fetch_platform_snapshot(
    *,
    account: AccountClient,
    execution: ExecutionClient,
    asset: str = "USDT",
) -> PlatformSnapshot:
    """Collect read-only account/order state without applying recovery logic."""

    if account.exchange != execution.exchange:
        raise ValueError(f"snapshot clients must use same exchange, got {account.exchange} and {execution.exchange}")
    if account.symbol != execution.symbol:
        raise ValueError(f"snapshot clients must use same symbol, got {account.symbol} and {execution.symbol}")
    return PlatformSnapshot(
        symbol=account.symbol,
        balance=await account.fetch_balance(asset),
        positions=await account.fetch_positions(),
        open_orders=await execution.fetch_open_orders(),
        open_stop_orders=await execution.fetch_open_stop_orders(),
        leverage=await account.fetch_leverage(),
        position_mode=await account.fetch_position_mode(),
    )
