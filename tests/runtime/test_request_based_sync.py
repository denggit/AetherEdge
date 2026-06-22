from __future__ import annotations

from decimal import Decimal

import pytest

from src.app.alerts import AppAlert
from src.platform import Balance, ExchangeName, LeverageInfo, Order, OrderSide, OrderStatus, Position, PositionMode, PositionSide
from src.runtime.account_sync import AccountStateSyncService, OrderStateSyncService, SyncExchangeContext
from src.runtime.requirements import AccountStateRequirement, OrderStateRequirement


class MemoryAlerts:
    def __init__(self) -> None:
        self.items: list[AppAlert] = []

    def emit(self, alert: AppAlert) -> None:
        self.items.append(alert)


class MemoryState:
    def __init__(self) -> None:
        self.snapshots = []
        self.orders = []

    def save_snapshot(self, snapshot):
        self.snapshots.append(snapshot)

    def save_order(self, order, *, is_stop_order=False):
        self.orders.append((order, is_stop_order))


class FakeAccount:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[str] = []

    async def fetch_balance(self, asset="USDT"):
        self.calls.append("fetch_balance")
        if self.fail:
            raise RuntimeError("account unavailable")
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("100"), available=Decimal("90"))

    async def fetch_positions(self, symbol=None):
        self.calls.append("fetch_positions")
        return [Position(exchange=self.exchange, symbol=self.symbol, raw_symbol=self.symbol, side=PositionSide.LONG, quantity=Decimal("1"))]

    async def fetch_leverage(self, *, margin_mode=None):
        self.calls.append("fetch_leverage")
        return LeverageInfo(exchange=self.exchange, symbol=self.symbol, raw_symbol=self.symbol, leverage=Decimal("10"))

    async def fetch_position_mode(self):
        self.calls.append("fetch_position_mode")
        return PositionMode.ONE_WAY


class FakeExecution:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def fetch_open_orders(self):
        self.calls.append("fetch_open_orders")
        return [Order(exchange=self.exchange, symbol=self.symbol, raw_symbol=self.symbol, order_id="o1", client_order_id="c1", status=OrderStatus.NEW, side=OrderSide.BUY)]

    async def fetch_open_stop_orders(self):
        self.calls.append("fetch_open_stop_orders")
        return [Order(exchange=self.exchange, symbol=self.symbol, raw_symbol=self.symbol, order_id="s1", client_order_id="sc1", status=OrderStatus.NEW, side=OrderSide.SELL)]


@pytest.mark.asyncio
async def test_account_sync_fetches_snapshot_and_persists_state():
    state = MemoryState()
    account = FakeAccount()
    service = AccountStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=FakeExecution(), state_store=state),),
        config=AccountStateRequirement(poll_interval_seconds=300),
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert account.calls == ["fetch_balance", "fetch_positions", "fetch_leverage", "fetch_position_mode"]
    assert len(state.snapshots) == 1
    assert state.snapshots[0].balance.available == Decimal("90")


@pytest.mark.asyncio
async def test_account_sync_failure_does_not_raise_and_alerts_after_threshold():
    alerts = MemoryAlerts()
    account = FakeAccount(fail=True)
    service = AccountStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=FakeExecution(), state_store=MemoryState()),),
        config=AccountStateRequirement(consecutive_failure_alert_threshold=2),
        alert_sink=alerts,
    )

    first = await service.sync_once()
    second = await service.sync_once()

    assert first[0].success is False
    assert second[0].success is False
    assert len(alerts.items) == 1


@pytest.mark.asyncio
async def test_order_sync_fetches_positions_open_orders_and_open_stop_orders_when_active():
    state = MemoryState()
    account = FakeAccount()
    execution = FakeExecution()
    service = OrderStateSyncService(
        contexts=(SyncExchangeContext(account=account, execution=execution, state_store=state),),
        config=OrderStateRequirement(poll_interval_seconds=20),
        active_check=lambda: True,
    )

    results = await service.sync_once()

    assert results[0].success is True
    assert "fetch_positions" in account.calls
    assert execution.calls == ["fetch_open_orders", "fetch_open_stop_orders"]
    assert [(order.order_id, is_stop) for order, is_stop in state.orders] == [("o1", False), ("s1", True)]
