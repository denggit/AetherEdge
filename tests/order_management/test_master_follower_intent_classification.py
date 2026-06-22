from __future__ import annotations

from decimal import Decimal

import pytest

from src.order_management import MasterFollowerExecutionPolicy, MultiExchangeOrderCoordinator, OrderIntent, SqliteOrderJournalStore
from src.platform import ExchangeName, Order, OrderStatus
from src.platform.exchanges.models import OrderSide, OrderType
from src.signals import SignalAction, TradeSignal


class _Client:
    def __init__(self, exchange: ExchangeName) -> None:
        self.exchange = exchange
        self.symbol = "ETH-USDT-PERP"
        self.orders = []
        self.stops = []
        self.cancel_stop_calls = 0

    async def place_order(self, request):
        self.orders.append(request)
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol=request.symbol, order_id=f"{self.exchange.value}-o", client_order_id=request.client_order_id, status=OrderStatus.FILLED, side=request.side, order_type=request.order_type, quantity=request.quantity, filled_quantity=request.quantity, raw={"avgPx": "2000"})

    async def place_stop_market_order(self, request):
        self.stops.append(request)
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol=request.symbol, order_id=f"{self.exchange.value}-s", client_order_id=request.client_order_id, status=OrderStatus.NEW, side=request.side, order_type=OrderType.MARKET, quantity=request.quantity, price=request.trigger_price)

    async def cancel_all_orders(self):
        return []

    async def cancel_all_stop_orders(self):
        self.cancel_stop_calls += 1
        return []


def _policy() -> MasterFollowerExecutionPolicy:
    return MasterFollowerExecutionPolicy(master_exchange=ExchangeName.OKX, follower_exchanges=(ExchangeName.BINANCE,))


@pytest.mark.asyncio
async def test_follower_stop_sync_bypasses_master_gating(tmp_path) -> None:
    binance = _Client(ExchangeName.BINANCE)
    coordinator = MultiExchangeOrderCoordinator(clients=[binance], repository=SqliteOrderJournalStore(tmp_path / "j.sqlite3"), master_follower_policy=_policy())
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.PLACE_STOP_LOSS_LONG,
        quantity=Decimal("0.2"),
        trigger_price=Decimal("1900"),
        metadata={"target_exchanges": ["binance"], "execution_purpose": "stop_sync"},
    )
    intent = OrderIntent(intent_id="i-stop", strategy_id="v9c", signal=signal, target_exchanges=(ExchangeName.BINANCE,))

    results = await coordinator.execute(intent)

    assert [item.exchange for item in results] == [ExchangeName.BINANCE]
    assert results[0].ok is True
    assert len(binance.stops) == 1


@pytest.mark.asyncio
async def test_follower_recovery_topup_bypasses_master_gating(tmp_path) -> None:
    binance = _Client(ExchangeName.BINANCE)
    coordinator = MultiExchangeOrderCoordinator(clients=[binance], repository=SqliteOrderJournalStore(tmp_path / "j.sqlite3"), master_follower_policy=_policy())
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.1"),
        metadata={"target_exchanges": ["binance"], "execution_purpose": "follower_recovery_topup", "position_id": "p1"},
    )
    intent = OrderIntent(intent_id="i-topup", strategy_id="v9c", signal=signal, target_exchanges=(ExchangeName.BINANCE,))

    results = await coordinator.execute(intent)

    assert results[0].exchange is ExchangeName.BINANCE
    assert results[0].ok is True
    assert len(binance.orders) == 1


@pytest.mark.asyncio
async def test_normal_entry_still_runs_master_first_then_followers(tmp_path) -> None:
    okx = _Client(ExchangeName.OKX)
    binance = _Client(ExchangeName.BINANCE)
    coordinator = MultiExchangeOrderCoordinator(clients=[okx, binance], repository=SqliteOrderJournalStore(tmp_path / "j.sqlite3"), master_follower_policy=_policy())
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.2"),
        metadata={"execution_purpose": "normal_entry"},
    )
    intent = OrderIntent(intent_id="i-entry", strategy_id="v9c", signal=signal, target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE))

    results = await coordinator.execute(intent)

    assert [item.exchange for item in results] == [ExchangeName.OKX, ExchangeName.BINANCE]
    assert len(okx.orders) == 1
    assert len(binance.orders) == 1
