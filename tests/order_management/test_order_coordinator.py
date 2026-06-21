from __future__ import annotations

from decimal import Decimal

import pytest

from src.order_management import MultiExchangeOrderCoordinator, OrderIntent, OrderIntentStatus, SqliteOrderJournalStore
from src.platform import ExchangeName, Order, OrderStatus
from src.signals import SignalAction, TradeSignal


class FakeExecutionClient:
    def __init__(self, exchange: ExchangeName, *, fail: bool = False) -> None:
        self.exchange = exchange
        self.symbol = "ETH-USDT-PERP"
        self.fail = fail
        self.orders = []
        self.stop_orders = []
        self.cancel_all_called = 0
        self.cancel_all_stop_called = 0

    @property
    def market_profile(self):  # pragma: no cover - not used by coordinator
        raise NotImplementedError

    async def place_order(self, request):
        if self.fail:
            raise RuntimeError(f"{self.exchange.value} failed")
        self.orders.append(request)
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol=request.symbol, order_id=f"{self.exchange.value}-1", client_order_id=request.client_order_id, status=OrderStatus.NEW, side=request.side, order_type=request.order_type, quantity=request.quantity)

    async def place_stop_market_order(self, request):
        if self.fail:
            raise RuntimeError(f"{self.exchange.value} failed")
        self.stop_orders.append(request)
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol=request.symbol, order_id=f"{self.exchange.value}-stop", client_order_id=request.client_order_id, status=OrderStatus.NEW, side=request.side, quantity=request.quantity)

    async def cancel_all_orders(self):
        self.cancel_all_called += 1
        return []

    async def cancel_all_stop_orders(self):
        self.cancel_all_stop_called += 1
        return []


@pytest.mark.asyncio
async def test_multi_exchange_order_coordinator_executes_one_intent_on_all_targets(tmp_path):
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    okx = FakeExecutionClient(ExchangeName.OKX)
    binance = FakeExecutionClient(ExchangeName.BINANCE)
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.1"), created_time_ms=100)
    intent = OrderIntent(intent_id="intent-1", strategy_id="v8", signal=signal, target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE))
    coordinator = MultiExchangeOrderCoordinator(clients=[okx, binance], repository=repo)

    results = await coordinator.execute(intent)

    assert [result.exchange for result in results] == [ExchangeName.OKX, ExchangeName.BINANCE]
    assert all(result.ok for result in results)
    assert len(okx.orders) == 1
    assert len(binance.orders) == 1
    assert okx.orders[0].client_order_id != binance.orders[0].client_order_id
    assert repo.get_intent("intent-1").status is OrderIntentStatus.SUBMITTED  # type: ignore[union-attr]
    assert len(repo.list_results(intent_id="intent-1")) == 2


@pytest.mark.asyncio
async def test_multi_exchange_order_coordinator_records_partial_failure(tmp_path):
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    okx = FakeExecutionClient(ExchangeName.OKX)
    binance = FakeExecutionClient(ExchangeName.BINANCE, fail=True)
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.1"), created_time_ms=100)
    intent = OrderIntent(intent_id="intent-2", strategy_id="v8", signal=signal, target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE))
    coordinator = MultiExchangeOrderCoordinator(clients=[okx, binance], repository=repo)

    results = await coordinator.execute(intent)

    assert [result.ok for result in results] == [True, False]
    assert repo.get_intent("intent-2").status is OrderIntentStatus.PARTIALLY_SUBMITTED  # type: ignore[union-attr]
    assert "failed" in (results[1].error or "")
