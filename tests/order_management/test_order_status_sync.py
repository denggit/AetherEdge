from __future__ import annotations

from decimal import Decimal

import pytest

from src.order_management import MultiExchangeOrderCoordinator, OrderIntent, SqliteOrderJournalStore
from src.platform import ExchangeName, Order, OrderStatus
from src.platform.exchanges.models import OrderQuery, OrderSide, OrderType
from src.signals import SignalAction, TradeSignal


class SyncingExecutionClient:
    def __init__(self) -> None:
        self.exchange = ExchangeName.OKX
        self.symbol = "ETH-USDT-PERP"
        self.queries: list[OrderQuery] = []

    @property
    def market_profile(self):  # pragma: no cover - quantity conversion not needed here
        raise NotImplementedError

    async def place_order(self, request):
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=request.symbol,
            order_id="okx-order-1",
            client_order_id=request.client_order_id,
            status=OrderStatus.NEW,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            raw={"ordId": "okx-order-1"},
        )

    async def fetch_order_status(self, query: OrderQuery):
        self.queries.append(query)
        return Order(
            exchange=self.exchange,
            symbol=query.symbol,
            raw_symbol=query.symbol,
            order_id=query.order_id,
            client_order_id=query.client_order_id,
            status=OrderStatus.FILLED,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("2500"),
            quantity=Decimal("50"),
            filled_quantity=Decimal("50"),
            raw={"avgPx": "2501.5", "fee": "-0.12", "feeCcy": "USDT"},
        )

    async def place_stop_market_order(self, request):  # pragma: no cover
        raise NotImplementedError

    async def cancel_all_orders(self):  # pragma: no cover
        return []

    async def cancel_all_stop_orders(self):  # pragma: no cover
        return []


@pytest.mark.asyncio
async def test_coordinator_queries_order_status_and_persists_fill_details(tmp_path):
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    client = SyncingExecutionClient()
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.5"), created_time_ms=100)
    intent = OrderIntent(intent_id="intent-sync", strategy_id="v8", signal=signal, target_exchanges=(ExchangeName.OKX,))
    coordinator = MultiExchangeOrderCoordinator(clients=[client], repository=repo)

    results = await coordinator.execute(intent)

    assert len(client.queries) == 1
    result = results[0]
    assert result.status is OrderStatus.FILLED
    assert result.avg_fill_price == Decimal("2501.5")
    assert result.filled_quantity == Decimal("50")
    assert result.fee == Decimal("-0.12")
    assert result.fee_asset == "USDT"
    saved = repo.list_results(intent_id="intent-sync")[0]
    assert saved.avg_fill_price == Decimal("2501.5")
    assert saved.fee == Decimal("-0.12")
    assert saved.raw["synced_order"]["avgPx"] == "2501.5"
