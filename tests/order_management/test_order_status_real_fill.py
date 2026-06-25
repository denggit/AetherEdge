from __future__ import annotations

from decimal import Decimal

import pytest

from src.order_management import MultiExchangeOrderCoordinator, OrderIntent, SqliteOrderJournalStore
from src.platform import ExchangeName, Order, OrderStatus
from src.platform.exchanges.models import OrderQuery, OrderSide, OrderType
from src.signals import SignalAction, TradeSignal


class _Client:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = None

    def __init__(
        self,
        *,
        synced_avg_price: Decimal | None,
        synced_filled_qty: Decimal | None = Decimal("0.5"),
        synced_order_price: Decimal | None = None,
    ) -> None:
        self.synced_avg_price = synced_avg_price
        self.synced_filled_qty = synced_filled_qty
        self.synced_order_price = synced_order_price
        self.queries: list[OrderQuery] = []

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
        raw = {}
        if self.synced_avg_price is not None:
            raw["avgPx"] = str(self.synced_avg_price)
        return Order(
            exchange=self.exchange,
            symbol=query.symbol,
            raw_symbol=query.symbol,
            order_id=query.order_id,
            client_order_id=query.client_order_id,
            status=OrderStatus.FILLED,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            price=self.synced_order_price,
            quantity=Decimal("0.5"),
            filled_quantity=self.synced_filled_qty,
            raw=raw,
        )

    async def place_stop_market_order(self, request):  # pragma: no cover
        raise NotImplementedError

    async def cancel_all_orders(self):  # pragma: no cover
        return []

    async def cancel_all_stop_orders(self):  # pragma: no cover
        return []


@pytest.mark.asyncio
async def test_market_entry_ack_without_avg_fill_queries_order_status(tmp_path) -> None:
    client = _Client(synced_avg_price=Decimal("1620.30"))
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=SqliteOrderJournalStore(tmp_path / "journal.sqlite3"),
    )
    intent = _open_intent()

    results = await coordinator.execute(intent)

    assert len(client.queries) == 1
    assert results[0].ok is True
    assert results[0].avg_fill_price == Decimal("1620.30")
    assert results[0].filled_quantity == Decimal("0.5")


@pytest.mark.asyncio
async def test_market_entry_without_real_fill_after_status_sync_fails(tmp_path) -> None:
    client = _Client(synced_avg_price=None)
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=SqliteOrderJournalStore(tmp_path / "journal.sqlite3"),
    )
    intent = _open_intent()

    results = await coordinator.execute(intent)

    assert len(client.queries) == 1
    assert results[0].ok is False
    assert results[0].error == "missing_real_fill_price_or_quantity"
    assert results[0].raw["real_fill_required"] is True


@pytest.mark.asyncio
async def test_market_entry_order_price_does_not_count_as_real_avg_fill_price(tmp_path) -> None:
    client = _Client(synced_avg_price=None, synced_order_price=Decimal("1620.30"))
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=SqliteOrderJournalStore(tmp_path / "journal.sqlite3"),
    )
    intent = _open_intent()

    results = await coordinator.execute(intent)

    assert len(client.queries) == 1
    assert results[0].ok is False
    assert results[0].avg_fill_price is None
    assert results[0].error == "missing_real_fill_price_or_quantity"


@pytest.mark.asyncio
async def test_market_entry_raw_avg_price_counts_as_real_avg_fill_price(tmp_path) -> None:
    client = _Client(synced_avg_price=Decimal("1620.30"), synced_order_price=Decimal("9999"))
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=SqliteOrderJournalStore(tmp_path / "journal.sqlite3"),
    )
    intent = _open_intent()

    results = await coordinator.execute(intent)

    assert results[0].ok is True
    assert results[0].avg_fill_price == Decimal("1620.30")


def _open_intent() -> OrderIntent:
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.5"),
        metadata={"execution_purpose": "normal_entry", "target_exchanges": ["okx"]},
    )
    return OrderIntent(
        intent_id="intent-real-fill",
        strategy_id="v8",
        signal=signal,
        target_exchanges=(ExchangeName.OKX,),
    )
