from __future__ import annotations

import asyncio
from decimal import Decimal

from src.order_management import MultiExchangeOrderCoordinator, OrderIntent, SqliteOrderJournalStore
from src.order_management.quantity import NativeQuantityConverter
from src.platform import ExchangeConfig, ExchangeName, Order, OrderRequest, OrderSide, OrderStatus, OrderType, create_execution_client, get_market_profile
from src.platform.exchanges.models import InstrumentRule
from src.signals import SignalAction, TradeSignal


def test_native_quantity_converter_maps_base_qty_to_binance_and_okx():
    profile = get_market_profile("ETH-USDT-PERP")
    converter = NativeQuantityConverter()

    binance = converter.convert_quantity(exchange=ExchangeName.BINANCE, symbol=profile.symbol, base_quantity=Decimal("0.5"), market_profile=profile)
    okx = converter.convert_quantity(exchange=ExchangeName.OKX, symbol=profile.symbol, base_quantity=Decimal("0.5"), market_profile=profile)

    assert binance.native_quantity == Decimal("0.5")
    assert okx.native_quantity == Decimal("5")


class NativeFakeExchange:
    def __init__(self, exchange: ExchangeName, *, step: Decimal) -> None:
        self.exchange = exchange
        self.step = step
        self.placed = []

    async def fetch_instrument_rule(self, symbol):
        return InstrumentRule(
            exchange=self.exchange,
            symbol=symbol,
            raw_symbol=symbol,
            quantity_step=self.step,
            min_quantity=self.step,
        )

    async def place_order(self, request):
        self.placed.append(request)
        return Order(exchange=self.exchange, symbol=request.symbol, raw_symbol=request.symbol, order_id="1", client_order_id=request.client_order_id, status=OrderStatus.NEW, quantity=request.quantity)


def test_coordinator_converts_before_existing_execution_step_normalization(tmp_path):
    okx_native = NativeFakeExchange(ExchangeName.OKX, step=Decimal("1"))
    binance_native = NativeFakeExchange(ExchangeName.BINANCE, step=Decimal("0.1"))
    okx = create_execution_client("okx", config=ExchangeConfig(sandbox=True), exchange_client=okx_native)
    binance = create_execution_client("binance", config=ExchangeConfig(sandbox=True), exchange_client=binance_native)
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    coordinator = MultiExchangeOrderCoordinator(clients=[okx, binance], repository=repo)
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.505"), created_time_ms=100)
    intent = OrderIntent(intent_id="intent-q", strategy_id="test", signal=signal, target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE))

    asyncio.run(coordinator.execute(intent))

    assert okx_native.placed[0].quantity == Decimal("5")
    assert binance_native.placed[0].quantity == Decimal("0.5")
