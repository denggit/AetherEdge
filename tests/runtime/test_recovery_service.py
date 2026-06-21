from __future__ import annotations

import asyncio
from decimal import Decimal

from src.order_management import OrderIntent, SqliteOrderJournalStore
from src.platform import Balance, ExchangeName, LeverageInfo, Order, OrderStatus, Position, PositionMode, PositionSide
from src.platform.state import SqliteStateStore
from src.runtime.recovery import RecoveryExchangeContext, RuntimeRecoveryService
from src.signals import SignalAction, TradeSignal


class FakeAccount:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    async def fetch_balance(self, asset="USDT"):
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("100"), available=Decimal("90"))

    async def fetch_positions(self, symbol=None):
        return [
            Position(
                exchange=self.exchange,
                symbol=self.symbol,
                raw_symbol="ETH-USDT-SWAP",
                side=PositionSide.BOTH,
                quantity=Decimal("0"),
                raw={"instId": "ETH-USDT-SWAP", "posSide": "both", "pos": "0"},
            )
        ]

    async def fetch_leverage(self):
        return LeverageInfo(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", leverage=Decimal("3"))

    async def fetch_position_mode(self):
        return PositionMode.ONE_WAY


class FakeExecution:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    async def fetch_open_orders(self):
        return [Order(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", order_id="ord-1", client_order_id="cid-1", status=OrderStatus.NEW)]

    async def fetch_open_stop_orders(self):
        return []


class RecoverableStrategy:
    def __init__(self):
        self.contexts = []

    async def recover(self, context):
        self.contexts.append(context)
        return [TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CANCEL_ALL_ORDERS)]


def test_runtime_recovery_collects_snapshot_reconciles_loads_intents_and_calls_strategy(tmp_path):
    state_store = SqliteStateStore(tmp_path / "state.sqlite3")
    journal = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CANCEL_ALL_ORDERS, created_time_ms=1)
    intent = OrderIntent(intent_id="intent-1", strategy_id="v8", signal=signal, target_exchanges=(ExchangeName.OKX,))
    journal.save_intent(intent)
    strategy = RecoverableStrategy()
    service = RuntimeRecoveryService(
        exchange_contexts=(RecoveryExchangeContext(account=FakeAccount(), execution=FakeExecution(), state_store=state_store),),
        order_journal=journal,
        intent_ids=("intent-1",),
    )

    report = asyncio.run(service.recover(strategy=strategy))

    assert report.ok is True
    assert len(report.snapshots) == 1
    assert len(report.reconcile_reports) == 1
    assert len(report.order_intents) == 1
    assert len(report.strategy_signals) == 1
    assert strategy.contexts[0].order_intent_ids == ("intent-1",)
    assert state_store.load_latest_account_snapshot(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP") is not None


def test_runtime_recovery_without_recoverable_strategy_is_noop_for_strategy_hook(tmp_path):
    state_store = SqliteStateStore(tmp_path / "state.sqlite3")
    service = RuntimeRecoveryService(
        exchange_contexts=(RecoveryExchangeContext(account=FakeAccount(), execution=FakeExecution(), state_store=state_store),)
    )

    report = asyncio.run(service.recover(strategy=object()))

    assert report.strategy_signals == ()
    assert report.ok is True
