from __future__ import annotations

import asyncio
from decimal import Decimal

from src.order_management import OrderIntent, SqliteOrderJournalStore
from src.order_management.position_plan.models import LegPlan, LegRole, LegSyncStatus, PositionPlan, PositionPlanStatus
from src.order_management.position_plan.store import SqlitePositionPlanStore
from src.platform import Balance, ExchangeName, LeverageInfo, Order, OrderSide, OrderStatus, OrderType, Position, PositionMode, PositionSide
from src.platform.state import SqliteStateStore
from src.runtime.recovery import RecoveryExchangeContext, RuntimeRecoveryService
from src.signals import SignalAction, TradeSignal
from strategies.eth_lf_portfolio_v8.strategy import Strategy


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


class ConfigurableFakeAccount:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self, positions=None):
        self._positions = positions if positions is not None else []

    async def fetch_balance(self, asset="USDT"):
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("100"), available=Decimal("90"))

    async def fetch_positions(self, symbol=None):
        return list(self._positions)

    async def fetch_leverage(self):
        return LeverageInfo(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", leverage=Decimal("3"))

    async def fetch_position_mode(self):
        return PositionMode.ONE_WAY


class ConfigurableFakeExecution:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self, *, open_orders=None, open_stop_orders=None):
        self._open_orders = open_orders if open_orders is not None else []
        self._open_stop_orders = open_stop_orders if open_stop_orders is not None else []

    async def fetch_open_orders(self):
        return list(self._open_orders)

    async def fetch_open_stop_orders(self):
        return list(self._open_stop_orders)


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


def test_startup_recovery_marks_stale_local_stop_closed_and_continues(tmp_path):
    state_store = SqliteStateStore(tmp_path / "state.sqlite3")
    plan_store = _active_short_plan_store(tmp_path / "plans.sqlite3")
    stale_stop = _stop_order(order_id="3681380310358618112", quantity=Decimal("2.82"))
    state_store.save_order(stale_stop, is_stop_order=True)
    strategy = Strategy()
    service = RuntimeRecoveryService(
        exchange_contexts=(
            RecoveryExchangeContext(
                account=ConfigurableFakeAccount(positions=[_short_okx_position()]),
                execution=ConfigurableFakeExecution(open_stop_orders=[]),
                state_store=state_store,
            ),
        ),
        position_plan_store=plan_store,
    )

    report = asyncio.run(service.recover(strategy=strategy))

    loaded = state_store.get_order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", order_id="3681380310358618112")
    assert loaded is not None
    assert loaded.status is OrderStatus.CANCELED
    assert loaded.raw["local_reconcile_reason"] == "startup_recovery_missing_from_exchange_open_stop_orders"
    assert report.ok is True
    assert report.issues == ()
    assert report.strategy_signals
    place = next(signal for signal in report.strategy_signals if signal.action is SignalAction.PLACE_STOP_LOSS_SHORT)
    assert place.quantity == Decimal("0.282")
    assert place.trigger_price == Decimal("1719.40")


def test_startup_recovery_marks_stale_local_regular_order_closed(tmp_path):
    state_store = SqliteStateStore(tmp_path / "state.sqlite3")
    state_store.save_order(
        Order(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            order_id="stale-entry",
            client_order_id="stale-entry-cid",
            status=OrderStatus.NEW,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal("2000"),
            quantity=Decimal("0.1"),
        ),
        is_stop_order=False,
    )
    service = RuntimeRecoveryService(
        exchange_contexts=(
            RecoveryExchangeContext(
                account=ConfigurableFakeAccount(),
                execution=ConfigurableFakeExecution(open_orders=[], open_stop_orders=[]),
                state_store=state_store,
            ),
        )
    )

    report = asyncio.run(service.recover(strategy=RecoverableStrategy()))

    loaded = state_store.get_order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", order_id="stale-entry")
    assert loaded is not None
    assert loaded.status is OrderStatus.CANCELED
    assert loaded.raw["local_reconcile_reason"] == "startup_recovery_missing_from_exchange_open_orders"
    assert report.ok is True
    assert report.issues == ()


def test_startup_recovery_saves_exchange_stop_missing_locally_and_continues(tmp_path):
    state_store = SqliteStateStore(tmp_path / "state.sqlite3")
    plan_store = _active_short_plan_store(tmp_path / "plans.sqlite3")
    live_stop = _stop_order(order_id="live-stop", client_order_id="pos-1-stop", quantity=Decimal("2.82"))
    strategy = Strategy()
    service = RuntimeRecoveryService(
        exchange_contexts=(
            RecoveryExchangeContext(
                account=ConfigurableFakeAccount(positions=[_short_okx_position()]),
                execution=ConfigurableFakeExecution(open_stop_orders=[live_stop]),
                state_store=state_store,
            ),
        ),
        position_plan_store=plan_store,
    )

    report = asyncio.run(service.recover(strategy=strategy))

    loaded = state_store.get_order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", order_id="live-stop")
    assert loaded is not None
    assert loaded.status is OrderStatus.NEW
    assert loaded.is_stop_order is True
    assert report.ok is True
    assert report.issues == ()
    assert not any(signal.action is SignalAction.PLACE_STOP_LOSS_SHORT for signal in report.strategy_signals)


def _short_okx_position() -> Position:
    return Position(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        side=PositionSide.BOTH,
        quantity=Decimal("-2.82"),
        entry_price=Decimal("1700"),
        raw={"instId": "ETH-USDT-SWAP", "posSide": "both", "pos": "-2.82"},
    )


def _stop_order(*, order_id: str, quantity: Decimal, client_order_id: str = "pos-1-stop") -> Order:
    return Order(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        order_id=order_id,
        client_order_id=client_order_id,
        status=OrderStatus.NEW,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        price=Decimal("1719.40"),
        quantity=quantity,
        raw={"reduceOnly": "true"},
    )


def _active_short_plan_store(path) -> SqlitePositionPlanStore:
    store = SqlitePositionPlanStore(path)
    store.upsert_position(
        PositionPlan(
            position_id="pos-1",
            strategy_id="eth_lf_portfolio_v9c_reclaim_priority",
            entry_engine="BULL_RECLAIM_V2",
            side="short",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=Decimal("1719.40"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.282"),
            master_filled_qty_base=Decimal("0.282"),
            created_time_ms=123,
        )
    )
    store.upsert_leg(
        LegPlan(
            position_id="pos-1",
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.282"),
            filled_qty_base=Decimal("0.282"),
            sync_status=LegSyncStatus.OPEN,
        )
    )
    return store
