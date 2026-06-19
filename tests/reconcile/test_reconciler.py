import asyncio
import json
from decimal import Decimal

from src.platform import (
    Balance,
    ExchangeName,
    LeverageInfo,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    PlatformSnapshot,
    Position,
    PositionMode,
    PositionSide,
    SqliteStateStore,
)
from src.reconcile import EmailReconcileNotifier, ReconcileCategory, ReconcileSeverity, Reconciler


SYMBOL = "ETH-USDT-PERP"
RAW = "ETH-USDT-SWAP"


def _order(order_id: str, *, status=OrderStatus.NEW) -> Order:
    return Order(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol=RAW,
        order_id=order_id,
        client_order_id=None,
        status=status,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        price=Decimal("3000"),
        quantity=Decimal("0.1"),
        raw={"uTime": "1710000000000"},
    )


def _stop(order_id: str, *, status=OrderStatus.NEW) -> Order:
    return Order(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol=RAW,
        order_id=order_id,
        client_order_id=None,
        status=status,
        side=OrderSide.SELL,
        order_type=None,
        price=None,
        quantity=Decimal("0.1"),
        raw={"algoId": order_id, "uTime": "1710000000000"},
    )


def _snapshot(position_qty: str = "0.1") -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol=SYMBOL,
        balance=Balance(exchange=ExchangeName.OKX, asset="USDT", total=Decimal("100"), available=Decimal("90")),
        positions=[
            Position(
                exchange=ExchangeName.OKX,
                symbol=SYMBOL,
                raw_symbol=RAW,
                side=PositionSide.BOTH,
                quantity=Decimal(position_qty),
                raw={"instId": RAW, "posSide": "net", "pos": position_qty},
            )
        ],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(exchange=ExchangeName.OKX, symbol=SYMBOL, raw_symbol=RAW, leverage=Decimal("3")),
        position_mode=PositionMode.ONE_WAY,
    )


class FakeExecution:
    exchange = ExchangeName.OKX
    symbol = SYMBOL

    def __init__(self, *, open_orders=None, open_stop_orders=None):
        self._open_orders = open_orders or []
        self._open_stop_orders = open_stop_orders or []

    async def fetch_open_orders(self):
        return self._open_orders

    async def fetch_open_stop_orders(self):
        return self._open_stop_orders


class FakeAccount:
    exchange = ExchangeName.OKX
    symbol = SYMBOL

    def __init__(self, *, positions=None):
        self._positions = positions or []

    async def fetch_positions(self):
        return self._positions


def test_reconciler_reports_ok_when_local_and_exchange_match(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    order = _order("1")
    stop = _stop("sl1")
    store.save_order(order)
    store.save_order(stop, is_stop_order=True)
    store.save_snapshot(_snapshot("0.1"))

    remote_position = Position(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol=RAW,
        side=PositionSide.BOTH,
        quantity=Decimal("0.1"),
        raw={"instId": RAW, "posSide": "net", "pos": "0.1"},
    )
    report = asyncio.run(
        Reconciler(
            account=FakeAccount(positions=[remote_position]),
            execution=FakeExecution(open_orders=[order], open_stop_orders=[stop]),
            state_store=store,
        ).check()
    )

    assert report.ok
    assert report.issues == []


def test_reconciler_detects_missing_local_and_missing_exchange_orders(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    local_stop = _stop("local-sl")
    store.save_order(local_stop, is_stop_order=True)
    store.save_snapshot(_snapshot("0"))

    remote_order = _order("remote-1")
    report = asyncio.run(
        Reconciler(
            account=FakeAccount(positions=[]),
            execution=FakeExecution(open_orders=[remote_order], open_stop_orders=[]),
            state_store=store,
        ).check()
    )

    categories = {issue.category for issue in report.issues}
    assert ReconcileCategory.MISSING_LOCAL_ORDER in categories
    assert ReconcileCategory.MISSING_EXCHANGE_STOP_ORDER in categories
    assert report.has_warnings


def test_reconciler_detects_position_mismatch_from_latest_snapshot(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    store.save_snapshot(_snapshot("0.1"))
    remote_position = Position(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol=RAW,
        side=PositionSide.BOTH,
        quantity=Decimal("0"),
        raw={"instId": RAW, "posSide": "net", "pos": "0"},
    )

    report = asyncio.run(
        Reconciler(
            account=FakeAccount(positions=[remote_position]),
            execution=FakeExecution(),
            state_store=store,
        ).check()
    )

    assert any(issue.category is ReconcileCategory.POSITION_MISMATCH for issue in report.issues)


def test_reconciler_email_notifier_is_optional_and_decoupled(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    store.save_snapshot(_snapshot("0"))
    remote_order = _order("remote-1")
    sent = []

    def fake_email_sender(*, subject, body):
        sent.append((subject, body))

    report = asyncio.run(
        Reconciler(
            account=FakeAccount(positions=[]),
            execution=FakeExecution(open_orders=[remote_order]),
            state_store=store,
            notifier=EmailReconcileNotifier(email_sender=fake_email_sender),
        ).check_and_notify()
    )

    assert not report.ok
    assert len(sent) == 1
    assert "remote-1" in sent[0][1]


def test_state_store_loads_latest_account_snapshot(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    store.save_snapshot(_snapshot("0.1"))
    store.save_snapshot(_snapshot("0.2"))

    snapshot = store.load_latest_account_snapshot(exchange=ExchangeName.OKX, symbol=SYMBOL)

    assert snapshot is not None
    positions = json.loads(snapshot.positions_json)
    assert positions[0]["pos"] == "0.2"


def test_email_notifier_supports_existing_async_content_signature(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    store.save_snapshot(_snapshot("0"))
    sent = []

    async def existing_sender(subject, content, content_type="plain"):
        sent.append((subject, content, content_type))
        return True

    report = asyncio.run(
        Reconciler(
            account=FakeAccount(positions=[]),
            execution=FakeExecution(open_orders=[_order("remote-async")]),
            state_store=store,
            notifier=EmailReconcileNotifier(email_sender=existing_sender),
        ).check_and_notify()
    )

    assert not report.ok
    assert len(sent) == 1
    assert sent[0][2] == "plain"
    assert "remote-async" in sent[0][1]
