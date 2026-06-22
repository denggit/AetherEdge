import asyncio
from decimal import Decimal

from src.platform import (
    AccountEvent,
    AccountEventType,
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


def test_state_store_saves_and_loads_open_orders(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    order = Order(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        order_id="1",
        client_order_id="c1",
        status=OrderStatus.NEW,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        price=Decimal("3000"),
        quantity=Decimal("0.1"),
        raw={"uTime": "1710000000000"},
    )

    store.save_order(order)
    loaded = store.get_order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", order_id="1")
    open_orders = store.list_open_orders(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP")

    assert loaded is not None
    assert loaded.order_id == "1"
    assert loaded.status is OrderStatus.NEW
    assert loaded.price == Decimal("3000")
    assert len(open_orders) == 1

    store.save_order(Order(**{**order.__dict__, "status": OrderStatus.FILLED}))
    assert store.list_open_orders(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP") == []


def test_state_store_records_private_order_event_and_fill(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    event = AccountEvent(
        exchange=ExchangeName.BINANCE,
        event_type=AccountEventType.ORDER,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETHUSDT",
        event_time_ms=1710000000000,
        order_id="123",
        client_order_id="c1",
        order_status=OrderStatus.PARTIALLY_FILLED,
        side=OrderSide.SELL,
        price=Decimal("3100"),
        quantity=Decimal("0.2"),
        filled_quantity=Decimal("0.1"),
        raw={"t": "trade-1", "L": "3100", "l": "0.1", "n": "0.01", "N": "USDT"},
    )

    store.save_account_event(event)

    order = store.get_order(exchange=ExchangeName.BINANCE, symbol="ETH-USDT-PERP", order_id="123")
    fills = store.load_recent_fills(exchange=ExchangeName.BINANCE, symbol="ETH-USDT-PERP")
    events = store.load_recent_events(exchange=ExchangeName.BINANCE, symbol="ETH-USDT-PERP")

    assert order is not None
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert fills[0].trade_id == "trade-1"
    assert fills[0].quantity == Decimal("0.1")
    assert fills[0].fee_asset == "USDT"
    assert events[0].event_type is AccountEventType.ORDER


def test_state_store_saves_platform_snapshot(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    snapshot = PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=ExchangeName.OKX, asset="USDT", total=Decimal("100"), available=Decimal("90"), raw={"ccy": "USDT"}),
        positions=[Position(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", side=PositionSide.BOTH, quantity=Decimal("0"), raw={"pos": "0"})],
        open_orders=[Order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", order_id="1", client_order_id=None, status=OrderStatus.NEW)],
        open_stop_orders=[Order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", order_id="sl1", client_order_id=None, status=OrderStatus.NEW)],
        leverage=LeverageInfo(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", leverage=Decimal("3")),
        position_mode=PositionMode.ONE_WAY,
    )

    store.save_snapshot(snapshot)

    open_orders = store.list_open_orders(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", include_stop_orders=True)
    normal_orders = store.list_open_orders(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", include_stop_orders=False)

    assert len(open_orders) == 2
    assert len(normal_orders) == 1
    assert normal_orders[0].order_id == "1"


def test_sqlite_store_reconcile_open_orders_snapshot_marks_missing_open_orders_closed(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    order = Order(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        order_id="stale-1",
        client_order_id="stale-c1",
        status=OrderStatus.NEW,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        price=Decimal("3000"),
        quantity=Decimal("0.1"),
        raw={"uTime": "1710000000000"},
    )
    store.save_order(order)

    changed = store.mark_missing_open_orders_closed(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        live_order_keys=set(),
        is_stop_order=False,
    )

    assert changed == 1
    assert store.list_open_orders(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP") == []
    loaded = store.get_order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", order_id="stale-1")
    assert loaded is not None
    assert loaded.status is OrderStatus.CANCELED
    assert loaded.raw["local_reconcile_reason"] == "missing_from_exchange_open_orders"


def test_sqlite_store_reconcile_missing_order_with_null_client_order_id(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    order = Order(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        order_id="stale-null-cid",
        client_order_id=None,
        status=OrderStatus.NEW,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        price=Decimal("3000"),
        quantity=Decimal("0.1"),
        raw={"uTime": "1710000000000"},
    )
    store.save_order(order)

    changed = store.mark_missing_open_orders_closed(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        live_order_keys=set(),
        is_stop_order=False,
    )

    assert changed == 1
    assert store.list_open_orders(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP") == []
    loaded = store.get_order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", order_id="stale-null-cid")
    assert loaded is not None
    assert loaded.status is OrderStatus.CANCELED
    assert loaded.raw["local_reconcile_reason"] == "missing_from_exchange_open_orders"


def test_sqlite_store_reconcile_missing_order_with_null_order_id(tmp_path):
    store = SqliteStateStore(tmp_path / "state.sqlite3")
    order = Order(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        order_id=None,
        client_order_id="stale-null-oid",
        status=OrderStatus.NEW,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        price=Decimal("3000"),
        quantity=Decimal("0.1"),
        raw={"uTime": "1710000000000"},
    )
    store.save_order(order)

    changed = store.mark_missing_open_orders_closed(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        live_order_keys=set(),
        is_stop_order=False,
    )

    assert changed == 1
    assert store.list_open_orders(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP") == []
    loaded = store.get_order(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", client_order_id="stale-null-oid")
    assert loaded is not None
    assert loaded.status is OrderStatus.CANCELED
    assert loaded.raw["local_reconcile_reason"] == "missing_from_exchange_open_orders"
