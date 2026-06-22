import asyncio
from decimal import Decimal

from src.platform import (
    AccountEvent,
    AccountEventType,
    Balance,
    ExchangeName,
    LeverageInfo,
    Order,
    OrderStatus,
    PlatformRuntime,
    Position,
    PositionMode,
    PositionSide,
    RuntimeConfig,
    RuntimeContext,
)
from src.platform.runtime.factory import build_runtime_context


class FakeData:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"


class FakeAccount:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    async def fetch_balance(self, asset="USDT"):
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("100"), available=Decimal("90"))

    async def fetch_positions(self, symbol=None):
        return [Position(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", side=PositionSide.BOTH, quantity=Decimal("0"))]

    async def fetch_leverage(self):
        return LeverageInfo(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", leverage=Decimal("3"))

    async def fetch_position_mode(self):
        return PositionMode.ONE_WAY


class FakeExecution:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    async def fetch_open_orders(self):
        return [Order(exchange=self.exchange, symbol=self.symbol, raw_symbol="ETH-USDT-SWAP", order_id="1", client_order_id=None, status=OrderStatus.NEW)]

    async def fetch_open_stop_orders(self):
        return []


class FakeEventStream:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self, events):
        self.events = list(events)

    async def stream_events(self):
        for event in self.events:
            yield event


class FakeStateStore:
    def __init__(self):
        self.snapshots = []
        self.events = []

    def save_snapshot(self, snapshot):
        self.snapshots.append(snapshot)

    def save_account_event(self, event):
        self.events.append(event)


class FakeHandler:
    def __init__(self):
        self.snapshots = []
        self.events = []

    async def on_snapshot(self, snapshot):
        self.snapshots.append(snapshot)

    async def on_account_event(self, event):
        self.events.append(event)


def test_runtime_legacy_private_stream_collects_events_when_explicitly_enabled():
    event = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        symbol="ETH-USDT-PERP",
        order_id="1",
        order_status=OrderStatus.FILLED,
        raw={"ordId": "1"},
    )
    store = FakeStateStore()
    handler = FakeHandler()
    runtime = PlatformRuntime(
        config=RuntimeConfig(exchange=ExchangeName.OKX, enable_private_event_stream=True),
        context=RuntimeContext(
            data=FakeData(),
            execution=FakeExecution(),
            account=FakeAccount(),
            state_store=store,
            account_event_stream=FakeEventStream([event]),
        ),
        handlers=[handler],
    )

    result = asyncio.run(runtime.run(max_account_events=1))

    assert len(store.snapshots) == 1
    assert len(store.events) == 1
    assert store.events[0].order_status is OrderStatus.FILLED
    assert len(handler.snapshots) == 1
    assert len(handler.events) == 1
    assert result.stats.snapshots_saved == 1
    assert result.stats.account_events_saved == 1


def test_runtime_can_collect_only_snapshot_when_private_stream_disabled():
    store = FakeStateStore()
    runtime = PlatformRuntime(
        config=RuntimeConfig(exchange=ExchangeName.OKX, enable_private_event_stream=False),
        context=RuntimeContext(
            data=FakeData(),
            execution=FakeExecution(),
            account=FakeAccount(),
            state_store=store,
            account_event_stream=None,
        ),
    )

    result = asyncio.run(runtime.run())

    assert len(store.snapshots) == 1
    assert store.events == []
    assert result.stats.account_events_saved == 0


def test_runtime_config_disables_private_event_stream_by_default():
    assert RuntimeConfig(exchange=ExchangeName.OKX).enable_private_event_stream is False


def test_build_runtime_context_default_does_not_create_account_event_stream(monkeypatch, tmp_path):
    called = False

    def fake_private_stream(*args, **kwargs):
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr("src.platform.runtime.factory.create_market_data_feed", lambda *args, **kwargs: FakeData())
    monkeypatch.setattr("src.platform.runtime.factory.create_execution_client", lambda *args, **kwargs: FakeExecution())
    monkeypatch.setattr("src.platform.runtime.factory.create_account_client", lambda *args, **kwargs: FakeAccount())
    monkeypatch.setattr("src.platform.runtime.factory.create_account_event_stream", fake_private_stream)

    context = build_runtime_context(RuntimeConfig(exchange=ExchangeName.OKX, state_db_path=tmp_path / "state.sqlite3"))

    assert context.account_event_stream is None
    assert called is False
