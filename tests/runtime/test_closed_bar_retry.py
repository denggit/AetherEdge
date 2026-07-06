from __future__ import annotations

from decimal import Decimal

import pytest

from src.app import AppConfig, AppContext
from src.market_data.events import MarketFeatureEvent
from src.market_data.models import TimeRange
from src.market_data.storage import SqliteKlineStore
from src.platform import Balance, ExchangeName, LeverageInfo, PositionMode
from src.platform.data.models import MarketKline
from src.platform.markets import get_market_profile
from src.platform.snapshot import PlatformSnapshot
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode


H4 = 4 * 60 * 60_000


class _Alerts:
    def __init__(self) -> None:
        self.items = []

    def emit(self, alert) -> None:
        self.items.append(alert)


class _Data:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    def __init__(self) -> None:
        self.calls = []
        self.responses = [[], [], [], [_kline(0)]]

    async def fetch_klines(self, **kwargs):
        self.calls.append(kwargs)
        if self.responses:
            return self.responses.pop(0)
        return [_kline(0)]

    async def stream_trades(self):
        if False:
            yield None

    async def stream_order_book(self):
        if False:
            yield None


class _StateStore:
    def save_snapshot(self, snapshot):
        self.snapshot = snapshot

    def list_open_orders(self, **kwargs):
        return []


class _Strategy:
    def __init__(self) -> None:
        self.events = []

    async def on_market_feature(self, event: MarketFeatureEvent):
        self.events.append(event)
        return []


class _FailingKlineStore:
    def save(self, rows):
        raise RuntimeError("disk unavailable")


class _RecordingKlineStore:
    def __init__(self) -> None:
        self.rows = []

    def save(self, rows):
        self.rows.extend(rows)
        return len(rows)


@pytest.mark.asyncio
async def test_closed_bar_retry_starts_at_05s_alerts_once_and_emits_once() -> None:
    data = _Data()
    strategy = _Strategy()
    alerts = _Alerts()
    runner = _runner(strategy=strategy, data=data, alerts=alerts)

    assert await runner.poll_closed_bar_once(now_ms=H4 + 4_000) == []
    assert data.calls == []

    assert await runner.poll_closed_bar_once(now_ms=H4 + 5_000) == []
    assert len(data.calls) == 1
    assert data.calls[-1]["use_cache"] is False
    assert data.calls[-1]["start_time_ms"] == 0

    assert await runner.poll_closed_bar_once(now_ms=H4 + 6_000) == []
    assert len(data.calls) == 1

    assert await runner.poll_closed_bar_once(now_ms=H4 + 10_000) == []
    assert len(data.calls) == 2

    assert await runner.poll_closed_bar_once(now_ms=H4 + 120_000) == []
    assert len(data.calls) == 3
    assert len(alerts.items) == 1
    assert alerts.items[0].subject == "AetherEdge closed bar missing"

    events = await runner.poll_closed_bar_once(now_ms=H4 + 125_000)
    assert [event.type_value for event in events] == ["closed_kline"]
    assert len(strategy.events) == 1

    assert await runner.poll_closed_bar_once(now_ms=H4 + 130_000) == []
    assert len(data.calls) == 4
    assert len(alerts.items) == 1


@pytest.mark.asyncio
async def test_closed_bar_poll_upserts_same_open_time_without_duplicate(
    tmp_path,
) -> None:
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    first_data = _Data()
    first_data.responses = [[_kline(0)]]
    second_data = _Data()
    second_data.responses = [[_kline(0)]]

    first = _runner(
        strategy=_Strategy(),
        data=first_data,
        alerts=_Alerts(),
        kline_store=store,
    )
    second = _runner(
        strategy=_Strategy(),
        data=second_data,
        alerts=_Alerts(),
        kline_store=store,
    )

    assert await first.poll_closed_bar_once(now_ms=H4 + 5_000)
    assert await second.poll_closed_bar_once(now_ms=H4 + 5_000)
    rows = store.load(
        symbol="ETH-USDT-PERP",
        interval="4h",
        time_range=TimeRange(0, H4),
    )

    assert len(rows) == 1
    assert rows[0].open_time_ms == 0


@pytest.mark.asyncio
async def test_closed_bar_store_failure_alerts_and_still_processes_feature(
    caplog,
) -> None:
    data = _Data()
    data.responses = [[_kline(0)]]
    strategy = _Strategy()
    alerts = _Alerts()
    runner = _runner(
        strategy=strategy,
        data=data,
        alerts=alerts,
        kline_store=_FailingKlineStore(),
    )

    events = await runner.poll_closed_bar_once(now_ms=H4 + 5_000)

    assert [event.type_value for event in events] == ["closed_kline"]
    assert len(strategy.events) == 1
    assert alerts.items[-1].subject == (
        "AetherEdge closed kline persistence failed"
    )
    assert "Failed to persist live closed kline" in caplog.text


def test_closed_bar_persistence_adds_no_unbounded_kline_cache() -> None:
    runner = _runner(
        strategy=_Strategy(),
        data=_Data(),
        alerts=_Alerts(),
        kline_store=_FailingKlineStore(),
    )

    assert not hasattr(runner, "_all_klines")


def _runner(
    *,
    strategy: _Strategy,
    data: _Data,
    alerts: _Alerts,
    kline_store=None,
) -> LiveRuntimeRunner:
    cfg = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="strategies.fake:Strategy",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=20,
        signal_queue_maxsize=20,
        alert_queue_maxsize=20,
        dry_run=True,
        enable_email_alerts=False,
    )
    resolved_kline_store = (
        _RecordingKlineStore()
        if kline_store is None
        else kline_store
    )
    context = AppContext(
        data=data,
        execution=object(),
        state_store=_StateStore(),
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=alerts,
    )
    return LiveRuntimeRunner(
        app_config=cfg,
        app_context=context,
        runtime_config=LiveRuntimeConfig(
            app=cfg,
            mode=RuntimeMode.LIVE_RUNTIME,
            warmup_enabled=False,
            closed_bar_buffer_ms=5_000,
            closed_bar_retry_interval_ms=5_000,
            closed_bar_missing_alert_after_ms=120_000,
        ),
        services={
            "recovery_service": None,
            "snapshot": _snapshot(),
            "kline_store": resolved_kline_store,
        },
    )


def _kline(open_time_ms: int) -> MarketKline:
    return MarketKline(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        interval="4h",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + H4 - 1,
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal("105"),
        volume=Decimal("10"),
        is_closed=True,
    )


def _snapshot() -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=ExchangeName.OKX, asset="USDT", total=Decimal("1000"), available=Decimal("1000")),
        positions=[],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", leverage=Decimal("1")),
        position_mode=PositionMode.ONE_WAY,
    )
