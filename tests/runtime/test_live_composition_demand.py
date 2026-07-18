from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from src.app import AppConfig, AppContext
from src.platform import ExchangeName
from src.platform.markets import get_market_profile
from src.runtime import LiveRuntimeConfig, RuntimeMode
from src.runtime.composition import compose_live_runtime
from src.runtime.requirements import (
    AccountStateRequirement,
    OrderBookRequirement,
    OrderStateRequirement,
    RangeBarRequirement,
    StrategyRuntimeRequirements,
    TradeStreamRequirement,
)
from src.runtime.services import RuntimeServices


class _IdleTradeStream:
    async def stream_trades(self):
        await asyncio.Event().wait()
        if False:
            yield None


class _IdleOrderBookStream:
    async def stream_order_book(self):
        await asyncio.Event().wait()
        if False:
            yield None


class _Strategy:
    def __init__(self, *, trade_features: bool = False) -> None:
        self.trade_features = trade_features
        self.config_reads = 0

    def trade_feature_runtime_config(self):
        self.config_reads += 1
        return {"enabled": self.trade_features}


def _app_config(tmp_path) -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="tests.fake:Strategy",
        data_streams=(),
        state_db_path=str(tmp_path / "state.sqlite3"),
        market_queue_maxsize=8,
        signal_queue_maxsize=8,
        alert_queue_maxsize=8,
        dry_run=True,
        enable_email_alerts=False,
    )


def _context(strategy: _Strategy) -> AppContext:
    return AppContext(
        data=SimpleNamespace(
            market_profile=get_market_profile("ETH-USDT-PERP")
        ),
        execution=object(),
        state_store=object(),
        strategy=strategy,
        planner=object(),
        alerts=SimpleNamespace(emit=lambda _alert: None),
    )


def _requirements(
    *,
    trades: bool = False,
    order_book: bool = False,
    range_bars: bool = False,
) -> StrategyRuntimeRequirements:
    return StrategyRuntimeRequirements(
        trades=TradeStreamRequirement(
            enabled=trades,
            stream_enabled=trades,
        ),
        order_book=OrderBookRequirement(
            enabled=order_book,
            stream_enabled=order_book,
        ),
        range_bars=RangeBarRequirement(enabled=range_bars),
        account_state=AccountStateRequirement(
            startup_snapshot_enabled=False,
            poll_enabled=False,
        ),
        order_state=OrderStateRequirement(
            post_submit_sync_enabled=False,
            poll_when_position_enabled=False,
        ),
    )


def _compose(tmp_path, requirements, strategy):
    app_config = _app_config(tmp_path)
    return compose_live_runtime(
        app_config,
        app_context=_context(strategy),
        runtime_config=LiveRuntimeConfig(
            app=app_config,
            mode=RuntimeMode.LIVE_RUNTIME,
        ),
        services=RuntimeServices(runtime_requirements=requirements),
    )


@pytest.mark.parametrize(
    ("requirements", "expected"),
    [
        (_requirements(), ()),
        (_requirements(trades=True), ("trade-stream",)),
        (_requirements(order_book=True), ("order-book-stream",)),
        (
            _requirements(range_bars=True),
            ("trade-stream", "range-bars"),
        ),
    ],
)
def test_formal_composition_resolves_exact_demand_without_opening_streams(
    tmp_path,
    monkeypatch,
    requirements,
    expected,
) -> None:
    created = {"trades": 0, "books": 0}

    def trade_factory(*_args, **_kwargs):
        created["trades"] += 1
        return _IdleTradeStream()

    def book_factory(*_args, **_kwargs):
        created["books"] += 1
        return _IdleOrderBookStream()

    monkeypatch.setattr("src.runtime.composition.create_trade_stream", trade_factory)
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        book_factory,
    )
    strategy = _Strategy()
    application = _compose(tmp_path, requirements, strategy)
    plan = application.market_data.plan(
        application.runner._market_data_capabilities
    )

    assert plan.module_ids == expected
    assert created == {"trades": 0, "books": 0}
    assert application.market_data.state().plan is None
    assert strategy.config_reads == 1


@pytest.mark.asyncio
async def test_formal_shared_features_use_one_trade_stream_and_shutdown_cleanly(
    tmp_path,
    monkeypatch,
) -> None:
    created = {"trades": 0, "books": 0}

    def trade_factory(*_args, **_kwargs):
        created["trades"] += 1
        return _IdleTradeStream()

    def book_factory(*_args, **_kwargs):
        created["books"] += 1
        return _IdleOrderBookStream()

    monkeypatch.setattr("src.runtime.composition.create_trade_stream", trade_factory)
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        book_factory,
    )
    strategy = _Strategy(trade_features=True)
    application = _compose(tmp_path, _requirements(), strategy)
    current = asyncio.current_task()

    plan = await application.market_data.start(
        application.runner._market_data_capabilities
    )
    assert plan.module_ids == (
        "trade-stream",
        "fixed-time-trade-bars",
        "range-footprint",
        "trade-footprint",
    )
    assert created == {"trades": 1, "books": 0}
    assert len(
        [module_id for module_id in plan.module_ids if module_id == "trade-stream"]
    ) == 1

    await application.market_data.stop()

    assert application.market_data.state().started_module_ids == ()
    leaked = [
        task
        for task in asyncio.all_tasks()
        if task is not current and not task.done()
    ]
    assert leaked == []
    assert strategy.config_reads == 1


@pytest.mark.asyncio
async def test_empty_formal_composition_starts_no_market_resources(
    tmp_path,
    monkeypatch,
) -> None:
    trade_factory = SimpleNamespace(calls=0)
    book_factory = SimpleNamespace(calls=0)

    def create_trade(*_args, **_kwargs):
        trade_factory.calls += 1
        return _IdleTradeStream()

    def create_book(*_args, **_kwargs):
        book_factory.calls += 1
        return _IdleOrderBookStream()

    monkeypatch.setattr("src.runtime.composition.create_trade_stream", create_trade)
    monkeypatch.setattr(
        "src.runtime.composition.create_order_book_stream",
        create_book,
    )
    application = _compose(tmp_path, _requirements(), _Strategy())

    plan = await application.market_data.start(
        application.runner._market_data_capabilities
    )
    assert plan.module_ids == ()
    assert application.market_data.state().health == ()
    assert trade_factory.calls == 0
    assert book_factory.calls == 0

    await application.market_data.stop()
    assert application.market_data.state().plan is None
