from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal

import pytest

from src.order_management.coordinator.position_plan_updater import (
    PositionPlanUpdater,
)
from src.order_management.models import ExchangeOrderResult
from src.order_management.position_plan import SqlitePositionPlanStore
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.data.models import MarketKline, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName, OrderStatus
from src.runtime.market_data.dispatcher import BoundedOrderedEventDispatcher
from src.runtime.orders import LiveOrderIntentFactory
from src.signals.models import SignalAction, TradeSignal


def _trades() -> tuple[MarketTrade, ...]:
    return tuple(
        MarketTrade(
            exchange=ExchangeName.OKX,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            price=Decimal(str(price)),
            quantity=Decimal("1"),
            side=TradeSide.BUY,
            trade_id=str(index),
            trade_time_ms=100 + index,
            event_time_ms=100 + index,
        )
        for index, price in enumerate((100, 101), start=1)
    )


def _signal() -> TradeSignal:
    return TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.4"),
        reason="portfolio_v1_deterministic_replay",
        created_time_ms=200,
        metadata={
            "sleeve_id": "mf",
            "position_id": "mf-replay-position",
            "engine": "MF_LOW_SWEEP_TIME48",
            "target_exchanges": ["okx"],
        },
    )


class _Repository:
    def add_event(self, _event) -> None:
        return None


async def _replay(*, refactored: bool, db_path):
    feature_order: list[tuple[str, str]] = []
    callback_order: list[tuple[str, str]] = []
    signals: list[TradeSignal] = []

    async def consume(name: str, trade: MarketTrade) -> None:
        feature_order.append((trade.trade_id or "", name))
        callback_order.append((trade.trade_id or "", f"feature:{name}"))

    async def raw(trade: MarketTrade) -> None:
        callback_order.append((trade.trade_id or "", "strategy:raw_trade"))
        if trade.trade_id == "2":
            signals.append(_signal())

    ordered_consumers = (
        ("range_footprint", 100),
        ("fixed_time_trade_bar", 200),
        ("trade_footprint", 300),
        ("range_bar", 400),
    )
    if refactored:
        dispatcher = BoundedOrderedEventDispatcher[MarketTrade](
            maxsize=8,
            event_time_ms=lambda trade: trade.trade_time_ms,
        )
        for name, order in reversed(ordered_consumers):
            dispatcher.subscribe(
                subscriber_id=name,
                handler=lambda trade, value=name: consume(value, trade),
                order=order,
            )
        dispatcher.subscribe(subscriber_id="raw", handler=raw, order=500)
        await dispatcher.start()
        for trade in _trades():
            dispatcher.publish(trade)
        barrier = await dispatcher.drain_through(102, timeout_seconds=1)
        assert barrier.completed
        await dispatcher.stop()
    else:
        # Parent contract captured before modularization: all derived feature
        # callbacks, then Range, then raw strategy callback for each Trade.
        for trade in _trades():
            for name, _order in ordered_consumers:
                await consume(name, trade)
            await raw(trade)

    kline = MarketKline(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        interval="4h",
        open_time_ms=0,
        close_time_ms=14_399_999,
        open=Decimal("100"),
        high=Decimal("102"),
        low=Decimal("99"),
        close=Decimal("101"),
        volume=Decimal("10"),
    )
    account = AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.POSITION,
        symbol="ETH-USDT-PERP",
        event_time_ms=150,
        quantity=Decimal("0"),
    )
    callback_order.extend(
        [
            (str(kline.open_time_ms), "strategy:closed_kline"),
            (str(account.event_time_ms), "strategy:account"),
            ("order-1", "strategy:order_result"),
        ]
    )

    factory = LiveOrderIntentFactory(
        strategy_id="strategies.eth_portfolio_v1:Strategy",
        target_exchanges=(ExchangeName.OKX,),
    )
    intents = tuple(
        factory.create(
            signal,
            source="fixed_time_trade_bar",
            event_time_ms=102,
        )
        for signal in signals
    )
    store = SqlitePositionPlanStore(db_path)
    updater = PositionPlanUpdater(
        repository=_Repository(),
        position_plan_store=store,
        master_follower_policy=None,
    )
    order_result = ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="order-1",
        status=OrderStatus.FILLED,
        quantity=Decimal("0.4"),
        filled_quantity=Decimal("0.4"),
        avg_fill_price=Decimal("101"),
    )
    for intent in intents:
        updater.record_position_plan(intent, (order_result,))
    position = store.get_position("mf-replay-position")
    legs = store.get_legs("mf-replay-position")

    def without_times(value):
        data = asdict(value)
        data.pop("created_time_ms", None)
        data.pop("updated_time_ms", None)
        return data

    return {
        "feature_order": feature_order,
        "callback_order": callback_order,
        "signals": signals,
        "intents": [
            (
                intent.intent_id,
                intent.signal,
                intent.target_exchanges,
                dict(intent.metadata),
            )
            for intent in intents
        ],
        "position": None if position is None else without_times(position),
        "legs": [without_times(leg) for leg in legs],
    }


@pytest.mark.asyncio
async def test_portfolio_v1_parent_and_refactor_deterministic_replay_parity(
    tmp_path,
) -> None:
    parent = await _replay(
        refactored=False,
        db_path=tmp_path / "parent.sqlite3",
    )
    refactored = await _replay(
        refactored=True,
        db_path=tmp_path / "refactored.sqlite3",
    )

    assert refactored == parent
