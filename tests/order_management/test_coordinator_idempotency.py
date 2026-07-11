from __future__ import annotations

import asyncio
import sqlite3
from decimal import Decimal
from unittest.mock import Mock

import pytest

from src.order_management import (
    DuplicateIntentError,
    MultiExchangeOrderCoordinator,
    OrderIntent,
    SqliteOrderJournalStore,
)
from src.planner import ExecutionPlanner
from src.platform import ExchangeName, InstrumentRule, Order, OrderStatus
from src.platform.markets import get_market_profile
from src.signals import SignalAction, TradeSignal


class CountingExecutionClient:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = None

    def __init__(self) -> None:
        self.place_order_calls = 0
        self.place_stop_calls = 0
        self.cancel_order_calls = 0
        self.cancel_stop_calls = 0
        self.fetch_rule_calls = 0

    async def place_order(self, request):
        self.place_order_calls += 1
        await asyncio.sleep(0)
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=request.symbol,
            order_id="order-1",
            client_order_id=request.client_order_id,
            status=OrderStatus.FILLED,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            filled_quantity=request.quantity,
            raw={"avgPx": "2000"},
        )

    async def place_stop_market_order(self, request):
        self.place_stop_calls += 1
        raise AssertionError("unexpected stop placement")

    async def cancel_all_orders(self):
        self.cancel_order_calls += 1
        raise AssertionError("unexpected order cancellation")

    async def cancel_all_stop_orders(self):
        self.cancel_stop_calls += 1
        raise AssertionError("unexpected stop cancellation")

    async def fetch_instrument_rule(self):
        self.fetch_rule_calls += 1
        return InstrumentRule(
            exchange=self.exchange,
            symbol=self.symbol,
            raw_symbol=self.symbol,
        )


class RecoveryTopupClient(CountingExecutionClient):
    exchange = ExchangeName.BINANCE
    market_profile = get_market_profile("ETH-USDT-PERP")


class CountingPlanner:
    def __init__(self) -> None:
        self.calls = 0
        self.delegate = ExecutionPlanner()

    def plan(self, signal):
        self.calls += 1
        return self.delegate.plan(signal)


def _intent(*, intent_id: str = "shared-intent") -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        strategy_id="strategy-live",
        signal=TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.OPEN_LONG,
            quantity=Decimal("0.1"),
            created_time_ms=100,
        ),
        target_exchanges=(ExchangeName.OKX,),
    )


@pytest.mark.asyncio
async def test_two_coordinators_execute_same_intent_only_once(tmp_path) -> None:
    db_path = tmp_path / "journal.sqlite3"
    repositories = (
        SqliteOrderJournalStore(db_path),
        SqliteOrderJournalStore(db_path),
    )
    client = CountingExecutionClient()
    planners = (CountingPlanner(), CountingPlanner())
    coordinators = tuple(
        MultiExchangeOrderCoordinator(
            clients=[client],
            repository=repository,
            planner=planner,
        )
        for repository, planner in zip(repositories, planners)
    )

    outcomes = await asyncio.gather(
        *(coordinator.execute(_intent()) for coordinator in coordinators),
        return_exceptions=True,
    )

    assert sum(isinstance(outcome, list) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, DuplicateIntentError) for outcome in outcomes) == 1
    assert client.place_order_calls == 1
    assert client.place_stop_calls == 0
    assert client.cancel_order_calls == 0
    assert client.cancel_stop_calls == 0
    assert sum(planner.calls for planner in planners) == 1
    assert len(SqliteOrderJournalStore(db_path).list_results(intent_id="shared-intent")) == 1
    assert _event_count(db_path, "intent_claimed") == 1
    assert _event_count(db_path, "intent_saved") == 1


@pytest.mark.asyncio
async def test_restart_duplicate_fails_before_planner_or_client(tmp_path) -> None:
    db_path = tmp_path / "journal.sqlite3"
    first_client = CountingExecutionClient()
    first = MultiExchangeOrderCoordinator(
        clients=[first_client],
        repository=SqliteOrderJournalStore(db_path),
    )
    await first.execute(_intent(intent_id="restart-intent"))

    restarted_client = CountingExecutionClient()
    restarted_planner = CountingPlanner()
    restarted = MultiExchangeOrderCoordinator(
        clients=[restarted_client],
        repository=SqliteOrderJournalStore(db_path),
        planner=restarted_planner,
    )

    with pytest.raises(DuplicateIntentError, match="restart-intent"):
        await restarted.execute(_intent(intent_id="restart-intent"))

    assert restarted_planner.calls == 0
    assert restarted_client.place_order_calls == 0
    assert restarted_client.fetch_rule_calls == 0
    assert len(SqliteOrderJournalStore(db_path).list_results(intent_id="restart-intent")) == 1


@pytest.mark.asyncio
async def test_recovery_topup_duplicate_claim_precedes_rule_fetch_and_mutation(
    tmp_path,
    caplog,
) -> None:
    db_path = tmp_path / "journal.sqlite3"
    repository = SqliteOrderJournalStore(db_path)
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.01"),
        metadata={
            "execution_purpose": "follower_recovery_topup",
            "reference_price": "2000",
            "position_id": "position-1",
            "canary": "canary_signal_metadata",
        },
        created_time_ms=100,
    )
    intent = OrderIntent(
        intent_id="recovery-topup-intent",
        strategy_id="strategy-live",
        signal=signal,
        target_exchanges=(ExchangeName.BINANCE,),
    )
    assert repository.claim_intent(intent) is True
    client = RecoveryTopupClient()
    planner = CountingPlanner()
    position_plan_store = Mock()
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=SqliteOrderJournalStore(db_path),
        planner=planner,
        position_plan_store=position_plan_store,
    )

    with pytest.raises(DuplicateIntentError) as exc_info:
        await coordinator.execute(intent)

    assert str(exc_info.value) == "duplicate order intent: recovery-topup-intent"
    assert "canary_signal_metadata" not in repr(exc_info.value)
    assert "canary_signal_metadata" not in caplog.text
    assert client.fetch_rule_calls == 0
    assert client.place_order_calls == 0
    assert client.place_stop_calls == 0
    assert client.cancel_order_calls == 0
    assert client.cancel_stop_calls == 0
    assert planner.calls == 0
    assert position_plan_store.method_calls == []
    assert repository.list_results(intent_id=intent.intent_id) == []
    assert _event_count(db_path, "intent_claimed") == 1


def _event_count(path, message: str) -> int:
    with sqlite3.connect(path) as connection:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM order_journal_events WHERE message = ?",
                (message,),
            ).fetchone()[0]
        )
