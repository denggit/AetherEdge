from __future__ import annotations

from decimal import Decimal

import pytest

from src.order_management import (
    ExchangeOrderResult,
    MasterFollowerDecisionStatus,
    MasterFollowerExecutionPolicy,
    MasterFollowerPolicyEvaluator,
    MultiExchangeOrderCoordinator,
    OrderIntent,
    OrderIntentStatus,
    RetryPolicy,
    SqliteOrderJournalStore,
)
from src.platform import ExchangeName, Order, OrderStatus
from src.platform.exchanges.models import OrderSide, OrderType
from src.signals import SignalAction, TradeSignal


def _intent() -> OrderIntent:
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.5"), created_time_ms=100)
    return OrderIntent(intent_id="mf-intent", strategy_id="v8", signal=signal, target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE))


def test_policy_keeps_master_when_follower_fails() -> None:
    decision = MasterFollowerPolicyEvaluator().evaluate(
        intent=_intent(),
        results=(
            ExchangeOrderResult(exchange=ExchangeName.OKX, ok=True, avg_fill_price=Decimal("2500")),
            ExchangeOrderResult(exchange=ExchangeName.BINANCE, ok=False, error="binance failed"),
        ),
    )

    assert decision.status is MasterFollowerDecisionStatus.FOLLOWER_FAILED_SKIPPED
    assert "follower_entry_failed_skipped" in decision.alerts
    assert "retry_then_skip_failed_followers" in decision.actions
    assert "close_master" not in decision.actions


def test_policy_orphan_follower_when_master_fails() -> None:
    decision = MasterFollowerPolicyEvaluator().evaluate(
        intent=_intent(),
        results=(
            ExchangeOrderResult(exchange=ExchangeName.OKX, ok=False, error="okx failed"),
            ExchangeOrderResult(exchange=ExchangeName.BINANCE, ok=True, avg_fill_price=Decimal("2500")),
        ),
    )

    assert decision.status is MasterFollowerDecisionStatus.ORPHAN_FOLLOWER_REQUIRES_MANUAL
    assert "master_failed_with_follower_position" in decision.alerts
    assert "wait_1800s_before_orphan_close" in decision.actions
    assert "close_orphan_follower_after_grace" in decision.actions


def test_policy_price_deviation_alert_only() -> None:
    decision = MasterFollowerPolicyEvaluator().evaluate(
        intent=_intent(),
        results=(
            ExchangeOrderResult(exchange=ExchangeName.OKX, ok=True, avg_fill_price=Decimal("2500")),
            ExchangeOrderResult(exchange=ExchangeName.BINANCE, ok=True, avg_fill_price=Decimal("2513")),
        ),
    )

    assert decision.status is MasterFollowerDecisionStatus.PRICE_DEVIATION_ALERT
    assert "entry_price_deviation_alert" in decision.alerts
    assert "alert_price_deviation_only" in decision.actions
    assert decision.metadata["entry_deviation"][0]["auto_fix"] is False


class FollowerFailsTwiceClient:
    def __init__(self, exchange: ExchangeName, *, fail_times: int = 0) -> None:
        self.exchange = exchange
        self.symbol = "ETH-USDT-PERP"
        self.fail_times = fail_times
        self.attempts = 0

    @property
    def market_profile(self):  # pragma: no cover
        raise NotImplementedError

    async def place_order(self, request):
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise RuntimeError(f"{self.exchange.value} failed")
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=request.symbol,
            order_id=f"{self.exchange.value}-1",
            client_order_id=request.client_order_id,
            status=OrderStatus.FILLED,
            side=request.side,
            order_type=OrderType.MARKET,
            quantity=request.quantity,
            filled_quantity=request.quantity,
            raw={"avgPx": "2500" if self.exchange is ExchangeName.OKX else "2501"},
        )

    async def place_stop_market_order(self, request):  # pragma: no cover
        raise NotImplementedError

    async def cancel_all_orders(self):  # pragma: no cover
        return []

    async def cancel_all_stop_orders(self):  # pragma: no cover
        return []


@pytest.mark.asyncio
async def test_master_follower_coordinator_retries_follower_without_closing_master(tmp_path):
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    okx = FollowerFailsTwiceClient(ExchangeName.OKX)
    binance = FollowerFailsTwiceClient(ExchangeName.BINANCE, fail_times=2)
    policy = MasterFollowerExecutionPolicy(follower_entry_retry=RetryPolicy(max_attempts=3, retry_delay_seconds=0))
    coordinator = MultiExchangeOrderCoordinator(clients=[okx, binance], repository=repo, master_follower_policy=policy)

    results = await coordinator.execute(_intent())

    assert okx.attempts == 1
    assert binance.attempts == 3
    assert [r.ok for r in results] == [True, True]
    assert repo.get_intent("mf-intent").status is OrderIntentStatus.SUBMITTED  # type: ignore[union-attr]
