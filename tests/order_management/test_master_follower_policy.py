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


def _policy() -> MasterFollowerExecutionPolicy:
    return MasterFollowerExecutionPolicy(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
    )


def test_policy_keeps_master_when_follower_fails() -> None:
    decision = MasterFollowerPolicyEvaluator(_policy()).evaluate(
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
    decision = MasterFollowerPolicyEvaluator(_policy()).evaluate(
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
    decision = MasterFollowerPolicyEvaluator(_policy()).evaluate(
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
    policy = MasterFollowerExecutionPolicy(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
        follower_entry_retry=RetryPolicy(max_attempts=3, retry_delay_seconds=0),
    )
    coordinator = MultiExchangeOrderCoordinator(clients=[okx, binance], repository=repo, master_follower_policy=policy)

    results = await coordinator.execute(_intent())

    assert okx.attempts == 1
    assert binance.attempts == 3
    assert [r.ok for r in results] == [True, True]
    assert repo.get_intent("mf-intent").status is OrderIntentStatus.SUBMITTED  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_follower_close_after_master_close_retries_at_least_three_times(tmp_path):
    """follower_close_after_master_close signals must retry at least 3 times on failure."""
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    binance = FollowerFailsTwiceClient(ExchangeName.BINANCE, fail_times=2)
    policy = MasterFollowerExecutionPolicy(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
        follower_close_retry=RetryPolicy(max_attempts=3, retry_delay_seconds=0),
    )
    coordinator = MultiExchangeOrderCoordinator(clients=[binance], repository=repo, master_follower_policy=policy)
    close_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
        metadata={"execution_purpose": "follower_close_after_master_close", "target_exchanges": ["binance"]},
    )
    intent = OrderIntent(intent_id="fc-intent", strategy_id="v8", signal=close_signal, target_exchanges=(ExchangeName.BINANCE,))

    results = await coordinator.execute(intent)

    assert binance.attempts == 3
    assert [r.ok for r in results] == [True]
    assert repo.get_intent("fc-intent").status is OrderIntentStatus.SUBMITTED  # type: ignore[union-attr]


# ── Close filled判断 tests ────────────────────────────────────────────────


class _FakeFilledCheckClient:
    """Execution client that returns a configurable order result."""

    def __init__(self, exchange: ExchangeName, *, ok: bool, status: OrderStatus, filled_quantity: Decimal):
        self.exchange = exchange
        self.symbol = "ETH-USDT-PERP"
        self._ok = ok
        self._status = status
        self._filled_qty = filled_quantity

    @property
    def market_profile(self):
        from src.platform import get_market_profile
        return get_market_profile("ETH-USDT-PERP")

    async def place_order(self, request):
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=request.symbol,
            order_id=f"{self.exchange.value}-1",
            client_order_id=request.client_order_id,
            status=self._status,
            side=request.side,
            order_type=OrderType.MARKET,
            quantity=request.quantity,
            filled_quantity=self._filled_qty,
            raw={},
        )

    async def place_stop_market_order(self, request):
        raise NotImplementedError

    async def cancel_all_orders(self):
        return []

    async def cancel_all_stop_orders(self):
        return []


@pytest.mark.asyncio
async def test_close_plan_does_not_mark_follower_closed_when_result_ok_but_not_filled(tmp_path):
    """result.ok=True but status=NEW with filled_quantity=0 must NOT mark leg CLOSED."""
    from src.order_management import (
        LegPlan,
        LegRole,
        LegSyncStatus,
        PositionPlan,
        PositionPlanStatus,
        SqlitePositionPlanStore,
    )

    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    plan_store = SqlitePositionPlanStore(tmp_path / "plan.sqlite3")
    position_id = "p-not-filled-1"

    # Pre-create the position plan with master CLOSED, follower PLANNED.
    plan_store.upsert_position(
        PositionPlan(
            position_id=position_id,
            strategy_id="v8",
            entry_engine="MOMENTUM_V3",
            side="long",
            status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
            canonical_stop_price=None,
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.1"),
            master_filled_qty_base=Decimal("0.1"),
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.1"),
            filled_qty_base=Decimal("0.1"),
            sync_status=LegSyncStatus.CLOSED,
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.1"),
            filled_qty_base=Decimal("0"),
            sync_status=LegSyncStatus.FOLLOWER_CLOSE_FAILED,
        )
    )

    # Follower close result: ok=True, but status=NEW, filled_quantity=0
    binance = _FakeFilledCheckClient(
        ExchangeName.BINANCE, ok=True, status=OrderStatus.NEW, filled_quantity=Decimal("0")
    )
    policy = MasterFollowerExecutionPolicy(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
        follower_close_retry=RetryPolicy(max_attempts=1, retry_delay_seconds=0),
    )
    coordinator = MultiExchangeOrderCoordinator(
        clients=[binance], repository=repo, master_follower_policy=policy, position_plan_store=plan_store,
    )
    close_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
        metadata={
            "execution_purpose": "follower_close_after_master_close",
            "target_exchanges": ["binance"],
            "position_id": position_id,
        },
    )
    intent = OrderIntent(
        intent_id="fc-not-filled", strategy_id="v8", signal=close_signal,
        target_exchanges=(ExchangeName.BINANCE,),
    )

    await coordinator.execute(intent)

    # Leg must NOT be CLOSED — result.ok=True but not FILLED.
    legs = {leg.exchange: leg for leg in plan_store.get_legs(position_id)}
    follower_leg = legs[ExchangeName.BINANCE]
    assert follower_leg.sync_status == LegSyncStatus.FOLLOWER_CLOSE_FAILED
    assert follower_leg.sync_status != LegSyncStatus.CLOSED
    # Plan must remain unresolved.
    plan = plan_store.get_position(position_id)
    assert plan is not None
    assert plan.status == PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED


@pytest.mark.asyncio
async def test_close_plan_marks_follower_closed_only_when_filled(tmp_path):
    """result.ok=True, status=FILLED, filled_quantity>0 → leg CLOSED, plan CLOSED."""
    from src.order_management import (
        LegPlan,
        LegRole,
        LegSyncStatus,
        PositionPlan,
        PositionPlanStatus,
        SqlitePositionPlanStore,
    )

    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    plan_store = SqlitePositionPlanStore(tmp_path / "plan.sqlite3")
    position_id = "p-filled-1"

    plan_store.upsert_position(
        PositionPlan(
            position_id=position_id,
            strategy_id="v8",
            entry_engine="MOMENTUM_V3",
            side="long",
            status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
            canonical_stop_price=None,
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.1"),
            master_filled_qty_base=Decimal("0.1"),
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.1"),
            filled_qty_base=Decimal("0.1"),
            sync_status=LegSyncStatus.CLOSED,
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.1"),
            filled_qty_base=Decimal("0"),
            sync_status=LegSyncStatus.FOLLOWER_CLOSE_FAILED,
        )
    )

    # Follower close result: ok=True, status=FILLED, filled_quantity>0
    binance = _FakeFilledCheckClient(
        ExchangeName.BINANCE, ok=True, status=OrderStatus.FILLED, filled_quantity=Decimal("0.1")
    )
    policy = MasterFollowerExecutionPolicy(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
        follower_close_retry=RetryPolicy(max_attempts=1, retry_delay_seconds=0),
    )
    coordinator = MultiExchangeOrderCoordinator(
        clients=[binance], repository=repo, master_follower_policy=policy, position_plan_store=plan_store,
    )
    close_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
        metadata={
            "execution_purpose": "follower_close_after_master_close",
            "target_exchanges": ["binance"],
            "position_id": position_id,
        },
    )
    intent = OrderIntent(
        intent_id="fc-filled", strategy_id="v8", signal=close_signal,
        target_exchanges=(ExchangeName.BINANCE,),
    )

    await coordinator.execute(intent)

    # Leg must be CLOSED.
    legs = {leg.exchange: leg for leg in plan_store.get_legs(position_id)}
    follower_leg = legs[ExchangeName.BINANCE]
    assert follower_leg.sync_status == LegSyncStatus.CLOSED
    # Master + all followers closed → plan CLOSED.
    plan = plan_store.get_position(position_id)
    assert plan is not None
    assert plan.status == PositionPlanStatus.CLOSED


# ── normal_close master-not-filled guard tests ──────────────────────────────


@pytest.mark.asyncio
async def test_normal_close_does_not_mark_master_closed_required_when_master_result_not_filled(tmp_path):
    """When normal_close master result is NOT FILLED, the position plan must NOT
    enter MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED, master leg must NOT be marked
    CLOSED, and follower leg must NOT be marked FOLLOWER_CLOSE_FAILED."""
    from src.order_management import (
        LegPlan,
        LegRole,
        LegSyncStatus,
        PositionPlan,
        PositionPlanStatus,
        SqlitePositionPlanStore,
    )

    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    plan_store = SqlitePositionPlanStore(tmp_path / "plan.sqlite3")
    position_id = "p-nc-master-not-filled-1"

    # Pre-create ACTIVE position plan with both legs OPEN.
    plan_store.upsert_position(
        PositionPlan(
            position_id=position_id,
            strategy_id="v8",
            entry_engine="MOMENTUM_V3",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=None,
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.1"),
            master_filled_qty_base=Decimal("0.1"),
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.1"),
            filled_qty_base=Decimal("0.1"),
            sync_status=LegSyncStatus.OPEN,
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.08"),
            filled_qty_base=Decimal("0.08"),
            sync_status=LegSyncStatus.OPEN,
        )
    )

    # Master close result: ok=True but status=NEW, filled_quantity=0
    okx = _FakeFilledCheckClient(
        ExchangeName.OKX, ok=True, status=OrderStatus.NEW, filled_quantity=Decimal("0")
    )
    # Follower result: missing or skipped
    binance = _FakeFilledCheckClient(
        ExchangeName.BINANCE, ok=False, status=OrderStatus.NEW, filled_quantity=Decimal("0")
    )
    policy = MasterFollowerExecutionPolicy(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
    )
    coordinator = MultiExchangeOrderCoordinator(
        clients=[okx, binance], repository=repo, master_follower_policy=policy,
        position_plan_store=plan_store,
    )
    close_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
        metadata={
            "execution_purpose": "normal_close",
            "target_exchanges": ["okx", "binance"],
            "position_id": position_id,
        },
    )
    intent = OrderIntent(
        intent_id="nc-not-filled", strategy_id="v8", signal=close_signal,
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )

    await coordinator.execute(intent)

    # Master leg must NOT be CLOSED.
    legs = {leg.exchange: leg for leg in plan_store.get_legs(position_id)}
    master_leg = legs[ExchangeName.OKX]
    assert master_leg.sync_status != LegSyncStatus.CLOSED
    # Follower leg must NOT be FOLLOWER_CLOSE_FAILED.
    follower_leg = legs[ExchangeName.BINANCE]
    assert follower_leg.sync_status != LegSyncStatus.FOLLOWER_CLOSE_FAILED
    # Position plan must NOT be MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED.
    plan = plan_store.get_position(position_id)
    assert plan is not None
    assert plan.status != PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED
    assert plan.status == PositionPlanStatus.ACTIVE


@pytest.mark.asyncio
async def test_normal_close_master_filled_follower_failed_enters_master_closed_follower_required(tmp_path):
    """When normal_close master result IS FILLED but follower is NOT filled,
    master leg is marked CLOSED, follower leg FOLLOWER_CLOSE_FAILED, and
    plan enters MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED."""
    from src.order_management import (
        LegPlan,
        LegRole,
        LegSyncStatus,
        PositionPlan,
        PositionPlanStatus,
        SqlitePositionPlanStore,
    )

    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    plan_store = SqlitePositionPlanStore(tmp_path / "plan.sqlite3")
    position_id = "p-nc-master-filled-follower-failed-1"

    # Pre-create ACTIVE position plan with both legs OPEN.
    plan_store.upsert_position(
        PositionPlan(
            position_id=position_id,
            strategy_id="v8",
            entry_engine="MOMENTUM_V3",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=None,
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.1"),
            master_filled_qty_base=Decimal("0.1"),
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.1"),
            filled_qty_base=Decimal("0.1"),
            sync_status=LegSyncStatus.OPEN,
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.08"),
            filled_qty_base=Decimal("0.08"),
            sync_status=LegSyncStatus.OPEN,
        )
    )

    # Master close result: ok=True, status=FILLED, filled_quantity>0
    okx = _FakeFilledCheckClient(
        ExchangeName.OKX, ok=True, status=OrderStatus.FILLED, filled_quantity=Decimal("0.1")
    )
    # Follower close result: ok=False, status=NEW
    binance = _FakeFilledCheckClient(
        ExchangeName.BINANCE, ok=False, status=OrderStatus.NEW, filled_quantity=Decimal("0")
    )
    policy = MasterFollowerExecutionPolicy(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
    )
    coordinator = MultiExchangeOrderCoordinator(
        clients=[okx, binance], repository=repo, master_follower_policy=policy,
        position_plan_store=plan_store,
    )
    close_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
        metadata={
            "execution_purpose": "normal_close",
            "target_exchanges": ["okx", "binance"],
            "position_id": position_id,
        },
    )
    intent = OrderIntent(
        intent_id="nc-master-filled", strategy_id="v8", signal=close_signal,
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )

    await coordinator.execute(intent)

    # Master leg must be CLOSED.
    legs = {leg.exchange: leg for leg in plan_store.get_legs(position_id)}
    master_leg = legs[ExchangeName.OKX]
    assert master_leg.sync_status == LegSyncStatus.CLOSED
    # Follower leg must be FOLLOWER_CLOSE_FAILED.
    follower_leg = legs[ExchangeName.BINANCE]
    assert follower_leg.sync_status == LegSyncStatus.FOLLOWER_CLOSE_FAILED
    # Position plan must be MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED.
    plan = plan_store.get_position(position_id)
    assert plan is not None
    assert plan.status == PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED


@pytest.mark.asyncio
async def test_normal_close_master_result_missing_does_not_escalate(tmp_path):
    """When normal_close has NO master result at all (missing), the position
    plan must stay ACTIVE and no follower should be marked CLOSE_FAILED."""
    from src.order_management import (
        LegPlan,
        LegRole,
        LegSyncStatus,
        PositionPlan,
        PositionPlanStatus,
        SqlitePositionPlanStore,
    )

    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    plan_store = SqlitePositionPlanStore(tmp_path / "plan.sqlite3")
    position_id = "p-nc-master-missing-1"

    plan_store.upsert_position(
        PositionPlan(
            position_id=position_id,
            strategy_id="v8",
            entry_engine="MOMENTUM_V3",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=None,
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.1"),
            master_filled_qty_base=Decimal("0.1"),
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.1"),
            filled_qty_base=Decimal("0.1"),
            sync_status=LegSyncStatus.OPEN,
        )
    )
    plan_store.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.08"),
            filled_qty_base=Decimal("0.08"),
            sync_status=LegSyncStatus.OPEN,
        )
    )

    # Only a follower client is available; master result is entirely missing.
    binance = _FakeFilledCheckClient(
        ExchangeName.BINANCE, ok=True, status=OrderStatus.FILLED, filled_quantity=Decimal("0.08")
    )
    policy = MasterFollowerExecutionPolicy(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
    )
    coordinator = MultiExchangeOrderCoordinator(
        clients=[binance], repository=repo, master_follower_policy=policy,
        position_plan_store=plan_store,
    )
    close_signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CLOSE_LONG,
        quantity=Decimal("0.1"),
        metadata={
            "execution_purpose": "normal_close",
            "target_exchanges": ["okx", "binance"],
            "position_id": position_id,
        },
    )
    intent = OrderIntent(
        intent_id="nc-master-missing", strategy_id="v8", signal=close_signal,
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )

    await coordinator.execute(intent)

    legs = {leg.exchange: leg for leg in plan_store.get_legs(position_id)}
    # Master must remain OPEN (no result → not CLOSED).
    assert legs[ExchangeName.OKX].sync_status == LegSyncStatus.OPEN
    # Follower must NOT be FOLLOWER_CLOSE_FAILED.
    assert legs[ExchangeName.BINANCE].sync_status != LegSyncStatus.FOLLOWER_CLOSE_FAILED
    # Plan must stay ACTIVE.
    plan = plan_store.get_position(position_id)
    assert plan is not None
    assert plan.status == PositionPlanStatus.ACTIVE
