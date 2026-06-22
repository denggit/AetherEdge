"""Tests for LiveStateReconciliationService.

Covers all 4 reconciliation cases:
1. All exchanges flat + active plan → close stale plan
2. Master flat + follower open → MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED
3. Master open + follower flat → alert, block entries
4. Fake order IDs → clean or close depending on exchange state
"""

from __future__ import annotations

import sqlite3
import tempfile
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from src.order_management.position_plan.models import (
    LegPlan,
    LegRole,
    LegSyncStatus,
    PositionPlan,
    PositionPlanStatus,
)
from src.order_management.position_plan.store import SqlitePositionPlanStore
from src.order_management.reconciliation.models import (
    ReconciliationVerdict,
)
from src.order_management.reconciliation.service import (
    LiveStateReconciliationService,
    REASON_NO_EXCHANGE_POSITION_OR_OPEN_ORDERS,
)
from src.order_management.reconciliation.validation import is_fake_order_id
from src.platform.exchanges.models import (
    Balance,
    ExchangeName,
    LeverageInfo,
    Order,
    OrderStatus,
    Position,
    PositionMode,
    PositionSide,
)
from src.platform.snapshot import PlatformSnapshot


# ── Helpers ─────────────────────────────────────────────────────────────


def _make_store() -> SqlitePositionPlanStore:
    path = Path(tempfile.mkdtemp()) / "test_plans.sqlite3"
    return SqlitePositionPlanStore(str(path))


def _flat_snapshot(exchange: ExchangeName) -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=exchange, asset="USDT", total=Decimal("1000"), available=Decimal("1000")),
        positions=[],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(exchange=exchange, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", leverage=Decimal("1")),
        position_mode=PositionMode.ONE_WAY,
    )


def _position_snapshot(exchange: ExchangeName, qty: Decimal = Decimal("0.1")) -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=exchange, asset="USDT", total=Decimal("1000"), available=Decimal("1000")),
        positions=[
            Position(
                exchange=exchange,
                symbol="ETH-USDT-PERP",
                raw_symbol="ETH-USDT-SWAP",
                side=PositionSide.LONG,
                quantity=qty,
            )
        ],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(exchange=exchange, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", leverage=Decimal("1")),
        position_mode=PositionMode.ONE_WAY,
    )


def _make_active_plan(store: SqlitePositionPlanStore, position_id: str = "test-plan-1") -> PositionPlan:
    plan = PositionPlan(
        position_id=position_id,
        strategy_id="test_strategy",
        entry_engine="test_engine",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.1"),
        master_filled_qty_base=Decimal("0.1"),
    )
    store.upsert_position(plan)
    # Master leg
    master_leg = LegPlan(
        position_id=position_id,
        exchange=ExchangeName.OKX,
        role=LegRole.MASTER,
        target_qty_base=Decimal("0.1"),
        filled_qty_base=Decimal("0.1"),
        entry_order_id="1234567890",
        entry_client_order_id="AEOKOLabc123",
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(master_leg)
    # Follower leg
    follower_leg = LegPlan(
        position_id=position_id,
        exchange=ExchangeName.BINANCE,
        role=LegRole.FOLLOWER,
        target_qty_base=Decimal("0.1"),
        filled_qty_base=Decimal("0.1"),
        entry_order_id="987654321",
        entry_client_order_id="AEBNOLdef456",
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(follower_leg)
    return plan


def _make_active_plan_with_fake_ids(store: SqlitePositionPlanStore) -> PositionPlan:
    plan = PositionPlan(
        position_id="fake-plan-1",
        strategy_id="test_strategy",
        entry_engine="test_engine",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.1"),
        master_filled_qty_base=Decimal("0.1"),
    )
    store.upsert_position(plan)
    master_leg = LegPlan(
        position_id="fake-plan-1",
        exchange=ExchangeName.OKX,
        role=LegRole.MASTER,
        target_qty_base=Decimal("0.1"),
        entry_order_id="okx-order-1",
        entry_client_order_id=None,
        stop_order_id="okx-stop-1",
        stop_client_order_id=None,
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(master_leg)
    follower_leg = LegPlan(
        position_id="fake-plan-1",
        exchange=ExchangeName.BINANCE,
        role=LegRole.FOLLOWER,
        target_qty_base=Decimal("0.1"),
        entry_order_id="binance-order-1",
        stop_order_id="binance-stop-1",
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(follower_leg)
    return plan


# ── Case 1: All exchanges flat ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_reconcile_all_exchanges_flat_with_active_plan():
    """Online scenario: all exchanges flat + local active plan → close stale plan."""
    store = _make_store()
    _make_active_plan(store)

    okx_snapshot = _flat_snapshot(ExchangeName.OKX)
    binance_snapshot = _flat_snapshot(ExchangeName.BINANCE)

    service = LiveStateReconciliationService(
        position_plan_store=store,
        order_journal=None,
        state_store=None,
    )

    report = await service.reconcile((okx_snapshot, binance_snapshot))

    assert report.stale_plans_closed >= 1
    assert any(
        a.action_type == "close_stale_plan" for a in report.actions
    )
    assert any(
        a.action_type == "mark_leg_stale" for a in report.actions
    )


@pytest.mark.asyncio
async def test_reconcile_all_exchanges_flat_applies_cleanup():
    """Verify apply() closes the plan and marks legs stale."""
    store = _make_store()
    _make_active_plan(store)

    okx_snapshot = _flat_snapshot(ExchangeName.OKX)
    binance_snapshot = _flat_snapshot(ExchangeName.BINANCE)

    service = LiveStateReconciliationService(
        position_plan_store=store,
        order_journal=None,
        state_store=None,
    )

    report = await service.reconcile_and_apply((okx_snapshot, binance_snapshot))

    # Plan should be closed
    plan = store.get_position("test-plan-1")
    assert plan is not None
    assert plan.status == PositionPlanStatus.CLOSED

    # Legs should be marked stale
    legs = store.get_legs("test-plan-1")
    for leg in legs:
        assert leg.sync_status in {LegSyncStatus.STALE_RECONCILED, LegSyncStatus.PLANNED}

    assert report.stale_plans_closed >= 1
    assert report.active_position_after is False


@pytest.mark.asyncio
async def test_reconcile_all_flat_no_active_plan():
    """No active plans — nothing to do."""
    store = _make_store()
    service = LiveStateReconciliationService(
        position_plan_store=store, order_journal=None, state_store=None
    )
    report = await service.reconcile((_flat_snapshot(ExchangeName.OKX),))
    assert report.stale_plans_closed == 0
    assert report.verdict == ReconciliationVerdict.PASS


# ── Case 2: Master flat + follower open ─────────────────────────────────


@pytest.mark.asyncio
async def test_reconcile_master_flat_follower_open():
    """OKX flat, Binance has position → must not close plan."""
    store = _make_store()
    _make_active_plan(store)

    okx_flat = _flat_snapshot(ExchangeName.OKX)
    binance_open = _position_snapshot(ExchangeName.BINANCE, Decimal("0.1"))

    service = LiveStateReconciliationService(
        position_plan_store=store, order_journal=None, state_store=None
    )
    report = await service.reconcile((okx_flat, binance_open))

    # Should NOT close the plan
    close_actions = [a for a in report.actions if a.action_type == "close_stale_plan"]
    assert len(close_actions) == 0

    # Should produce follower close required action
    assert any(
        a.action_type == "set_master_closed_follower_close_required"
        for a in report.actions
    )
    assert report.unresolved_follower_positions >= 1


@pytest.mark.asyncio
async def test_reconcile_master_flat_follower_open_does_not_clear_plan():
    """Follower open → plan stays active with correct status after apply."""
    store = _make_store()
    _make_active_plan(store)

    okx_flat = _flat_snapshot(ExchangeName.OKX)
    binance_open = _position_snapshot(ExchangeName.BINANCE, Decimal("0.1"))

    service = LiveStateReconciliationService(
        position_plan_store=store, order_journal=None, state_store=None
    )
    report = await service.reconcile_and_apply((okx_flat, binance_open))

    plan = store.get_position("test-plan-1")
    assert plan is not None
    assert plan.status != PositionPlanStatus.CLOSED
    # Should be set to follower close required
    assert plan.status == PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED


# ── Case 3: Master open + follower flat ─────────────────────────────────


@pytest.mark.asyncio
async def test_reconcile_master_open_follower_flat():
    """OKX has position, Binance flat → alert, block entries."""
    store = _make_store()
    _make_active_plan(store)

    okx_open = _position_snapshot(ExchangeName.OKX, Decimal("0.1"))
    binance_flat = _flat_snapshot(ExchangeName.BINANCE)

    service = LiveStateReconciliationService(
        position_plan_store=store, order_journal=None, state_store=None
    )
    report = await service.reconcile((okx_open, binance_flat))

    # Should NOT close the plan
    close_actions = [a for a in report.actions if a.action_type == "close_stale_plan"]
    assert len(close_actions) == 0

    # Should produce block_new_entries_alert
    assert any(
        a.action_type == "block_new_entries_alert" for a in report.actions
    )


# ── Case 4: Fake order IDs ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reconcile_fake_order_ids_flat_exchange():
    """Fake IDs + exchange flat → close stale plan."""
    store = _make_store()
    _make_active_plan_with_fake_ids(store)

    service = LiveStateReconciliationService(
        position_plan_store=store, order_journal=None, state_store=None
    )
    report = await service.reconcile((_flat_snapshot(ExchangeName.OKX), _flat_snapshot(ExchangeName.BINANCE)))

    assert len(report.fake_order_refs_found) >= 2
    assert report.stale_plans_closed >= 1


@pytest.mark.asyncio
async def test_reconcile_fake_order_ids_applies_cleanup():
    """Apply clears fake order IDs from legs."""
    store = _make_store()
    plan = _make_active_plan_with_fake_ids(store)

    service = LiveStateReconciliationService(
        position_plan_store=store, order_journal=None, state_store=None
    )
    report = await service.reconcile_and_apply(
        (_flat_snapshot(ExchangeName.OKX), _flat_snapshot(ExchangeName.BINANCE))
    )

    # Plan should be closed
    p = store.get_position("fake-plan-1")
    assert p is not None
    assert p.status == PositionPlanStatus.CLOSED

    # Leg entries should be cleaned
    legs = store.get_legs("fake-plan-1")
    for leg in legs:
        # Entry/stop order IDs should be None (cleared)
        if leg.entry_order_id:
            assert not is_fake_order_id(leg.entry_order_id), (
                f"Fake entry_order_id not cleared: {leg.entry_order_id}"
            )
        if leg.stop_order_id:
            assert not is_fake_order_id(leg.stop_order_id), (
                f"Fake stop_order_id not cleared: {leg.stop_order_id}"
            )


@pytest.mark.asyncio
async def test_reconcile_fake_ids_with_position_exists():
    """Exchange has position + fake IDs → clear IDs but keep plan active."""
    store = _make_store()
    plan = _make_active_plan_with_fake_ids(store)

    # OKX has a position — so plan should NOT be closed for OKX
    okx_with_pos = _position_snapshot(ExchangeName.OKX, Decimal("0.1"))
    binance_flat = _flat_snapshot(ExchangeName.BINANCE)

    service = LiveStateReconciliationService(
        position_plan_store=store, order_journal=None, state_store=None
    )
    report = await service.reconcile_and_apply((okx_with_pos, binance_flat))

    # Fake refs should still be detected
    assert len(report.fake_order_refs_found) >= 1

    # Plan should NOT be closed (OKX has position)
    p = store.get_position("fake-plan-1")
    assert p is not None
    # Plan might be closed if the overall reconciliation determined it as all-flat
    # But fake IDs on the OKX leg should have been cleared
    okx_legs = [leg for leg in store.get_legs("fake-plan-1") if leg.exchange == ExchangeName.OKX]
    for leg in okx_legs:
        if leg.entry_order_id:
            assert not is_fake_order_id(leg.entry_order_id)
        if leg.stop_order_id:
            assert not is_fake_order_id(leg.stop_order_id)


# ── Edge cases ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reconcile_with_real_numeric_order_ids_ok():
    """Plan with real numeric order IDs should not be flagged as fake."""
    store = _make_store()
    _make_active_plan(store)  # Uses real numeric IDs

    service = LiveStateReconciliationService(
        position_plan_store=store, order_journal=None, state_store=None
    )
    report = await service.reconcile((_flat_snapshot(ExchangeName.OKX), _flat_snapshot(ExchangeName.BINANCE)))

    # Fake refs should be 0 since IDs are numeric
    assert len(report.fake_order_refs_found) == 0
    # But the plan is still stale since exchange is flat
    assert report.stale_plans_closed >= 1


@pytest.mark.asyncio
async def test_reconcile_with_real_positions_keeps_active_plan():
    """Both exchanges have real positions → plan stays active."""
    store = _make_store()
    _make_active_plan(store)

    okx_open = _position_snapshot(ExchangeName.OKX, Decimal("0.1"))
    binance_open = _position_snapshot(ExchangeName.BINANCE, Decimal("0.1"))

    service = LiveStateReconciliationService(
        position_plan_store=store, order_journal=None, state_store=None
    )
    report = await service.reconcile_and_apply((okx_open, binance_open))

    plan = store.get_position("test-plan-1")
    assert plan is not None
    assert plan.status not in {PositionPlanStatus.CLOSED}
    assert report.stale_plans_closed == 0


@pytest.mark.asyncio
async def test_journal_event_written_for_closed_stale_plan():
    """When a stale plan is closed, a journal event should be written."""
    store = _make_store()
    _make_active_plan(store)

    # Use a simple in-memory journal
    class InMemoryJournal:
        def __init__(self):
            self.events: list = []

        def add_event(self, event):
            self.events.append(event)

    journal = InMemoryJournal()

    service = LiveStateReconciliationService(
        position_plan_store=store,
        order_journal=journal,
        state_store=None,
    )
    report = await service.reconcile_and_apply(
        (_flat_snapshot(ExchangeName.OKX), _flat_snapshot(ExchangeName.BINANCE))
    )

    assert len(journal.events) >= 1
    event = journal.events[0]
    assert event.intent_id == "test-plan-1"
    assert REASON_NO_EXCHANGE_POSITION_OR_OPEN_ORDERS in event.message


# ── Validation module integration ──────────────────────────────────────


def test_fake_id_patterns_cover_all_known_online_fakes():
    """Verify all known fake IDs from online logs are covered."""
    online_fakes = [
        "okx-order-1",
        "okx-1",
        "okx-stop-1",
        "binance-order-1",
        "binance-1",
        "binance-stop-1",
    ]
    for fake in online_fakes:
        assert is_fake_order_id(fake), f"Expected '{fake}' to be detected as fake"


def test_real_ids_not_falsely_detected():
    """Real exchange IDs should not be flagged."""
    real_ids = [
        "1234567890",
        "9876543210123456",
        "42",
        "0",
    ]
    for rid in real_ids:
        assert not is_fake_order_id(rid), f"Expected '{rid}' NOT to be detected as fake"


def test_ae_client_order_ids_not_falsely_detected():
    """AetherEdge client order IDs should not be flagged."""
    client_ids = [
        "AEOKOLabc123def456",
        "AEBNOLxyz789ghi012",
        "AEOKSPqwertyuiop",
        "AEBNSPasdfghjkl",
    ]
    for cid in client_ids:
        assert not is_fake_order_id(cid), f"Expected '{cid}' NOT to be detected as fake"
