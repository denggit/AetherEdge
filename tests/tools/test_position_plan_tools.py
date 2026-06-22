"""Tests for position plan store tool operations.

Covers:
- clear_leg_order_ids() bypasses COALESCE
- list_active_positions() correctly filters
- STALE_RECONCILED integration
"""

from __future__ import annotations

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
from src.platform.exchanges.models import ExchangeName


@pytest.fixture
def store():
    path = Path(tempfile.mkdtemp()) / "test_plans.sqlite3"
    return SqlitePositionPlanStore(str(path))


def test_clear_leg_entry_order_ids(store):
    """clear_leg_order_ids() bypasses COALESCE to set NULL."""
    plan = PositionPlan(
        position_id="test-1",
        strategy_id="test",
        entry_engine="test",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.1"),
    )
    store.upsert_position(plan)

    leg = LegPlan(
        position_id="test-1",
        exchange=ExchangeName.OKX,
        role=LegRole.MASTER,
        target_qty_base=Decimal("0.1"),
        entry_order_id="1234567890",
        entry_client_order_id="AEOKOLabc",
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(leg)

    # Verify it was saved
    legs = store.get_legs("test-1")
    assert legs[0].entry_order_id == "1234567890"

    # Clear it
    store.clear_leg_order_ids(
        position_id="test-1",
        exchange=ExchangeName.OKX,
        clear_entry_order_id=True,
    )

    # Verify cleared
    legs = store.get_legs("test-1")
    assert legs[0].entry_order_id is None
    assert legs[0].entry_client_order_id is None


def test_clear_leg_stop_order_ids(store):
    """clear_leg_order_ids() clears stop order IDs."""
    plan = PositionPlan(
        position_id="test-2",
        strategy_id="test",
        entry_engine="test",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.BINANCE,
        master_target_qty_base=Decimal("0.1"),
    )
    store.upsert_position(plan)

    leg = LegPlan(
        position_id="test-2",
        exchange=ExchangeName.BINANCE,
        role=LegRole.FOLLOWER,
        target_qty_base=Decimal("0.1"),
        stop_order_id="999888777",
        stop_client_order_id="AEBNSPxyz",
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(leg)

    # Verify saved
    legs = store.get_legs("test-2")
    assert legs[0].stop_order_id == "999888777"

    # Clear stop
    store.clear_leg_order_ids(
        position_id="test-2",
        exchange=ExchangeName.BINANCE,
        clear_stop_order_id=True,
    )

    # Verify cleared
    legs = store.get_legs("test-2")
    assert legs[0].stop_order_id is None
    assert legs[0].stop_client_order_id is None


def test_clear_leg_order_ids_nonexistent_no_error(store):
    """Clearing IDs on a non-existent leg should not raise."""
    store.clear_leg_order_ids(
        position_id="nonexistent",
        exchange=ExchangeName.OKX,
        clear_entry_order_id=True,
    )


def test_upsert_leg_coalesce_does_not_overwrite_with_none(store):
    """COALESCE in upsert_leg prevents None from clearing — verify the issue."""
    plan = PositionPlan(
        position_id="test-3",
        strategy_id="test",
        entry_engine="test",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.1"),
    )
    store.upsert_position(plan)

    # Insert with order ID
    leg = LegPlan(
        position_id="test-3",
        exchange=ExchangeName.OKX,
        role=LegRole.MASTER,
        target_qty_base=Decimal("0.1"),
        entry_order_id="1234567890",
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(leg)

    # Try to "clear" via upsert with None (COALESCE should preserve old value)
    from dataclasses import replace
    leg_none = replace(leg, entry_order_id=None)
    store.upsert_leg(leg_none)

    # Verify the old value is still there (COALESCE behavior)
    legs = store.get_legs("test-3")
    assert legs[0].entry_order_id == "1234567890", (
        "COALESCE should preserve the old value when upserting None"
    )

    # Now use the dedicated clear method
    store.clear_leg_order_ids(
        position_id="test-3",
        exchange=ExchangeName.OKX,
        clear_entry_order_id=True,
    )

    legs = store.get_legs("test-3")
    assert legs[0].entry_order_id is None, (
        "clear_leg_order_ids must bypass COALESCE and set NULL"
    )


def test_list_active_positions_excludes_closed_and_stale_closed(store):
    """Active position list should exclude CLOSED plans after reconciliation."""
    # Create an active plan
    plan = PositionPlan(
        position_id="p1",
        strategy_id="test",
        entry_engine="test",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.1"),
    )
    store.upsert_position(plan)

    # Should be in active list
    active = store.list_active_positions()
    assert len(active) == 1

    # Close it
    store.upsert_position(replace(plan, status=PositionPlanStatus.CLOSED))

    # Should NOT be in active list
    active = store.list_active_positions()
    assert len(active) == 0


def test_stale_reconciled_status_available(store):
    """STALE_RECONCILED is a valid LegSyncStatus value."""
    plan = PositionPlan(
        position_id="p2",
        strategy_id="test",
        entry_engine="test",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.1"),
    )
    store.upsert_position(plan)

    leg = LegPlan(
        position_id="p2",
        exchange=ExchangeName.OKX,
        role=LegRole.MASTER,
        target_qty_base=Decimal("0.1"),
        sync_status=LegSyncStatus.STALE_RECONCILED,
    )
    store.upsert_leg(leg)

    legs = store.get_legs("p2")
    assert legs[0].sync_status == LegSyncStatus.STALE_RECONCILED


def test_follower_close_failed_still_active(store):
    """FOLLOWER_CLOSE_FAILED plan should still be returned by list_active_positions."""
    plan = PositionPlan(
        position_id="p3",
        strategy_id="test",
        entry_engine="test",
        side="long",
        status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
        canonical_stop_price=Decimal("0"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.1"),
    )
    store.upsert_position(plan)

    active = store.list_active_positions()
    matching = [p for p in active if p.position_id == "p3"]
    assert len(matching) == 1, (
        "MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED should be in active list"
    )
