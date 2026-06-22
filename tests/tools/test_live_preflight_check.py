"""Tests for live_preflight_check.py tool functionality."""

from __future__ import annotations

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
from src.order_management.reconciliation.validation import is_fake_order_id
from src.platform.exchanges.models import ExchangeName


def test_preflight_detects_fake_order_ids_in_position_plan():
    """Verify a PositionPlan with fake order IDs is detected."""
    import tempfile
    store_path = Path(tempfile.mkdtemp()) / "test_plans.sqlite3"
    store = SqlitePositionPlanStore(str(store_path))

    plan = PositionPlan(
        position_id="test-fake-plan",
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
        position_id="test-fake-plan",
        exchange=ExchangeName.OKX,
        role=LegRole.MASTER,
        target_qty_base=Decimal("0.1"),
        entry_order_id="okx-order-1",
        stop_order_id="okx-stop-1",
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(leg)

    # Scan for fakes
    from src.order_management.reconciliation.service import _detect_fake_order_refs
    fake_refs = _detect_fake_order_refs(plan, store.get_legs("test-fake-plan"))

    assert len(fake_refs) >= 2
    assert any(f.field == "entry_order_id" and f.value == "okx-order-1" for f in fake_refs)
    assert any(f.field == "stop_order_id" and f.value == "okx-stop-1" for f in fake_refs)


def test_preflight_clean_plan_no_fakes():
    """A clean plan with real numeric IDs should have no fake detections."""
    import tempfile
    store_path = Path(tempfile.mkdtemp()) / "test_clean_plans.sqlite3"
    store = SqlitePositionPlanStore(str(store_path))

    plan = PositionPlan(
        position_id="test-clean-plan",
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
        position_id="test-clean-plan",
        exchange=ExchangeName.BINANCE,
        role=LegRole.MASTER,
        target_qty_base=Decimal("0.1"),
        entry_order_id="987654321",
        entry_client_order_id="AEBNOLabc123",
        sync_status=LegSyncStatus.OPEN,
    )
    store.upsert_leg(leg)

    from src.order_management.reconciliation.service import _detect_fake_order_refs
    fake_refs = _detect_fake_order_refs(plan, store.get_legs("test-clean-plan"))

    assert len(fake_refs) == 0


def test_fake_id_patterns_match_documentation():
    """All fake patterns from the task spec should be detected."""
    specs = [
        "okx-order-1", "okx-1", "okx-stop-1",
        "binance-order-1", "binance-1", "binance-stop-1",
    ]
    for spec in specs:
        assert is_fake_order_id(spec), f"Task spec '{spec}' should be detected as fake"


def test_real_ids_are_not_fake():
    """Real order IDs must not match fake patterns."""
    real = ["1234567890", "987654321", "42", "AEOKOLabc123", "AEBNSPxyz789"]
    for r in real:
        assert not is_fake_order_id(r), f"'{r}' should NOT be fake"
