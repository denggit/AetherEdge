from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

from src.order_management import (
    LegPlan,
    LegRole,
    LegSyncStatus,
    PositionPlan,
    PositionPlanStatus,
    SqlitePositionPlanStore,
)
from src.platform import ExchangeName
from strategies.eth_portfolio_v1.domain.mf_signal import MF_ENGINE_NAME
from strategies.eth_portfolio_v1.preflight.live_gate import (
    PortfolioV1LiveGateReport,
)
from strategies.eth_portfolio_v1.preflight.recovery import (
    audit_preflight_recovery,
)


def _lf(position_id: str = "v9e-lf-preflight") -> PositionPlan:
    return PositionPlan(
        position_id=position_id,
        strategy_id="eth_portfolio_v1",
        entry_engine="BULL_RECLAIM_V2",
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=Decimal("1900"),
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.6"),
        master_filled_qty_base=Decimal("0.6"),
        metadata={
            "sleeve_id": "lf",
            "position_id": position_id,
            "engine": "BULL_RECLAIM_V2",
        },
    )


def _mf(
    position_id: str = "mf-low-sweep-time48-preflight",
    *,
    complete: bool = True,
) -> PositionPlan:
    metadata = {
        "sleeve_id": "mf",
        "position_id": position_id,
        "engine": MF_ENGINE_NAME,
    }
    if complete:
        metadata.update(
            {
                "entry_execution_time_ms": 1_700_000_060_000,
                "entry_tradebar_open_time_ms": 1_700_000_060_000,
                "signal_time_ms": 1_700_000_000_000,
                "time48_holding_minutes": 48,
                "exit_variant": "time48",
                "quantity_scope": "mf_sleeve_quantity",
                "protective_stop_required": False,
                "average_entry_price": "2000",
            }
        )
    return PositionPlan(
        position_id=position_id,
        strategy_id="eth_portfolio_v1",
        entry_engine=MF_ENGINE_NAME,
        side="long",
        status=PositionPlanStatus.ACTIVE,
        canonical_stop_price=None,
        master_exchange=ExchangeName.OKX,
        master_target_qty_base=Decimal("0.4"),
        master_filled_qty_base=Decimal("0.4"),
        metadata=metadata,
    )


def _check(store: SqlitePositionPlanStore) -> dict[str, object]:
    return audit_preflight_recovery(plan_store=store)


def _save(store: SqlitePositionPlanStore, plan: PositionPlan) -> None:
    store.upsert_position(plan)
    for exchange, role in (
        (ExchangeName.OKX, LegRole.MASTER),
        (ExchangeName.BINANCE, LegRole.FOLLOWER),
    ):
        store.upsert_leg(
            LegPlan(
                position_id=plan.position_id,
                exchange=exchange,
                role=role,
                target_qty_base=plan.master_target_qty_base,
                filled_qty_base=plan.master_filled_qty_base,
                sync_status=LegSyncStatus.OPEN,
            )
        )


def test_no_active_plans_passes(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")

    audit = _check(store)

    assert audit["recovery_ok"] is True
    assert audit["plans"]["active_count"] == 0


def test_lf_and_mf_active_plans_with_required_metadata_pass(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    _save(store, _lf())
    _save(store, _mf())

    audit = _check(store)

    assert audit["recovery_ok"] is True
    assert audit["active_sleeves"] == [
        "lf",
        "mf",
    ]


def test_duplicated_mf_active_plans_fail_config(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    _save(store, _mf())
    _save(store, _mf("mf-low-sweep-time48-preflight-2"))

    audit = _check(store)

    assert audit["recovery_ok"] is False
    assert "duplicated_active_plan:mf" in audit["issues"]


def test_mf_missing_time_metadata_fails_config(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    _save(store, _mf(complete=False))

    audit = _check(store)

    assert audit["recovery_ok"] is False
    assert any(
        "mf_missing_metadata:entry_execution_time_ms" in issue
        for issue in audit["issues"]
    )


def test_unparseable_sleeve_id_fails_config(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    plan = _lf()
    _save(
        store,
        PositionPlan(
            **{
                **plan.__dict__,
                "metadata": {
                    **dict(plan.metadata),
                    "sleeve_id": "unknown-sleeve",
                },
            }
        ),
    )

    audit = _check(store)

    assert audit["recovery_ok"] is False
    assert any(
        "unparseable_sleeve_id" in issue
        for issue in audit["issues"]
    )


def test_report_json_contains_portfolio_v1_recovery_audit(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    report = PortfolioV1LiveGateReport()
    report.recovery_audit_summary = _check(store)

    payload = json.loads(report.to_json())

    assert "recovery_audit_summary" in payload


def test_exchange_position_without_local_plan_fails_config(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")

    audit = audit_preflight_recovery(
        plan_store=store,
        snapshots=(
            SimpleNamespace(
                positions=(SimpleNamespace(quantity=Decimal("1")),)
            ),
        ),
    )

    assert audit["recovery_ok"] is False
    assert "exchange_position_without_local_plan" in (
        audit["issues"]
    )
