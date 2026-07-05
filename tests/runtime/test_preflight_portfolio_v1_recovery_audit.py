from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

from src.app import AppConfig
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
from tools.live_preflight_check import (
    PreflightReport,
    _check_portfolio_v1_recovery_audit,
)


def _app() -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        strategy="eth_portfolio_v1",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
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


def _check(store: SqlitePositionPlanStore) -> PreflightReport:
    report = PreflightReport(started_time_ms=1)
    _check_portfolio_v1_recovery_audit(
        report,
        strategy_id="eth_portfolio_v1",
        app_config=_app(),
        plan_store=store,
    )
    return report


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

    report = _check(store)

    assert report.verdict == "pass"
    assert report.portfolio_v1_recovery_audit["plans"]["active_count"] == 0


def test_lf_and_mf_active_plans_with_required_metadata_pass(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    _save(store, _lf())
    _save(store, _mf())

    report = _check(store)

    assert report.verdict == "pass"
    assert report.portfolio_v1_recovery_audit["active_sleeves"] == [
        "lf",
        "mf",
    ]


def test_duplicated_mf_active_plans_fail_config(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    _save(store, _mf())
    _save(store, _mf("mf-low-sweep-time48-preflight-2"))

    report = _check(store)

    assert report.verdict == "fail_config"
    assert "duplicated_active_plan:mf" in report.portfolio_v1_recovery_audit[
        "issues"
    ]


def test_mf_missing_time_metadata_fails_config(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    _save(store, _mf(complete=False))

    report = _check(store)

    assert report.verdict == "fail_config"
    assert any(
        "mf_missing_metadata:entry_execution_time_ms" in issue
        for issue in report.portfolio_v1_recovery_audit["issues"]
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

    report = _check(store)

    assert report.verdict == "fail_config"
    assert any(
        "unparseable_sleeve_id" in issue
        for issue in report.portfolio_v1_recovery_audit["issues"]
    )


def test_report_json_contains_portfolio_v1_recovery_audit(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")

    payload = json.loads(_check(store).to_json())

    assert "portfolio_v1_recovery_audit" in payload


def test_exchange_position_without_local_plan_fails_config(tmp_path) -> None:
    store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    report = PreflightReport(started_time_ms=1)

    ok = _check_portfolio_v1_recovery_audit(
        report,
        strategy_id="eth_portfolio_v1",
        app_config=_app(),
        plan_store=store,
        snapshots=(
            SimpleNamespace(
                positions=(SimpleNamespace(quantity=Decimal("1")),)
            ),
        ),
    )

    assert ok is False
    assert report.verdict == "fail_config"
    assert "exchange_position_without_local_plan" in (
        report.portfolio_v1_recovery_audit["issues"]
    )
