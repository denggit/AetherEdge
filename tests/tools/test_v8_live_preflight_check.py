from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from src.app import AppConfig
from src.order_management.position_plan.models import LegPlan, LegRole, LegSyncStatus, PositionPlan, PositionPlanStatus
from src.order_management.position_plan.store import SqlitePositionPlanStore
from src.platform import ExchangeName, MarginMode
from src.platform import config as platform_config
from src.platform.config import load_project_env_config
from src.platform.exchanges.models import Position, PositionMode, PositionSide
from src.runtime import RuntimeMode, LiveRuntimeConfig
from src.runtime.requirements import StrategyRuntimeRequirements
from src.order_management import MasterFollowerPolicyConfig
import tools.v8_live_preflight_check as preflight
from tools.v8_live_preflight_check import PreflightReport, _check_recovery_start_state, _check_runtime_config, _check_writable_file


def _app() -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        strategy="strategies.eth_lf_portfolio_v8:Strategy",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=100,
        signal_queue_maxsize=100,
        alert_queue_maxsize=100,
        dry_run=True,
        enable_email_alerts=False,
    )


def test_preflight_runtime_config_accepts_v8_requirements(monkeypatch) -> None:
    app = _app()
    runtime = LiveRuntimeConfig(
        app=app,
        mode=RuntimeMode.LIVE_RUNTIME,
        master_follower_policy=MasterFollowerPolicyConfig.from_env(
            app_exchanges=app.exchanges,
            data_exchange=app.data_exchange,
            env={"AETHER_MASTER_EXCHANGE": "okx", "AETHER_FOLLOWER_EXCHANGES": "binance"},
        ),
    )
    req = StrategyRuntimeRequirements.from_mapping(
        {
            "closed_kline": {"enabled": True, "interval": "4h"},
            "trades": {"enabled": True, "stream_enabled": True, "warmup_enabled": True},
            "range_bars": {"enabled": True, "range_pct": "0.002"},
            "order_book": {"enabled": False},
            "account_state": {"poll_enabled": True, "poll_interval_seconds": 300},
            "order_state": {"poll_when_position_enabled": True, "poll_interval_seconds": 20},
        }
    )
    monkeypatch.setenv("AETHER_LIVE_TRADING", "false")
    monkeypatch.setenv("OKX_SANDBOX", "true")
    monkeypatch.setenv("BINANCE_SANDBOX", "true")
    report = PreflightReport(started_time_ms=1)

    _check_runtime_config(
        report,
        app=app,
        runtime_mode=RuntimeMode.LIVE_RUNTIME,
        runtime=runtime,
        requirements=req,
        expect_real_live=False,
        env={
            "AETHER_LIVE_TRADING": "false",
            "OKX_SANDBOX": "true",
            "BINANCE_SANDBOX": "true",
        },
    )

    assert not any(check.status == "fail" for check in report.checks)
    assert any(check.name == "v8_runtime_requirements" and check.status == "ok" for check in report.checks)
    assert any(check.name == "master_follower_policy" and check.detail["followers"] == ["binance"] for check in report.checks)


def test_preflight_runtime_config_fails_wrong_strategy() -> None:
    app = _app().__class__(**{**_app().__dict__, "strategy": "strategies.empty_strategy:Strategy"})
    runtime = LiveRuntimeConfig(app=app, mode=RuntimeMode.LIVE_RUNTIME)
    req = StrategyRuntimeRequirements.from_mapping({})
    report = PreflightReport(started_time_ms=1)

    _check_runtime_config(
        report,
        app=app,
        runtime_mode=RuntimeMode.LIVE_RUNTIME,
        runtime=runtime,
        requirements=req,
        expect_real_live=False,
        env={},
    )

    assert any(check.name == "v8_strategy_configured" and check.status == "fail" for check in report.checks)


def test_preflight_writable_file_creates_sqlite_path(tmp_path) -> None:
    report = PreflightReport(started_time_ms=1)
    path = tmp_path / "state" / "preflight.sqlite3"

    _check_writable_file(report, "db", path)

    assert path.exists()
    assert report.checks[-1].status == "ok"


def test_preflight_uses_env_file_snapshot_for_all_config_helpers(
    tmp_path,
    monkeypatch,
) -> None:
    order_journal = tmp_path / "custom-order-journal.sqlite3"
    position_plan = tmp_path / "custom-position-plan.sqlite3"
    env_file = tmp_path / "v8.env"
    env_file.write_text(
        "AETHER_LIVE_TRADING=true\n"
        "AETHER_DRY_RUN=false\n"
        "OKX_SANDBOX=false\n"
        f"AETHER_ORDER_JOURNAL_DB={order_journal}\n"
        f"AETHER_POSITION_PLAN_DB={position_plan}\n"
        "MARGIN_MODE=isolated\n"
        "OKX_LEVERAGE=15\n",
        encoding="utf-8",
    )
    for key in (
        "AETHER_LIVE_TRADING",
        "AETHER_DRY_RUN",
        "OKX_SANDBOX",
        "AETHER_ORDER_JOURNAL_DB",
        "AETHER_POSITION_PLAN_DB",
        "MARGIN_MODE",
        "OKX_LEVERAGE",
    ):
        monkeypatch.delenv(key, raising=False)

    project_env = load_project_env_config(env_file=env_file)
    monkeypatch.setattr(platform_config, "_PROJECT_ENV_CONFIG", project_env)
    app = replace(
        _app(),
        exchanges=(ExchangeName.OKX,),
        dry_run=False,
        state_db_path=str(tmp_path / "state.sqlite3"),
    )
    runtime = SimpleNamespace(
        master_follower_policy=SimpleNamespace(
            master_exchange=ExchangeName.OKX,
            follower_exchanges=(),
            entry_deviation_alert_pct=Decimal("0"),
        )
    )
    requirements = StrategyRuntimeRequirements.from_mapping(
        {
            "closed_kline": {"enabled": True, "interval": "4h"},
            "trades": {
                "enabled": True,
                "stream_enabled": True,
                "warmup_enabled": True,
            },
            "range_bars": {"enabled": True, "range_pct": "0.002"},
            "order_book": {"enabled": False},
            "account_state": {
                "poll_enabled": True,
                "poll_interval_seconds": 300,
            },
            "order_state": {
                "poll_when_position_enabled": True,
                "poll_interval_seconds": 20,
            },
        }
    )
    report = PreflightReport(started_time_ms=1)

    _check_runtime_config(
        report,
        app=app,
        runtime_mode=RuntimeMode.LIVE_RUNTIME,
        runtime=runtime,
        requirements=requirements,
        expect_real_live=True,
        env=project_env.values,
    )
    preflight._check_local_writable(
        report,
        app=app,
        env=project_env.values,
    )

    store_factory = Mock()
    store_factory.return_value.serialize_active_positions.return_value = []
    monkeypatch.setattr(preflight, "SqlitePositionPlanStore", store_factory)
    monkeypatch.setattr(
        preflight,
        "_check_recoverable_stale_local_orders",
        Mock(),
    )
    _check_recovery_start_state(
        report,
        app=app,
        runtime=runtime,
        snapshots={ExchangeName.OKX: {"positions": []}},
        strategy_id="v8-test",
        env=project_env.values,
    )

    account_loader = Mock(
        return_value=SimpleNamespace(
            margin_mode=MarginMode.ISOLATED,
            targets=(),
            missing_leverage=(),
        )
    )
    monkeypatch.setattr(preflight, "load_account_config_env", account_loader)
    asyncio.run(
        preflight._check_account_config(
            report,
            app=app,
            env=project_env.values,
            apply_account_config=False,
        )
    )

    safety = _result(report, "real_live_safety_switches")
    writable = _result(report, "order_journal_db_writable")
    assert project_env.get("AETHER_LIVE_TRADING") == "true"
    assert safety.detail["live_trading"] is True
    assert safety.detail["sandbox"]["okx"] is False
    assert writable.detail["path"] == str(order_journal)
    assert order_journal.is_file()
    assert Path(store_factory.call_args.args[0]) == position_plan
    assert account_loader.call_args.kwargs["require_leverage"] is True
    assert account_loader.call_args.kwargs["environ"] is project_env.values
    assert "env_file" not in account_loader.call_args.kwargs
    assert _result(report, "account_config_env_loaded").detail[
        "live_trading"
    ] is True
    assert project_env.get("MARGIN_MODE") == "isolated"
    assert project_env.get("OKX_LEVERAGE") == "15"


def test_preflight_recovery_start_allows_master_position_missing_stop_when_plan_exists(tmp_path, monkeypatch) -> None:
    app = _app().__class__(**{**_app().__dict__, "state_db_path": str(tmp_path / "state.sqlite3")})
    runtime = _runtime(app)
    _write_active_short_plan(tmp_path / "plans.sqlite3")
    monkeypatch.setenv("AETHER_POSITION_PLAN_DB", str(tmp_path / "plans.sqlite3"))
    report = PreflightReport(started_time_ms=1)

    _check_recovery_start_state(
        report,
        app=app,
        runtime=runtime,
        snapshots={
            ExchangeName.OKX: {
                "positions": [_short_okx_position()],
                "open_orders": [],
                "open_stop_orders": [],
                "position_mode": PositionMode.ONE_WAY,
            },
            ExchangeName.BINANCE: {"positions": [], "open_orders": [], "open_stop_orders": [], "position_mode": PositionMode.ONE_WAY},
        },
        strategy_id="eth_lf_portfolio_v9c_reclaim_priority",
        env={"AETHER_POSITION_PLAN_DB": str(tmp_path / "plans.sqlite3")},
    )

    assert report.ok is True
    assert any(check.name == "recovery_start" and check.status == "warn" and check.error == "stop_missing_but_recoverable" for check in report.checks)


def test_preflight_recovery_start_fails_active_position_without_plan(tmp_path, monkeypatch) -> None:
    app = _app().__class__(**{**_app().__dict__, "state_db_path": str(tmp_path / "state.sqlite3")})
    runtime = _runtime(app)
    monkeypatch.setenv("AETHER_POSITION_PLAN_DB", str(tmp_path / "missing_plans.sqlite3"))
    report = PreflightReport(started_time_ms=1)

    _check_recovery_start_state(
        report,
        app=app,
        runtime=runtime,
        snapshots={
            ExchangeName.OKX: {
                "positions": [_short_okx_position()],
                "open_orders": [],
                "open_stop_orders": [],
                "position_mode": PositionMode.ONE_WAY,
            }
        },
        strategy_id="eth_lf_portfolio_v9c_reclaim_priority",
        env={
            "AETHER_POSITION_PLAN_DB": str(
                tmp_path / "missing_plans.sqlite3"
            )
        },
    )

    assert report.ok is False
    assert any(check.name == "recovery_start" and check.status == "fail" and check.error == "active_position_without_plan" for check in report.checks)


def _runtime(app: AppConfig) -> LiveRuntimeConfig:
    return LiveRuntimeConfig(
        app=app,
        mode=RuntimeMode.LIVE_RUNTIME,
        master_follower_policy=MasterFollowerPolicyConfig.from_env(
            app_exchanges=app.exchanges,
            data_exchange=app.data_exchange,
            env={"AETHER_MASTER_EXCHANGE": "okx", "AETHER_FOLLOWER_EXCHANGES": "binance"},
        ),
    )


def _result(report: PreflightReport, name: str):
    return next(item for item in report.checks if item.name == name)


def _short_okx_position() -> Position:
    return Position(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        side=PositionSide.BOTH,
        quantity=Decimal("-2.82"),
        entry_price=Decimal("1700"),
    )


def _write_active_short_plan(path) -> None:
    store = SqlitePositionPlanStore(path)
    store.upsert_position(
        PositionPlan(
            position_id="pos-1",
            strategy_id="eth_lf_portfolio_v9c_reclaim_priority",
            entry_engine="BULL_RECLAIM_V2",
            side="short",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=Decimal("1719.40"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.282"),
            master_filled_qty_base=Decimal("0.282"),
        )
    )
    store.upsert_leg(
        LegPlan(
            position_id="pos-1",
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.282"),
            filled_qty_base=Decimal("0.282"),
            sync_status=LegSyncStatus.OPEN,
        )
    )
