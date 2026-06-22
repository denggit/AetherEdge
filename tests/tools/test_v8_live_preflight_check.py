from __future__ import annotations

from decimal import Decimal

from src.app import AppConfig
from src.platform import ExchangeName
from src.runtime import RuntimeMode, LiveRuntimeConfig
from src.runtime.requirements import StrategyRuntimeRequirements
from src.order_management import MasterFollowerPolicyConfig
from tools.v8_live_preflight_check import PreflightReport, _check_runtime_config, _check_writable_file


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
            "private_account_stream": {"enabled": True},
        }
    )
    monkeypatch.setenv("AETHER_LIVE_TRADING", "false")
    monkeypatch.setenv("OKX_SANDBOX", "true")
    monkeypatch.setenv("BINANCE_SANDBOX", "true")
    report = PreflightReport(started_time_ms=1)

    _check_runtime_config(report, app=app, runtime_mode=RuntimeMode.LIVE_RUNTIME, runtime=runtime, requirements=req, expect_real_live=False)

    assert not any(check.status == "fail" for check in report.checks)
    assert any(check.name == "v8_runtime_requirements" and check.status == "ok" for check in report.checks)
    assert any(check.name == "master_follower_policy" and check.detail["followers"] == ["binance"] for check in report.checks)


def test_preflight_runtime_config_fails_wrong_strategy() -> None:
    app = _app().__class__(**{**_app().__dict__, "strategy": "strategies.empty_strategy:Strategy"})
    runtime = LiveRuntimeConfig(app=app, mode=RuntimeMode.LIVE_RUNTIME)
    req = StrategyRuntimeRequirements.from_mapping({})
    report = PreflightReport(started_time_ms=1)

    _check_runtime_config(report, app=app, runtime_mode=RuntimeMode.LIVE_RUNTIME, runtime=runtime, requirements=req, expect_real_live=False)

    assert any(check.name == "v8_strategy_configured" and check.status == "fail" for check in report.checks)


def test_preflight_writable_file_creates_sqlite_path(tmp_path) -> None:
    report = PreflightReport(started_time_ms=1)
    path = tmp_path / "state" / "preflight.sqlite3"

    _check_writable_file(report, "db", path)

    assert path.exists()
    assert report.checks[-1].status == "ok"
