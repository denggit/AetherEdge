from __future__ import annotations

import json
from decimal import Decimal

import pytest

from src.app import AppConfig
from src.platform.exchanges.models import (
    Balance,
    ExchangeName,
    LeverageInfo,
    MarginMode,
    PositionMode,
)
from src.platform.snapshot import PlatformSnapshot
from src.runtime.hedge_mode_gate import position_mode_status
from tools.live_preflight_check import (
    EXIT_FAIL_CONFIG,
    PreflightReport,
    _check_portfolio_v1_hedge_mode,
)


def _app(strategy: str = "eth_portfolio_v1") -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        strategy=strategy,
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )


def _snapshot(
    exchange: ExchangeName, mode: PositionMode
) -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(
            exchange=exchange,
            asset="USDT",
            total=Decimal("1000"),
            available=Decimal("1000"),
        ),
        positions=[],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(
            exchange=exchange,
            symbol="ETH-USDT-PERP",
            raw_symbol="ETH-USDT-SWAP",
            leverage=Decimal("15"),
            margin_mode=MarginMode.CROSS,
        ),
        position_mode=mode,
    )


def _check(
    *,
    okx: PositionMode = PositionMode.HEDGE,
    binance: PositionMode = PositionMode.HEDGE,
    strategy: str = "eth_portfolio_v1",
    omit: ExchangeName | None = None,
):
    app = _app(strategy)
    report = PreflightReport(started_time_ms=1)
    snapshots = tuple(
        snapshot
        for snapshot in (
            _snapshot(ExchangeName.OKX, okx),
            _snapshot(ExchangeName.BINANCE, binance),
        )
        if snapshot.balance.exchange is not omit
    )
    result = _check_portfolio_v1_hedge_mode(
        report,
        strategy_id=strategy,
        app_config=app,
        snapshots=snapshots,
    )
    return result, report


def test_portfolio_v1_preflight_both_hedge_passes() -> None:
    result, report = _check()
    assert result is True
    aggregate = next(
        check
        for check in report.checks
        if check.name == "portfolio_v1_hedge_mode"
    )
    assert aggregate.status == "ok"
    assert aggregate.detail["hedge_mode_ok"] is True
    assert json.loads(report.to_json())["checks"]


@pytest.mark.parametrize(
    ("okx", "binance", "failed_exchange"),
    (
        (PositionMode.ONE_WAY, PositionMode.HEDGE, "okx"),
        (PositionMode.HEDGE, PositionMode.ONE_WAY, "binance"),
    ),
)
def test_portfolio_v1_preflight_one_way_is_hard_failure(
    okx: PositionMode,
    binance: PositionMode,
    failed_exchange: str,
) -> None:
    result, report = _check(okx=okx, binance=binance)
    assert result is False
    assert report.verdict == "fail_config"
    assert report.ok is False
    assert EXIT_FAIL_CONFIG != 0
    failed = next(
        check
        for check in report.checks
        if check.name
        == f"portfolio_v1_hedge_mode_{failed_exchange}"
    )
    assert failed.status == "fail"
    assert failed.detail["hedge_mode_ok"] is False


def test_portfolio_v1_preflight_missing_mode_is_unknown_and_fails() -> None:
    result, report = _check(omit=ExchangeName.BINANCE)
    assert result is False
    failed = next(
        check
        for check in report.checks
        if check.name == "portfolio_v1_hedge_mode_binance"
    )
    assert failed.detail["actual_mode"] == "unknown"
    assert failed.detail["error"] == "snapshot_missing"


def test_v10b_preflight_does_not_apply_portfolio_gate() -> None:
    result, report = _check(
        okx=PositionMode.ONE_WAY,
        binance=PositionMode.ONE_WAY,
        strategy="eth_lf_portfolio_v10b",
    )
    assert result is True
    assert report.checks == []


@pytest.mark.parametrize(
    ("exchange", "raw", "mode", "hedge"),
    (
        (
            ExchangeName.OKX,
            {"posMode": "long_short_mode"},
            "hedge",
            True,
        ),
        (
            ExchangeName.OKX,
            {"posMode": "net_mode"},
            "one_way",
            False,
        ),
        (
            ExchangeName.BINANCE,
            {"dualSidePosition": True},
            "hedge",
            True,
        ),
        (
            ExchangeName.BINANCE,
            {"dualSidePosition": False},
            "one_way",
            False,
        ),
        (
            ExchangeName.BINANCE,
            {},
            "unknown",
            False,
        ),
    ),
)
def test_exchange_position_mode_field_normalization(
    exchange: ExchangeName,
    raw: object,
    mode: str,
    hedge: bool,
) -> None:
    status = position_mode_status(
        exchange=exchange,
        symbol="ETH-USDT-PERP",
        value=raw,
        source="test",
    )
    assert status.mode == mode
    assert status.hedge_mode is hedge
