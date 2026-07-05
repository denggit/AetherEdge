from __future__ import annotations

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
from src.runtime.position_mode_gate import (
    position_mode_status,
    resolve_position_mode_requirements,
)
from strategies.eth_portfolio_v1.preflight.live_gate import (
    PortfolioV1LiveGate,
)
from strategies.eth_portfolio_v1.strategy import Strategy


def _app() -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        strategy="strategies.eth_portfolio_v1:Strategy",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )


def _snapshot(
    exchange: ExchangeName,
    mode: PositionMode,
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


def _gate_issues(
    *,
    okx: PositionMode = PositionMode.HEDGE,
    binance: PositionMode = PositionMode.HEDGE,
    omit: ExchangeName | None = None,
) -> tuple[list[str], dict[str, object]]:
    gate = object.__new__(PortfolioV1LiveGate)
    gate.app_config = _app()
    snapshots = tuple(
        snapshot
        for snapshot in (
            _snapshot(ExchangeName.OKX, okx),
            _snapshot(ExchangeName.BINANCE, binance),
        )
        if snapshot.balance.exchange is not omit
    )
    issues = gate._hedge_mode_issues(snapshots)
    return issues, gate._last_hedge_audit


def test_portfolio_v1_preflight_both_hedge_passes() -> None:
    issues, audit = _gate_issues()
    assert issues == []
    assert audit["okx"]["ok"] is True
    assert audit["binance"]["ok"] is True


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
    issues, audit = _gate_issues(okx=okx, binance=binance)
    assert f"{failed_exchange}_hedge_mode_required" in issues
    assert audit[failed_exchange]["ok"] is False


def test_portfolio_v1_preflight_missing_mode_is_unknown_and_fails() -> None:
    issues, audit = _gate_issues(omit=ExchangeName.BINANCE)
    assert "binance_hedge_mode_required" in issues
    assert audit["binance"]["actual"] == "unknown"


def test_strategy_plugin_declares_hedge_requirement() -> None:
    requirements = resolve_position_mode_requirements(Strategy())
    assert len(requirements) == 1
    requirement = requirements[0]
    assert requirement.required_mode is PositionMode.HEDGE
    assert requirement.exchanges == (
        ExchangeName.OKX,
        ExchangeName.BINANCE,
    )


def test_strategy_without_provider_has_no_position_mode_gate() -> None:
    assert resolve_position_mode_requirements(object()) == ()


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
