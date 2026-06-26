from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

import tools.v8_live_preflight_check as preflight
from src.app import AppConfig
from src.platform.exchanges.models import Balance, ExchangeName, LeverageInfo, MarginMode, Order, Position, PositionMode
from src.platform.markets import get_market_profile
from tools.v8_live_preflight_check import PreflightReport


@pytest.mark.asyncio
async def test_preflight_account_config_default_is_read_only(monkeypatch) -> None:
    env_file = _write_env_file("read_only")
    account = FakeAccount(leverage=Decimal("3"), margin_mode=MarginMode.CROSS)
    execution = FakeExecution()
    monkeypatch.setattr(preflight, "create_account_client", lambda *args, **kwargs: account)
    monkeypatch.setattr(preflight, "create_execution_client", lambda *args, **kwargs: execution)
    monkeypatch.setenv("AETHER_LIVE_TRADING", "true")
    report = PreflightReport(started_time_ms=1)

    await preflight._check_account_config(
        report,
        app=_app(dry_run=False),
        env_file=str(env_file),
        apply_account_config=False,
    )

    assert account.set_margin_mode_calls == []
    assert account.set_leverage_calls == []
    assert any(check.name == "account_config_env_loaded" and check.status == "ok" for check in report.checks)
    assert any(check.name == "account_config_applied:okx" and check.status == "ok" for check in report.checks)
    assert any(check.name == "account_config_verified:okx" and check.status == "warn" for check in report.checks)


@pytest.mark.asyncio
async def test_preflight_apply_account_config_writes_and_verifies(monkeypatch) -> None:
    env_file = _write_env_file("apply")
    account = FakeAccount(leverage=Decimal("3"), margin_mode=MarginMode.CROSS)
    execution = FakeExecution()
    monkeypatch.setattr(preflight, "create_account_client", lambda *args, **kwargs: account)
    monkeypatch.setattr(preflight, "create_execution_client", lambda *args, **kwargs: execution)
    monkeypatch.setenv("AETHER_LIVE_TRADING", "true")
    report = PreflightReport(started_time_ms=1)

    await preflight._check_account_config(
        report,
        app=_app(dry_run=False),
        env_file=str(env_file),
        apply_account_config=True,
    )

    assert account.set_margin_mode_calls == [MarginMode.ISOLATED]
    assert account.set_leverage_calls == [(Decimal("15"), MarginMode.ISOLATED)]
    assert any(check.name == "account_config_applied:okx" and check.status == "ok" and check.detail["applied"] is True for check in report.checks)
    assert any(check.name == "account_config_verified:okx" and check.status == "ok" for check in report.checks)


def _app(*, dry_run: bool) -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="strategies.eth_lf_portfolio_v8:Strategy",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=100,
        signal_queue_maxsize=100,
        alert_queue_maxsize=100,
        dry_run=dry_run,
        enable_email_alerts=False,
    )


def _write_env_file(name: str) -> Path:
    path = Path(".tmp_pytest") / f"preflight_account_config_{name}.env"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("MARGIN_MODE=isolated\nOKX_LEVERAGE=15\n", encoding="utf-8")
    return path


class FakeAccount:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    def __init__(self, *, leverage: Decimal, margin_mode: MarginMode) -> None:
        self.leverage = leverage
        self.margin_mode = margin_mode
        self.set_margin_mode_calls: list[MarginMode] = []
        self.set_leverage_calls: list[tuple[Decimal, MarginMode]] = []

    async def fetch_balance(self, asset: str = "USDT") -> Balance:
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("100"), available=Decimal("100"))

    async def fetch_positions(self, symbol: str | None = None) -> list[Position]:
        return []

    async def fetch_leverage(self, *, margin_mode: MarginMode = MarginMode.CROSS) -> LeverageInfo:
        return LeverageInfo(
            exchange=self.exchange,
            symbol=self.symbol,
            raw_symbol=self.symbol,
            leverage=self.leverage,
            margin_mode=self.margin_mode,
        )

    async def set_margin_mode(self, margin_mode: MarginMode):
        self.set_margin_mode_calls.append(margin_mode)
        self.margin_mode = margin_mode
        return {"margin_mode": margin_mode.value}

    async def set_leverage(self, leverage: Decimal, *, margin_mode: MarginMode = MarginMode.CROSS) -> LeverageInfo:
        self.set_leverage_calls.append((leverage, margin_mode))
        self.leverage = leverage
        self.margin_mode = margin_mode
        return await self.fetch_leverage(margin_mode=margin_mode)

    async def fetch_position_mode(self) -> PositionMode:
        return PositionMode.ONE_WAY


class FakeExecution:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    async def fetch_open_orders(self) -> list[Order]:
        return []

    async def fetch_open_stop_orders(self) -> list[Order]:
        return []
