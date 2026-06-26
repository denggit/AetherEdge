from __future__ import annotations

from decimal import Decimal

import pytest

from src.app import AppConfig, AppContext
from src.platform.exchanges.models import (
    Balance,
    ExchangeName,
    LeverageInfo,
    MarginMode,
    Order,
    OrderStatus,
    Position,
    PositionMode,
    PositionSide,
)
from src.platform.markets import get_market_profile
from src.runtime.config import LiveRuntimeConfig
from src.runtime.account_config import (
    AccountConfigBootstrapError,
    AccountConfigTarget,
    bootstrap_account_config,
    load_account_config_env,
    raise_on_failed_account_config,
)
from src.runtime.models import RuntimeMode
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.runner import LiveRuntimeRunner


def test_load_account_config_env_parses_margin_mode_and_exchange_leverage() -> None:
    env = load_account_config_env(
        exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        symbol="ETH-USDT-PERP",
        environ={"MARGIN_MODE": "isolated", "OKX_LEVERAGE": "15", "BINANCE_LEVERAGE": "15"},
    )

    assert env.margin_mode is MarginMode.ISOLATED
    assert [target.exchange for target in env.targets] == [ExchangeName.OKX, ExchangeName.BINANCE]
    assert [target.leverage for target in env.targets] == [Decimal("15"), Decimal("15")]


def test_load_account_config_env_missing_leverage_fails_when_required() -> None:
    with pytest.raises(AccountConfigBootstrapError, match="OKX_LEVERAGE"):
        load_account_config_env(
            exchanges=(ExchangeName.OKX,),
            symbol="ETH-USDT-PERP",
            environ={"MARGIN_MODE": "isolated"},
            require_leverage=True,
        )


@pytest.mark.parametrize("raw", ["0", "-1", "nope"])
def test_load_account_config_env_invalid_leverage_fails(raw: str) -> None:
    with pytest.raises(AccountConfigBootstrapError):
        load_account_config_env(
            exchanges=(ExchangeName.OKX,),
            symbol="ETH-USDT-PERP",
            environ={"OKX_LEVERAGE": raw},
        )


@pytest.mark.asyncio
async def test_preflight_read_only_does_not_call_set_leverage() -> None:
    account = FakeAccount(ExchangeName.OKX, leverage=Decimal("3"), margin_mode=MarginMode.CROSS)
    execution = FakeExecution(ExchangeName.OKX)

    result = (
        await bootstrap_account_config(
            targets=[_target(ExchangeName.OKX)],
            account_clients=[account],
            execution_clients=[execution],
            apply=False,
            dry_run=False,
        )
    )[0]

    assert result.skipped_write is True
    assert result.verified is False
    assert account.set_margin_mode_calls == []
    assert account.set_leverage_calls == []


@pytest.mark.asyncio
async def test_apply_account_config_sets_margin_then_leverage_and_verifies() -> None:
    account = FakeAccount(ExchangeName.OKX, leverage=Decimal("3"), margin_mode=MarginMode.CROSS)
    execution = FakeExecution(ExchangeName.OKX)

    result = (
        await bootstrap_account_config(
            targets=[_target(ExchangeName.OKX)],
            account_clients=[account],
            execution_clients=[execution],
            apply=True,
            dry_run=False,
        )
    )[0]

    assert result.applied is True
    assert result.verified is True
    assert account.set_margin_mode_calls == [MarginMode.ISOLATED]
    assert account.set_leverage_calls == [(Decimal("15"), MarginMode.ISOLATED)]


@pytest.mark.asyncio
async def test_dry_run_does_not_write_account_config() -> None:
    account = FakeAccount(ExchangeName.OKX, leverage=Decimal("3"), margin_mode=MarginMode.CROSS)
    execution = FakeExecution(ExchangeName.OKX)

    result = (
        await bootstrap_account_config(
            targets=[_target(ExchangeName.OKX)],
            account_clients=[account],
            execution_clients=[execution],
            apply=True,
            dry_run=True,
        )
    )[0]

    assert result.reason == "dry_run"
    assert account.set_margin_mode_calls == []
    assert account.set_leverage_calls == []


@pytest.mark.asyncio
async def test_active_position_mismatch_fails_without_set() -> None:
    account = FakeAccount(
        ExchangeName.OKX,
        leverage=Decimal("3"),
        margin_mode=MarginMode.CROSS,
        positions=[_position(ExchangeName.OKX)],
    )
    execution = FakeExecution(ExchangeName.OKX)

    result = (
        await bootstrap_account_config(
            targets=[_target(ExchangeName.OKX)],
            account_clients=[account],
            execution_clients=[execution],
            apply=True,
            dry_run=False,
        )
    )[0]

    assert result.verified is False
    assert result.reason == "blocked_by_existing_position_or_order"
    assert account.set_leverage_calls == []


@pytest.mark.asyncio
async def test_open_regular_or_stop_orders_mismatch_fails_without_set() -> None:
    account = FakeAccount(ExchangeName.OKX, leverage=Decimal("3"), margin_mode=MarginMode.CROSS)
    execution = FakeExecution(
        ExchangeName.OKX,
        open_orders=[_order(ExchangeName.OKX, "regular")],
        open_stop_orders=[_order(ExchangeName.OKX, "stop")],
    )

    result = (
        await bootstrap_account_config(
            targets=[_target(ExchangeName.OKX)],
            account_clients=[account],
            execution_clients=[execution],
            apply=True,
            dry_run=False,
        )
    )[0]

    assert result.error is not None
    assert account.set_margin_mode_calls == []
    assert account.set_leverage_calls == []


@pytest.mark.asyncio
async def test_already_matching_with_position_is_verified_without_set() -> None:
    account = FakeAccount(
        ExchangeName.OKX,
        leverage=Decimal("15"),
        margin_mode=MarginMode.ISOLATED,
        positions=[_position(ExchangeName.OKX)],
    )
    execution = FakeExecution(ExchangeName.OKX)

    result = (
        await bootstrap_account_config(
            targets=[_target(ExchangeName.OKX)],
            account_clients=[account],
            execution_clients=[execution],
            apply=True,
            dry_run=False,
        )
    )[0]

    assert result.verified is True
    assert result.reason == "already_configured"
    assert account.set_margin_mode_calls == []
    assert account.set_leverage_calls == []


@pytest.mark.asyncio
async def test_set_then_fetch_mismatch_fails() -> None:
    account = FakeAccount(ExchangeName.OKX, leverage=Decimal("3"), margin_mode=MarginMode.CROSS, mismatch_after_set=True)
    execution = FakeExecution(ExchangeName.OKX)

    result = (
        await bootstrap_account_config(
            targets=[_target(ExchangeName.OKX)],
            account_clients=[account],
            execution_clients=[execution],
            apply=True,
            dry_run=False,
        )
    )[0]

    assert result.applied is True
    assert result.verified is False
    with pytest.raises(AccountConfigBootstrapError):
        raise_on_failed_account_config([result])


@pytest.mark.asyncio
async def test_live_runtime_startup_hook_applies_account_config_from_env(monkeypatch) -> None:
    monkeypatch.setenv("AETHER_LIVE_TRADING", "true")
    monkeypatch.setenv("OKX_SANDBOX", "false")
    monkeypatch.setenv("MARGIN_MODE", "isolated")
    monkeypatch.setenv("OKX_LEVERAGE", "15")
    account = FakeAccount(ExchangeName.OKX, leverage=Decimal("3"), margin_mode=MarginMode.CROSS)
    execution = FakeExecution(ExchangeName.OKX)
    app = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="strategies.fake:Strategy",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=100,
        signal_queue_maxsize=100,
        alert_queue_maxsize=100,
        dry_run=False,
        enable_email_alerts=False,
    )
    runner = LiveRuntimeRunner(
        app_config=app,
        app_context=AppContext(data=object(), execution=object(), state_store=object(), strategy=object(), planner=object(), alerts=object()),
        runtime_config=LiveRuntimeConfig(app=app, mode=RuntimeMode.LIVE_RUNTIME),
        services={
            "account_clients": [account],
            "execution_clients": [execution],
            "runtime_requirements": StrategyRuntimeRequirements.from_mapping({}),
        },
    )

    await runner._bootstrap_account_config_if_enabled()

    assert account.set_margin_mode_calls == [MarginMode.ISOLATED]
    assert account.set_leverage_calls == [(Decimal("15"), MarginMode.ISOLATED)]


class FakeAccount:
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    def __init__(
        self,
        exchange: ExchangeName,
        *,
        leverage: Decimal,
        margin_mode: MarginMode,
        positions=(),
        mismatch_after_set: bool = False,
    ) -> None:
        self.exchange = exchange
        self.leverage = leverage
        self.margin_mode = margin_mode
        self.positions = list(positions)
        self.mismatch_after_set = mismatch_after_set
        self.set_margin_mode_calls: list[MarginMode] = []
        self.set_leverage_calls: list[tuple[Decimal, MarginMode]] = []

    async def fetch_balance(self, asset: str = "USDT") -> Balance:
        return Balance(exchange=self.exchange, asset=asset, total=Decimal("100"), available=Decimal("100"))

    async def fetch_positions(self, symbol: str | None = None) -> list[Position]:
        return list(self.positions)

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
        if not self.mismatch_after_set:
            self.margin_mode = margin_mode
        return {"margin_mode": margin_mode.value}

    async def set_leverage(self, leverage: Decimal, *, margin_mode: MarginMode = MarginMode.CROSS) -> LeverageInfo:
        self.set_leverage_calls.append((leverage, margin_mode))
        if not self.mismatch_after_set:
            self.leverage = leverage
            self.margin_mode = margin_mode
        return await self.fetch_leverage(margin_mode=margin_mode)

    async def fetch_position_mode(self) -> PositionMode:
        return PositionMode.ONE_WAY


class FakeExecution:
    symbol = "ETH-USDT-PERP"
    market_profile = get_market_profile("ETH-USDT-PERP")

    def __init__(self, exchange: ExchangeName, *, open_orders=(), open_stop_orders=()) -> None:
        self.exchange = exchange
        self.open_orders = list(open_orders)
        self.open_stop_orders = list(open_stop_orders)

    async def fetch_open_orders(self) -> list[Order]:
        return list(self.open_orders)

    async def fetch_open_stop_orders(self) -> list[Order]:
        return list(self.open_stop_orders)


def _target(exchange: ExchangeName) -> AccountConfigTarget:
    return AccountConfigTarget(exchange=exchange, symbol="ETH-USDT-PERP", margin_mode=MarginMode.ISOLATED, leverage=Decimal("15"))


def _position(exchange: ExchangeName) -> Position:
    return Position(
        exchange=exchange,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-PERP",
        side=PositionSide.BOTH,
        quantity=Decimal("1"),
    )


def _order(exchange: ExchangeName, order_id: str) -> Order:
    return Order(
        exchange=exchange,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-PERP",
        order_id=order_id,
        client_order_id=None,
        status=OrderStatus.NEW,
    )
