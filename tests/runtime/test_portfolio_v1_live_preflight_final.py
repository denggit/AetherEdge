from __future__ import annotations

import sqlite3
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.app import AppConfig
from src.order_management import (
    LegPlan,
    LegRole,
    LegSyncStatus,
    MasterFollowerPolicyConfig,
    PositionPlan,
    PositionPlanStatus,
    SqliteOrderJournalStore,
    SqlitePositionPlanStore,
)
from src.platform import (
    Balance,
    ExchangeName,
    LeverageInfo,
    MarginMode,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    PositionMode,
    PositionSide,
    get_market_profile,
)
from src.runtime import RuntimeMode
from src.runtime.config import LiveRuntimeConfig
from strategies.eth_portfolio_v1.preflight.live_gate import (
    EXIT_FAIL_CONFIG,
    EXIT_FAIL_MARKET_DATA,
    EXIT_FAIL_RECOVERY,
    EXIT_PASS,
    PortfolioV1LiveGate,
    PortfolioV1LiveGateReport,
)
from strategies.eth_portfolio_v1.preflight.readiness import (
    PortfolioV1ReadinessResult,
)


SYMBOL = "ETH-USDT-PERP"


class _Account:
    def __init__(
        self,
        exchange: ExchangeName,
        *,
        mode: PositionMode = PositionMode.HEDGE,
        positions: tuple[Position, ...] = (),
    ) -> None:
        self.exchange = exchange
        self.symbol = SYMBOL
        self.market_profile = get_market_profile(SYMBOL)
        self.mode = mode
        self.positions = positions

    async def fetch_balance(self, asset="USDT"):
        return Balance(
            exchange=self.exchange,
            asset=asset,
            total=Decimal("10000"),
            available=Decimal("9000"),
        )

    async def fetch_positions(self, *args, **kwargs):
        return list(self.positions)

    async def fetch_leverage(self, *args, **kwargs):
        return LeverageInfo(
            exchange=self.exchange,
            symbol=SYMBOL,
            raw_symbol=SYMBOL,
            leverage=Decimal("3"),
            margin_mode=MarginMode.CROSS,
        )

    async def fetch_position_mode(self):
        return self.mode


class _Execution:
    def __init__(self, exchange: ExchangeName, *, stops=()) -> None:
        self.exchange = exchange
        self.symbol = SYMBOL
        self.market_profile = get_market_profile(SYMBOL)
        self._sandbox = False
        self._live_trading_enabled = True
        self.stops = tuple(stops)

    async def fetch_open_orders(self):
        return []

    async def fetch_open_stop_orders(self):
        return list(self.stops)


class _Inspector:
    def __init__(
        self,
        *,
        lf_ok: bool = True,
        mf_ok: bool = True,
        causal_ok: bool = True,
    ) -> None:
        self.result = PortfolioV1ReadinessResult(
            lf={"ok": lf_ok},
            mf={"ok": mf_ok},
            causal={"ok": causal_ok},
            issues=tuple(
                issue
                for ok, issue in (
                    (lf_ok, "lf_closed_kline_stale"),
                    (mf_ok, "mf_tradebar_stale"),
                    (causal_ok, "causal_future_violation"),
                )
                if not ok
            ),
        )

    def inspect(self):
        return self.result


def _app(
    *,
    exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    data_exchange=ExchangeName.OKX,
) -> AppConfig:
    return AppConfig(
        symbol=SYMBOL,
        exchanges=exchanges,
        data_exchange=data_exchange,
        strategy="strategies.eth_portfolio_v1:Strategy",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=False,
        enable_email_alerts=False,
    )


def _strategy(*, mf_enabled: bool = True):
    return SimpleNamespace(
        config=SimpleNamespace(
            strategy_id="eth_portfolio_v1",
            symbol=SYMBOL,
            mf=SimpleNamespace(
                enabled=mf_enabled,
                exit_variant="time48",
            ),
        )
    )


def _position(exchange: ExchangeName, quantity: str) -> Position:
    return Position(
        exchange=exchange,
        symbol=SYMBOL,
        raw_symbol=SYMBOL,
        side=PositionSide.LONG,
        quantity=Decimal(quantity),
        entry_price=Decimal("2000"),
    )


def _gate(
    tmp_path,
    *,
    app: AppConfig | None = None,
    strategy=None,
    okx_mode=PositionMode.HEDGE,
    binance_mode=PositionMode.HEDGE,
    okx_positions=(),
    binance_positions=(),
    okx_stops=(),
    inspector=None,
):
    app = app or _app()
    strategy = strategy or _strategy()
    policy = MasterFollowerPolicyConfig(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
    )
    runtime = LiveRuntimeConfig(
        app=app,
        mode=RuntimeMode.LIVE_RUNTIME,
        master_follower_policy=policy,
    )
    plan_store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    journal = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    db_paths = {
        "state": tmp_path / "state.sqlite3",
        "position_plan": tmp_path / "plans.sqlite3",
        "order_journal": tmp_path / "journal.sqlite3",
        "range_checkpoint": tmp_path / "checkpoint.sqlite3",
        "mf_feature": tmp_path / "features.sqlite3",
    }
    for name in ("state", "range_checkpoint", "mf_feature"):
        sqlite3.connect(db_paths[name]).close()
    accounts = []
    executions = []
    for exchange in app.exchanges:
        if exchange is ExchangeName.OKX:
            accounts.append(
                _Account(
                    exchange,
                    mode=okx_mode,
                    positions=tuple(okx_positions),
                )
            )
        else:
            accounts.append(
                _Account(
                    exchange,
                    mode=binance_mode,
                    positions=tuple(binance_positions),
                )
            )
        executions.append(
            _Execution(
                exchange,
                stops=okx_stops if exchange is ExchangeName.OKX else (),
            )
        )
    gate = PortfolioV1LiveGate(
        app_config=app,
        runtime_config=runtime,
        strategy=strategy,
        account_clients=accounts,
        execution_clients=executions,
        position_plan_store=plan_store,
        order_journal=journal,
        readiness_inspector=inspector or _Inspector(),
        database_paths=db_paths,
        repo_root=tmp_path,
        required_master_exchange=ExchangeName.OKX,
        required_follower_exchange=ExchangeName.BINANCE,
        call_strategy_on_start=False,
    )
    return gate, plan_store


def _save_mf_plan(
    store: SqlitePositionPlanStore,
    *,
    complete_metadata: bool,
) -> None:
    position_id = "mf-low-sweep-time48-preflight-final"
    metadata = {
        "sleeve_id": "mf",
        "position_id": position_id,
        "engine": "MF_LOW_SWEEP_TIME48",
    }
    if complete_metadata:
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
    store.upsert_position(
        PositionPlan(
            position_id=position_id,
            strategy_id="eth_portfolio_v1",
            entry_engine="MF_LOW_SWEEP_TIME48",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=None,
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.4"),
            master_filled_qty_base=Decimal("0.4"),
            metadata=metadata,
        )
    )
    for exchange, role in (
        (ExchangeName.OKX, LegRole.MASTER),
        (ExchangeName.BINANCE, LegRole.FOLLOWER),
    ):
        store.upsert_leg(
            LegPlan(
                position_id=position_id,
                exchange=exchange,
                role=role,
                target_qty_base=Decimal("0.4"),
                filled_qty_base=Decimal("0.4"),
                sync_status=LegSyncStatus.OPEN,
            )
        )


@pytest.mark.asyncio
async def test_mf_disabled_fails_config(tmp_path) -> None:
    gate, _ = _gate(
        tmp_path,
        strategy=_strategy(mf_enabled=False),
    )

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_CONFIG
    assert "mf_must_be_enabled" in report.issues


@pytest.mark.asyncio
async def test_missing_binance_exchange_fails_config(tmp_path) -> None:
    gate, _ = _gate(
        tmp_path,
        app=_app(exchanges=(ExchangeName.OKX,)),
    )

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_CONFIG
    assert "required_master_and_follower_exchanges_missing" in report.issues


@pytest.mark.asyncio
async def test_non_okx_data_exchange_fails_config(tmp_path) -> None:
    gate, _ = _gate(
        tmp_path,
        app=_app(data_exchange=ExchangeName.BINANCE),
    )

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_CONFIG
    assert "data_exchange_must_match_required_master" in report.issues


@pytest.mark.asyncio
async def test_hedge_mode_failure_is_nonzero(tmp_path) -> None:
    gate, _ = _gate(
        tmp_path,
        binance_mode=PositionMode.ONE_WAY,
    )

    report = await gate.run()

    assert report.exit_code != EXIT_PASS
    assert "binance_hedge_mode_required" in report.issues


@pytest.mark.asyncio
async def test_exchange_position_without_local_plan_fails(tmp_path) -> None:
    gate, _ = _gate(
        tmp_path,
        okx_positions=(_position(ExchangeName.OKX, "4"),),
    )

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_RECOVERY
    assert "exchange_position_without_local_plan" in report.issues


@pytest.mark.asyncio
async def test_mf_active_missing_time_metadata_fails(tmp_path) -> None:
    positions = (
        _position(ExchangeName.OKX, "4"),
        _position(ExchangeName.BINANCE, "0.4"),
    )
    gate, store = _gate(
        tmp_path,
        okx_positions=(positions[0],),
        binance_positions=(positions[1],),
    )
    _save_mf_plan(store, complete_metadata=False)

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_RECOVERY
    assert any(
        "mf_missing_metadata:entry_execution_time_ms" in issue
        for issue in report.issues
    )


@pytest.mark.asyncio
async def test_lf_missing_required_stop_fails(tmp_path) -> None:
    gate, store = _gate(
        tmp_path,
        okx_positions=(_position(ExchangeName.OKX, "6"),),
        binance_positions=(_position(ExchangeName.BINANCE, "0.6"),),
    )
    position_id = "v9e-lf-final"
    store.upsert_position(
        PositionPlan(
            position_id=position_id,
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=Decimal("1900"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.6"),
            master_filled_qty_base=Decimal("0.6"),
            metadata={"sleeve_id": "lf"},
        )
    )
    for exchange, role in (
        (ExchangeName.OKX, LegRole.MASTER),
        (ExchangeName.BINANCE, LegRole.FOLLOWER),
    ):
        store.upsert_leg(
            LegPlan(
                position_id=position_id,
                exchange=exchange,
                role=role,
                target_qty_base=Decimal("0.6"),
                filled_qty_base=Decimal("0.6"),
                sync_status=LegSyncStatus.OPEN,
            )
        )

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_RECOVERY
    assert any(
        issue.startswith("lf_required_stop_missing")
        for issue in report.issues
    )


@pytest.mark.asyncio
async def test_unknown_stop_scope_fails_recovery_audit(tmp_path) -> None:
    unknown_stop = Order(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol=SYMBOL,
        order_id="unscoped-stop",
        client_order_id=None,
        status=OrderStatus.NEW,
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        price=Decimal("1900"),
        quantity=Decimal("1"),
        raw={"reduceOnly": "true", "posSide": "long"},
    )
    gate, _ = _gate(tmp_path, okx_stops=(unknown_stop,))

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_RECOVERY
    assert any(
        issue.startswith("unknown_stop_scope")
        for issue in report.issues
    )


@pytest.mark.asyncio
async def test_mf_explicit_no_stop_policy_does_not_fail(tmp_path) -> None:
    gate, store = _gate(
        tmp_path,
        okx_positions=(_position(ExchangeName.OKX, "4"),),
        binance_positions=(_position(ExchangeName.BINANCE, "0.4"),),
    )
    _save_mf_plan(store, complete_metadata=True)

    report = await gate.run()

    assert report.exit_code == EXIT_PASS
    assert report.recovery_audit_summary["stop_scope_audit"][
        "mf_protective_stop_required"
    ] is False


@pytest.mark.parametrize(
    ("inspector", "expected_issue"),
    (
        (_Inspector(lf_ok=False), "lf_closed_kline_stale"),
        (_Inspector(mf_ok=False), "mf_tradebar_stale"),
        (_Inspector(causal_ok=False), "causal_future_violation"),
    ),
)
@pytest.mark.asyncio
async def test_readiness_or_causal_failure_is_market_data_failure(
    tmp_path,
    inspector,
    expected_issue,
) -> None:
    gate, _ = _gate(tmp_path, inspector=inspector)

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_MARKET_DATA
    assert expected_issue in report.issues


@pytest.mark.asyncio
async def test_report_has_required_sections_and_no_secrets(tmp_path) -> None:
    gate, _ = _gate(tmp_path)

    report = await gate.run()
    report.add(
        "secret-test",
        ok=True,
        detail={
            "API_KEY": "should-not-leak",
            "API_SECRET": "should-not-leak",
            "PASSPHRASE": "should-not-leak",
            "EMAIL_PASSWORD": "should-not-leak",
        },
    )
    text = report.to_json()
    payload = report.to_dict()

    for section in (
        "hedge_mode",
        "account_snapshot_summary",
        "position_plan_summary",
        "recovery_audit_summary",
        "lf_data_readiness",
        "mf_data_readiness",
        "causal_audit",
        "startup_gate_results",
    ):
        assert section in payload
    assert "should-not-leak" not in text
    for sensitive_name in (
        "API_KEY",
        "API_SECRET",
        "PASSPHRASE",
        "EMAIL_PASSWORD",
    ):
        assert sensitive_name not in text
