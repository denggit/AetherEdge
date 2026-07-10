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

    async def fetch_instrument_rule(self):
        from src.platform.exchanges.models import InstrumentRule

        return InstrumentRule(
            exchange=self.exchange,
            symbol=SYMBOL,
            raw_symbol=(
                "ETH-USDT-SWAP"
                if self.exchange == ExchangeName.OKX
                else "ETHUSDT"
            ),
            price_tick=Decimal("0.05"),
            quantity_step=Decimal("0.01"),
            min_quantity=Decimal("0.01"),
            contract_value=Decimal("1"),
        )


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
            mf={
                "ok": mf_ok,
                "mf_freshness_mode": "historical_preflight",
                "historical_coverage_ready": mf_ok,
                "live_fresh_ready": False,
                "archive_publish_lag_hours": 8.0,
                "safe_archive_end_ms": 1_782_927_999_999,
                "calendar_safe_archive_end_ms": 1_783_014_399_999,
                "safe_archive_end_okx": "2026-07-05 23:59:59+08",
                "calendar_safe_archive_end_okx": (
                    "2026-07-06 23:59:59+08"
                ),
                "latest_archive_day_deferred": True,
            },
            causal={"ok": causal_ok},
            issues=tuple(
                issue
                for ok, issue in (
                    (lf_ok, "lf_range_aggregate_stale"),
                    (mf_ok, "mf_tradebar_stale"),
                    (causal_ok, "causal_future_violation"),
                )
                if not ok
            ),
        )

    def inspect(self):
        return self.result


class _LfRangeAggregateFailInspector:
    """Simulates LF range aggregate missing — a hard blocker."""

    def __init__(self) -> None:
        self.result = PortfolioV1ReadinessResult(
            lf={"ok": False, "warnings": []},
            mf={
                "ok": True,
                "mf_freshness_mode": "historical_preflight",
                "historical_coverage_ready": True,
                "live_fresh_ready": False,
                "archive_publish_lag_hours": 8.0,
                "safe_archive_end_ms": 1_782_927_999_999,
                "calendar_safe_archive_end_ms": 1_783_014_399_999,
                "safe_archive_end_okx": "2026-07-05 23:59:59+08",
                "calendar_safe_archive_end_okx": (
                    "2026-07-06 23:59:59+08"
                ),
                "latest_archive_day_deferred": True,
            },
            causal={"ok": True},
            issues=("lf_range_aggregate_missing",),
        )

    def inspect(self):
        return self.result


class _LfKlineStaleWarningInspector:
    """Simulates LF closed-kline stale only — non-blocking warning."""

    def __init__(self) -> None:
        self.result = PortfolioV1ReadinessResult(
            lf={
                "ok": True,
                "closed_kline_stale": True,
                "warnings": ["lf_closed_kline_stale"],
            },
            mf={
                "ok": True,
                "mf_freshness_mode": "historical_preflight",
                "historical_coverage_ready": True,
                "live_fresh_ready": False,
                "archive_publish_lag_hours": 8.0,
                "safe_archive_end_ms": 1_782_927_999_999,
                "calendar_safe_archive_end_ms": 1_783_014_399_999,
                "safe_archive_end_okx": "2026-07-05 23:59:59+08",
                "calendar_safe_archive_end_okx": (
                    "2026-07-06 23:59:59+08"
                ),
                "latest_archive_day_deferred": True,
            },
            causal={"ok": True},
            issues=(),
        )

    def inspect(self):
        return self.result


class _MfGapInspector:
    def __init__(self) -> None:
        self.result = PortfolioV1ReadinessResult(
            lf={"ok": True},
            mf={
                "ok": False,
                "mf_freshness_mode": "historical_preflight",
                "historical_coverage_ready": False,
                "live_fresh_ready": False,
            },
            causal={
                "ok": False,
                "lf_closed_bar_not_future": True,
                "lf_range_available_not_future": True,
                "mf_tradebar_available_by_signal": False,
                "mf_range_footprint_available_by_signal": False,
                "mf_fixed_time_footprint_available_by_signal": False,
                "no_future_feature_rows": True,
            },
            issues=(
                "mf_tradebar_stale",
                "causal_future_violation",
            ),
        )

    def inspect(self):
        return self.result


class _OnStartNotReadyStrategy:
    def __init__(self) -> None:
        self.config = _strategy().config
        self.last_mf_signal_audit = {
            "data_ready": False,
            "readiness_source": "test",
        }

    async def on_start(self, snapshot):
        return ()


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
    binance_stops=(),
    inspector=None,
    startup_feature_backfill_enabled=True,
    call_strategy_on_start=False,
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
        if exchange is ExchangeName.OKX:
            stops_for_exchange = okx_stops
        else:
            stops_for_exchange = binance_stops
        executions.append(
            _Execution(
                exchange,
                stops=tuple(stops_for_exchange),
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
        call_strategy_on_start=call_strategy_on_start,
        report_kind="preflight",
        startup_feature_backfill_enabled=(
            startup_feature_backfill_enabled
        ),
    )
    return gate, plan_store


@pytest.mark.asyncio
async def test_explicitly_disabled_mf_backfill_passes_when_mf_ready(
    tmp_path,
) -> None:
    gate, _ = _gate(
        tmp_path,
        startup_feature_backfill_enabled=False,
    )

    report = await gate.run()

    assert report.exit_code == EXIT_PASS
    assert report.ok is True


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


@pytest.mark.asyncio
async def test_lf_and_mf_ready_passes(tmp_path) -> None:
    gate, _ = _gate(tmp_path, inspector=_Inspector())

    report = await gate.run()

    assert report.exit_code == EXIT_PASS
    assert report.ok is True
    assert report.warnings == []
    assert report.mf_data_readiness["sleeve_ready"] is True
    assert report.mf_data_readiness["signals_enabled"] is True


@pytest.mark.asyncio
async def test_mf_not_ready_with_backfill_is_nonblocking_warning(
    tmp_path,
) -> None:
    gate, _ = _gate(
        tmp_path,
        inspector=_MfGapInspector(),
        startup_feature_backfill_enabled=True,
    )

    report = await gate.run()

    assert report.exit_code == EXIT_PASS
    assert report.ok is True
    assert report.issues == []
    assert report.warnings == [
        "mf_data_not_ready_sleeve_disabled_until_ready"
    ]
    assert report.mf_data_readiness["ok"] is False
    assert report.mf_data_readiness["blocking"] is False
    assert report.mf_data_readiness["sleeve_ready"] is False
    assert report.mf_data_readiness["signals_enabled"] is False
    assert report.mf_data_readiness[
        "background_prebuild_required"
    ] is True
    assert report.mf_data_readiness["readiness_scope"] == "mf_sleeve"
    assert "mf_tradebar_stale" in report.mf_data_readiness["issues"]
    assert report.causal_audit["ok"] is False
    assert report.causal_audit["blocking"] is False


@pytest.mark.asyncio
async def test_mf_degraded_on_start_gate_remains_nonblocking(
    tmp_path,
) -> None:
    gate, _ = _gate(
        tmp_path,
        strategy=_OnStartNotReadyStrategy(),
        inspector=_MfGapInspector(),
        startup_feature_backfill_enabled=True,
        call_strategy_on_start=True,
    )

    report = await gate.run()

    assert report.exit_code == EXIT_PASS
    on_start = next(
        check
        for check in report.startup_gate_results
        if check.name == "strategy_on_start_read_only"
    )
    assert on_start.status == "ok"
    assert on_start.detail["mf_signals_enabled"] is False
    assert on_start.detail["mf_not_ready_blocking"] is False


@pytest.mark.asyncio
async def test_mf_not_ready_without_backfill_is_market_data_failure(
    tmp_path,
) -> None:
    gate, _ = _gate(
        tmp_path,
        inspector=_Inspector(mf_ok=False),
        startup_feature_backfill_enabled=False,
    )

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_MARKET_DATA
    assert "mf_tradebar_stale" in report.issues
    assert report.mf_data_readiness["blocking"] is True


@pytest.mark.asyncio
async def test_lf_not_ready_remains_market_data_failure(tmp_path) -> None:
    """LF range aggregate missing is still a hard market-data failure."""
    gate, _ = _gate(
        tmp_path, inspector=_LfRangeAggregateFailInspector()
    )

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_MARKET_DATA
    assert "lf_range_aggregate_missing" in report.issues


@pytest.mark.asyncio
async def test_lf_closed_kline_stale_alone_passes(tmp_path) -> None:
    """When the only LF issue is closed-kline stale, the preflight gate
    passes (ok=true, verdict=pass, exit_code=0).  Runner warmup handles
    kline backfill at startup."""
    gate, _ = _gate(
        tmp_path, inspector=_LfKlineStaleWarningInspector()
    )

    report = await gate.run()

    assert report.exit_code == EXIT_PASS
    assert report.ok is True
    assert report.verdict == "pass"
    assert "lf_closed_kline_stale" not in report.issues
    # The stale kline should be visible in the LF readiness detail.
    assert report.lf_data_readiness["closed_kline_stale"] is True
    assert "lf_closed_kline_stale" in report.lf_data_readiness.get(
        "warnings", []
    )


@pytest.mark.asyncio
async def test_lf_and_mf_not_ready_keeps_lf_hard_and_mf_warning(
    tmp_path,
) -> None:
    """LF range aggregate stale is hard-blocking; MF tradebar stale is
    a warning when backfill is enabled."""
    gate, _ = _gate(
        tmp_path,
        inspector=_Inspector(lf_ok=False, mf_ok=False),
        startup_feature_backfill_enabled=True,
    )

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_MARKET_DATA
    assert "lf_range_aggregate_stale" in report.issues
    assert "mf_tradebar_stale" not in report.issues
    assert report.warnings == [
        "mf_data_not_ready_sleeve_disabled_until_ready"
    ]


@pytest.mark.asyncio
async def test_causal_failure_remains_market_data_failure(tmp_path) -> None:
    gate, _ = _gate(tmp_path, inspector=_Inspector(causal_ok=False))

    report = await gate.run()

    assert report.exit_code == EXIT_FAIL_MARKET_DATA
    assert "causal_future_violation" in report.issues


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
        "generated_at_ms",
        "report_kind",
        "data_exchange",
        "hedge_mode",
        "account_snapshot_summary",
        "position_plan_summary",
        "recovery_audit_summary",
        "lf_data_readiness",
        "mf_data_readiness",
        "causal_audit",
        "startup_gate_results",
        "warnings",
    ):
        assert section in payload
    assert payload["report_kind"] == "preflight"
    assert payload["mf_data_readiness"]["mf_freshness_mode"] == (
        "historical_preflight"
    )
    assert payload["mf_data_readiness"][
        "archive_publish_lag_hours"
    ] == 8.0
    assert payload["mf_data_readiness"]["safe_archive_end_okx"] == (
        "2026-07-05 23:59:59+08"
    )
    assert payload["mf_data_readiness"][
        "calendar_safe_archive_end_okx"
    ] == "2026-07-06 23:59:59+08"
    assert "should-not-leak" not in text
    for sensitive_name in (
        "API_KEY",
        "API_SECRET",
        "PASSPHRASE",
        "EMAIL_PASSWORD",
    ):
        assert sensitive_name not in text


# ═══════════════════════════════════════════════════════════════════════
# Legacy Stop Scope Preflight Tests
# ═══════════════════════════════════════════════════════════════════════


LEGACY_LF_POSITION_ID = "legacy-lf-preflight-001"
LEGACY_THEORETICAL_STOP = Decimal("1738.2542231936259150")
LEGACY_EXCHANGE_STOP = Decimal("1738.25")


def _legacy_lf_plan(store: SqlitePositionPlanStore) -> None:
    """Create an LF PositionPlan with empty stop IDs — the legacy state."""
    store.upsert_position(
        PositionPlan(
            position_id=LEGACY_LF_POSITION_ID,
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=LEGACY_THEORETICAL_STOP,
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
                position_id=LEGACY_LF_POSITION_ID,
                exchange=exchange,
                role=role,
                target_qty_base=Decimal("0.6"),
                filled_qty_base=Decimal("0.6"),
                sync_status=LegSyncStatus.OPEN,
            )
        )


def _legacy_okx_preflight_stop(
    *,
    order_id: str = "1111222233334444",
    client_order_id: str = "AEOKSL0123456789ABCDEF",
    price: Decimal = LEGACY_EXCHANGE_STOP,
) -> Order:
    """Realistic OKX stop with no position_id in raw."""
    return Order(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol="ETH-USDT-SWAP",
        order_id=order_id,
        client_order_id=client_order_id,
        price=price,
        quantity=Decimal("6"),
        side=OrderSide.SELL,
        status=OrderStatus.NEW,
        raw={
            "algoId": order_id,
            "algoClOrdId": client_order_id,
            "instId": "ETH-USDT-SWAP",
            "posSide": "long",
            "side": "sell",
            "sz": "6",
            "slTriggerPx": str(price),
            "reduceOnly": "true",
            "state": "live",
        },
    )


def _legacy_binance_preflight_stop(
    *,
    order_id: str = "5555666677778888",
    client_order_id: str = "AEBISL0123456789ABCDEF",
    price: Decimal = LEGACY_EXCHANGE_STOP,
) -> Order:
    """Realistic Binance stop with no position_id in raw."""
    return Order(
        exchange=ExchangeName.BINANCE,
        symbol=SYMBOL,
        raw_symbol="ETHUSDT",
        order_id=order_id,
        client_order_id=client_order_id,
        price=price,
        quantity=None,
        side=OrderSide.SELL,
        status=OrderStatus.NEW,
        raw={
            "algoId": order_id,
            "clientAlgoId": client_order_id,
            "symbol": "ETHUSDT",
            "positionSide": "LONG",
            "side": "SELL",
            "triggerPrice": str(price),
            "closePosition": "true",
            "algoStatus": "NEW",
        },
    )


@pytest.mark.asyncio
async def test_legacy_stop_adoptable_passes_preflight_with_warning(
    tmp_path,
) -> None:
    """Preflight passes when legacy stop is adoptable, with warning."""
    plan_store = SqlitePositionPlanStore(
        tmp_path / "plan.sqlite3"
    )
    _legacy_lf_plan(plan_store)

    okx_stop = _legacy_okx_preflight_stop()
    binance_stop = _legacy_binance_preflight_stop()

    okx_pos = _position(ExchangeName.OKX, "6")
    binance_pos = _position(ExchangeName.BINANCE, "6")

    gate, _ = _gate(
        tmp_path,
        strategy=_strategy(),
        okx_positions=(okx_pos,),
        binance_positions=(binance_pos,),
        okx_stops=(okx_stop,),
        binance_stops=(binance_stop,),
        call_strategy_on_start=False,
        startup_feature_backfill_enabled=False,
    )
    gate.position_plan_store = plan_store

    report = await gate.run()

    assert report.ok is True
    assert report.verdict == "pass"

    # Check the recovery audit for legacy adoption info
    recovery_audit = report.recovery_audit_summary
    stop_scope = recovery_audit.get("stop_scope_audit", {})
    assert stop_scope.get("ok") is True

    has_legacy_warning = any(
        "legacy_stop_scope_will_be_adopted_during_runtime_recovery"
        in w
        for w in report.warnings
    )
    assert has_legacy_warning, (
        f"Expected legacy adoption warning, got warnings={report.warnings}"
    )

    # No mutations during preflight
    assert not report.mutation_attempted
    assert not report.mutation_attempts


@pytest.mark.asyncio
async def test_legacy_stop_ambiguous_blocks_preflight(tmp_path) -> None:
    """Multiple valid bot stops → ambiguous → preflight blocked."""
    plan_store = SqlitePositionPlanStore(
        tmp_path / "plan.sqlite3"
    )
    _legacy_lf_plan(plan_store)

    stop1 = _legacy_okx_preflight_stop(
        order_id="111", client_order_id="AEOKSL00000000000000A1"
    )
    stop2 = _legacy_okx_preflight_stop(
        order_id="222", client_order_id="AEOKSL00000000000000A2"
    )

    okx_pos = _position(ExchangeName.OKX, "6")
    binance_pos = _position(ExchangeName.BINANCE, "6")

    gate, _ = _gate(
        tmp_path,
        strategy=_strategy(),
        okx_positions=(okx_pos,),
        binance_positions=(binance_pos,),
        okx_stops=(stop1, stop2),
        call_strategy_on_start=False,
        startup_feature_backfill_enabled=False,
    )
    gate.position_plan_store = plan_store

    report = await gate.run()

    # Should fail because ambiguous stop scope
    assert report.ok is False
    assert report.verdict == "fail_recovery"
    assert any(
        "legacy_stop_scope_ambiguous" in issue
        or "unknown_stop_scope" in issue
        for issue in report.issues
    ), f"Expected ambiguous/unknown in issues: {report.issues}"


# ═══════════════════════════════════════════════════════════════════════
# Preflight Margin Mode Tests
# ═══════════════════════════════════════════════════════════════════════


class _MarginTrackingAccount:
    """Fake account that tracks which margin_mode was used in fetch_leverage."""

    def __init__(
        self,
        exchange: ExchangeName,
        *,
        isolated_leverage: int = 15,
        cross_leverage: int = 20,
        mode: PositionMode = PositionMode.HEDGE,
        positions: tuple[Position, ...] = (),
    ) -> None:
        self.exchange = exchange
        self.symbol = SYMBOL
        self.isolated_leverage = isolated_leverage
        self.cross_leverage = cross_leverage
        self.mode = mode
        self.positions = positions
        self.fetch_leverage_calls: list[MarginMode | None] = []

    async def fetch_balance(self, asset="USDT"):
        return Balance(
            exchange=self.exchange,
            asset=asset,
            total=Decimal("10000"),
            available=Decimal("9000"),
        )

    async def fetch_positions(self, *args, **kwargs):
        return list(self.positions)

    async def fetch_leverage(self, margin_mode=MarginMode.CROSS):
        self.fetch_leverage_calls.append(margin_mode)
        leverage = (
            self.isolated_leverage
            if margin_mode is MarginMode.ISOLATED
            else self.cross_leverage
        )
        return LeverageInfo(
            exchange=self.exchange,
            symbol=SYMBOL,
            raw_symbol=SYMBOL,
            leverage=Decimal(str(leverage)),
            margin_mode=margin_mode,
        )

    async def fetch_position_mode(self):
        return self.mode


class _MarginTrackingExecution:
    def __init__(self, exchange: ExchangeName, *, stops=()) -> None:
        self.exchange = exchange
        self.symbol = SYMBOL
        self._sandbox = False
        self._live_trading_enabled = True
        self.stops = tuple(stops)

    async def fetch_open_orders(self):
        return []

    async def fetch_open_stop_orders(self):
        return list(self.stops)

    async def fetch_instrument_rule(self):
        from src.platform.exchanges.models import InstrumentRule

        return InstrumentRule(
            exchange=self.exchange,
            symbol=SYMBOL,
            raw_symbol=(
                "ETH-USDT-SWAP"
                if self.exchange == ExchangeName.OKX
                else "ETHUSDT"
            ),
            price_tick=Decimal("0.05"),
            quantity_step=Decimal("0.01"),
            min_quantity=Decimal("0.01"),
            contract_value=Decimal("1"),
        )


@pytest.mark.asyncio
async def test_preflight_uses_isolated_leverage_from_config(
    tmp_path,
) -> None:
    """Preflight fetches leverage with ISOLATED when MARGIN_MODE=isolated."""
    from src.runtime.account_config import (
        AccountConfigEnv,
        AccountConfigTarget,
    )

    okx_account = _MarginTrackingAccount(ExchangeName.OKX)
    binance_account = _MarginTrackingAccount(ExchangeName.BINANCE)

    okx_exec = _MarginTrackingExecution(ExchangeName.OKX)
    binance_exec = _MarginTrackingExecution(ExchangeName.BINANCE)

    app = _app()
    policy = MasterFollowerPolicyConfig(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
    )
    runtime_config = LiveRuntimeConfig(
        app=app,
        mode=RuntimeMode.LIVE_RUNTIME,
        master_follower_policy=policy,
    )

    plan_store = SqlitePositionPlanStore(
        tmp_path / "plan.sqlite3"
    )
    journal = SqliteOrderJournalStore(
        tmp_path / "journal.sqlite3"
    )

    # Ensure DB files exist (required by _probe_databases)
    db_paths = {
        "state": tmp_path / "state.sqlite3",
        "position_plan": tmp_path / "plan.sqlite3",
        "order_journal": tmp_path / "journal.sqlite3",
        "range_checkpoint": tmp_path / "range_checkpoint.sqlite3",
        "mf_feature": tmp_path / "features.sqlite3",
    }
    for name in ("state", "range_checkpoint", "mf_feature"):
        sqlite3.connect(db_paths[name]).close()

    # Build an AccountConfigEnv with ISOLATED + 15x leverage
    account_config_env = AccountConfigEnv(
        margin_mode=MarginMode.ISOLATED,
        targets=(
            AccountConfigTarget(
                exchange=ExchangeName.OKX,
                symbol=SYMBOL,
                margin_mode=MarginMode.ISOLATED,
                leverage=Decimal("15"),
            ),
            AccountConfigTarget(
                exchange=ExchangeName.BINANCE,
                symbol=SYMBOL,
                margin_mode=MarginMode.ISOLATED,
                leverage=Decimal("15"),
            ),
        ),
    )

    gate = PortfolioV1LiveGate(
        app_config=app,
        runtime_config=runtime_config,
        strategy=_strategy(),
        account_clients=(okx_account, binance_account),
        execution_clients=(okx_exec, binance_exec),
        position_plan_store=plan_store,
        order_journal=journal,
        readiness_inspector=_Inspector(),
        database_paths=db_paths,
        repo_root=tmp_path,
        required_master_exchange=ExchangeName.OKX,
        required_follower_exchange=ExchangeName.BINANCE,
        call_strategy_on_start=False,
        startup_feature_backfill_enabled=False,
        account_config_env=account_config_env,
    )

    report = await gate.run()
    assert report.ok is True

    # Verify fetch_leverage was called with ISOLATED, never CROSS
    for account in (okx_account, binance_account):
        assert len(account.fetch_leverage_calls) >= 1
        for call in account.fetch_leverage_calls:
            assert call is MarginMode.ISOLATED, (
                f"{account.exchange.value}: expected ISOLATED, got {call}"
            )

    # Verify leverage in snapshots
    summary = report.account_snapshot_summary
    for exchange_val in ("okx", "binance"):
        assert summary[exchange_val]["leverage_read"] is True


@pytest.mark.asyncio
async def test_preflight_leverage_matches_account_config_target(
    tmp_path,
) -> None:
    """MARGIN_MODE=isolated, OKX_LEVERAGE=15 → snapshot shows 15."""
    from src.runtime.account_config import (
        AccountConfigEnv,
        AccountConfigTarget,
    )

    okx_account = _MarginTrackingAccount(
        ExchangeName.OKX, isolated_leverage=15, cross_leverage=20
    )
    binance_account = _MarginTrackingAccount(
        ExchangeName.BINANCE, isolated_leverage=15, cross_leverage=20
    )

    okx_exec = _MarginTrackingExecution(ExchangeName.OKX)
    binance_exec = _MarginTrackingExecution(ExchangeName.BINANCE)

    app = _app()
    policy = MasterFollowerPolicyConfig(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
    )
    runtime_config = LiveRuntimeConfig(
        app=app,
        mode=RuntimeMode.LIVE_RUNTIME,
        master_follower_policy=policy,
    )

    plan_store = SqlitePositionPlanStore(
        tmp_path / "plan.sqlite3"
    )
    journal = SqliteOrderJournalStore(
        tmp_path / "journal.sqlite3"
    )

    # Ensure DB files exist
    db_paths = {
        "state": tmp_path / "state.sqlite3",
        "position_plan": tmp_path / "plan.sqlite3",
        "order_journal": tmp_path / "journal.sqlite3",
        "range_checkpoint": tmp_path / "range_checkpoint.sqlite3",
        "mf_feature": tmp_path / "features.sqlite3",
    }
    for name in ("state", "range_checkpoint", "mf_feature"):
        sqlite3.connect(db_paths[name]).close()

    account_config_env = AccountConfigEnv(
        margin_mode=MarginMode.ISOLATED,
        targets=(
            AccountConfigTarget(
                exchange=ExchangeName.OKX,
                symbol=SYMBOL,
                margin_mode=MarginMode.ISOLATED,
                leverage=Decimal("15"),
            ),
            AccountConfigTarget(
                exchange=ExchangeName.BINANCE,
                symbol=SYMBOL,
                margin_mode=MarginMode.ISOLATED,
                leverage=Decimal("15"),
            ),
        ),
    )

    gate = PortfolioV1LiveGate(
        app_config=app,
        runtime_config=runtime_config,
        strategy=_strategy(),
        account_clients=(okx_account, binance_account),
        execution_clients=(okx_exec, binance_exec),
        position_plan_store=plan_store,
        order_journal=journal,
        readiness_inspector=_Inspector(),
        database_paths=db_paths,
        repo_root=tmp_path,
        required_master_exchange=ExchangeName.OKX,
        required_follower_exchange=ExchangeName.BINANCE,
        call_strategy_on_start=False,
        startup_feature_backfill_enabled=False,
        account_config_env=account_config_env,
    )

    report = await gate.run()
    assert report.ok is True

    # ISOLATED leverage should be 15, not CROSS leverage of 20
    for account in (okx_account, binance_account):
        for call in account.fetch_leverage_calls:
            assert call is MarginMode.ISOLATED
            assert call is not MarginMode.CROSS
