#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""V8 live preflight check.

Read-only checks before starting ``bash scripts/start_live_watchdog.sh``. This
script does not place, cancel, amend, or close orders.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.app import AppConfig
from src.market_data.derived import RangeBarBuilder
from src.market_data.models import TimeRange
from src.market_data.storage import SqliteRangeBarStore
from src.market_data.warmup.gap_detector import interval_to_ms
from src.order_management.position_plan.store import SqlitePositionPlanStore
from src.order_management.quantity import NativeQuantityConverter
from src.order_management.safety import RecoveryExitOrderValidator
from src.platform import ExchangeName
from src.platform.account.factory import create_account_client
from src.platform.data.factory import create_market_data_feed
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.execution.factory import create_execution_client
from src.platform.exchanges.models import ExchangeConfig, MarginMode, Position, PositionMode, PositionSide
from src.platform.markets import get_market_profile
from src.runtime import RuntimeMode, live_runtime_config_from_app, runtime_mode_from_env
from src.runtime.requirements import resolve_strategy_runtime_requirements
from src.runtime.tasks.scheduler import closed_bar_open_time_ms
from src.strategy import load_strategy
from src.utils.log import get_logger

logger = get_logger(__name__)


@dataclass
class CheckResult:
    name: str
    status: str
    detail: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass
class PreflightReport:
    started_time_ms: int
    symbol: str | None = None
    strategy: str | None = None
    runtime_mode: str | None = None
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(item.status == "fail" for item in self.checks)

    def add(self, name: str, status: str, *, detail: dict[str, Any] | None = None, error: str | None = None) -> None:
        self.checks.append(CheckResult(name=name, status=status, detail=detail or {}, error=error))
        prefix = {"ok": "[ok]", "warn": "[warn]", "fail": "[fail]"}.get(status, "[info]")
        msg = f"{prefix} {name}"
        if error:
            msg += f": {error}"
        if status == "fail":
            logger.error(msg)
        elif status == "warn":
            logger.warning(msg)
        else:
            logger.info(msg)

    def to_json(self) -> str:
        payload = asdict(self)
        payload["ok"] = self.ok
        payload["summary"] = {
            "ok": sum(1 for item in self.checks if item.status == "ok"),
            "warn": sum(1 for item in self.checks if item.status == "warn"),
            "fail": sum(1 for item in self.checks if item.status == "fail"),
        }
        return json.dumps(payload, ensure_ascii=False, indent=2, default=str)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only V8 live preflight check. No orders are placed or canceled.")
    parser.add_argument("--defaults", default="config/aether_defaults.json", help="Defaults JSON path")
    parser.add_argument("--env-file", default=None, help="Optional .env path")
    parser.add_argument("--report", default="data/state/v8_live_preflight_report.json", help="Output JSON report path")
    parser.add_argument("--expect-real-live", action="store_true", help="Fail unless dry_run=false, live_trading=true, and exchange sandbox flags are false")
    parser.add_argument("--allow-recovery-start", action="store_true", help="Allow existing recoverable live master position/stop state instead of requiring flat start")
    parser.add_argument("--expect-recovery-live", action="store_true", help="Alias for --allow-recovery-start")
    parser.add_argument("--allow-existing-position", action="store_true", help="Do not fail when positions already exist")
    parser.add_argument("--allow-open-orders", action="store_true", help="Do not fail when open regular/stop orders already exist")
    parser.add_argument("--skip-api", action="store_true", help="Skip exchange REST read checks")
    parser.add_argument("--skip-kline", action="store_true", help="Skip latest closed 4H kline read check")
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    report = PreflightReport(started_time_ms=_now_ms())

    try:
        app = AppConfig.from_env(defaults_path=args.defaults, env_file=args.env_file)
        runtime_mode = runtime_mode_from_env(defaults_path=args.defaults, env_file=args.env_file)
        runtime = live_runtime_config_from_app(app, defaults_path=args.defaults, env_file=args.env_file)
        strategy = load_strategy(app.strategy)
        requirements = resolve_strategy_runtime_requirements(strategy, fallback_data_streams=app.data_streams)
    except Exception as exc:
        report.add("load_config_and_strategy", "fail", error=str(exc))
        await _write_report(args.report, report)
        logger.info("V8 live preflight report | report=%s", report.to_json())
        return 1

    report.symbol = app.symbol
    report.strategy = app.strategy
    report.runtime_mode = runtime_mode.value

    _check_runtime_config(report, app=app, runtime_mode=runtime_mode, runtime=runtime, requirements=requirements, expect_real_live=args.expect_real_live)
    _check_strategy_identity(report, strategy)
    _check_local_writable(report, app=app)

    if not args.skip_api:
        allow_recovery_start = bool(args.allow_recovery_start or args.expect_recovery_live)
        strategy_id = getattr(getattr(strategy, "config", None), "strategy_id", None)
        await _check_exchange_read_apis(
            report,
            app=app,
            runtime=runtime,
            allow_existing_position=args.allow_existing_position or allow_recovery_start,
            allow_open_orders=args.allow_open_orders or allow_recovery_start,
            allow_recovery_start=allow_recovery_start,
            strategy_id=strategy_id,
        )
        await _check_follower_min_notional_balance(report, app=app, runtime=runtime)
    if not args.skip_kline:
        await _check_latest_closed_kline(report, app=app, runtime=runtime)
        report.add(
            "current_4h_trade_warmup_api_coverage",
            "ok",
            detail={
                "trades_warmup": False,
                "removed": True,
                "startup_behavior": "live-only trades; current partial 4H range bucket uses micro NO_CONTEXT until a fully captured bucket is available",
            },
        )
    _check_local_rangebar_builder(report, app=app)

    await _write_report(args.report, report)
    logger.info("V8 live preflight report | report=%s", report.to_json())
    return 0 if report.ok else 1


def _check_runtime_config(report: PreflightReport, *, app: AppConfig, runtime_mode: RuntimeMode, runtime, requirements, expect_real_live: bool) -> None:
    detail = {
        "symbol": app.symbol,
        "strategy": app.strategy,
        "runtime_mode": runtime_mode.value,
        "exchanges": [exchange.value for exchange in app.exchanges],
        "data_exchange": app.data_exchange.value,
        "data_streams": list(app.data_streams),
        "dry_run": app.dry_run,
        "live_trading_env": os.getenv("AETHER_LIVE_TRADING"),
    }
    if runtime_mode is not RuntimeMode.LIVE_RUNTIME:
        report.add("runtime_mode_live_runtime", "fail", detail=detail, error="AETHER_RUNTIME_MODE must be live_runtime for V8 live")
    else:
        report.add("runtime_mode_live_runtime", "ok", detail=detail)

    if app.strategy != "strategies.eth_lf_portfolio_v8:Strategy":
        report.add("v8_strategy_configured", "fail", detail=detail, error="AETHER_STRATEGY must be strategies.eth_lf_portfolio_v8:Strategy")
    else:
        report.add("v8_strategy_configured", "ok", detail=detail)

    req_detail = {
        "closed_kline": requirements.closed_kline.enabled,
        "closed_kline_interval": requirements.closed_kline.interval,
        "trades": requirements.trades.enabled,
        "trades_stream": requirements.trades.stream_enabled,
        "trades_warmup": requirements.trades.warmup_enabled,
        "range_bars": requirements.range_bars.enabled,
        "range_pct": str(requirements.range_bars.range_pct),
        "order_book": requirements.order_book.enabled,
        "private_account_stream": requirements.private_account_stream.enabled,
        "account_state_poll": requirements.account_state.poll_enabled,
        "account_state_poll_interval_seconds": requirements.account_state.poll_interval_seconds,
        "order_state_poll_when_position": requirements.order_state.poll_when_position_enabled,
        "order_state_poll_interval_seconds": requirements.order_state.poll_interval_seconds,
    }
    if (
        not requirements.closed_kline.enabled
        or requirements.closed_kline.interval.lower() != "4h"
        or not requirements.trades.stream_enabled
        or not requirements.range_bars.enabled
        or requirements.order_book.enabled
        or requirements.private_account_stream.enabled
        or not requirements.account_state.poll_enabled
        or requirements.account_state.poll_interval_seconds != 300
        or not requirements.order_state.poll_when_position_enabled
        or requirements.order_state.poll_interval_seconds != 20
    ):
        report.add("v8_runtime_requirements", "fail", detail=req_detail, error="V8 requirements must be closed 4H + live trades + range bars + request account/order sync, without order_book/private account stream")
    else:
        report.add("v8_runtime_requirements", "ok", detail=req_detail)

    if runtime.master_follower_policy is None:
        report.add("master_follower_policy", "fail", error="master/follower policy did not resolve")
    else:
        policy_detail = {
            "master": runtime.master_follower_policy.master_exchange.value,
            "followers": [exchange.value for exchange in runtime.master_follower_policy.follower_exchanges],
            "entry_deviation_alert_pct": str(runtime.master_follower_policy.entry_deviation_alert_pct),
        }
        if runtime.master_follower_policy.master_exchange not in app.exchanges:
            report.add("master_follower_policy", "fail", detail=policy_detail, error="master is not in AETHER_EXCHANGES")
        else:
            report.add("master_follower_policy", "ok", detail=policy_detail)

    live_trading = _bool(os.getenv("AETHER_LIVE_TRADING", "false"))
    sandbox_values = {exchange.value: _bool(os.getenv(f"{exchange.value.upper()}_SANDBOX", "true")) for exchange in app.exchanges}
    if expect_real_live:
        if app.dry_run or not live_trading or any(sandbox_values.values()):
            report.add("real_live_safety_switches", "fail", detail={"dry_run": app.dry_run, "live_trading": live_trading, "sandbox": sandbox_values}, error="expected real live: set AETHER_DRY_RUN=false, AETHER_LIVE_TRADING=true, and *_SANDBOX=false")
        else:
            report.add("real_live_safety_switches", "ok", detail={"dry_run": app.dry_run, "live_trading": live_trading, "sandbox": sandbox_values})
    else:
        status = "warn" if app.dry_run or not live_trading else "ok"
        report.add("live_safety_switches", status, detail={"dry_run": app.dry_run, "live_trading": live_trading, "sandbox": sandbox_values})


def _check_strategy_identity(report: PreflightReport, strategy: Any) -> None:
    strategy_id = getattr(getattr(strategy, "config", None), "strategy_id", None)
    if strategy_id != "eth_lf_portfolio_v9c_reclaim_priority":
        report.add("strategy_id_v9c", "fail", detail={"strategy_id": strategy_id}, error="strategy_id must be eth_lf_portfolio_v9c_reclaim_priority")
    else:
        report.add("strategy_id_v9c", "ok", detail={"strategy_id": strategy_id})


def _check_local_writable(report: PreflightReport, *, app: AppConfig) -> None:
    _check_writable_file(report, "state_db_writable", Path(app.state_db_path))
    journal_path = Path(os.getenv("AETHER_ORDER_JOURNAL_DB", "data/state/aether_order_journal.sqlite3"))
    _check_writable_file(report, "order_journal_db_writable", journal_path)


def _check_writable_file(report: PreflightReport, name: str, path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS _preflight_write_check (id INTEGER PRIMARY KEY, ts INTEGER)")
            conn.execute("INSERT INTO _preflight_write_check(ts) VALUES (?)", (_now_ms(),))
            conn.execute("DELETE FROM _preflight_write_check WHERE id NOT IN (SELECT MAX(id) FROM _preflight_write_check)")
        report.add(name, "ok", detail={"path": str(path)})
    except Exception as exc:
        report.add(name, "fail", detail={"path": str(path)}, error=str(exc))


async def _check_exchange_read_apis(
    report: PreflightReport,
    *,
    app: AppConfig,
    runtime,
    allow_existing_position: bool,
    allow_open_orders: bool,
    allow_recovery_start: bool = False,
    strategy_id: str | None = None,
) -> None:
    data_feed = create_market_data_feed(
        app.data_exchange,
        symbol=app.symbol,
        config=ExchangeConfig.from_env(app.data_exchange),
        enable_trade_stream=False,
        enable_order_book_stream=False,
    )
    await _step(report, "data_exchange_ticker", app.data_exchange, data_feed.fetch_ticker)

    snapshots: dict[ExchangeName, dict[str, Any]] = {}
    for exchange in app.exchanges:
        account = create_account_client(exchange, symbol=app.symbol, config=ExchangeConfig.from_env(exchange))
        execution = create_execution_client(exchange, symbol=app.symbol, config=ExchangeConfig.from_env(exchange))
        balance = await _step(report, "fetch_balance", exchange, account.fetch_balance, "USDT")
        positions = await _step(report, "fetch_positions", exchange, account.fetch_positions)
        leverage = await _step(report, "fetch_leverage", exchange, account.fetch_leverage, margin_mode=MarginMode.ISOLATED)
        mode = await _step(report, "fetch_position_mode", exchange, account.fetch_position_mode)
        open_orders = await _step(report, "fetch_open_orders", exchange, execution.fetch_open_orders)
        open_stop_orders = await _step(report, "fetch_open_stop_orders", exchange, execution.fetch_open_stop_orders)
        snapshots[exchange] = {
            "positions": positions or [],
            "open_orders": open_orders or [],
            "open_stop_orders": open_stop_orders or [],
            "position_mode": mode or PositionMode.ONE_WAY,
        }

        if balance is not None:
            available = getattr(balance, "available", Decimal("0"))
            status = "ok" if available > 0 else "warn"
            report.add(f"balance_available:{exchange.value}", status, detail={"available": str(available), "asset": getattr(balance, "asset", "USDT")})
        if leverage is not None:
            report.add(f"leverage_read:{exchange.value}", "ok", detail={"leverage": str(leverage.leverage), "margin_mode": None if leverage.margin_mode is None else leverage.margin_mode.value})
        if mode is not None:
            report.add(f"position_mode_read:{exchange.value}", "ok", detail={"position_mode": mode.value})

        active_positions = [pos for pos in (positions or []) if getattr(pos, "quantity", Decimal("0")) != 0]
        if active_positions:
            status = "warn" if allow_existing_position else "fail"
            report.add(f"no_existing_position:{exchange.value}", status, detail={"positions": [_position_payload(pos) for pos in active_positions]}, error=None if allow_existing_position else "existing position found")
        else:
            report.add(f"no_existing_position:{exchange.value}", "ok")

        open_count = len(open_orders or [])
        stop_count = len(open_stop_orders or [])
        if open_count or stop_count:
            status = "warn" if allow_open_orders else "fail"
            report.add(f"no_open_orders:{exchange.value}", status, detail={"open_orders": open_count, "open_stop_orders": stop_count}, error=None if allow_open_orders else "open regular/stop orders found")
        else:
            report.add(f"no_open_orders:{exchange.value}", "ok")

    if allow_recovery_start:
        _check_recovery_start_state(
            report,
            app=app,
            runtime=runtime,
            snapshots=snapshots,
            strategy_id=strategy_id,
        )


def _check_recovery_start_state(
    report: PreflightReport,
    *,
    app: AppConfig,
    runtime,
    snapshots: dict[ExchangeName, dict[str, Any]],
    strategy_id: str | None,
) -> None:
    policy = getattr(runtime, "master_follower_policy", None)
    master_exchange = getattr(policy, "master_exchange", app.data_exchange)
    master_snapshot = snapshots.get(master_exchange)
    if master_snapshot is None:
        report.add("recovery_start", "fail", error=f"master snapshot missing: {master_exchange.value}")
        return

    active_master_positions = _active_positions(master_snapshot.get("positions", ()))
    active_master = active_master_positions[0] if active_master_positions else None
    active_plans = _load_active_position_plan_payloads(report)
    active_plan = _find_recovery_plan(active_plans, master_exchange=master_exchange)
    _check_recoverable_stale_local_orders(report, app=app, snapshots=snapshots)

    if active_master is None:
        if active_plans:
            report.add("recovery_start", "warn", detail={"active_plans": len(active_plans)}, error="active plan exists but master has no active position; startup reconciliation should resolve follower/plan state")
        else:
            report.add("recovery_start", "ok", detail={"mode": "flat"})
        return

    detail = {
        "master_exchange": master_exchange.value,
        "position": _position_payload(active_master),
        "active_plans": len(active_plans),
    }
    if active_plan is None:
        report.add("recovery_start", "fail", detail=detail, error="active_position_without_plan")
        return

    position_payload = dict(active_plan.get("position", {}))
    canonical_stop = _decimal_or_none(position_payload.get("canonical_stop_price"))
    detail["position_id"] = position_payload.get("position_id")
    detail["canonical_stop_price"] = None if canonical_stop is None else str(canonical_stop)
    if canonical_stop is None:
        report.add("recovery_start", "fail", detail=detail, error="missing_canonical_stop")
        return

    plan_side = str(position_payload.get("side") or "").strip().lower()
    actual_side = _position_side(active_master)
    detail["plan_side"] = plan_side
    detail["actual_side"] = None if actual_side is None else actual_side.value
    if actual_side is None or plan_side not in {"long", "short"}:
        report.add("recovery_start", "fail", detail=detail, error="reverse_position_manual_required")
        return
    if plan_side != actual_side.value:
        report.add("recovery_start", "fail", detail=detail, error="reverse_position_manual_required")
        return

    try:
        market_profile = get_market_profile(app.symbol)
        validation = RecoveryExitOrderValidator(quantity_converter=NativeQuantityConverter()).validate_stop_orders(
            exchange=master_exchange,
            symbol=app.symbol,
            strategy_id=strategy_id or str(position_payload.get("strategy_id") or ""),
            position_id=str(position_payload.get("position_id") or ""),
            position_side=actual_side,
            position_mode=master_snapshot.get("position_mode") or PositionMode.ONE_WAY,
            current_position_native_quantity=abs(active_master.quantity),
            canonical_stop_price=canonical_stop,
            open_stop_orders=master_snapshot.get("open_stop_orders", ()),
            open_orders=master_snapshot.get("open_orders", ()),
            market_profile=market_profile,
        )
    except Exception as exc:
        report.add("recovery_start", "fail", detail=detail, error=str(exc))
        return

    detail.update(
        {
            "valid_bot_stops": len(validation.valid_bot_owned_orders),
            "invalid_bot_stops": len(validation.invalid_bot_owned_orders),
            "unknown_exit_orders": len(validation.unknown_exit_orders),
            "expected_native_quantity": str(validation.expected_native_quantity),
            "current_position_base_quantity": str(validation.current_position_base_quantity),
        }
    )
    if validation.has_unknown_exit_orders or validation.unsupported_bot_exit_orders:
        report.add("recovery_start", "fail", detail=detail, error="unknown_stop_blocks_recovery")
        return
    if validation.should_keep_existing_stop:
        report.add("recovery_start", "ok", detail=detail)
        return
    if validation.should_place_new_stop:
        report.add("recovery_start", "warn", detail=detail, error="stop_missing_but_recoverable")
        return
    report.add("recovery_start", "warn", detail=detail, error="stop_invalid_but_recoverable")


def _load_active_position_plan_payloads(report: PreflightReport) -> list[dict[str, Any]]:
    try:
        path = Path(os.getenv("AETHER_POSITION_PLAN_DB", "data/state/aether_position_plan.sqlite3"))
        store = SqlitePositionPlanStore(path)
        plans = store.serialize_active_positions()
        report.add("recovery_position_plan_store", "ok", detail={"path": str(path), "active_plans": len(plans)})
        return plans
    except Exception as exc:
        report.add("recovery_position_plan_store", "fail", error=str(exc))
        return []


def _find_recovery_plan(plans: Iterable[dict[str, Any]], *, master_exchange: ExchangeName) -> dict[str, Any] | None:
    for item in plans:
        position = dict(item.get("position", {}))
        status = str(position.get("status") or "").strip().lower()
        if status not in {"active", "manual_required", "master_active_plan_unknown"}:
            continue
        if str(position.get("master_exchange") or "").strip().lower() == master_exchange.value:
            return item
    return None


def _check_recoverable_stale_local_orders(report: PreflightReport, *, app: AppConfig, snapshots: dict[ExchangeName, dict[str, Any]]) -> None:
    path = Path(app.state_db_path)
    if not path.exists():
        report.add("recovery_stale_local_orders", "ok", detail={"path": str(path), "state_db_exists": False})
        return
    try:
        total_stale = 0
        detail: dict[str, Any] = {"path": str(path), "by_exchange": {}}
        with sqlite3.connect(path) as conn:
            exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'").fetchone()
            if exists is None:
                report.add("recovery_stale_local_orders", "ok", detail={"path": str(path), "orders_table_exists": False})
                return
            for exchange, snapshot in snapshots.items():
                live_regular = _order_keys(snapshot.get("open_orders", ()))
                live_stops = _order_keys(snapshot.get("open_stop_orders", ()))
                stale_regular = _count_stale_local_open_orders(conn, exchange=exchange, symbol=app.symbol, is_stop_order=False, live_keys=live_regular)
                stale_stops = _count_stale_local_open_orders(conn, exchange=exchange, symbol=app.symbol, is_stop_order=True, live_keys=live_stops)
                total_stale += stale_regular + stale_stops
                detail["by_exchange"][exchange.value] = {"regular": stale_regular, "stop": stale_stops}
        status = "warn" if total_stale else "ok"
        report.add("recovery_stale_local_orders", status, detail=detail, error="stale_local_orders_can_be_auto_closed" if total_stale else None)
    except Exception as exc:
        report.add("recovery_stale_local_orders", "fail", detail={"path": str(path)}, error=str(exc))


def _count_stale_local_open_orders(
    conn: sqlite3.Connection,
    *,
    exchange: ExchangeName,
    symbol: str,
    is_stop_order: bool,
    live_keys: set[tuple[str, str]],
) -> int:
    rows = conn.execute(
        """
        SELECT COALESCE(order_id, ''), COALESCE(client_order_id, '')
        FROM orders
        WHERE exchange = ?
          AND symbol = ?
          AND is_stop_order = ?
          AND status IN ('new', 'partially_filled', 'unknown')
        """,
        (exchange.value, symbol, 1 if is_stop_order else 0),
    ).fetchall()
    return sum(1 for order_id, client_order_id in rows if (str(order_id or ""), str(client_order_id or "")) not in live_keys)


def _order_keys(orders: Iterable[Any]) -> set[tuple[str, str]]:
    return {(str(getattr(order, "order_id", "") or ""), str(getattr(order, "client_order_id", "") or "")) for order in orders}


def _active_positions(positions: Iterable[Position]) -> list[Position]:
    return [position for position in positions if getattr(position, "quantity", Decimal("0")) != 0]


def _position_side(position: Position) -> PositionSide | None:
    if position.side in {PositionSide.LONG, PositionSide.SHORT}:
        return position.side
    if position.quantity > 0:
        return PositionSide.LONG
    if position.quantity < 0:
        return PositionSide.SHORT
    return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    result = Decimal(str(value))
    if result <= 0:
        return None
    return result


async def _check_latest_closed_kline(report: PreflightReport, *, app: AppConfig, runtime) -> None:
    try:
        interval_ms = interval_to_ms(runtime.closed_bar_interval)
        open_time_ms = closed_bar_open_time_ms(int(time.time() * 1000), interval_ms=interval_ms, close_buffer_ms=runtime.closed_bar_buffer_ms)
        data_feed = create_market_data_feed(
            app.data_exchange,
            symbol=app.symbol,
            config=ExchangeConfig.from_env(app.data_exchange),
            enable_trade_stream=False,
            enable_order_book_stream=False,
        )
        rows = await data_feed.fetch_klines(
            interval=runtime.closed_bar_interval,
            limit=10,
            use_cache=False,
            oldest_first=True,
        )
        closed = [row for row in rows if row.is_closed and row.open_time_ms == open_time_ms]
        if not closed:
            report.add("latest_closed_4h_kline", "fail", detail={"expected_open_time_ms": open_time_ms, "rows": len(rows)}, error="latest closed 4H kline not returned")
            return
        row = closed[-1]
        report.add("latest_closed_4h_kline", "ok", detail={"open_time_ms": row.open_time_ms, "close_time_ms": row.close_time_ms, "close": str(row.close)})
    except Exception as exc:
        report.add("latest_closed_4h_kline", "fail", error=str(exc))


async def _check_follower_min_notional_balance(report: PreflightReport, *, app: AppConfig, runtime) -> None:
    try:
        policy = runtime.master_follower_policy
        if policy is None:
            report.add("follower_min_notional_balance", "warn", error="master/follower policy is not configured")
            return
        data_feed = create_market_data_feed(
            app.data_exchange,
            symbol=app.symbol,
            config=ExchangeConfig.from_env(app.data_exchange),
            enable_trade_stream=False,
            enable_order_book_stream=False,
        )
        ticker = await data_feed.fetch_ticker()
        profile = data_feed.market_profile
        for follower in policy.follower_exchanges:
            account = create_account_client(follower, symbol=app.symbol, config=ExchangeConfig.from_env(follower))
            balance = await account.fetch_balance("USDT")
            min_qty = profile.min_quantity(follower) or Decimal("0")
            min_notional = min_qty * ticker.price
            status = "ok" if balance.available >= min_notional else "fail"
            report.add(
                f"follower_min_notional_balance:{follower.value}",
                status,
                detail={"available": str(balance.available), "min_quantity": str(min_qty), "ticker_price": str(ticker.price), "estimated_min_notional": str(min_notional)},
                error=None if status == "ok" else "follower balance is below estimated minimum notional",
            )
    except Exception as exc:
        report.add("follower_min_notional_balance", "fail", error=str(exc))


def _check_local_rangebar_builder(report: PreflightReport, *, app: AppConfig) -> None:
    try:
        data_feed = create_market_data_feed(
            app.data_exchange,
            symbol=app.symbol,
            config=ExchangeConfig.from_env(app.data_exchange),
            enable_trade_stream=False,
            enable_order_book_stream=False,
        )
        contract_value = data_feed.market_profile.contract_value(app.data_exchange) or Decimal("1")
        builder = RangeBarBuilder(range_pct=Decimal("0.002"), contract_value=contract_value)
        trade1 = MarketTrade(exchange=app.data_exchange, symbol=app.symbol, raw_symbol=app.symbol, price=Decimal("100"), quantity=Decimal("1"), side=TradeSide.BUY, trade_time_ms=1)
        trade2 = MarketTrade(exchange=app.data_exchange, symbol=app.symbol, raw_symbol=app.symbol, price=Decimal("100.2"), quantity=Decimal("1"), side=TradeSide.SELL, trade_time_ms=2)
        builder.on_trade(trade1)
        closed = builder.on_trade(trade2)
        if not closed:
            report.add("rangebar_builder_local", "fail", detail={"contract_value": str(contract_value)}, error="rangebar did not close on 0.2% move")
        else:
            report.add("rangebar_builder_local", "ok", detail={"contract_value": str(contract_value), "closed_bars": len(closed)})
    except Exception as exc:
        report.add("rangebar_builder_local", "fail", error=str(exc))


async def _step(report: PreflightReport, name: str, exchange: ExchangeName, func: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any | None:
    try:
        result = await func(*args, **kwargs)
        report.add(f"{name}:{exchange.value}", "ok", detail=_to_detail(result))
        return result
    except Exception as exc:
        report.add(f"{name}:{exchange.value}", "fail", error=str(exc))
        return None


def _to_detail(result: Any) -> dict[str, Any]:
    if result is None:
        return {}
    if isinstance(result, Sequence) and not isinstance(result, (str, bytes, bytearray)):
        return {"items": len(result)}
    values: dict[str, Any] = {}
    for key in ("exchange", "symbol", "asset", "available", "total", "price", "leverage", "margin_mode", "raw_symbol"):
        if hasattr(result, key):
            value = getattr(result, key)
            values[key] = getattr(value, "value", value)
    return values


def _position_payload(position: Any) -> dict[str, Any]:
    return {
        "exchange": getattr(getattr(position, "exchange", None), "value", getattr(position, "exchange", None)),
        "symbol": getattr(position, "symbol", None),
        "side": getattr(getattr(position, "side", None), "value", getattr(position, "side", None)),
        "quantity": str(getattr(position, "quantity", "")),
        "entry_price": str(getattr(position, "entry_price", "")),
    }


async def _write_report(path: str | None, report: PreflightReport) -> None:
    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(report.to_json(), encoding="utf-8")


def _bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _now_ms() -> int:
    return int(time.time() * 1000)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
