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
from src.market_data.warmup.gap_detector import interval_to_ms
from src.platform import ExchangeName
from src.platform.account.factory import create_account_client
from src.platform.data.factory import create_market_data_feed
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.execution.factory import create_execution_client
from src.platform.exchanges.models import ExchangeConfig, MarginMode
from src.runtime import RuntimeMode, live_runtime_config_from_app, runtime_mode_from_env
from src.runtime.requirements import resolve_strategy_runtime_requirements
from src.runtime.tasks.scheduler import closed_bar_open_time_ms
from src.strategy import load_strategy


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
        print(msg)

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
        print(report.to_json())
        return 1

    report.symbol = app.symbol
    report.strategy = app.strategy
    report.runtime_mode = runtime_mode.value

    _check_runtime_config(report, app=app, runtime_mode=runtime_mode, runtime=runtime, requirements=requirements, expect_real_live=args.expect_real_live)
    _check_local_writable(report, app=app)

    if not args.skip_api:
        await _check_exchange_read_apis(report, app=app, allow_existing_position=args.allow_existing_position, allow_open_orders=args.allow_open_orders)
    if not args.skip_kline:
        await _check_latest_closed_kline(report, app=app, runtime=runtime)
    _check_local_rangebar_builder(report, app=app)

    await _write_report(args.report, report)
    print(report.to_json())
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
    }
    if not requirements.closed_kline.enabled or requirements.closed_kline.interval.lower() != "4h" or not requirements.trades.stream_enabled or not requirements.trades.warmup_enabled or not requirements.range_bars.enabled or requirements.order_book.enabled or not requirements.private_account_stream.enabled:
        report.add("v8_runtime_requirements", "fail", detail=req_detail, error="V8 requirements must be closed 4H + trades stream/warmup + range bars + private account stream, without order_book")
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


async def _check_exchange_read_apis(report: PreflightReport, *, app: AppConfig, allow_existing_position: bool, allow_open_orders: bool) -> None:
    data_feed = create_market_data_feed(
        app.data_exchange,
        symbol=app.symbol,
        config=ExchangeConfig.from_env(app.data_exchange),
        enable_trade_stream=False,
        enable_order_book_stream=False,
    )
    await _step(report, "data_exchange_ticker", app.data_exchange, data_feed.fetch_ticker)

    for exchange in app.exchanges:
        account = create_account_client(exchange, symbol=app.symbol, config=ExchangeConfig.from_env(exchange))
        execution = create_execution_client(exchange, symbol=app.symbol, config=ExchangeConfig.from_env(exchange))
        balance = await _step(report, "fetch_balance", exchange, account.fetch_balance, "USDT")
        positions = await _step(report, "fetch_positions", exchange, account.fetch_positions)
        leverage = await _step(report, "fetch_leverage", exchange, account.fetch_leverage, margin_mode=MarginMode.ISOLATED)
        mode = await _step(report, "fetch_position_mode", exchange, account.fetch_position_mode)
        open_orders = await _step(report, "fetch_open_orders", exchange, execution.fetch_open_orders)
        open_stop_orders = await _step(report, "fetch_open_stop_orders", exchange, execution.fetch_open_stop_orders)

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
