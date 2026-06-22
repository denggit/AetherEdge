from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.app import AppConfig
from src.order_management import (
    MasterFollowerExecutionPolicy,
    MultiExchangeOrderCoordinator,
    RepositoryDuplicateOrderGuard,
    SqliteOrderJournalStore,
)
from src.order_management.models import ExchangeOrderResult, OrderIntent
from src.platform import ExchangeName
from src.platform.account.factory import create_account_client
from src.platform.data.factory import create_market_data_feed
from src.platform.execution.factory import create_execution_client
from src.platform.exchanges.models import (
    CancelStopOrderRequest,
    ExchangeConfig,
    MarginMode,
    PositionMode,
    StopMarketOrderRequest,
    TriggerPriceType,
)
from src.runtime.config import live_runtime_config_from_app
from src.signals import SignalAction, TradeSignal


@dataclass
class StepResult:
    name: str
    ok: bool
    exchange: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass
class SmokeReport:
    started_time_ms: int
    symbol: str
    exchanges: list[str]
    data_exchange: str
    live: bool
    margin_usdt: str
    leverage: str
    side: str
    steps: list[StepResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(step.ok for step in self.steps)

    def add(self, step: StepResult) -> None:
        self.steps.append(step)

    def to_json(self) -> str:
        payload = asdict(self)
        payload["ok"] = self.ok
        return json.dumps(payload, ensure_ascii=False, indent=2, default=str)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="AetherEdge live API connectivity smoke test for market data, account, execution, order sync, stop order, and cleanup."
    )
    parser.add_argument("--symbol", default=None, help="Canonical symbol, default from AETHER_MARKET/config.")
    parser.add_argument("--margin-usdt", type=Decimal, default=Decimal("2"), help="Margin budget in USDT. Default: 2")
    parser.add_argument("--leverage", type=Decimal, default=Decimal("10"), help="Leverage to set before test. Default: 10")
    parser.add_argument("--side", choices=("long", "short"), default="long", help="Test direction. Default: long")
    parser.add_argument("--hold-seconds", type=float, default=3.0, help="Seconds to hold before close. Default: 3")
    parser.add_argument("--stop-distance-pct", type=Decimal, default=Decimal("0.05"), help="Temporary stop distance from ticker. Default: 5%%")
    parser.add_argument("--live", action="store_true", help="Actually place orders. Without this flag the script is read-only + dry preview.")
    parser.add_argument("--skip-stop-test", action="store_true", help="Skip temporary stop placement/fetch/cancel test.")
    parser.add_argument("--skip-order-test", action="store_true", help="Only test read/config APIs; do not place entry/stop/close orders.")
    parser.add_argument("--no-cleanup", action="store_true", help="Do not auto-close the test position on failure. Not recommended.")
    parser.add_argument("--journal-db", default="data/state/connectivity_smoke_order_journal.sqlite3", help="Order journal DB path.")
    parser.add_argument("--report", default=None, help="Optional report JSON output path.")
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    app_config = AppConfig.from_env()
    if args.symbol:
        app_config = _replace_app_symbol(app_config, args.symbol)
    runtime_config = live_runtime_config_from_app(app_config)
    live = bool(args.live and not args.skip_order_test)
    report = SmokeReport(
        started_time_ms=_now_ms(),
        symbol=app_config.symbol,
        exchanges=[exchange.value for exchange in app_config.exchanges],
        data_exchange=app_config.data_exchange.value,
        live=live,
        margin_usdt=str(args.margin_usdt),
        leverage=str(args.leverage),
        side=args.side,
    )

    if args.margin_usdt <= 0 or args.leverage <= 0:
        raise SystemExit("--margin-usdt and --leverage must be positive")

    print("[config] symbol=", app_config.symbol)
    print("[config] exchanges=", ",".join(exchange.value for exchange in app_config.exchanges))
    print("[config] data_exchange=", app_config.data_exchange.value)
    if runtime_config.master_follower_policy is not None:
        print("[config] master=", runtime_config.master_follower_policy.master_exchange.value)
        print("[config] followers=", ",".join(exchange.value for exchange in runtime_config.master_follower_policy.follower_exchanges) or "<none>")
    print("[config] live_orders=", live)

    data_feed = create_market_data_feed(
        app_config.data_exchange,
        symbol=app_config.symbol,
        config=ExchangeConfig.from_env(app_config.data_exchange),
        enable_trade_stream=False,
        enable_order_book_stream=False,
    )
    ticker = await _step(report, "fetch_data_exchange_ticker", app_config.data_exchange, data_feed.fetch_ticker)
    if ticker is None:
        await _write_report(args.report, report)
        return 2
    price = Decimal(str(ticker.price))
    base_qty = _floor_decimal((args.margin_usdt * args.leverage) / price, Decimal("0.000001"))
    print(f"[sizing] price={price} notional={args.margin_usdt * args.leverage} base_qty={base_qty}")

    account_clients = {
        exchange: create_account_client(exchange, symbol=app_config.symbol, config=ExchangeConfig.from_env(exchange))
        for exchange in app_config.exchanges
    }
    execution_clients = [
        create_execution_client(exchange, symbol=app_config.symbol, config=ExchangeConfig.from_env(exchange))
        for exchange in app_config.exchanges
    ]

    for exchange, client in account_clients.items():
        await _step(report, "fetch_balance_usdt", exchange, client.fetch_balance, "USDT")
        await _step(report, "fetch_positions_before", exchange, client.fetch_positions)
        await _step(report, "fetch_position_mode", exchange, client.fetch_position_mode)
        await _step(report, "set_position_mode_one_way", exchange, client.set_position_mode, PositionMode.ONE_WAY, soft=True)
        await _step(report, "set_margin_mode_isolated", exchange, client.set_margin_mode, MarginMode.ISOLATED, soft=True)
        await _step(report, "set_leverage", exchange, client.set_leverage, args.leverage, margin_mode=MarginMode.ISOLATED, soft=True)
        await _step(report, "fetch_leverage", exchange, client.fetch_leverage, margin_mode=MarginMode.ISOLATED, soft=True)

    if not live:
        report.add(
            StepResult(
                name="order_preview_only",
                ok=True,
                detail={
                    "reason": "orders skipped; pass --live and set AETHER_LIVE_TRADING=true or sandbox=true to place orders",
                    "base_quantity": str(base_qty),
                },
            )
        )
        await _write_report(args.report, report)
        print(report.to_json())
        return 0 if report.ok else 1

    if os.getenv("AETHER_DRY_RUN", "true").strip().lower() in {"1", "true", "yes", "on"}:
        raise SystemExit("Refusing live smoke order while AETHER_DRY_RUN=true. Set AETHER_DRY_RUN=false and pass --live.")

    journal = SqliteOrderJournalStore(args.journal_db)
    policy = None if runtime_config.master_follower_policy is None else MasterFollowerExecutionPolicy.from_config(runtime_config.master_follower_policy)
    coordinator = MultiExchangeOrderCoordinator(
        clients=execution_clients,
        repository=journal,
        duplicate_guard=RepositoryDuplicateOrderGuard(journal),
        master_follower_policy=policy,
    )

    opened = False
    stop_results: list[ExchangeOrderResult] = []
    try:
        entry_action = SignalAction.OPEN_LONG if args.side == "long" else SignalAction.OPEN_SHORT
        close_action = SignalAction.CLOSE_LONG if args.side == "long" else SignalAction.CLOSE_SHORT
        stop_action = SignalAction.PLACE_STOP_LOSS_LONG if args.side == "long" else SignalAction.PLACE_STOP_LOSS_SHORT
        stop_price = _stop_price(price, side=args.side, distance_pct=args.stop_distance_pct)

        entry_results = await _execute_intent(
            report,
            coordinator,
            strategy_id="tools.exchange_connectivity_smoke",
            intent_suffix="entry",
            signal=TradeSignal(
                symbol=app_config.symbol,
                action=entry_action,
                quantity=base_qty,
                reason="connectivity_smoke_entry",
                metadata={"tool": "exchange_connectivity_smoke"},
            ),
        )
        opened = any(result.ok for result in entry_results)
        for exchange, client in account_clients.items():
            await _step(report, "fetch_positions_after_entry", exchange, client.fetch_positions, soft=True)

        if not args.skip_stop_test and opened:
            stop_results = await _execute_intent(
                report,
                coordinator,
                strategy_id="tools.exchange_connectivity_smoke",
                intent_suffix="stop",
                signal=TradeSignal(
                    symbol=app_config.symbol,
                    action=stop_action,
                    quantity=base_qty,
                    trigger_price=stop_price,
                    reason="connectivity_smoke_temp_stop",
                    metadata={"tool": "exchange_connectivity_smoke", "temporary_stop": True},
                ),
            )
            await _cancel_own_stop_orders(report, execution_clients, stop_results)

        if args.hold_seconds > 0:
            await asyncio.sleep(args.hold_seconds)
        if opened:
            await _execute_intent(
                report,
                coordinator,
                strategy_id="tools.exchange_connectivity_smoke",
                intent_suffix="close",
                signal=TradeSignal(
                    symbol=app_config.symbol,
                    action=close_action,
                    quantity=base_qty,
                    reason="connectivity_smoke_close",
                    metadata={"tool": "exchange_connectivity_smoke"},
                ),
            )
            opened = False
        for exchange, client in account_clients.items():
            await _step(report, "fetch_positions_after_close", exchange, client.fetch_positions, soft=True)
    finally:
        if opened and not args.no_cleanup:
            close_action = SignalAction.CLOSE_LONG if args.side == "long" else SignalAction.CLOSE_SHORT
            try:
                await _execute_intent(
                    report,
                    coordinator,
                    strategy_id="tools.exchange_connectivity_smoke",
                    intent_suffix="emergency-close",
                    signal=TradeSignal(
                        symbol=app_config.symbol,
                        action=close_action,
                        quantity=base_qty,
                        reason="connectivity_smoke_emergency_close",
                        metadata={"tool": "exchange_connectivity_smoke", "emergency_cleanup": True},
                    ),
                )
            except Exception as exc:  # pragma: no cover - live cleanup path
                report.add(StepResult(name="emergency_close", ok=False, error=str(exc)))

    await _write_report(args.report, report)
    print(report.to_json())
    return 0 if report.ok else 1


async def _execute_intent(
    report: SmokeReport,
    coordinator: MultiExchangeOrderCoordinator,
    *,
    strategy_id: str,
    intent_suffix: str,
    signal: TradeSignal,
) -> list[ExchangeOrderResult]:
    intent = OrderIntent(
        intent_id=f"smoke-{intent_suffix}-{_now_ms()}",
        strategy_id=strategy_id,
        signal=signal,
        target_exchanges=tuple(ExchangeName(exchange) for exchange in report.exchanges),
        metadata={"tool": "exchange_connectivity_smoke", "intent_suffix": intent_suffix},
    )
    try:
        results = await coordinator.execute(intent)
        report.add(
            StepResult(
                name=f"execute_{intent_suffix}",
                ok=all(result.ok for result in results),
                detail={"results": [_result_json(result) for result in results]},
            )
        )
        return list(results)
    except Exception as exc:
        report.add(StepResult(name=f"execute_{intent_suffix}", ok=False, error=str(exc)))
        raise


async def _cancel_own_stop_orders(report: SmokeReport, execution_clients: Sequence[Any], stop_results: Sequence[ExchangeOrderResult]) -> None:
    clients = {client.exchange: client for client in execution_clients}
    for result in stop_results:
        if not result.ok:
            continue
        client = clients.get(result.exchange)
        if client is None:
            continue
        await _step(
            report,
            "cancel_temp_stop_order",
            result.exchange,
            client.cancel_stop_order,
            CancelStopOrderRequest(symbol=report.symbol, stop_order_id=result.order_id, client_order_id=result.client_order_id),
            soft=True,
        )


async def _step(report: SmokeReport, name: str, exchange: ExchangeName | None, fn, *args, soft: bool = False, **kwargs):
    try:
        value = await fn(*args, **kwargs)
        report.add(StepResult(name=name, ok=True, exchange=None if exchange is None else exchange.value, detail=_jsonable(value)))
        print(f"[ok] {name}" + (f" {exchange.value}" if exchange else ""))
        return value
    except Exception as exc:
        ok = bool(soft and _is_expected_noop_error(exc))
        report.add(StepResult(name=name, ok=ok, exchange=None if exchange is None else exchange.value, error=str(exc)))
        label = "warn-ok" if ok else ("warn-fail" if soft else "fail")
        print(f"[{label}] {name}" + (f" {exchange.value}" if exchange else "") + f": {exc}")
        return None


def _is_expected_noop_error(exc: Exception) -> bool:
    text = str(exc)
    expected_fragments = (
        "No need to change position side",
        "No need to change margin type",
        "code': -4059",
        "code': -4046",
        '"code": -4059',
        '"code": -4046',
    )
    return any(fragment in text for fragment in expected_fragments)


def _stop_price(price: Decimal, *, side: str, distance_pct: Decimal) -> Decimal:
    if side == "long":
        return _floor_decimal(price * (Decimal("1") - distance_pct), Decimal("0.01"))
    return _floor_decimal(price * (Decimal("1") + distance_pct), Decimal("0.01"))


def _floor_decimal(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _result_json(result: ExchangeOrderResult) -> dict[str, Any]:
    return {
        "exchange": result.exchange.value,
        "ok": result.ok,
        "order_id": result.order_id,
        "client_order_id": result.client_order_id,
        "status": None if result.status is None else result.status.value,
        "quantity": None if result.quantity is None else str(result.quantity),
        "filled_quantity": None if result.filled_quantity is None else str(result.filled_quantity),
        "avg_fill_price": None if result.avg_fill_price is None else str(result.avg_fill_price),
        "fee": None if result.fee is None else str(result.fee),
        "fee_asset": result.fee_asset,
        "error": result.error,
    }


def _jsonable(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, (list, tuple)):
        return {"items": [_jsonable(item) for item in value]}
    if hasattr(value, "__dataclass_fields__"):
        return {key: _json_scalar(val) for key, val in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _json_scalar(val) for key, val in value.items()}
    return {"value": _json_scalar(value)}


def _json_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "value") and isinstance(getattr(value, "value"), str):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json_scalar(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_scalar(item) for item in value]
    return value


def _replace_app_symbol(app_config: AppConfig, symbol: str) -> AppConfig:
    from dataclasses import replace

    return replace(app_config, symbol=symbol)


def _now_ms() -> int:
    return int(time.time() * 1000)


async def _write_report(path: str | None, report: SmokeReport) -> None:
    if not path:
        return
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report.to_json(), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
