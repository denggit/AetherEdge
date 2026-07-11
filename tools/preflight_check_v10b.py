from __future__ import annotations

import argparse
import asyncio
import csv
import importlib
import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v10b.execution.structural_stop import (
    StructuralStopConfig,
    evaluate_swing_structural_stop,
)
from src.platform.config import load_project_env_config, set_project_env_config


EXPECTED_STRATEGY = "strategies.eth_lf_portfolio_v10b:Strategy"
EXPECTED_STRATEGY_MODULE = "strategies.eth_lf_portfolio_v10b.strategy"
EXPECTED_STRATEGY_ID = "eth_lf_portfolio_v10b_all_swing_structural_stop"
EXPECTED_STRATEGY_VERSION = "V10B"
EXPECTED_SYMBOL = "ETH-USDT-PERP"
ACTIVE_PLAN_STATUSES = (
    "active",
    "master_active_plan_unknown",
    "manual_required",
    "master_closed_follower_close_required",
)
FORBIDDEN_PLUGIN_MARKERS = (
    "coinbacktest",
    "from backtest",
    "import backtest",
    "from research",
    "import research",
    "okxclient",
    "binanceclient",
    "trading_client._client",
    "/api/v5",
    "api/v3",
    "fapi",
    "dapi",
)


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    message: str = ""


@dataclass
class PreflightReport:
    commit: str | None = None
    strategy: str = EXPECTED_STRATEGY
    strategy_id: str | None = None
    strategy_version: str | None = None
    structural_stop: dict[str, Any] = field(default_factory=dict)
    checks: list[CheckResult] = field(default_factory=list)

    def add(self, name: str, status: str, message: str = "") -> None:
        normalized = status.strip().upper()
        if normalized not in {"PASS", "WARN", "FAIL"}:
            raise ValueError(f"unsupported preflight status: {status}")
        self.checks.append(CheckResult(name=name, status=normalized, message=message))

    @property
    def result(self) -> str:
        if self.failures:
            return "FAIL"
        if self.warnings:
            return "PASS_WITH_WARNINGS"
        return "PASS"

    @property
    def failures(self) -> list[dict[str, str]]:
        return [
            asdict(check)
            for check in self.checks
            if check.status == "FAIL"
        ]

    @property
    def warnings(self) -> list[dict[str, str]]:
        return [
            asdict(check)
            for check in self.checks
            if check.status == "WARN"
        ]

    def to_dict(self) -> dict[str, Any]:
        return {
            "result": self.result,
            "commit": self.commit,
            "strategy": self.strategy,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "structural_stop": dict(self.structural_stop),
            "checks": [asdict(check) for check in self.checks],
            "warnings": self.warnings,
            "failures": self.failures,
        }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only AetherEdge V10B preflight. It does not simulate, place, "
            "amend, or cancel orders."
        )
    )
    parser.add_argument("--bars-csv", help="Optional local closed 4H bars CSV")
    parser.add_argument(
        "--json-output",
        help="Optional JSON report path, for example data/reports/preflight/v10b_preflight.json",
    )
    parser.add_argument(
        "--with-api",
        action="store_true",
        help="Run read-only exchange snapshot and active-stop recovery checks",
    )
    return parser.parse_args(argv)


async def run_preflight(
    *,
    bars_csv: str | Path | None = None,
    with_api: bool = False,
    environ: Mapping[str, str] | None = None,
    repo_root: str | Path = REPO_ROOT,
    plugin_root: str | Path | None = None,
) -> PreflightReport:
    root = Path(repo_root).resolve()
    plugin = (
        Path(plugin_root).resolve()
        if plugin_root is not None
        else root / "strategies" / "eth_lf_portfolio_v10b"
    )
    project_env = load_project_env_config(
        env_file=root / ".env",
        process_env=os.environ if environ is None else environ,
    )
    set_project_env_config(project_env)
    env = dict(project_env.values)
    report = PreflightReport(commit=git_commit(root))

    _check_repo_root(report, root)
    strategy = _load_and_check_strategy(report)
    if strategy is not None:
        _check_strategy_identity(report, strategy, env)
        _check_structural_stop_config(report, strategy)
        _check_runtime_warmup(report, strategy)
    else:
        report.add(
            "strategy_identity",
            "FAIL",
            "strategy unavailable after import/instantiation failure",
        )
        report.add(
            "structural_stop_config",
            "FAIL",
            "strategy unavailable after import/instantiation failure",
        )
        report.add(
            "runtime_warmup",
            "WARN",
            "cannot_verify_runtime_warmup_from_config",
        )

    if bars_csv is None:
        report.add(
            "bars_csv_not_provided_skip_local_bar_check",
            "WARN",
            "local closed-bar availability was not checked",
        )
    else:
        _check_bars_csv(report, Path(bars_csv))

    _check_structural_stop_self_test(report)
    _check_plugin_boundary(report, plugin)
    _check_v10a_clean(report, root)

    if with_api:
        await _check_api_position_safety(report, env=env, repo_root=root)
    else:
        report.add(
            "api_position_check_skipped",
            "WARN",
            "api_position_check=SKIPPED; reason=--with-api_not_requested",
        )

    report.add(
        "stop_replace_mode",
        "WARN",
        "STOP_REPLACE_MODE=cancel_then_place_validated; ATOMIC_REPLACE=false; "
        "first structural stop update should be monitored manually",
    )
    return report


def _check_repo_root(report: PreflightReport, root: Path) -> None:
    required = (
        root / "strategies" / "eth_lf_portfolio_v10b",
        root / "src",
        root / "tools",
    )
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        report.add("repo_root", "FAIL", f"missing repository paths: {missing}")
        return
    cwd_note = "cwd_is_repo_root" if Path.cwd().resolve() == root else f"bootstrapped_from={root}"
    report.add("repo_root", "PASS", cwd_note)


def _load_and_check_strategy(report: PreflightReport) -> Any | None:
    try:
        module = importlib.import_module("strategies.eth_lf_portfolio_v10b")
        strategy_type = getattr(module, "Strategy")
    except Exception as exc:
        report.add("import_strategy", "FAIL", str(exc))
        report.add("instantiate_strategy", "FAIL", "import failed")
        return None

    actual_module = str(getattr(strategy_type, "__module__", ""))
    if actual_module != EXPECTED_STRATEGY_MODULE:
        report.add(
            "import_strategy",
            "FAIL",
            f"Strategy resolves to {actual_module or 'unknown'}, expected {EXPECTED_STRATEGY_MODULE}",
        )
    else:
        report.add("import_strategy", "PASS", actual_module)

    try:
        from src.strategy import load_strategy

        strategy = load_strategy(EXPECTED_STRATEGY)
    except Exception as exc:
        report.add("instantiate_strategy", "FAIL", str(exc))
        return None
    report.add(
        "instantiate_strategy",
        "PASS",
        f"{type(strategy).__module__}.{type(strategy).__name__}",
    )
    return strategy


def _check_strategy_identity(
    report: PreflightReport,
    strategy: Any,
    env: Mapping[str, str],
) -> None:
    config = getattr(strategy, "config", None)
    strategy_id = getattr(config, "strategy_id", None)
    strategy_version = getattr(config, "strategy_version", None)
    display_name = str(getattr(config, "display_name", ""))
    report.strategy_id = None if strategy_id is None else str(strategy_id)
    report.strategy_version = None if strategy_version is None else str(strategy_version)

    mismatches: list[str] = []
    if strategy_id != EXPECTED_STRATEGY_ID:
        mismatches.append(f"strategy_id={strategy_id!r}")
    if strategy_version != EXPECTED_STRATEGY_VERSION:
        mismatches.append(f"strategy_version={strategy_version!r}")
    if "V10B" not in display_name.upper():
        mismatches.append(f"display_name={display_name!r}")
    if mismatches:
        report.add("strategy_identity", "FAIL", "; ".join(mismatches))
    else:
        report.add(
            "strategy_identity",
            "PASS",
            f"strategy_id={strategy_id}; strategy_version={strategy_version}; display_name={display_name}",
        )

    configured = str(env.get("AETHER_STRATEGY", "")).strip()
    if not configured:
        report.add(
            "aether_strategy_env",
            "WARN",
            "AETHER_STRATEGY is not set; config-based loading may still select V10B",
        )
    elif configured != EXPECTED_STRATEGY:
        report.add(
            "aether_strategy_env",
            "FAIL",
            f"AETHER_STRATEGY={configured!r}, expected {EXPECTED_STRATEGY!r}",
        )
    else:
        report.add("aether_strategy_env", "PASS", configured)


def _check_structural_stop_config(report: PreflightReport, strategy: Any) -> None:
    config = getattr(getattr(strategy, "config", None), "structural_stop", None)
    if config is None:
        report.add("structural_stop_config", "FAIL", "missing strategy.config.structural_stop")
        return
    summary = {
        "enabled": bool(getattr(config, "enabled", False)),
        "engine_scope": str(getattr(config, "engine_scope", "")),
        "source": str(getattr(config, "source", "")),
        "lookback_bars": getattr(config, "lookback_bars", None),
        "buffer_atr": _json_scalar(getattr(config, "buffer_atr", None)),
        "trigger_mfe_r": _json_scalar(getattr(config, "trigger_mfe_r", None)),
        "min_hold_bars": getattr(config, "min_hold_bars", None),
        "require_full_window": bool(getattr(config, "require_full_window", False)),
        "closed_bar_only": bool(getattr(config, "closed_bar_only", False)),
        "effective_from_next_bar": bool(getattr(config, "effective_from_next_bar", False)),
    }
    report.structural_stop = summary
    expected = {
        "enabled": True,
        "engine_scope": "ALL",
        "source": "swing",
        "lookback_bars": 21,
        "buffer_atr": "0",
        "trigger_mfe_r": "0",
        "min_hold_bars": 0,
        "require_full_window": True,
        "closed_bar_only": True,
        "effective_from_next_bar": True,
    }
    mismatches: list[str] = []
    for name, expected_value in expected.items():
        actual = summary[name]
        if name in {"buffer_atr", "trigger_mfe_r"}:
            try:
                matches = Decimal(str(actual)) == Decimal(str(expected_value))
            except (InvalidOperation, ValueError):
                matches = False
        else:
            matches = actual == expected_value
        if not matches:
            mismatches.append(f"{name}={actual!r} expected={expected_value!r}")
    if mismatches:
        report.add("structural_stop_config", "FAIL", "; ".join(mismatches))
    else:
        report.add(
            "structural_stop_config",
            "PASS",
            "; ".join(f"{name}={value}" for name, value in summary.items()),
        )


def _check_runtime_warmup(report: PreflightReport, strategy: Any) -> None:
    requirements = getattr(getattr(strategy, "config", None), "runtime_requirements", None)
    if not isinstance(requirements, Mapping):
        report.add("runtime_warmup", "WARN", "cannot_verify_runtime_warmup_from_config")
        return
    closed = requirements.get("closed_kline")
    if not isinstance(closed, Mapping):
        report.add("runtime_warmup", "WARN", "cannot_verify_runtime_warmup_from_config")
        return
    if not _truthy(closed.get("enabled")):
        report.add("runtime_warmup", "FAIL", "closed_kline.enabled must be true")
        return
    interval = str(closed.get("interval") or closed.get("timeframe") or "").strip().lower()
    if interval != "4h":
        report.add("runtime_warmup", "FAIL", f"closed_kline interval/timeframe must be 4h, got {interval!r}")
        return

    estimates: list[int] = []
    details: list[str] = ["closed_kline.enabled=true", "interval=4h"]
    warmup_days = _int_or_none(closed.get("warmup_days"))
    min_records = _int_or_none(
        closed.get("min_records")
        if closed.get("min_records") is not None
        else closed.get("warmup_records")
    )
    if warmup_days is not None:
        bars_from_days = warmup_days * 6
        estimates.append(bars_from_days)
        details.extend((f"warmup_days={warmup_days}", f"bars_from_days={bars_from_days}"))
    if min_records is not None:
        estimates.append(min_records)
        details.append(f"min_records={min_records}")
    if not estimates:
        report.add("runtime_warmup", "WARN", "cannot_verify_runtime_warmup_from_config")
        return

    guaranteed = min(estimates)
    details.append(f"configured_bar_floor={guaranteed}")
    message = "; ".join(details)
    if guaranteed < 21:
        report.add("runtime_warmup", "FAIL", message)
    elif guaranteed < 100:
        report.add("runtime_warmup", "WARN", message)
    else:
        report.add("runtime_warmup", "PASS", message)


def _check_bars_csv(report: PreflightReport, path: Path) -> None:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except Exception as exc:
        report.add("local_closed_bars", "FAIL", f"{path}: {exc}")
        return
    if len(rows) < 21:
        report.add("local_closed_bars", "FAIL", f"closed_bar_rows={len(rows)}; required=21")
        return
    if not rows or not rows[0]:
        report.add("local_closed_bars", "FAIL", "CSV has no readable columns")
        return
    fields = {str(name).strip().lower(): name for name in rows[0]}
    time_name = next(
        (fields[name] for name in ("close_time", "close_time_ms", "bar_close_time") if name in fields),
        None,
    )
    if time_name is None:
        report.add(
            "local_closed_bars",
            "FAIL",
            "missing close_time, close_time_ms, or bar_close_time",
        )
        return
    missing = [name for name in ("high", "low", "close") if name not in fields]
    if missing:
        report.add("local_closed_bars", "FAIL", f"missing columns: {missing}")
        return

    try:
        times = [_parse_time(row.get(time_name)) for row in rows]
    except ValueError as exc:
        report.add("local_closed_bars", "FAIL", str(exc))
        return
    if any(current <= previous for previous, current in zip(times, times[1:])):
        report.add("local_closed_bars", "FAIL", "bar close times must be strictly increasing")
        return

    for row_number, row in enumerate(rows[-21:], start=len(rows) - 20):
        for field_name in ("high", "low", "close"):
            try:
                value = Decimal(str(row.get(fields[field_name], "")))
            except (InvalidOperation, ValueError):
                report.add(
                    "local_closed_bars",
                    "FAIL",
                    f"row={row_number} {field_name} is not numeric",
                )
                return
            if value <= 0:
                report.add(
                    "local_closed_bars",
                    "FAIL",
                    f"row={row_number} {field_name} must be positive",
                )
                return

    timeframe_name = fields.get("timeframe")
    if timeframe_name is not None:
        invalid = sorted(
            {
                str(row.get(timeframe_name, "")).strip().lower()
                for row in rows
                if str(row.get(timeframe_name, "")).strip().lower() != "4h"
            }
        )
        if invalid:
            report.add("local_closed_bars", "FAIL", f"timeframe must be 4h, got {invalid}")
            return
    exchange_name = fields.get("exchange")
    if exchange_name is not None:
        invalid = sorted(
            {
                str(row.get(exchange_name, "")).strip().lower()
                for row in rows
                if str(row.get(exchange_name, "")).strip().lower() != "okx"
            }
        )
        if invalid:
            report.add("local_closed_bars", "FAIL", f"exchange must be okx, got {invalid}")
            return
    report.add(
        "local_closed_bars",
        "PASS",
        f"path={path}; closed_bar_rows={len(rows)}; latest_window=21",
    )


def structural_stop_self_test() -> list[str]:
    config = StructuralStopConfig()
    failures: list[str] = []

    def evaluate(
        count: int,
        *,
        side: Side,
        old_stop: Decimal,
        close: Decimal,
        low: Decimal,
        high: Decimal,
        current_bar_exit: bool = False,
    ):
        bars = [
            SimpleNamespace(low=low, high=high, close=close)
            for _ in range(count)
        ]
        return evaluate_swing_structural_stop(
            closed_bars=bars,
            side=side,
            old_stop=old_stop,
            base_v10a_stop=old_stop,
            current_close=close,
            atr=Decimal("1"),
            engine="MOMENTUM_V3",
            hold_bars=0,
            mfe_r=Decimal("0"),
            bar_close_time=count,
            config=config,
            current_bar_exit=current_bar_exit,
        )

    twenty = evaluate(
        20,
        side=Side.LONG,
        old_stop=Decimal("90"),
        close=Decimal("100"),
        low=Decimal("95"),
        high=Decimal("105"),
    )
    if twenty.accepted or twenty.reject_reason != "insufficient_closed_bars":
        failures.append(f"20_bar_window={twenty}")

    long_ok = evaluate(
        21,
        side=Side.LONG,
        old_stop=Decimal("90"),
        close=Decimal("100"),
        low=Decimal("95"),
        high=Decimal("105"),
    )
    if not long_ok.accepted or long_ok.final_stop != Decimal("95"):
        failures.append(f"long_accept={long_ok}")

    short_ok = evaluate(
        21,
        side=Side.SHORT,
        old_stop=Decimal("110"),
        close=Decimal("100"),
        low=Decimal("95"),
        high=Decimal("105"),
    )
    if not short_ok.accepted or short_ok.final_stop != Decimal("105"):
        failures.append(f"short_accept={short_ok}")

    long_cross = evaluate(
        21,
        side=Side.LONG,
        old_stop=Decimal("90"),
        close=Decimal("100"),
        low=Decimal("100"),
        high=Decimal("105"),
    )
    if long_cross.accepted or "crosses_close" not in long_cross.reject_reason:
        failures.append(f"long_cross={long_cross}")

    short_cross = evaluate(
        21,
        side=Side.SHORT,
        old_stop=Decimal("110"),
        close=Decimal("100"),
        low=Decimal("95"),
        high=Decimal("100"),
    )
    if short_cross.accepted or "crosses_close" not in short_cross.reject_reason:
        failures.append(f"short_cross={short_cross}")

    current_exit = evaluate(
        21,
        side=Side.LONG,
        old_stop=Decimal("90"),
        close=Decimal("100"),
        low=Decimal("95"),
        high=Decimal("105"),
        current_bar_exit=True,
    )
    if current_exit.accepted or current_exit.reject_reason != "current_bar_exit":
        failures.append(f"current_bar_exit={current_exit}")
    return failures


def _check_structural_stop_self_test(report: PreflightReport) -> None:
    failures = structural_stop_self_test()
    if failures:
        report.add("structural_stop_self_test", "FAIL", "; ".join(failures))
    else:
        report.add(
            "structural_stop_self_test",
            "PASS",
            "20/21 window, long/short, close crossing, and current-bar exit checks passed",
        )


def scan_plugin_boundary(plugin_root: str | Path) -> list[str]:
    root = Path(plugin_root)
    violations: list[str] = []
    if not root.is_dir():
        return [f"missing_plugin_root:{root}"]
    for path in sorted(root.rglob("*.py")):
        try:
            text = path.read_text(encoding="utf-8").lower()
        except Exception as exc:
            violations.append(f"{path}:read_error:{exc}")
            continue
        for marker in FORBIDDEN_PLUGIN_MARKERS:
            if marker in text:
                violations.append(f"{path}:{marker}")
    return violations


def _check_plugin_boundary(report: PreflightReport, plugin_root: Path) -> None:
    violations = scan_plugin_boundary(plugin_root)
    if violations:
        report.add("plugin_boundary", "FAIL", "; ".join(violations))
    else:
        report.add("plugin_boundary", "PASS", str(plugin_root))


def git_commit(repo_root: str | Path) -> str | None:
    result = _run_git(repo_root, ("rev-parse", "HEAD"))
    if result is None or result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _check_v10a_clean(report: PreflightReport, repo_root: Path) -> None:
    result = _run_git(
        repo_root,
        ("status", "--porcelain", "--", "strategies/eth_lf_portfolio_v10a"),
    )
    if result is None or result.returncode != 0:
        detail = "git unavailable" if result is None else (result.stderr.strip() or "git status failed")
        report.add("v10a_clean", "WARN", detail)
        return
    changes = result.stdout.strip()
    if changes:
        report.add("v10a_clean", "FAIL", changes)
    else:
        report.add("v10a_clean", "PASS", "strategies/eth_lf_portfolio_v10a is clean")


def _run_git(repo_root: str | Path, args: Sequence[str]) -> subprocess.CompletedProcess[str] | None:
    root = Path(repo_root).resolve()
    try:
        return subprocess.run(
            (
                "git",
                "-c",
                f"safe.directory={root.as_posix()}",
                *args,
            ),
            cwd=root,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None


async def _check_api_position_safety(
    report: PreflightReport,
    *,
    env: Mapping[str, str],
    repo_root: Path,
) -> None:
    try:
        from src.app import AppConfig
        from src.order_management.quantity import NativeQuantityConverter
        from src.order_management.safety import RecoveryExitOrderValidator
        from src.platform.account.factory import create_account_client
        from src.platform.exchanges.credentials import validate_private_credentials
        from src.platform.exchanges.errors import ExchangeConfigError
        from src.platform.exchanges.models import ExchangeConfig, ExchangeName, PositionSide
        from src.platform.execution.factory import create_execution_client
        from src.platform.markets import get_market_profile
        from src.platform.snapshot import fetch_platform_snapshot
    except Exception as exc:
        report.add(
            "api_position_check",
            "WARN",
            f"api_position_check=SKIPPED; reason=existing_snapshot_interface_not_found:{exc}",
        )
        return

    try:
        app = AppConfig.from_env(
            defaults_path=repo_root / "config" / "aether_defaults.json",
            environ=env,
        )
    except Exception as exc:
        report.add("api_position_check", "FAIL", f"app config load failed: {exc}")
        return
    if app.symbol != EXPECTED_SYMBOL:
        report.add("api_position_check", "FAIL", f"symbol={app.symbol!r}, expected={EXPECTED_SYMBOL!r}")
        return
    if app.data_exchange is not ExchangeName.OKX:
        report.add("api_position_check", "FAIL", f"master exchange must be okx, got {app.data_exchange.value}")
        return

    exchange_configs = {}
    try:
        for exchange in app.exchanges:
            exchange_config = ExchangeConfig.from_env(exchange, env)
            validate_private_credentials(exchange, exchange_config)
            exchange_configs[exchange] = exchange_config
    except ExchangeConfigError as exc:
        report.add("api_position_check", "FAIL", str(exc))
        return

    snapshots: dict[str, Any] = {}
    for exchange in app.exchanges:
        try:
            exchange_config = exchange_configs[exchange]
            account = create_account_client(exchange, symbol=app.symbol, config=exchange_config)
            execution = create_execution_client(exchange, symbol=app.symbol, config=exchange_config)
            snapshots[exchange.value] = await fetch_platform_snapshot(
                account=account,
                execution=execution,
            )
        except Exception as exc:
            report.add(
                "api_position_check",
                "FAIL",
                f"read-only snapshot failed for {exchange.value}: {exc}",
            )
            return

    master = snapshots.get("okx")
    if master is None:
        report.add("api_position_check", "FAIL", "OKX master snapshot missing")
        return
    master_positions = _active_positions(master.positions)
    follower = snapshots.get("binance")
    follower_positions = _active_positions(follower.positions) if follower is not None else []

    if not master_positions:
        if follower_positions:
            report.add(
                "api_position_check",
                "FAIL",
                "OKX master is flat while Binance follower has an active position",
            )
        else:
            report.add("api_position_check", "PASS", "active_position=false")
        return
    if len(master_positions) != 1:
        report.add("api_position_check", "FAIL", f"OKX active position count={len(master_positions)}")
        return

    position_plan_db = Path(
        env.get(
            "AETHER_POSITION_PLAN_DB",
            str(repo_root / "data" / "state" / "aether_position_plan.sqlite3"),
        )
    )
    if not position_plan_db.is_absolute():
        position_plan_db = repo_root / position_plan_db
    plan = _read_active_position_plan(position_plan_db)
    if plan is None:
        report.add(
            "api_position_check",
            "FAIL",
            f"active OKX position has no active position plan in {position_plan_db}",
        )
        return
    if str(plan.get("strategy_id") or "") != EXPECTED_STRATEGY_ID:
        report.add(
            "api_position_check",
            "FAIL",
            f"active position plan strategy_id={plan.get('strategy_id')!r}, "
            f"expected={EXPECTED_STRATEGY_ID!r}",
        )
        return
    if str(plan.get("master_exchange") or "").lower() != "okx":
        report.add(
            "api_position_check",
            "FAIL",
            f"active position plan master_exchange={plan.get('master_exchange')!r}, expected='okx'",
        )
        return
    canonical_stop = _decimal_or_none(plan.get("canonical_stop_price"))
    if canonical_stop is None or canonical_stop <= 0:
        report.add(
            "api_position_check",
            "FAIL",
            "active position plan is missing canonical_stop_price",
        )
        return

    validator = RecoveryExitOrderValidator(quantity_converter=NativeQuantityConverter())
    market_profile = get_market_profile(app.symbol)
    master_position = master_positions[0]
    master_side = _position_side(master_position, PositionSide)
    if master_side is None:
        report.add("api_position_check", "FAIL", "cannot determine OKX master position side")
        return
    if str(plan.get("side") or "").lower() != master_side.value:
        report.add(
            "api_position_check",
            "FAIL",
            f"position plan side={plan.get('side')!r} differs from OKX side={master_side.value!r}",
        )
        return
    try:
        master_validation = validator.validate_stop_orders(
            exchange=ExchangeName.OKX,
            symbol=app.symbol,
            strategy_id=EXPECTED_STRATEGY_ID,
            position_id=str(plan.get("position_id") or ""),
            position_side=master_side,
            position_mode=master.position_mode,
            current_position_native_quantity=abs(master_position.quantity),
            canonical_stop_price=canonical_stop,
            open_stop_orders=master.open_stop_orders,
            open_orders=master.open_orders,
            market_profile=market_profile,
        )
    except Exception as exc:
        report.add("api_position_check", "FAIL", f"OKX stop validation failed: {exc}")
        return
    if not master_validation.should_keep_existing_stop:
        report.add(
            "api_position_check",
            "FAIL",
            f"OKX canonical bot stop is not valid: {master_validation.primary_invalid_reason}",
        )
        return
    if master_validation.has_unknown_exit_orders:
        report.add(
            "api_unknown_manual_stop",
            "WARN",
            f"OKX unknown/manual exit orders={len(master_validation.unknown_exit_orders)}",
        )

    if follower_positions:
        if len(follower_positions) != 1 or follower is None:
            report.add(
                "api_position_check",
                "FAIL",
                f"Binance active position count={len(follower_positions)}",
            )
            return
        follower_position = follower_positions[0]
        follower_side = _position_side(follower_position, PositionSide)
        if follower_side is None or follower_side is not master_side:
            report.add("api_position_check", "FAIL", "Binance follower side differs from OKX master")
            return
        try:
            follower_validation = validator.validate_stop_orders(
                exchange=ExchangeName.BINANCE,
                symbol=app.symbol,
                strategy_id=EXPECTED_STRATEGY_ID,
                position_id=str(plan.get("position_id") or ""),
                position_side=follower_side,
                position_mode=follower.position_mode,
                current_position_native_quantity=abs(follower_position.quantity),
                canonical_stop_price=canonical_stop,
                open_stop_orders=follower.open_stop_orders,
                open_orders=follower.open_orders,
                market_profile=market_profile,
            )
        except Exception as exc:
            report.add("api_position_check", "FAIL", f"Binance stop validation failed: {exc}")
            return
        if not follower_validation.should_keep_existing_stop:
            report.add(
                "api_position_check",
                "FAIL",
                f"Binance follower stop is missing or differs from canonical stop: "
                f"{follower_validation.primary_invalid_reason}",
            )
            return
        if follower_validation.has_unknown_exit_orders:
            report.add(
                "api_unknown_manual_stop",
                "WARN",
                f"Binance unknown/manual exit orders={len(follower_validation.unknown_exit_orders)}",
            )

    report.add(
        "api_position_check",
        "PASS",
        f"active_position=true; canonical_stop_price={canonical_stop}; "
        f"okx_stop_valid=true; binance_follower_active={bool(follower_positions)}",
    )


def _read_active_position_plan(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    placeholders = ",".join("?" for _ in ACTIVE_PLAN_STATUSES)
    try:
        with sqlite3.connect(uri, uri=True) as connection:
            row = connection.execute(
                f"""
                SELECT position_id, strategy_id, side, status,
                       canonical_stop_price, master_exchange, updated_time_ms
                FROM position_plans
                WHERE status IN ({placeholders})
                ORDER BY updated_time_ms DESC
                LIMIT 1
                """,
                ACTIVE_PLAN_STATUSES,
            ).fetchone()
    except (sqlite3.Error, OSError):
        return None
    if row is None:
        return None
    return {
        "position_id": row[0],
        "strategy_id": row[1],
        "side": row[2],
        "status": row[3],
        "canonical_stop_price": row[4],
        "master_exchange": row[5],
        "updated_time_ms": row[6],
    }


def _active_positions(positions: Sequence[Any]) -> list[Any]:
    return [
        position
        for position in positions
        if getattr(position, "symbol", None) == EXPECTED_SYMBOL
        and abs(getattr(position, "quantity", Decimal("0"))) > 0
    ]


def _position_side(position: Any, position_side_type: Any) -> Any | None:
    side = getattr(position, "side", None)
    if side is position_side_type.LONG:
        return position_side_type.LONG
    if side is position_side_type.SHORT:
        return position_side_type.SHORT
    quantity = getattr(position, "quantity", Decimal("0"))
    if quantity > 0:
        return position_side_type.LONG
    if quantity < 0:
        return position_side_type.SHORT
    return None


def write_json_report(path: str | Path, report: PreflightReport) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    return target


def print_report(report: PreflightReport) -> None:
    print("AetherEdge V10B Preflight Check")
    print(f"commit: {report.commit or 'unavailable'}")
    print(f"strategy: {report.strategy}")
    print(f"strategy_id: {report.strategy_id or 'unavailable'}")
    print(f"strategy_version: {report.strategy_version or 'unavailable'}")
    if report.structural_stop:
        print("")
        for name, value in report.structural_stop.items():
            print(f"structural_stop.{name}={_display(value)}")
    print("")
    for check in report.checks:
        suffix = f": {check.message}" if check.message else ""
        print(f"[{check.status}] {check.name}{suffix}")
    print("")
    print("STOP_REPLACE_MODE = cancel_then_place_validated")
    print("ATOMIC_REPLACE = false")
    print("first structural stop update should be monitored manually")
    print("")
    print(f"RESULT: {report.result}")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = asyncio.run(
            run_preflight(
                bars_csv=args.bars_csv,
                with_api=args.with_api,
            )
        )
    except Exception as exc:
        report = PreflightReport(commit=git_commit(REPO_ROOT))
        report.add("preflight_internal_error", "FAIL", str(exc))
    if args.json_output:
        try:
            write_json_report(args.json_output, report)
        except Exception as exc:
            report.add("json_output", "FAIL", str(exc))
    print_report(report)
    return 1 if report.result == "FAIL" else 0


def _parse_time(value: Any) -> Decimal:
    text = str(value or "").strip()
    if not text:
        raise ValueError("bar close time cannot be empty")
    try:
        return Decimal(text)
    except InvalidOperation:
        try:
            normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
            return Decimal(str(datetime.fromisoformat(normalized).timestamp()))
        except (ValueError, TypeError) as exc:
            raise ValueError(f"invalid bar close time: {text!r}") from exc


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _json_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    return value


def _display(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
