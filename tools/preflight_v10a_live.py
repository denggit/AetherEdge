#!/usr/bin/env python
"""Read-only local preflight for the V10A real-live configuration.

The tool validates local configuration and inspects existing SQLite files
through read-only connections. It never starts the runtime or mutates exchange
or database state.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.platform.config import load_env_config
from src.strategy import load_strategy
from strategies.eth_lf_portfolio_v10a import Strategy


EXPECTED_STRATEGY = "strategies.eth_lf_portfolio_v10a:Strategy"
EXPECTED_STRATEGY_ID = "eth_lf_portfolio_v10a_momentum_micro_short_speed_filter"
FORBIDDEN_STRATEGY_ENV_KEYS = (
    "enable_momentum_long_not_aligned_block",
    "enable_momentum_short_fast_speed_block",
    "range_speed_rolling_window_bars",
    "range_speed_min_periods",
    "range_speed_fast_quantile",
    "global_risk_scale",
    "range_exit",
    "micro_context",
    "bull_reclaim",
    "momentum_v3",
    "bear_v3",
)
SENSITIVE_ENV_KEYS = frozenset(
    {
        "OKX_API_KEY",
        "OKX_SECRET_KEY",
        "OKX_PASSPHRASE",
        "BINANCE_API_KEY",
        "BINANCE_SECRET_KEY",
        "EMAIL_SENDER",
        "EMAIL_PASSWORD",
        "EMAIL_RECEIVER",
    }
)
SECTION_ORDER = (
    "ENV",
    "CREDENTIALS",
    "MASTER_FOLLOWER",
    "MARKET_STREAM",
    "RUNTIME_TIMING",
    "LEVERAGE",
    "STRATEGY",
    "RUNTIME_REQUIREMENTS",
    "ENV_STRATEGY_PARAM_BOUNDARY",
    "STATE",
    "RANGE_STATE",
    "EXCHANGE_READ",
)
DEFAULT_MANUAL_CHECKLIST = (
    "Confirm OKX has no ETH-USDT-SWAP position",
    "Confirm Binance has no ETHUSDT position",
    "Confirm OKX has no stale open/stop orders",
    "Confirm Binance has no stale open/stop orders",
    "Backup state DB and order journal DB manually",
)


def _generated_at() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class CheckResult:
    section: str
    status: str
    name: str
    detail: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "section": self.section,
            "status": self.status,
            "name": self.name,
            "detail": self.detail,
        }


@dataclass
class PreflightReport:
    env_file: str = ""
    expect_real_live: bool = False
    generated_at: str = field(default_factory=_generated_at)
    checks: list[CheckResult] = field(default_factory=list)
    manual_checklist: list[str] = field(default_factory=lambda: list(DEFAULT_MANUAL_CHECKLIST))
    _sensitive_values: tuple[str, ...] = field(default=(), repr=False)

    def add(self, section: str, status: str, name: str, detail: str = "") -> None:
        self.checks.append(
            CheckResult(section=section, status=status, name=name, detail=detail)
        )

    @property
    def ok(self) -> bool:
        return self.fail_count == 0

    @property
    def final_status(self) -> str:
        return "PASS_READY_FOR_MANUAL_LIVE_START" if self.ok else "FAIL_FIX_REQUIRED"

    @property
    def fail_count(self) -> int:
        return sum(check.status == "FAIL" for check in self.checks)

    @property
    def warn_count(self) -> int:
        return sum(check.status == "WARN" for check in self.checks)

    @property
    def skipped_count(self) -> int:
        return sum(check.status == "SKIPPED" for check in self.checks)

    @property
    def pass_count(self) -> int:
        return sum(check.status.upper() == "PASS" for check in self.checks)

    @property
    def failures(self) -> list[dict[str, str]]:
        return [
            {"section": c.section, "name": c.name, "detail": c.detail}
            for c in self.checks
            if c.status.upper() == "FAIL"
        ]

    @property
    def warnings(self) -> list[dict[str, str]]:
        return [
            {"section": c.section, "name": c.name, "detail": c.detail}
            for c in self.checks
            if c.status.upper() == "WARN"
        ]

    def named(self, name: str) -> list[CheckResult]:
        return [check for check in self.checks if check.name == name]

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "final_status": self.final_status,
            "generated_at": self.generated_at,
            "env_file": self.env_file,
            "expect_real_live": self.expect_real_live,
            "fail_count": self.fail_count,
            "warn_count": self.warn_count,
            "skipped_count": self.skipped_count,
            "summary": {
                "pass": self.pass_count,
                "warn": self.warn_count,
                "fail": self.fail_count,
                "skipped": self.skipped_count,
            },
            "failures": self.failures,
            "warnings": self.warnings,
            "checks": [check.to_dict() for check in self.checks],
            "manual_checklist": list(self.manual_checklist),
        }
        return _redact_payload(payload, self._sensitive_values)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only local V10A live preflight. It does not start the runtime "
            "or mutate exchange/database state."
        )
    )
    parser.add_argument(
        "--env-file",
        default=str(REPO_ROOT / ".env"),
        help="Path to the runtime .env file",
    )
    parser.add_argument(
        "--expect-real-live",
        action="store_true",
        help="Require all real-live safety gates to be enabled",
    )
    parser.add_argument("--report", help="Optional JSON report output path")
    parser.add_argument(
        "--check-exchange-read",
        action="store_true",
        help="Request safe exchange reads; skipped unless an isolated read-only adapter is available",
    )
    return parser.parse_args(argv)


def run_preflight(
    *,
    env_file: str | Path,
    environ: Mapping[str, str] | None = None,
    repo_root: str | Path = REPO_ROOT,
    expect_real_live: bool = False,
    check_exchange_read: bool = False,
) -> PreflightReport:
    env_path = Path(env_file)
    root = Path(repo_root)
    raw_env = _read_env_file(env_path)
    report = PreflightReport(
        env_file=str(env_file),
        expect_real_live=expect_real_live,
        _sensitive_values=_sensitive_values(raw_env),
    )
    effective_env = load_env_config(env_path, environ=environ)

    if env_path.is_file():
        report.add("ENV", "PASS", "env_file", str(env_path))
    else:
        report.add("ENV", "FAIL", "env_file", f"missing: {env_path}")

    _check_live_safety_gate(report, effective_env, expect_real_live)
    _check_credentials(report, raw_env)
    _check_master_follower(report, effective_env)
    _check_market_stream(report, effective_env)
    _check_runtime_timing(report, effective_env)
    _check_leverage(report, effective_env)
    strategy = _check_strategy(report, effective_env)
    if strategy is not None:
        _check_runtime_requirements(report, strategy, effective_env)
    _check_strategy_env_boundary(report, raw_env)
    _check_state_dbs(report, effective_env, root)
    _check_range_state(report, effective_env, root)
    _check_exchange_read_availability(report, check_exchange_read)
    return report


def write_json_report(path: str | Path, report: PreflightReport) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


def render_report(report: PreflightReport) -> str:
    lines = ["[V10A LIVE PREFLIGHT]", ""]
    for section in SECTION_ORDER:
        lines.append(f"{section}:")
        for check in (item for item in report.checks if item.section == section):
            suffix = f" {check.detail}" if check.detail else ""
            lines.append(f"{check.status} {check.name}{suffix}")
        lines.append("")
    lines.append("MANUAL CHECKLIST:")
    lines.extend(f"- {item}" for item in report.manual_checklist)
    lines.append("")
    lines.append("SUMMARY:")
    lines.append(f"PASS: {report.pass_count}")
    lines.append(f"WARN: {report.warn_count}")
    lines.append(f"FAIL: {report.fail_count}")
    lines.append(f"SKIPPED: {report.skipped_count}")
    lines.append("")
    lines.append("FAILURES:")
    if report.failures:
        for i, f in enumerate(report.failures, 1):
            lines.append(f"{i}. [{f['section']}] {f['name']}")
            lines.append(f"   {f['detail']}")
            lines.append("")
    else:
        lines.append("none")
        lines.append("")
    lines.append("WARNINGS:")
    if report.warnings:
        for i, w in enumerate(report.warnings, 1):
            lines.append(f"{i}. [{w['section']}] {w['name']}")
            lines.append(f"   {w['detail']}")
            lines.append("")
    else:
        lines.append("none")
        lines.append("")
    lines.append("FINAL:")
    lines.append(report.final_status)
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = run_preflight(
        env_file=args.env_file,
        expect_real_live=args.expect_real_live,
        check_exchange_read=args.check_exchange_read,
    )
    if args.report:
        try:
            write_json_report(args.report, report)
        except OSError as exc:
            report.add("ENV", "FAIL", "json_report_write", str(exc))
    print(render_report(report))
    return 0 if report.ok else 1


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = _unquote(value.strip())
    return values


def _check_live_safety_gate(
    report: PreflightReport,
    env: Mapping[str, str],
    expect_real_live: bool,
) -> None:
    expected = {
        "AETHER_RUNTIME_MODE": "live_runtime",
        "AETHER_DRY_RUN": "false",
        "AETHER_LIVE_TRADING": "true",
        "OKX_SANDBOX": "false",
        "BINANCE_SANDBOX": "false",
    }
    for key, wanted in expected.items():
        actual = env.get(key)
        status = "PASS" if _normalized(actual) == wanted else "FAIL"
        detail = f"{_display(actual)} expected={wanted}"
        if expect_real_live:
            detail += " required-for-real-live"
        report.add("ENV", status, key, detail)


def _check_credentials(
    report: PreflightReport,
    raw_env: Mapping[str, str],
) -> None:
    required = (
        "OKX_API_KEY",
        "OKX_SECRET_KEY",
        "OKX_PASSPHRASE",
        "BINANCE_API_KEY",
        "BINANCE_SECRET_KEY",
    )
    for key in required:
        value = raw_env.get(key, "")
        if value.strip():
            report.add("CREDENTIALS", "PASS", f"{key} present")
        else:
            report.add("CREDENTIALS", "FAIL", f"{key} missing")

    email_alert = raw_env.get("AETHER_ENABLE_EMAIL_ALERT", "").strip().lower()
    if email_alert == "true":
        email_keys = ("EMAIL_SENDER", "EMAIL_PASSWORD", "EMAIL_RECEIVER")
        for key in email_keys:
            value = raw_env.get(key, "")
            if value.strip():
                report.add("CREDENTIALS", "PASS", f"{key} present")
            else:
                report.add("CREDENTIALS", "FAIL", f"{key} missing")


def _check_master_follower(report: PreflightReport, env: Mapping[str, str]) -> None:
    exact_values = {
        "AETHER_DATA_EXCHANGE": "okx",
        "AETHER_MASTER_EXCHANGE": "okx",
    }
    for key, wanted in exact_values.items():
        actual = env.get(key)
        report.add(
            "MASTER_FOLLOWER",
            "PASS" if _normalized(actual) == wanted else "FAIL",
            key,
            _display(actual),
        )

    exchanges = _csv_values(env.get("AETHER_EXCHANGES"))
    report.add(
        "MASTER_FOLLOWER",
        "PASS" if exchanges == ("okx", "binance") else "FAIL",
        "AETHER_EXCHANGES",
        ",".join(exchanges) if exchanges else "<missing>",
    )
    followers = _csv_values(env.get("AETHER_FOLLOWER_EXCHANGES"))
    report.add(
        "MASTER_FOLLOWER",
        "PASS" if followers == ("binance",) else "FAIL",
        "AETHER_FOLLOWER_EXCHANGES",
        ",".join(followers) if followers else "<missing>",
    )

    deviation = env.get("AETHER_ENTRY_DEVIATION_ALERT_PCT")
    parsed_deviation = _parse_decimal(deviation)
    if parsed_deviation is None or parsed_deviation < 0:
        deviation_status = "FAIL"
        deviation_detail = f"{_display(deviation)} must be a non-negative Decimal"
    elif parsed_deviation == Decimal("0.005"):
        deviation_status = "PASS"
        deviation_detail = str(parsed_deviation)
    else:
        deviation_status = "WARN"
        deviation_detail = f"{parsed_deviation} recommended=0.005"
    report.add(
        "MASTER_FOLLOWER",
        deviation_status,
        "AETHER_ENTRY_DEVIATION_ALERT_PCT",
        deviation_detail,
    )

    for key in (
        "AETHER_FOLLOWER_ENTRY_MAX_ATTEMPTS",
        "AETHER_MASTER_ENTRY_MAX_ATTEMPTS",
    ):
        value = env.get(key)
        parsed = _parse_int(value)
        report.add(
            "MASTER_FOLLOWER",
            "PASS" if parsed is not None and parsed >= 1 else "FAIL",
            key,
            _display(value),
        )

    for key in (
        "AETHER_FOLLOWER_ENTRY_RETRY_DELAY_SECONDS",
        "AETHER_MASTER_ENTRY_RETRY_DELAY_SECONDS",
        "AETHER_MASTER_FAIL_MANUAL_GRACE_SECONDS",
    ):
        value = env.get(key)
        parsed = _parse_decimal(value)
        report.add(
            "MASTER_FOLLOWER",
            "PASS" if parsed is not None and parsed >= 0 else "FAIL",
            key,
            _display(value),
        )

    for key in (
        "AETHER_CLOSE_ORPHAN_FOLLOWER_AFTER_GRACE",
        "AETHER_DO_NOT_REJOIN_MID_POSITION_AFTER_FOLLOWER_DESYNC",
    ):
        value = env.get(key)
        report.add(
            "MASTER_FOLLOWER",
            "PASS" if _normalized(value) == "true" else "FAIL",
            key,
            _display(value),
        )


def _check_market_stream(report: PreflightReport, env: Mapping[str, str]) -> None:
    market = env.get("AETHER_MARKET")
    report.add(
        "MARKET_STREAM",
        "PASS" if _normalized(market) == "eth-usdt-perp" else "FAIL",
        "AETHER_MARKET",
        _display(market),
    )
    streams = _csv_values(env.get("AETHER_DATA_STREAMS"))
    report.add(
        "MARKET_STREAM",
        "PASS" if "trades" in streams else "FAIL",
        "AETHER_DATA_STREAMS",
        ",".join(streams) if streams else "<missing>",
    )
    interval = env.get("AETHER_CLOSED_BAR_INTERVAL")
    report.add(
        "MARKET_STREAM",
        "PASS" if _normalized(interval) == "4h" else "FAIL",
        "AETHER_CLOSED_BAR_INTERVAL",
        _display(interval),
    )
    range_pct = env.get("AETHER_RANGE_PCT")
    report.add(
        "MARKET_STREAM",
        "PASS" if _decimal_equal(range_pct, "0.002") else "FAIL",
        "AETHER_RANGE_PCT",
        _display(range_pct),
    )
    warmup = env.get("AETHER_WARMUP_ENABLED")
    report.add(
        "MARKET_STREAM",
        "PASS" if _normalized(warmup) == "true" else "FAIL",
        "AETHER_WARMUP_ENABLED",
        _display(warmup),
    )


def _check_runtime_timing(report: PreflightReport, env: Mapping[str, str]) -> None:
    buffer_ms = env.get("AETHER_CLOSED_BAR_BUFFER_MS")
    report.add(
        "RUNTIME_TIMING",
        "PASS" if buffer_ms == "5000" else "WARN",
        "AETHER_CLOSED_BAR_BUFFER_MS",
        f"{_display(buffer_ms)} expected=5000",
    )
    scheduler = env.get("AETHER_SCHEDULER_POLL_SECONDS")
    report.add(
        "RUNTIME_TIMING",
        "PASS" if _decimal_equal(scheduler, "1.0") else "WARN",
        "AETHER_SCHEDULER_POLL_SECONDS",
        f"{_display(scheduler)} expected=1.0",
    )
    stale_timeout = env.get("AETHER_PRODUCER_STALE_TIMEOUT_MS")
    report.add(
        "RUNTIME_TIMING",
        "PASS" if stale_timeout == "60000" else "WARN",
        "AETHER_PRODUCER_STALE_TIMEOUT_MS",
        f"{_display(stale_timeout)} expected=60000",
    )


def _check_leverage(report: PreflightReport, env: Mapping[str, str]) -> None:
    okx = env.get("OKX_LEVERAGE")
    binance = env.get("BINANCE_LEVERAGE")
    okx_decimal = _parse_decimal(okx)
    binance_decimal = _parse_decimal(binance)
    valid = (
        okx_decimal is not None
        and binance_decimal is not None
        and okx_decimal > 0
        and binance_decimal > 0
    )
    if not valid or okx_decimal != binance_decimal:
        report.add(
            "LEVERAGE",
            "FAIL",
            "leverage_match",
            f"OKX={_display(okx)} BINANCE={_display(binance)}",
        )
    else:
        report.add(
            "LEVERAGE",
            "PASS",
            "leverage_match",
            f"OKX={okx_decimal} BINANCE={binance_decimal}",
        )
        status = "WARN" if okx_decimal == Decimal("15") else "PASS"
        detail = str(okx_decimal)
        if status == "WARN":
            detail += " confirm manually before real-live start"
            report.manual_checklist.append("Confirm leverage 15x is intended if configured")
        report.add("LEVERAGE", status, "configured_leverage", detail)

    margin_mode = env.get("MARGIN_MODE")
    report.add(
        "LEVERAGE",
        "PASS" if _normalized(margin_mode) == "isolated" else "FAIL",
        "MARGIN_MODE",
        _display(margin_mode),
    )


def _check_strategy(
    report: PreflightReport,
    env: Mapping[str, str],
) -> Strategy | None:
    configured = env.get("AETHER_STRATEGY")
    report.add(
        "STRATEGY",
        "PASS" if configured == EXPECTED_STRATEGY else "FAIL",
        "AETHER_STRATEGY",
        _display(configured),
    )
    try:
        loaded = load_strategy(EXPECTED_STRATEGY)
    except Exception as exc:
        report.add("STRATEGY", "FAIL", "strategy_load", str(exc))
        return None
    if not isinstance(loaded, Strategy):
        report.add("STRATEGY", "FAIL", "strategy_load", EXPECTED_STRATEGY)
        return None
    report.add("STRATEGY", "PASS", "strategy_load", EXPECTED_STRATEGY)

    config = loaded.config
    checks = (
        ("strategy_id", config.strategy_id, EXPECTED_STRATEGY_ID),
        (
            "enable_momentum_long_not_aligned_block",
            config.entry_filters.enable_momentum_long_not_aligned_block,
            True,
        ),
        (
            "enable_momentum_short_fast_speed_block",
            config.entry_filters.enable_momentum_short_fast_speed_block,
            True,
        ),
        (
            "range_speed_rolling_window_bars",
            config.entry_filters.range_speed_rolling_window_bars,
            1080,
        ),
        ("range_speed_min_periods", config.entry_filters.range_speed_min_periods, 100),
        ("range_speed_fast_quantile", config.entry_filters.range_speed_fast_quantile, 0.75),
    )
    for name, actual, expected in checks:
        report.add(
            "STRATEGY",
            "PASS" if actual == expected else "FAIL",
            name,
            str(actual),
        )
    return loaded


def _check_runtime_requirements(
    report: PreflightReport,
    strategy: Strategy,
    env: Mapping[str, str],
) -> None:
    requirements = strategy.runtime_requirements()
    expected = (
        ("closed_kline.enabled", _nested(requirements, "closed_kline", "enabled"), True),
        ("closed_kline.interval", _nested(requirements, "closed_kline", "interval"), "4h"),
        ("trades.enabled", _nested(requirements, "trades", "enabled"), True),
        ("trades.stream_enabled", _nested(requirements, "trades", "stream_enabled"), True),
        ("range_bars.enabled", _nested(requirements, "range_bars", "enabled"), True),
        ("range_bars.range_pct", _nested(requirements, "range_bars", "range_pct"), "0.002"),
        (
            "range_bars.aggregate_interval",
            _nested(requirements, "range_bars", "aggregate_interval"),
            "4h",
        ),
        (
            "account_state.poll_enabled",
            _nested(requirements, "account_state", "poll_enabled"),
            True,
        ),
        (
            "order_state.poll_when_position_enabled",
            _nested(requirements, "order_state", "poll_when_position_enabled"),
            True,
        ),
    )
    for name, actual, wanted in expected:
        if name == "range_bars.range_pct":
            passed = _decimal_equal(actual, wanted)
        elif isinstance(wanted, str):
            passed = _normalized(actual) == _normalized(wanted)
        else:
            passed = actual is wanted
        report.add(
            "RUNTIME_REQUIREMENTS",
            "PASS" if passed else "FAIL",
            name,
            str(actual),
        )

    mismatches: list[str] = []
    if _normalized(env.get("AETHER_CLOSED_BAR_INTERVAL")) != _normalized(
        _nested(requirements, "closed_kline", "interval")
    ):
        mismatches.append("closed_bar_interval")
    if not _decimal_equal(
        env.get("AETHER_RANGE_PCT"),
        _nested(requirements, "range_bars", "range_pct"),
    ):
        mismatches.append("range_pct")
    if bool(_nested(requirements, "trades", "stream_enabled")) and "trades" not in _csv_values(
        env.get("AETHER_DATA_STREAMS")
    ):
        mismatches.append("trades_stream")
    report.add(
        "RUNTIME_REQUIREMENTS",
        "FAIL" if mismatches else "PASS",
        "env_runtime_alignment",
        ",".join(mismatches) if mismatches else "aligned",
    )


def _check_strategy_env_boundary(
    report: PreflightReport,
    raw_env: Mapping[str, str],
) -> None:
    offending = sorted(
        key
        for key in raw_env
        if any(token in key.strip().lower() for token in FORBIDDEN_STRATEGY_ENV_KEYS)
    )
    report.add(
        "ENV_STRATEGY_PARAM_BOUNDARY",
        "FAIL" if offending else "PASS",
        "strategy_params_absent_from_env",
        ",".join(offending) if offending else "none",
    )


def _check_state_dbs(
    report: PreflightReport,
    env: Mapping[str, str],
    repo_root: Path,
) -> None:
    for env_key, db_kind in (
        ("AETHER_STATE_DB", "state"),
        ("AETHER_ORDER_JOURNAL_DB", "journal"),
    ):
        configured = env.get(env_key)
        if not configured or not configured.strip():
            report.add("STATE", "FAIL", env_key, "<missing>")
            continue
        path = _resolve_path(configured, repo_root)
        if not path.is_file():
            report.add("STATE", "PASS", env_key, f"{path} not present")
            continue
        report.add(
            "STATE",
            "WARN",
            env_key,
            f"{path} backup manually before live start",
        )
        _inspect_sqlite_read_only(report, path, db_kind)


def _inspect_sqlite_read_only(
    report: PreflightReport,
    path: Path,
    db_kind: str,
) -> None:
    try:
        uri = f"{path.resolve().as_uri()}?mode=ro"
        with sqlite3.connect(uri, uri=True) as connection:
            connection.execute("PRAGMA query_only = ON")
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            if db_kind == "state":
                _inspect_state_tables(report, connection, tables)
            else:
                _inspect_journal_tables(report, connection, tables)
    except (OSError, sqlite3.Error) as exc:
        report.add(
            "STATE",
            "WARN",
            f"{db_kind}_db_read_only_inspection",
            f"unable to inspect safely: {exc}; check manually",
        )


def _inspect_state_tables(
    report: PreflightReport,
    connection: sqlite3.Connection,
    tables: set[str],
) -> None:
    if "orders" not in tables:
        report.add(
            "STATE",
            "SKIPPED",
            "state_db_pending_orders",
            "orders table not present",
        )
    else:
        count = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM orders
                WHERE lower(status) IN ('new', 'partially_filled', 'unknown')
                """
            ).fetchone()[0]
        )
        report.add(
            "STATE",
            "FAIL" if count else "PASS",
            "state_db_pending_orders",
            f"{count} suspected active/pending local order rows",
        )

    if "account_snapshots" in tables:
        report.add(
            "STATE",
            "WARN",
            "state_db_active_positions",
            "historical snapshots cannot prove current flat state; check exchanges manually",
        )
    else:
        report.add(
            "STATE",
            "SKIPPED",
            "state_db_active_positions",
            "account_snapshots table not present",
        )


def _check_range_state(
    report: PreflightReport,
    env: Mapping[str, str],
    repo_root: Path,
) -> None:
    configured = env.get(
        "AETHER_RANGE_CHECKPOINT_DB",
        "data/state/range_builder_checkpoint.sqlite3",
    )
    path = _resolve_path(configured, repo_root)
    if not path.is_file():
        report.add(
            "RANGE_STATE",
            "WARN",
            "range_checkpoint_db",
            f"missing: {path}",
        )
        report.add(
            "RANGE_STATE",
            "WARN",
            "completed_range_aggregate_history_count",
            "0",
        )
        report.add(
            "RANGE_STATE",
            "WARN",
            "complete_range_history_min_periods",
            "V10A short-speed block unavailable until range history reaches min_periods",
        )
        report.add(
            "RANGE_STATE",
            "WARN",
            "current_bucket_checkpoint",
            "first current bucket will be COLD_START_PARTIAL",
        )
        return

    report.add("RANGE_STATE", "PASS", "range_checkpoint_db", str(path))
    try:
        uri = f"{path.resolve().as_uri()}?mode=ro"
        with sqlite3.connect(uri, uri=True) as connection:
            connection.execute("PRAGMA query_only = ON")
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            if "completed_range_aggregates" not in tables:
                total_count = complete_count = 0
            else:
                row = connection.execute(
                    """
                    SELECT COUNT(*),
                           SUM(CASE WHEN coverage_status = 'COMPLETE' THEN 1 ELSE 0 END)
                    FROM completed_range_aggregates
                    WHERE exchange = ? AND symbol = ? AND range_pct = ?
                    """,
                    (
                        _normalized(env.get("AETHER_DATA_EXCHANGE")) or "okx",
                        env.get("AETHER_MARKET", "ETH-USDT-PERP"),
                        _normalized_decimal_text(
                            env.get("AETHER_RANGE_PCT", "0.002")
                        ),
                    ),
                ).fetchone()
                total_count = int(row[0] or 0)
                complete_count = int(row[1] or 0)
            report.add(
                "RANGE_STATE",
                "PASS" if total_count else "WARN",
                "completed_range_aggregate_history_count",
                str(total_count),
            )
            report.add(
                "RANGE_STATE",
                "PASS" if complete_count >= 100 else "WARN",
                "complete_range_history_min_periods",
                (
                    f"COMPLETE={complete_count} min_periods=100"
                    if complete_count >= 100
                    else "V10A short-speed block unavailable until range history reaches min_periods "
                    f"(COMPLETE={complete_count}, min_periods=100)"
                ),
            )

            now_ms = int(time.time() * 1000)
            bucket_ms = 4 * 60 * 60 * 1000
            current_bucket_ms = (now_ms // bucket_ms) * bucket_ms
            checkpoint = None
            if "range_builder_checkpoints" in tables:
                checkpoint = connection.execute(
                    """
                    SELECT checkpoint_updated_at_ms, coverage_status, missing_gap_ms
                    FROM range_builder_checkpoints
                    WHERE exchange = ? AND symbol = ? AND range_pct = ?
                      AND bucket_start_ms = ?
                    """,
                    (
                        _normalized(env.get("AETHER_DATA_EXCHANGE")) or "okx",
                        env.get("AETHER_MARKET", "ETH-USDT-PERP"),
                        _normalized_decimal_text(
                            env.get("AETHER_RANGE_PCT", "0.002")
                        ),
                        current_bucket_ms,
                    ),
                ).fetchone()
            if checkpoint is None:
                report.add(
                    "RANGE_STATE",
                    "WARN",
                    "current_bucket_checkpoint",
                    "first current bucket will be COLD_START_PARTIAL",
                )
            else:
                age_ms = max(0, now_ms - int(checkpoint[0]))
                max_minor_age_ms = int(
                    env.get(
                        "AETHER_RANGE_CHECKPOINT_MAX_AGE_FOR_RECOVERED_MINOR_MS",
                        "60000",
                    )
                )
                status = "PASS" if age_ms <= max_minor_age_ms else "WARN"
                detail = (
                    "watchdog restart can recover current bucket as "
                    "RECOVERED_DEGRADED_MINOR"
                    if status == "PASS"
                    else "checkpoint is too old for RECOVERED_DEGRADED_MINOR"
                )
                report.add(
                    "RANGE_STATE",
                    status,
                    "current_bucket_checkpoint",
                    f"{detail}; age_ms={age_ms}",
                )
                report.add(
                    "RANGE_STATE",
                    status,
                    "current_bucket_checkpoint_age_ms",
                    str(age_ms),
                )
    except (OSError, sqlite3.Error, ValueError) as exc:
        report.add(
            "RANGE_STATE",
            "WARN",
            "range_checkpoint_db_read_only_inspection",
            f"unable to inspect safely: {exc}; check manually",
        )


def _inspect_journal_tables(
    report: PreflightReport,
    connection: sqlite3.Connection,
    tables: set[str],
) -> None:
    if "order_intents" not in tables:
        report.add(
            "STATE",
            "SKIPPED",
            "journal_pending_intents",
            "order_intents table not present",
        )
    else:
        count = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM order_intents
                WHERE lower(status) IN
                    ('created', 'planned', 'submitted', 'partially_submitted')
                """
            ).fetchone()[0]
        )
        report.add(
            "STATE",
            "FAIL" if count else "PASS",
            "journal_pending_intents",
            f"{count} pending local intent rows",
        )

    if "exchange_order_results" not in tables:
        report.add(
            "STATE",
            "SKIPPED",
            "journal_unclosed_results",
            "exchange_order_results table not present",
        )
    else:
        count = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM exchange_order_results
                WHERE lower(coalesce(status, '')) IN
                    ('new', 'partially_filled', 'unknown')
                """
            ).fetchone()[0]
        )
        report.add(
            "STATE",
            "FAIL" if count else "PASS",
            "journal_unclosed_results",
            f"{count} suspected unclosed exchange result rows",
        )


def _check_exchange_read_availability(
    report: PreflightReport,
    requested: bool,
) -> None:
    if requested:
        detail = (
            "requested, but no isolated read-only V10A adapter is available; "
            "no exchange client constructed"
        )
    else:
        detail = "not requested; no exchange client constructed"
    report.add(
        "EXCHANGE_READ",
        "SKIPPED",
        "EXCHANGE_READ_CHECK_SKIPPED",
        detail,
    )


def _nested(values: Mapping[str, object], section: str, key: str) -> object | None:
    nested = values.get(section)
    return nested.get(key) if isinstance(nested, Mapping) else None


def _resolve_path(value: str, repo_root: Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else repo_root / path


def _csv_values(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    return tuple(item.strip().lower() for item in value.split(",") if item.strip())


def _normalized(value: object | None) -> str:
    return "" if value is None else str(value).strip().lower()


def _display(value: object | None) -> str:
    return "<missing>" if value is None else str(value)


def _parse_decimal(value: object | None) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _parse_int(value: object | None) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _decimal_equal(left: object | None, right: object | None) -> bool:
    left_decimal = _parse_decimal(left)
    right_decimal = _parse_decimal(right)
    return (
        left_decimal is not None
        and right_decimal is not None
        and left_decimal == right_decimal
    )


def _normalized_decimal_text(value: object) -> str:
    parsed = _parse_decimal(value)
    if parsed is None:
        return str(value)
    return format(parsed.normalize(), "f")


def _sensitive_values(raw_env: Mapping[str, str]) -> tuple[str, ...]:
    values = {
        value
        for key, value in raw_env.items()
        if key.upper() in SENSITIVE_ENV_KEYS and value
    }
    return tuple(sorted(values, key=len, reverse=True))


def _redact_payload(value: Any, sensitive_values: Sequence[str]) -> Any:
    if isinstance(value, dict):
        return {
            key: (
                "<redacted>"
                if str(key).upper() in SENSITIVE_ENV_KEYS
                else _redact_payload(item, sensitive_values)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_payload(item, sensitive_values) for item in value]
    if isinstance(value, str):
        redacted = value
        for secret in sensitive_values:
            redacted = redacted.replace(secret, "<redacted>")
        return redacted
    return value


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
