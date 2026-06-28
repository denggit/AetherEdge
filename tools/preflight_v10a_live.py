#!/usr/bin/env python
"""Read-only local preflight for the V10A real-live strategy configuration.

This tool does not create exchange clients, start the runtime, mutate local
state, or submit/cancel orders. Exchange state must be checked manually.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping, Sequence

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
    "micro_context",
    "range_exit",
    "global_risk_scale",
    "bull_reclaim",
    "momentum_v3",
    "bear_v3",
)
SECTION_ORDER = (
    "ENV",
    "RUNTIME_TIMING",
    "LEVERAGE",
    "STRATEGY",
    "RUNTIME_REQUIREMENTS",
    "ENV_STRATEGY_PARAM_BOUNDARY",
    "STATE",
    "EXCHANGE_READ",
)


@dataclass(frozen=True)
class CheckResult:
    section: str
    status: str
    name: str
    detail: str = ""


@dataclass
class PreflightReport:
    checks: list[CheckResult] = field(default_factory=list)

    def add(self, section: str, status: str, name: str, detail: str = "") -> None:
        self.checks.append(CheckResult(section=section, status=status, name=name, detail=detail))

    @property
    def ok(self) -> bool:
        return not any(check.status == "FAIL" for check in self.checks)

    def named(self, name: str) -> list[CheckResult]:
        return [check for check in self.checks if check.name == name]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only local V10A real-live preflight. Does not connect to exchanges or mutate state."
    )
    parser.add_argument("--env-file", default=str(REPO_ROOT / ".env"), help="Path to the runtime .env file")
    return parser.parse_args(argv)


def run_preflight(
    *,
    env_file: str | Path,
    environ: Mapping[str, str] | None = None,
    repo_root: str | Path = REPO_ROOT,
) -> PreflightReport:
    report = PreflightReport()
    env_path = Path(env_file)
    root = Path(repo_root)
    raw_env = _read_env_file(env_path)
    effective_env = load_env_config(env_path, environ=environ)

    if env_path.is_file():
        report.add("ENV", "PASS", "env_file", str(env_path))
    else:
        report.add("ENV", "FAIL", "env_file", f"missing: {env_path}")

    _check_env_safety(report, effective_env)
    _check_runtime_timing(report, effective_env)
    _check_leverage(report, effective_env)
    strategy = _check_strategy(report)
    if strategy is not None:
        _check_runtime_requirements(report, strategy, effective_env)
    _check_strategy_env_boundary(report, raw_env)
    _check_state_db_reminders(report, root)
    _mark_exchange_read_skipped(report)
    return report


def render_report(report: PreflightReport) -> str:
    lines = ["[V10A LIVE PREFLIGHT]", ""]
    for section in SECTION_ORDER:
        lines.append(f"{section}:")
        for check in (item for item in report.checks if item.section == section):
            suffix = f" {check.detail}" if check.detail else ""
            lines.append(f"{check.status} {check.name}{suffix}")
        if section == "EXCHANGE_READ":
            lines.extend(
                [
                    "Manual check required before live start:",
                    "- no existing ETH-USDT-PERP positions",
                    "- no stale open orders",
                    "- no stale stop orders",
                ]
            )
        lines.append("")
    lines.extend(
        [
            "FINAL:",
            "PASS_READY_FOR_MANUAL_LIVE_START" if report.ok else "FAIL_FIX_REQUIRED",
        ]
    )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = run_preflight(env_file=args.env_file)
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


def _check_env_safety(report: PreflightReport, env: Mapping[str, str]) -> None:
    expected = {
        "AETHER_RUNTIME_MODE": "live_runtime",
        "AETHER_STRATEGY": EXPECTED_STRATEGY,
        "AETHER_DRY_RUN": "false",
        "AETHER_LIVE_TRADING": "true",
        "OKX_SANDBOX": "false",
        "BINANCE_SANDBOX": "false",
        "AETHER_MASTER_EXCHANGE": "okx",
        "AETHER_DATA_EXCHANGE": "okx",
        "AETHER_MARKET": "ETH-USDT-PERP",
        "MARGIN_MODE": "isolated",
    }
    for key, wanted in expected.items():
        actual = env.get(key)
        status = "PASS" if _normalized(actual) == _normalized(wanted) else "FAIL"
        report.add("ENV", status, key, _display(actual))

    exchanges = _csv_values(env.get("AETHER_EXCHANGES"))
    report.add(
        "ENV",
        "PASS" if exchanges == ("okx", "binance") else "FAIL",
        "AETHER_EXCHANGES",
        ",".join(exchanges) if exchanges else "<missing>",
    )
    followers = _csv_values(env.get("AETHER_FOLLOWER_EXCHANGES"))
    report.add(
        "ENV",
        "PASS" if "binance" in followers else "FAIL",
        "AETHER_FOLLOWER_EXCHANGES",
        ",".join(followers) if followers else "<missing>",
    )
    streams = _csv_values(env.get("AETHER_DATA_STREAMS"))
    report.add(
        "ENV",
        "PASS" if "trades" in streams else "FAIL",
        "AETHER_DATA_STREAMS",
        ",".join(streams) if streams else "<missing>",
    )


def _check_runtime_timing(report: PreflightReport, env: Mapping[str, str]) -> None:
    interval = env.get("AETHER_CLOSED_BAR_INTERVAL")
    report.add(
        "RUNTIME_TIMING",
        "PASS" if _normalized(interval) == "4h" else "FAIL",
        "AETHER_CLOSED_BAR_INTERVAL",
        _display(interval),
    )

    buffer_ms = env.get("AETHER_CLOSED_BAR_BUFFER_MS")
    report.add(
        "RUNTIME_TIMING",
        "PASS" if buffer_ms == "5000" else "WARN",
        "AETHER_CLOSED_BAR_BUFFER_MS",
        f"{_display(buffer_ms)} expected=5000",
    )

    range_pct = env.get("AETHER_RANGE_PCT")
    report.add(
        "RUNTIME_TIMING",
        "PASS" if _decimal_equal(range_pct, "0.002") else "FAIL",
        "AETHER_RANGE_PCT",
        _display(range_pct),
    )

    warmup = env.get("AETHER_WARMUP_ENABLED")
    report.add(
        "RUNTIME_TIMING",
        "PASS" if _normalized(warmup) == "true" else "FAIL",
        "AETHER_WARMUP_ENABLED",
        _display(warmup),
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
    if okx is None or binance is None:
        report.add(
            "LEVERAGE",
            "FAIL",
            "leverage_match",
            f"OKX={_display(okx)} BINANCE={_display(binance)}",
        )
        return
    if not _decimal_equal(okx, binance):
        report.add("LEVERAGE", "FAIL", "leverage_match", f"OKX={okx} BINANCE={binance}")
        return
    report.add("LEVERAGE", "PASS", "leverage_match", f"OKX={okx} BINANCE={binance}")
    status = "WARN" if _decimal_equal(okx, "15") else "PASS"
    detail = f"{okx} confirm manually before real-live start" if status == "WARN" else okx
    report.add("LEVERAGE", status, "configured_leverage", detail)


def _check_strategy(report: PreflightReport) -> Strategy | None:
    try:
        loaded = load_strategy(EXPECTED_STRATEGY)
        strategy = Strategy()
    except Exception as exc:
        report.add("STRATEGY", "FAIL", "strategy_load", str(exc))
        return None

    report.add(
        "STRATEGY",
        "PASS" if isinstance(loaded, Strategy) else "FAIL",
        "strategy_load",
        EXPECTED_STRATEGY,
    )
    config = strategy.config
    checks = (
        ("strategy_id", config.strategy_id, EXPECTED_STRATEGY_ID),
        (
            "v10_long_block_enabled",
            config.entry_filters.enable_momentum_long_not_aligned_block,
            True,
        ),
        (
            "v10a_short_speed_block_enabled",
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
    return strategy


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


def _check_strategy_env_boundary(report: PreflightReport, raw_env: Mapping[str, str]) -> None:
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


def _check_state_db_reminders(report: PreflightReport, repo_root: Path) -> None:
    for relative in (
        Path("data/state/aether_state.sqlite3"),
        Path("data/state/aether_order_journal.sqlite3"),
    ):
        path = repo_root / relative
        if path.is_file():
            report.add(
                "STATE",
                "WARN",
                str(relative).replace("\\", "/"),
                "Before live start, backup this DB file manually.",
            )
        else:
            report.add("STATE", "PASS", str(relative).replace("\\", "/"), "not present")


def _mark_exchange_read_skipped(report: PreflightReport) -> None:
    report.add(
        "EXCHANGE_READ",
        "SKIPPED",
        "EXCHANGE_READ_CHECK_SKIPPED",
        "exchange clients intentionally not constructed",
    )


def _nested(values: Mapping[str, object], section: str, key: str) -> object | None:
    nested = values.get(section)
    return nested.get(key) if isinstance(nested, Mapping) else None


def _csv_values(value: str | None) -> tuple[str, ...]:
    if value is None:
        return ()
    return tuple(item.strip().lower() for item in value.split(",") if item.strip())


def _normalized(value: object | None) -> str:
    return "" if value is None else str(value).strip().lower()


def _display(value: object | None) -> str:
    return "<missing>" if value is None else str(value)


def _decimal_equal(left: object | None, right: object | None) -> bool:
    try:
        return Decimal(str(left)) == Decimal(str(right))
    except (InvalidOperation, TypeError, ValueError):
        return False


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
