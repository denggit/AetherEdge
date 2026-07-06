from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.app import AppConfig


@dataclass(frozen=True)
class LiveLaunchGateResult:
    ok: bool
    issues: tuple[str, ...]
    reports: Mapping[str, Mapping[str, Any]]


def validate_live_launch_reports(
    *,
    app_config: AppConfig,
    preflight_report_path: str | Path,
    smoke_report_path: str | Path,
    max_age_seconds: float = 600.0,
    now_ms: int | None = None,
) -> LiveLaunchGateResult:
    checked_at_ms = (
        int(time.time() * 1000) if now_ms is None else int(now_ms)
    )
    max_age_ms = max(0, int(float(max_age_seconds) * 1000))
    issues: list[str] = []
    reports: dict[str, Mapping[str, Any]] = {}
    for kind, raw_path in (
        ("preflight", preflight_report_path),
        ("smoke", smoke_report_path),
    ):
        path = Path(raw_path)
        report = _read_report(path)
        if report is None:
            issues.append(f"{kind}_report_missing_or_invalid")
            continue
        reports[kind] = report
        issues.extend(
            _report_issues(
                report,
                kind=kind,
                app_config=app_config,
                checked_at_ms=checked_at_ms,
                max_age_ms=max_age_ms,
            )
        )
    return LiveLaunchGateResult(
        ok=not issues,
        issues=tuple(dict.fromkeys(issues)),
        reports=reports,
    )


def strategy_identity(value: object) -> str:
    normalized = str(value or "").strip()
    module = normalized.split(":", 1)[0]
    if module.startswith("strategies."):
        module = module[len("strategies.") :]
    return module.rsplit(".", 1)[-1].strip().lower()


def live_reports_required(
    *,
    runtime_mode: object,
    strategy: object,
    configured: bool = False,
) -> bool:
    mode = getattr(runtime_mode, "value", runtime_mode)
    return (
        str(mode).strip().lower() == "live_runtime"
        and (
            configured
            or strategy_identity(strategy) == "eth_portfolio_v1"
        )
    )


def _read_report(path: Path) -> Mapping[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, Mapping) else None


def _report_issues(
    report: Mapping[str, Any],
    *,
    kind: str,
    app_config: AppConfig,
    checked_at_ms: int,
    max_age_ms: int,
) -> list[str]:
    issues: list[str] = []
    if report.get("ok") is not True:
        issues.append(f"{kind}_report_not_ok")
    if _int_or_none(report.get("exit_code")) != 0:
        issues.append(f"{kind}_report_exit_nonzero")
    if str(report.get("verdict", "")).strip().lower() != "pass":
        issues.append(f"{kind}_report_verdict_not_pass")
    if str(report.get("report_kind", "")).strip().lower() != kind:
        issues.append(f"{kind}_report_kind_mismatch")
    if bool(report.get("mutation_attempted", False)):
        issues.append(f"{kind}_mutation_attempted")

    generated_at_ms = _positive_int(report.get("generated_at_ms"))
    if generated_at_ms is None:
        issues.append(f"{kind}_report_timestamp_missing")
    else:
        age_ms = checked_at_ms - generated_at_ms
        if age_ms < 0 or age_ms > max_age_ms:
            issues.append(f"{kind}_report_stale")

    if strategy_identity(report.get("strategy")) != strategy_identity(
        app_config.strategy
    ):
        issues.append(f"{kind}_strategy_mismatch")
    if str(report.get("symbol", "")) != app_config.symbol:
        issues.append(f"{kind}_symbol_mismatch")
    if _normalized_exchanges(report.get("exchanges")) != {
        exchange.value for exchange in app_config.exchanges
    }:
        issues.append(f"{kind}_exchanges_mismatch")
    if (
        str(report.get("data_exchange", "")).strip().lower()
        != app_config.data_exchange.value
    ):
        issues.append(f"{kind}_data_exchange_mismatch")

    checks = report.get("startup_gate_results")
    if not isinstance(checks, Sequence) or isinstance(
        checks, (str, bytes)
    ):
        issues.append(f"{kind}_startup_gate_results_missing")
    elif any(
        not isinstance(check, Mapping)
        or str(check.get("status", "")).strip().lower() != "ok"
        for check in checks
    ):
        issues.append(f"{kind}_startup_gate_failed")
    return issues


def _normalized_exchanges(value: object) -> set[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return set()
    return {
        str(exchange).strip().lower()
        for exchange in value
        if str(exchange).strip()
    }


def _positive_int(value: object) -> int | None:
    parsed = _int_or_none(value)
    if parsed is None:
        return None
    return parsed if parsed > 0 else None


def _int_or_none(value: object) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed


__all__ = [
    "LiveLaunchGateResult",
    "live_reports_required",
    "strategy_identity",
    "validate_live_launch_reports",
]
