from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, Side


@dataclass(frozen=True)
class ExpectedSignalAuditRow:
    key_time_ms: int
    signal: int
    selected_engine: str
    selected_priority: int
    micro_context_available: bool
    micro_aligned: bool
    micro_contra: bool
    micro_entry_risk_scale: Decimal
    final_entry_risk_scale: Decimal
    micro_filter_action: str
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ParityComparison:
    key_time_ms: int
    matched: bool
    missing_expected: bool = False
    mismatches: Mapping[str, tuple[Any, Any]] = field(default_factory=dict)
    expected: ExpectedSignalAuditRow | None = None
    actual: Mapping[str, Any] = field(default_factory=dict)


class SignalAuditReference:
    """Load CoinBacktest V8 signal_audit rows for readonly parity checks.

    This is a read-only comparison helper. It must never import CoinBacktest and
    must never drive live orders.
    """

    def __init__(self, rows: Mapping[int, ExpectedSignalAuditRow]) -> None:
        self.rows = dict(rows)

    @classmethod
    def from_csv(cls, path: str | Path, *, timestamp_is_bar_open: bool = True) -> "SignalAuditReference":
        rows: dict[int, ExpectedSignalAuditRow] = {}
        with Path(path).open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                key_time_ms = _parse_timestamp_ms(str(row.get("timestamp", "")))
                rows[key_time_ms] = ExpectedSignalAuditRow(
                    key_time_ms=key_time_ms,
                    signal=int(_num(row.get("signal"), 0)),
                    selected_engine=str(row.get("selected_engine") or "NONE"),
                    selected_priority=int(_num(row.get("selected_priority"), 0)),
                    micro_context_available=_bool(row.get("micro_context_available")),
                    micro_aligned=_bool(row.get("micro_aligned")),
                    micro_contra=_bool(row.get("micro_contra")),
                    micro_entry_risk_scale=_dec(row.get("micro_entry_risk_scale"), Decimal("1")),
                    final_entry_risk_scale=_dec(row.get("final_entry_risk_scale"), Decimal("1")),
                    micro_filter_action=str(row.get("micro_filter_action") or "UNKNOWN"),
                    raw=dict(row),
                )
        return cls(rows)

    def get(self, key_time_ms: int) -> ExpectedSignalAuditRow | None:
        return self.rows.get(key_time_ms)


class ReadonlyParityChecker:
    def __init__(self, reference: SignalAuditReference, *, timestamp_key: str = "open_time_ms", decimal_tolerance: Decimal = Decimal("0.000001")) -> None:
        if timestamp_key not in {"open_time_ms", "close_time_ms"}:
            raise ValueError("timestamp_key must be open_time_ms or close_time_ms")
        self.reference = reference
        self.timestamp_key = timestamp_key
        self.decimal_tolerance = decimal_tolerance
        self.comparisons: list[ParityComparison] = []

    def compare(self, context: BarReadyContext) -> ParityComparison:
        key_time_ms = context.kline.open_time_ms if self.timestamp_key == "open_time_ms" else context.kline.close_time_ms
        expected = self.reference.get(key_time_ms)
        actual = _actual_from_context(context)
        if expected is None:
            comparison = ParityComparison(key_time_ms=key_time_ms, matched=False, missing_expected=True, actual=actual)
            self.comparisons.append(comparison)
            return comparison
        mismatches: dict[str, tuple[Any, Any]] = {}
        _compare_int(mismatches, "signal", expected.signal, actual["signal"])
        _compare_str(mismatches, "selected_engine", expected.selected_engine, actual["selected_engine"])
        _compare_int(mismatches, "selected_priority", expected.selected_priority, actual["selected_priority"])
        _compare_bool(mismatches, "micro_context_available", expected.micro_context_available, actual["micro_context_available"])
        _compare_bool(mismatches, "micro_aligned", expected.micro_aligned, actual["micro_aligned"])
        _compare_bool(mismatches, "micro_contra", expected.micro_contra, actual["micro_contra"])
        _compare_decimal(mismatches, "micro_entry_risk_scale", expected.micro_entry_risk_scale, actual["micro_entry_risk_scale"], self.decimal_tolerance)
        _compare_decimal(mismatches, "final_entry_risk_scale", expected.final_entry_risk_scale, actual["final_entry_risk_scale"], self.decimal_tolerance)
        _compare_str(mismatches, "micro_filter_action", expected.micro_filter_action, actual["micro_filter_action"])
        comparison = ParityComparison(
            key_time_ms=key_time_ms,
            matched=not mismatches,
            mismatches=mismatches,
            expected=expected,
            actual=actual,
        )
        self.comparisons.append(comparison)
        return comparison

    @property
    def mismatch_count(self) -> int:
        return sum(1 for comparison in self.comparisons if not comparison.matched)


def _actual_from_context(context: BarReadyContext) -> dict[str, Any]:
    signal = int(context.routed_signal.side.value)
    return {
        "signal": signal,
        "selected_engine": "NONE" if signal == 0 else context.routed_signal.engine,
        "selected_priority": 0 if signal == 0 else context.routed_signal.priority,
        "micro_context_available": context.micro.context_available,
        "micro_aligned": context.micro.aligned,
        "micro_contra": context.micro.contra,
        "micro_entry_risk_scale": context.micro.entry_risk_scale,
        "final_entry_risk_scale": context.final_entry_risk_scale,
        "micro_filter_action": context.micro.action,
    }


def _parse_timestamp_ms(value: str) -> int:
    text = value.strip()
    if not text:
        raise ValueError("empty timestamp")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _num(value: Any, default: int | float) -> float:
    if value is None or value == "" or str(value).lower() == "nan":
        return float(default)
    return float(value)


def _dec(value: Any, default: Decimal) -> Decimal:
    if value is None or value == "" or str(value).lower() == "nan":
        return default
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return default


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"true", "1", "yes", "y"}


def _compare_int(out: dict[str, tuple[Any, Any]], name: str, expected: int, actual: Any) -> None:
    if int(expected) != int(actual):
        out[name] = (expected, actual)


def _compare_str(out: dict[str, tuple[Any, Any]], name: str, expected: str, actual: Any) -> None:
    if str(expected) != str(actual):
        out[name] = (expected, actual)


def _compare_bool(out: dict[str, tuple[Any, Any]], name: str, expected: bool, actual: Any) -> None:
    if bool(expected) != bool(actual):
        out[name] = (expected, actual)


def _compare_decimal(out: dict[str, tuple[Any, Any]], name: str, expected: Decimal, actual: Any, tolerance: Decimal) -> None:
    actual_dec = actual if isinstance(actual, Decimal) else Decimal(str(actual))
    if abs(expected - actual_dec) > tolerance:
        out[name] = (expected, actual_dec)
