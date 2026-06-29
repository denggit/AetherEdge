from __future__ import annotations

import argparse
import csv
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v10b.execution.structural_stop import (
    StructuralStopConfig,
    evaluate_swing_structural_stop,
)


DEFAULT_OUTPUT = Path("data/reports/aetheredge_v10b_parity/structural_stop_parity.csv")
OUTPUT_FIELDS = (
    "bar_close_time",
    "side",
    "old_stop",
    "swing_low_21",
    "swing_high_21",
    "raw_candidate",
    "rounded_candidate",
    "accepted",
    "final_stop",
    "mismatch_reason",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline AetherEdge V10B swing structural-stop parity audit."
    )
    parser.add_argument("--bars-csv", required=True, help="Closed strategy bars CSV.")
    parser.add_argument(
        "--reference-csv",
        default=None,
        help="Optional external structural audit CSV keyed by bar_close_time.",
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument(
        "--side",
        choices=("long", "short"),
        default=None,
        help="Default side when the bars CSV has no side column.",
    )
    parser.add_argument(
        "--initial-stop",
        default=None,
        help="Initial canonical stop when the bars CSV has no old_stop column.",
    )
    parser.add_argument("--price-tick", default=None)
    return parser.parse_args()


def run(args: argparse.Namespace) -> Path:
    bars_rows = _read_csv(Path(args.bars_csv))
    reference = _reference_by_time(Path(args.reference_csv)) if args.reference_csv else {}
    config = StructuralStopConfig.from_mapping(
        {
            "price_tick": args.price_tick,
        }
    )
    rolling_bars: list[SimpleNamespace] = []
    canonical_stop = _decimal(args.initial_stop)
    output_rows: list[dict[str, Any]] = []

    for row in bars_rows:
        bar_close_time = _time_value(row)
        low = _required_decimal(row, "low")
        high = _required_decimal(row, "high")
        close = _required_decimal(row, "close")
        rolling_bars.append(SimpleNamespace(low=low, high=high, close=close))

        side = _side(row.get("side") or args.side)
        old_stop = _decimal(row.get("old_stop")) or canonical_stop
        base_v10a_stop = _decimal(row.get("base_v10a_stop")) or old_stop
        decision = evaluate_swing_structural_stop(
            closed_bars=rolling_bars,
            side=side,
            old_stop=old_stop,
            base_v10a_stop=base_v10a_stop,
            current_close=close,
            atr=_decimal(row.get("atr")) or Decimal("0"),
            engine=str(row.get("engine") or row.get("entry_engine") or "PARITY"),
            hold_bars=int(row.get("hold_bars") or len(rolling_bars) - 1),
            mfe_r=_decimal(row.get("mfe_r")) or Decimal("0"),
            bar_close_time=bar_close_time,
            config=config,
            current_bar_exit=_bool(row.get("current_bar_exit")),
        )
        if decision.accepted and decision.final_stop is not None:
            canonical_stop = decision.final_stop
        elif old_stop is not None:
            canonical_stop = old_stop

        actual = {
            "bar_close_time": bar_close_time,
            "side": decision.side,
            "old_stop": _text(old_stop),
            "swing_low_21": _text(decision.swing_low_21),
            "swing_high_21": _text(decision.swing_high_21),
            "raw_candidate": _text(decision.raw_candidate),
            "rounded_candidate": _text(decision.rounded_candidate),
            "accepted": str(decision.accepted).lower(),
            "final_stop": _text(decision.final_stop),
        }
        actual["mismatch_reason"] = _mismatch_reason(
            actual,
            reference.get(str(bar_close_time)),
            local_reject_reason=decision.reject_reason,
        )
        output_rows.append(actual)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(output_rows)
    return output_path


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError(f"no rows found in {path}")
    return rows


def _reference_by_time(path: Path) -> dict[str, dict[str, str]]:
    return {str(_time_value(row)): row for row in _read_csv(path)}


def _mismatch_reason(
    actual: dict[str, Any],
    expected: dict[str, str] | None,
    *,
    local_reject_reason: str,
) -> str:
    if expected is None:
        return local_reject_reason if local_reject_reason else ""
    mismatches: list[str] = []
    aliases = {
        "raw_candidate": ("raw_candidate", "structural_candidate"),
        "rounded_candidate": ("rounded_candidate", "candidate"),
        "accepted": ("accepted", "structural_accepted"),
        "final_stop": ("final_stop", "stop_price"),
    }
    for field, possible_names in aliases.items():
        expected_value = next(
            (expected.get(name) for name in possible_names if expected.get(name) not in (None, "")),
            None,
        )
        if expected_value is None:
            continue
        if not _equivalent(actual.get(field), expected_value):
            mismatches.append(f"{field}:local={actual.get(field)} reference={expected_value}")
    return "; ".join(mismatches)


def _equivalent(left: Any, right: Any) -> bool:
    left_text = str(left).strip().lower()
    right_text = str(right).strip().lower()
    if left_text in {"true", "false"} or right_text in {"true", "false"}:
        return left_text == right_text
    try:
        return Decimal(left_text) == Decimal(right_text)
    except Exception:
        return left_text == right_text


def _time_value(row: dict[str, str]) -> str:
    value = row.get("bar_close_time") or row.get("close_time") or row.get("timestamp")
    if value in (None, ""):
        raise ValueError("bars CSV requires bar_close_time, close_time, or timestamp")
    return str(value)


def _required_decimal(row: dict[str, str], field: str) -> Decimal:
    value = _decimal(row.get(field))
    if value is None:
        raise ValueError(f"bars CSV requires numeric {field}")
    return value


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _side(value: Any) -> Side:
    normalized = str(value or "").strip().lower()
    if normalized in {"long", "1", "buy"}:
        return Side.LONG
    if normalized in {"short", "-1", "sell"}:
        return Side.SHORT
    return Side.FLAT


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _text(value: Decimal | None) -> str:
    return "" if value is None else str(value)


if __name__ == "__main__":
    result = run(parse_args())
    print(result)
