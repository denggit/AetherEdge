import argparse
import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

import pandas as pd

from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, ClosedKlineContext, RangeAggregateContext, Side
from strategies.eth_lf_portfolio_v8.strategy import DEFAULT_CONFIG_PATH, FOUR_HOURS_MS, Strategy


DEFAULT_OUT_DIR = Path("data/parity/v9c_signal")
REPLAY_AUDIT_FILENAME = "aetheredge_v9c_replay_signal_audit.csv"
MISMATCH_FILENAME = "signal_mismatches.csv"
SUMMARY_FILENAME = "parity_summary.json"
FINGERPRINT_FILENAME = "fingerprint.json"

REQUIRED_COLUMNS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "signal",
    "selected_engine",
    "selected_priority",
    "risk_mult",
    "quality_mult",
    "momentum_signal",
    "bear_signal",
    "bull_signal",
    "micro_context_available",
    "micro_aligned",
    "micro_contra",
    "micro_entry_risk_scale",
    "micro_filter_action",
    "rf_bar_count",
    "rf_micro_return_pct",
    "rf_close_pos",
    "rf_delta_sum",
    "rf_imbalance",
    "rf_taker_buy_ratio",
]

AUDIT_COLUMNS = list(REQUIRED_COLUMNS)

STRICT_COMPARE_FIELDS = [
    "signal",
    "selected_engine",
    "selected_priority",
    "momentum_signal",
    "bear_signal",
    "bull_signal",
    "micro_context_available",
    "micro_aligned",
    "micro_contra",
    "micro_filter_action",
]

FLOAT_COMPARE_FIELDS = [
    "risk_mult",
    "quality_mult",
    "micro_entry_risk_scale",
    "rf_bar_count",
    "rf_micro_return_pct",
    "rf_close_pos",
    "rf_delta_sum",
    "rf_imbalance",
    "rf_taker_buy_ratio",
]


@dataclass(frozen=True)
class CompareResult:
    mismatches: pd.DataFrame
    mismatch_fields: dict[str, int]
    joined_rows: int
    compared_rows: int

    @property
    def mismatch_count(self) -> int:
        return int(len(self.mismatches))


def validate_coin_audit_columns(df: pd.DataFrame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def timestamp_to_open_close_ms(timestamp: Any) -> tuple[int, int]:
    parsed = pd.to_datetime(timestamp, utc=True)
    open_time_ms = int(parsed.value // 1_000_000)
    return open_time_ms, open_time_ms + FOUR_HOURS_MS - 1


def build_range_context_from_rf_columns(
    row: Mapping[str, Any],
    *,
    strategy: Strategy | None = None,
    open_time_ms: int | None = None,
    close_time_ms: int | None = None,
) -> RangeAggregateContext | None:
    bar_count_value = row.get("rf_bar_count")
    if _is_missing(bar_count_value):
        return None
    bar_count = int(float(bar_count_value))
    if bar_count <= 0:
        return None

    if open_time_ms is None or close_time_ms is None:
        open_time_ms, close_time_ms = timestamp_to_open_close_ms(row["timestamp"])

    symbol = "ETH-USDT-PERP"
    exchange = "okx"
    range_pct = Decimal("0.002")
    if strategy is not None:
        symbol = strategy.config.symbol
        exchange = strategy.config.data_exchange
        range_pct = _strategy_range_pct(strategy)

    imbalance = _decimal(row.get("rf_imbalance"), Decimal("0"))
    return RangeAggregateContext(
        symbol=symbol,
        exchange=exchange,
        timeframe="4h",
        bucket_start_ms=open_time_ms,
        bucket_end_ms=close_time_ms,
        range_pct=range_pct,
        bar_count=bar_count,
        first_open=_decimal(row.get("open")),
        last_close=_decimal(row.get("close")),
        high=_decimal(row.get("high")),
        low=_decimal(row.get("low")),
        buy_notional_sum=(Decimal("1") + imbalance) / Decimal("2"),
        sell_notional_sum=(Decimal("1") - imbalance) / Decimal("2"),
        delta_notional_sum=_decimal(row.get("rf_delta_sum"), Decimal("0")),
        notional_sum=Decimal("1"),
        micro_return_pct=_decimal(row.get("rf_micro_return_pct"), Decimal("0")),
        imbalance=imbalance,
        taker_buy_ratio=_decimal(row.get("rf_taker_buy_ratio"), Decimal("0")),
        close_pos=_decimal(row.get("rf_close_pos"), Decimal("0")),
    )


def replay_aetheredge_signal_audit(coin_df: pd.DataFrame, *, max_rows: int | None = None) -> pd.DataFrame:
    coin_df = coin_df.head(max_rows).copy() if max_rows is not None else coin_df.copy()
    validate_coin_audit_columns(coin_df)
    strategy = Strategy()
    rows: list[dict[str, Any]] = []

    for _, row in coin_df.iterrows():
        open_time_ms, close_time_ms = timestamp_to_open_close_ms(row["timestamp"])
        kline = ClosedKlineContext(
            symbol=strategy.config.symbol,
            exchange=strategy.config.data_exchange,
            timeframe="4h",
            open_time_ms=open_time_ms,
            close_time_ms=close_time_ms,
            open=_decimal(row["open"]),
            high=_decimal(row["high"]),
            low=_decimal(row["low"]),
            close=_decimal(row["close"]),
            volume=_decimal(row["volume"]),
        )
        strategy.buffer.put_kline(kline)

        aggregate = build_range_context_from_rf_columns(
            row.to_dict(),
            strategy=strategy,
            open_time_ms=open_time_ms,
            close_time_ms=close_time_ms,
        )
        if aggregate is not None:
            strategy.buffer.put_range_aggregate(aggregate)

        feature_rows = strategy.feature_builder.build_latest(strategy.buffer.closed_klines, target_close_time_ms=close_time_ms)
        engine_features = {
            "momentum": feature_rows.momentum or {},
            "bear": feature_rows.bear or {},
            "bull": feature_rows.bull or {},
        }
        bootstrap_micro = strategy.micro_engine.evaluate(signal_side=Side.FLAT, aggregate=aggregate)
        bootstrap_context = BarReadyContext(
            kline=kline,
            range_aggregate=aggregate,
            micro=bootstrap_micro,
            global_risk_scale=strategy.config.global_risk_scale,
            engine_features=engine_features,
        )
        routed = strategy.router.evaluate(bootstrap_context)
        micro = strategy.micro_engine.evaluate(signal_side=routed.side, aggregate=aggregate)
        ready = BarReadyContext(
            kline=kline,
            range_aggregate=aggregate,
            micro=micro,
            global_risk_scale=strategy.config.global_risk_scale,
            routed_signal=routed,
            engine_features=engine_features,
        )
        strategy.bar_ready_events.append(ready)

        rows.append(_audit_row_from_context(row, ready))

    return pd.DataFrame(rows, columns=AUDIT_COLUMNS)


def compare_signal_audits(
    coin_df: pd.DataFrame,
    aetheredge_df: pd.DataFrame,
    *,
    tolerance: float = 1e-9,
    skip_warmup_bars: int = 250,
) -> CompareResult:
    validate_coin_audit_columns(coin_df)
    _validate_aetheredge_audit_columns(aetheredge_df)
    joined = pd.merge(coin_df, aetheredge_df, on="timestamp", how="inner", suffixes=("_coin", "_aetheredge"))
    compared = joined.iloc[skip_warmup_bars:] if skip_warmup_bars else joined
    mismatches: list[dict[str, Any]] = []
    mismatch_fields: dict[str, int] = {}

    for _, row in compared.iterrows():
        timestamp = row["timestamp"]
        for field in STRICT_COMPARE_FIELDS:
            coin_value = row[f"{field}_coin"]
            ae_value = row[f"{field}_aetheredge"]
            if _canonical_strict_value(coin_value) != _canonical_strict_value(ae_value):
                _append_mismatch(mismatches, mismatch_fields, timestamp, field, coin_value, ae_value, None)
        for field in FLOAT_COMPARE_FIELDS:
            coin_value = row[f"{field}_coin"]
            ae_value = row[f"{field}_aetheredge"]
            coin_float = _optional_float(coin_value)
            ae_float = _optional_float(ae_value)
            if coin_float is None or ae_float is None:
                if coin_float != ae_float:
                    _append_mismatch(mismatches, mismatch_fields, timestamp, field, coin_value, ae_value, None)
                continue
            abs_diff = abs(coin_float - ae_float)
            if abs_diff > tolerance:
                _append_mismatch(mismatches, mismatch_fields, timestamp, field, coin_value, ae_value, abs_diff)

    mismatch_df = pd.DataFrame(mismatches, columns=["timestamp", "field", "coin_value", "aetheredge_value", "abs_diff"])
    return CompareResult(
        mismatches=mismatch_df,
        mismatch_fields=mismatch_fields,
        joined_rows=int(len(joined)),
        compared_rows=int(len(compared)),
    )


def strategy_fingerprint(strategy: Strategy | None = None) -> dict[str, Any]:
    strategy = strategy or Strategy()
    config_data = json.loads(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    micro_context = strategy.config.micro_context
    fingerprint: dict[str, Any] = {
        "strategy_module": "strategies.eth_lf_portfolio_v8.strategy",
        "strategy_class": "Strategy",
        "strategy_id": strategy.config.strategy_id,
        "priority_mode": config_data.get("portfolio_router", {}).get("priority_mode"),
        "global_risk_scale": str(strategy.config.global_risk_scale),
        "range_pct": str(_strategy_range_pct(strategy)),
        "micro_filter_mode": micro_context.mode,
        "micro_min_range_bars": micro_context.min_range_bars,
        "engine_params": {name: _jsonable(asdict(params)) for name, params in sorted(strategy.engine_params.items())},
    }
    fingerprint["fingerprint_hash"] = _fingerprint_hash(fingerprint)
    return fingerprint


def write_outputs(
    *,
    coin_audit_path: Path,
    out_dir: Path,
    coin_df: pd.DataFrame,
    aetheredge_df: pd.DataFrame,
    compare_result: CompareResult,
    tolerance: float,
    skip_warmup_bars: int,
    fingerprint: Mapping[str, Any],
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    aetheredge_df.to_csv(out_dir / REPLAY_AUDIT_FILENAME, index=False)
    compare_result.mismatches.to_csv(out_dir / MISMATCH_FILENAME, index=False)
    Path(out_dir / FINGERPRINT_FILENAME).write_text(json.dumps(fingerprint, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    summary = {
        "passed": compare_result.mismatch_count == 0,
        "coin_audit_path": str(coin_audit_path),
        "out_dir": str(out_dir),
        "coin_rows": int(len(coin_df)),
        "aetheredge_rows": int(len(aetheredge_df)),
        "joined_rows": compare_result.joined_rows,
        "skip_warmup_bars": int(skip_warmup_bars),
        "compared_rows": compare_result.compared_rows,
        "mismatch_count": compare_result.mismatch_count,
        "mismatch_fields": compare_result.mismatch_fields,
        "tolerance": float(tolerance),
    }
    Path(out_dir / SUMMARY_FILENAME).write_text(json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay AetherEdge V9C signals and compare with an exported signal_audit.csv.")
    parser.add_argument("--coin-audit", required=True, type=Path, help="Path to exported signal_audit.csv.")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help=f"Output directory. Default: {DEFAULT_OUT_DIR}")
    parser.add_argument("--fail-on-mismatch", action="store_true", help="Exit with code 1 when any mismatch is found.")
    parser.add_argument("--tolerance", type=float, default=1e-9, help="Floating-point comparison tolerance. Default: 1e-9")
    parser.add_argument("--skip-warmup-bars", type=int, default=250, help="Rows to skip before comparing. Default: 250")
    parser.add_argument("--max-rows", type=int, default=None, help="Replay only the first N rows. Default: all rows")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        coin_df = pd.read_csv(args.coin_audit)
        if args.max_rows is not None:
            coin_df = coin_df.head(args.max_rows)
        validate_coin_audit_columns(coin_df)
        aetheredge_df = replay_aetheredge_signal_audit(coin_df)
        compare_result = compare_signal_audits(
            coin_df,
            aetheredge_df,
            tolerance=args.tolerance,
            skip_warmup_bars=args.skip_warmup_bars,
        )
        summary = write_outputs(
            coin_audit_path=args.coin_audit,
            out_dir=args.out_dir,
            coin_df=coin_df,
            aetheredge_df=aetheredge_df,
            compare_result=compare_result,
            tolerance=args.tolerance,
            skip_warmup_bars=args.skip_warmup_bars,
            fingerprint=strategy_fingerprint(),
        )
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(json.dumps(summary, sort_keys=True, default=str))
    if args.fail_on_mismatch and summary["mismatch_count"] > 0:
        return 1
    return 0


def _audit_row_from_context(input_row: Mapping[str, Any], context: BarReadyContext) -> dict[str, Any]:
    routed = context.routed_signal
    is_flat = routed.side is Side.FLAT
    return {
        "timestamp": _timestamp_string(input_row["timestamp"]),
        "open": input_row["open"],
        "high": input_row["high"],
        "low": input_row["low"],
        "close": input_row["close"],
        "volume": input_row["volume"],
        "signal": int(routed.side.value),
        "selected_engine": "NONE" if is_flat else routed.engine,
        "selected_priority": 0 if is_flat else int(routed.priority),
        "risk_mult": float(getattr(routed, "risk_mult", Decimal("1"))),
        "quality_mult": float(getattr(routed, "quality_mult", Decimal("1"))),
        "momentum_signal": _engine_signal(context.engine_features.get("momentum")),
        "bear_signal": _engine_signal(context.engine_features.get("bear")),
        "bull_signal": _engine_signal(context.engine_features.get("bull")),
        "micro_context_available": bool(context.micro.context_available),
        "micro_aligned": bool(context.micro.aligned),
        "micro_contra": bool(context.micro.contra),
        "micro_entry_risk_scale": float(context.micro.entry_risk_scale),
        "micro_filter_action": context.micro.action,
        "rf_bar_count": input_row["rf_bar_count"],
        "rf_micro_return_pct": input_row["rf_micro_return_pct"],
        "rf_close_pos": input_row["rf_close_pos"],
        "rf_delta_sum": input_row["rf_delta_sum"],
        "rf_imbalance": input_row["rf_imbalance"],
        "rf_taker_buy_ratio": input_row["rf_taker_buy_ratio"],
    }


def _validate_aetheredge_audit_columns(df: pd.DataFrame) -> None:
    missing = [column for column in AUDIT_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _append_mismatch(
    mismatches: list[dict[str, Any]],
    mismatch_fields: dict[str, int],
    timestamp: Any,
    field: str,
    coin_value: Any,
    aetheredge_value: Any,
    abs_diff: float | None,
) -> None:
    mismatches.append(
        {
            "timestamp": timestamp,
            "field": field,
            "coin_value": coin_value,
            "aetheredge_value": aetheredge_value,
            "abs_diff": abs_diff,
        }
    )
    mismatch_fields[field] = mismatch_fields.get(field, 0) + 1


def _canonical_strict_value(value: Any) -> Any:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip()
    lowered = text.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"nan", "none", ""}:
        return None
    number = _optional_float(text)
    if number is not None and number.is_integer():
        return int(number)
    return text.upper() if field_like_engine_value(text) else text


def field_like_engine_value(value: str) -> bool:
    return value.upper() in {"NONE", "MOMENTUM_V3", "BEAR_V3_ONLY", "BULL_RECLAIM_V2"}


def _engine_signal(row: Mapping[str, Any] | None) -> int:
    if not row:
        return 0
    value = row.get("signal", 0)
    if _is_missing(value):
        return 0
    return int(value)


def _strategy_range_pct(strategy: Strategy) -> Decimal:
    range_bars = strategy.config.runtime_requirements.get("range_bars", {})
    return Decimal(str(range_bars.get("range_pct", "0.002")))


def _fingerprint_hash(fingerprint: Mapping[str, Any]) -> str:
    payload = {key: value for key, value in fingerprint.items() if key != "fingerprint_hash"}
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _decimal(value: Any, default: Decimal | None = None) -> Decimal:
    if _is_missing(value):
        if default is not None:
            return default
        raise ValueError("Cannot convert missing value to Decimal")
    return Decimal(str(value))


def _optional_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _is_missing(value: Any) -> bool:
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _timestamp_string(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return str(value.tz_convert(None) if value.tzinfo is not None else value)
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
