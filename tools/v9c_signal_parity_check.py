import argparse
import asyncio
import logging
import sys
import time
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


logger = logging.getLogger("v9c_signal_parity")

DEFAULT_OUT_DIR = Path("data/parity/v9c_signal")
REPLAY_AUDIT_FILENAME = "aetheredge_v9c_replay_signal_audit.csv"
MISMATCH_FILENAME = "signal_mismatches.csv"
SUMMARY_FILENAME = "parity_summary.json"
FINGERPRINT_FILENAME = "fingerprint.json"
MISMATCH_CONTEXT_FILENAME = "signal_mismatch_context.csv"

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

REPLAY_DIAGNOSTIC_COLUMNS = [
    "atr",
    "atr_pct",
    "adx",
    "momentum_long_exit_channel",
    "momentum_short_exit_channel",
    "bear_short_exit_channel",
    "bull_long_exit_channel",
]

AUDIT_COLUMNS = list(REQUIRED_COLUMNS) + REPLAY_DIAGNOSTIC_COLUMNS

FEATURE_WARMUP_REQUIRED_COLUMNS = [
    "atr",
    "atr_pct",
    "adx",
]

FEATURE_WARMUP_ENGINE_COLUMNS = [
    "momentum_long_exit_channel",
    "momentum_short_exit_channel",
    "bear_short_exit_channel",
    "bull_long_exit_channel",
]

ACTION_CRITICAL_FIELDS = [
    "signal",
    "selected_engine",
    "selected_priority",
    "momentum_signal",
    "bear_signal",
    "bull_signal",
]

SIGNAL_SCOPED_STRICT_FIELDS = [
    "micro_context_available",
    "micro_aligned",
    "micro_contra",
    "micro_filter_action",
]

SIGNAL_SCOPED_FLOAT_FIELDS = [
    "risk_mult",
    "quality_mult",
    "micro_entry_risk_scale",
]

DIAGNOSTIC_FLOAT_FIELDS = [
    "rf_bar_count",
    "rf_micro_return_pct",
    "rf_close_pos",
    "rf_delta_sum",
    "rf_imbalance",
    "rf_taker_buy_ratio",
]


def configure_logging(*, verbose: bool = True) -> None:
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )
    logger.setLevel(level)


@dataclass(frozen=True)
class CompareResult:
    mismatches: pd.DataFrame
    mismatch_fields: dict[str, int]
    action_critical_mismatch_fields: dict[str, int]
    signal_scoped_mismatch_fields: dict[str, int]
    diagnostic_mismatch_fields: dict[str, int]
    joined_rows: int
    compared_rows: int

    @property
    def mismatch_count(self) -> int:
        return int(len(self.mismatches))

    @property
    def action_critical_mismatch_count(self) -> int:
        return _sum_counts(self.action_critical_mismatch_fields)

    @property
    def signal_scoped_mismatch_count(self) -> int:
        return _sum_counts(self.signal_scoped_mismatch_fields)

    @property
    def diagnostic_mismatch_count(self) -> int:
        return _sum_counts(self.diagnostic_mismatch_fields)

    @property
    def passed(self) -> bool:
        return self.action_critical_mismatch_count == 0 and self.signal_scoped_mismatch_count == 0

    @property
    def first_action_critical_mismatch(self) -> dict[str, Any] | None:
        if self.mismatches.empty:
            return None
        action_mismatches = self.mismatches[self.mismatches["category"] == "action_critical"]
        if action_mismatches.empty:
            return None
        row = action_mismatches.iloc[0]
        return {
            "timestamp": row["timestamp"],
            "field": row["field"],
            "coin_value": row["coin_value"],
            "aetheredge_value": row["aetheredge_value"],
        }


def validate_coin_audit_columns(df: pd.DataFrame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def detect_feature_warmup(aetheredge_df: pd.DataFrame) -> dict[str, Any]:
    required_valid = pd.Series(True, index=aetheredge_df.index)
    for column in FEATURE_WARMUP_REQUIRED_COLUMNS:
        if column not in aetheredge_df.columns:
            required_valid &= False
            continue
        required_valid &= aetheredge_df[column].notna()

    engine_valid = pd.Series(False, index=aetheredge_df.index)
    for column in FEATURE_WARMUP_ENGINE_COLUMNS:
        if column in aetheredge_df.columns:
            engine_valid |= aetheredge_df[column].notna()

    feature_valid = required_valid & engine_valid
    valid_positions = [idx for idx, is_valid in enumerate(feature_valid.tolist()) if bool(is_valid)]
    valid_rows = int(feature_valid.sum())
    invalid_rows = int(len(aetheredge_df) - valid_rows)
    if not valid_positions:
        return {
            "first_valid_ae_feature_index": None,
            "first_valid_ae_feature_timestamp": None,
            "recommended_skip_warmup_bars": int(len(aetheredge_df)),
            "ae_feature_valid_rows": 0,
            "ae_feature_invalid_rows": int(len(aetheredge_df)),
        }

    first_valid_position = int(valid_positions[0])
    timestamp = None
    if "timestamp" in aetheredge_df.columns:
        timestamp = _timestamp_string(aetheredge_df.iloc[first_valid_position]["timestamp"])
    return {
        "first_valid_ae_feature_index": first_valid_position,
        "first_valid_ae_feature_timestamp": timestamp,
        "recommended_skip_warmup_bars": first_valid_position,
        "ae_feature_valid_rows": valid_rows,
        "ae_feature_invalid_rows": invalid_rows,
    }


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


def replay_aetheredge_signal_audit(
    coin_df: pd.DataFrame,
    *,
    max_rows: int | None = None,
    log_every_rows: int = 500,
) -> pd.DataFrame:
    coin_df = coin_df.head(max_rows).copy() if max_rows is not None else coin_df.copy()
    validate_coin_audit_columns(coin_df)
    strategy = Strategy()
    rows: list[dict[str, Any]] = []
    total_rows = len(coin_df)
    started = time.perf_counter()

    for row_idx, (_, row) in enumerate(coin_df.iterrows(), start=1):
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
        if log_every_rows > 0 and (row_idx == 1 or row_idx % log_every_rows == 0 or row_idx == total_rows):
            elapsed = time.perf_counter() - started
            logger.info(
                "Replay progress | row=%s/%s timestamp=%s selected_engine=%s signal=%s elapsed_sec=%.3f",
                row_idx,
                total_rows,
                row["timestamp"],
                rows[-1].get("selected_engine") if rows else None,
                rows[-1].get("signal") if rows else None,
                elapsed,
            )

    return pd.DataFrame(rows, columns=AUDIT_COLUMNS)


def compare_signal_audits(
    coin_df: pd.DataFrame,
    aetheredge_df: pd.DataFrame,
    *,
    tolerance: float = 1e-9,
    skip_warmup_bars: int = 250,
    log_every_rows: int = 500,
) -> CompareResult:
    validate_coin_audit_columns(coin_df)
    _validate_aetheredge_audit_columns(aetheredge_df)
    joined = pd.merge(coin_df, aetheredge_df, on="timestamp", how="inner", suffixes=("_coin", "_aetheredge"))
    compared = joined.iloc[skip_warmup_bars:] if skip_warmup_bars else joined
    mismatches: list[dict[str, Any]] = []
    mismatch_fields: dict[str, int] = {}
    action_critical_mismatch_fields: dict[str, int] = {}
    signal_scoped_mismatch_fields: dict[str, int] = {}
    diagnostic_mismatch_fields: dict[str, int] = {}
    total_rows = len(compared)
    started = time.perf_counter()

    for row_idx, (_, row) in enumerate(compared.iterrows(), start=1):
        timestamp = row["timestamp"]
        has_signal = _canonical_int(row["signal_coin"]) != 0 or _canonical_int(row["signal_aetheredge"]) != 0
        for field in ACTION_CRITICAL_FIELDS:
            coin_value = row[f"{field}_coin"]
            ae_value = row[f"{field}_aetheredge"]
            if _canonical_strict_value(coin_value) != _canonical_strict_value(ae_value):
                _append_mismatch(
                    mismatches,
                    mismatch_fields,
                    action_critical_mismatch_fields,
                    "action_critical",
                    timestamp,
                    field,
                    coin_value,
                    ae_value,
                    None,
                )
        if has_signal:
            for field in SIGNAL_SCOPED_STRICT_FIELDS:
                coin_value = row[f"{field}_coin"]
                ae_value = row[f"{field}_aetheredge"]
                if field == "micro_filter_action":
                    coin_canonical = _canonical_micro_action(coin_value, has_signal=has_signal)
                    ae_canonical = _canonical_micro_action(ae_value, has_signal=has_signal)
                else:
                    coin_canonical = _canonical_strict_value(coin_value)
                    ae_canonical = _canonical_strict_value(ae_value)
                if coin_canonical != ae_canonical:
                    _append_mismatch(
                        mismatches,
                        mismatch_fields,
                        signal_scoped_mismatch_fields,
                        "signal_scoped",
                        timestamp,
                        field,
                        coin_value,
                        ae_value,
                        None,
                    )
            for field in SIGNAL_SCOPED_FLOAT_FIELDS:
                _compare_float_field(
                    row,
                    mismatches,
                    mismatch_fields,
                    signal_scoped_mismatch_fields,
                    "signal_scoped",
                    timestamp,
                    field,
                    tolerance,
                )
        for field in DIAGNOSTIC_FLOAT_FIELDS:
            _compare_float_field(
                row,
                mismatches,
                mismatch_fields,
                diagnostic_mismatch_fields,
                "diagnostic",
                timestamp,
                field,
                tolerance,
            )
        if log_every_rows > 0 and (row_idx == 1 or row_idx % log_every_rows == 0 or row_idx == total_rows):
            elapsed = time.perf_counter() - started
            logger.info(
                "Compare progress | row=%s/%s timestamp=%s mismatches=%s action_critical=%s signal_scoped=%s diagnostic=%s elapsed_sec=%.3f",
                row_idx,
                total_rows,
                timestamp,
                len(mismatches),
                _sum_counts(action_critical_mismatch_fields),
                _sum_counts(signal_scoped_mismatch_fields),
                _sum_counts(diagnostic_mismatch_fields),
                elapsed,
            )

    mismatch_df = pd.DataFrame(mismatches, columns=["timestamp", "category", "field", "coin_value", "aetheredge_value", "abs_diff"])
    return CompareResult(
        mismatches=mismatch_df,
        mismatch_fields=mismatch_fields,
        action_critical_mismatch_fields=action_critical_mismatch_fields,
        signal_scoped_mismatch_fields=signal_scoped_mismatch_fields,
        diagnostic_mismatch_fields=diagnostic_mismatch_fields,
        joined_rows=int(len(joined)),
        compared_rows=int(len(compared)),
    )


def _compare_float_field(
    row: pd.Series,
    mismatches: list[dict[str, Any]],
    mismatch_fields: dict[str, int],
    category_mismatch_fields: dict[str, int],
    category: str,
    timestamp: Any,
    field: str,
    tolerance: float,
) -> None:
    coin_value = row[f"{field}_coin"]
    ae_value = row[f"{field}_aetheredge"]
    coin_float = _optional_float(coin_value)
    ae_float = _optional_float(ae_value)
    if coin_float is None or ae_float is None:
        if coin_float != ae_float:
            _append_mismatch(
                mismatches,
                mismatch_fields,
                category_mismatch_fields,
                category,
                timestamp,
                field,
                coin_value,
                ae_value,
                None,
            )
        return
    abs_diff = abs(coin_float - ae_float)
    if abs_diff > tolerance:
        _append_mismatch(
            mismatches,
            mismatch_fields,
            category_mismatch_fields,
            category,
            timestamp,
            field,
            coin_value,
            ae_value,
            abs_diff,
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
    requested_skip_warmup_bars: int,
    effective_skip_warmup_bars: int,
    auto_skip_feature_warmup: bool,
    feature_warmup: Mapping[str, Any],
    fingerprint: Mapping[str, Any],
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    aetheredge_df.to_csv(out_dir / REPLAY_AUDIT_FILENAME, index=False)
    compare_result.mismatches.to_csv(out_dir / MISMATCH_FILENAME, index=False)
    context_df = build_signal_mismatch_context(
        coin_df,
        aetheredge_df,
        compare_result,
        recommended_skip_warmup_bars=feature_warmup.get("recommended_skip_warmup_bars"),
    )
    context_df.to_csv(out_dir / MISMATCH_CONTEXT_FILENAME, index=False)
    Path(out_dir / FINGERPRINT_FILENAME).write_text(json.dumps(fingerprint, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    summary = {
        "passed": compare_result.passed,
        "coin_audit_path": str(coin_audit_path),
        "out_dir": str(out_dir),
        "coin_rows": int(len(coin_df)),
        "aetheredge_rows": int(len(aetheredge_df)),
        "joined_rows": compare_result.joined_rows,
        "skip_warmup_bars": int(effective_skip_warmup_bars),
        "requested_skip_warmup_bars": int(requested_skip_warmup_bars),
        "effective_skip_warmup_bars": int(effective_skip_warmup_bars),
        "auto_skip_feature_warmup": bool(auto_skip_feature_warmup),
        "feature_warmup": dict(feature_warmup),
        "compared_rows": compare_result.compared_rows,
        "action_critical_mismatch_count": compare_result.action_critical_mismatch_count,
        "signal_scoped_mismatch_count": compare_result.signal_scoped_mismatch_count,
        "diagnostic_mismatch_count": compare_result.diagnostic_mismatch_count,
        "mismatch_count": compare_result.mismatch_count,
        "mismatch_fields": compare_result.mismatch_fields,
        "action_critical_mismatch_fields": compare_result.action_critical_mismatch_fields,
        "signal_scoped_mismatch_fields": compare_result.signal_scoped_mismatch_fields,
        "diagnostic_mismatch_fields": compare_result.diagnostic_mismatch_fields,
        "first_action_critical_mismatch": compare_result.first_action_critical_mismatch,
        "tolerance": float(tolerance),
    }
    Path(out_dir / SUMMARY_FILENAME).write_text(json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return summary


def build_signal_mismatch_context(
    coin_df: pd.DataFrame,
    aetheredge_df: pd.DataFrame,
    compare_result: CompareResult,
    *,
    recommended_skip_warmup_bars: int | None = None,
) -> pd.DataFrame:
    columns = [
        "timestamp",
        "row_index",
        "warmup_invalid",
        "coin_signal",
        "ae_signal",
        "coin_selected_engine",
        "ae_selected_engine",
        "coin_selected_priority",
        "ae_selected_priority",
        "coin_momentum_signal",
        "ae_momentum_signal",
        "coin_bear_signal",
        "ae_bear_signal",
        "coin_bull_signal",
        "ae_bull_signal",
        "coin_risk_mult",
        "ae_risk_mult",
        "coin_quality_mult",
        "ae_quality_mult",
        "coin_micro_context_available",
        "ae_micro_context_available",
        "coin_micro_filter_action",
        "ae_micro_filter_action",
        "coin_micro_entry_risk_scale",
        "ae_micro_entry_risk_scale",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "coin_atr",
        "ae_atr",
        "coin_atr_pct",
        "ae_atr_pct",
        "coin_adx",
        "ae_adx",
        "coin_momentum_long_exit_channel",
        "ae_momentum_long_exit_channel",
        "coin_momentum_short_exit_channel",
        "ae_momentum_short_exit_channel",
        "coin_bear_short_exit_channel",
        "ae_bear_short_exit_channel",
        "coin_bull_long_exit_channel",
        "ae_bull_long_exit_channel",
    ]
    if compare_result.mismatches.empty:
        return pd.DataFrame(columns=columns)

    action_mismatches = compare_result.mismatches[compare_result.mismatches["category"] == "action_critical"]
    if action_mismatches.empty:
        return pd.DataFrame(columns=columns)

    action_timestamps = list(dict.fromkeys(action_mismatches["timestamp"].tolist()))
    coin_context_df = coin_df.copy()
    aetheredge_context_df = aetheredge_df.copy()
    for field in REPLAY_DIAGNOSTIC_COLUMNS:
        if field not in coin_context_df.columns:
            coin_context_df[field] = None
        if field not in aetheredge_context_df.columns:
            aetheredge_context_df[field] = None
    joined = pd.merge(coin_context_df, aetheredge_context_df, on="timestamp", how="inner", suffixes=("_coin", "_aetheredge"))
    joined = joined.reset_index(drop=True)
    joined["row_index"] = joined.index
    joined = joined[joined["timestamp"].isin(action_timestamps)]

    rows: list[dict[str, Any]] = []
    for timestamp in action_timestamps:
        match = joined[joined["timestamp"] == timestamp]
        if match.empty:
            continue
        row = match.iloc[0]
        row_index = int(row["row_index"])
        rows.append(
            {
                "timestamp": row["timestamp"],
                "row_index": row_index,
                "warmup_invalid": (
                    False
                    if recommended_skip_warmup_bars is None
                    else bool(row_index < int(recommended_skip_warmup_bars))
                ),
                "coin_signal": _get_joined_value(row, "signal", "coin"),
                "ae_signal": _get_joined_value(row, "signal", "aetheredge"),
                "coin_selected_engine": _get_joined_value(row, "selected_engine", "coin"),
                "ae_selected_engine": _get_joined_value(row, "selected_engine", "aetheredge"),
                "coin_selected_priority": _get_joined_value(row, "selected_priority", "coin"),
                "ae_selected_priority": _get_joined_value(row, "selected_priority", "aetheredge"),
                "coin_momentum_signal": _get_joined_value(row, "momentum_signal", "coin"),
                "ae_momentum_signal": _get_joined_value(row, "momentum_signal", "aetheredge"),
                "coin_bear_signal": _get_joined_value(row, "bear_signal", "coin"),
                "ae_bear_signal": _get_joined_value(row, "bear_signal", "aetheredge"),
                "coin_bull_signal": _get_joined_value(row, "bull_signal", "coin"),
                "ae_bull_signal": _get_joined_value(row, "bull_signal", "aetheredge"),
                "coin_risk_mult": _get_joined_value(row, "risk_mult", "coin"),
                "ae_risk_mult": _get_joined_value(row, "risk_mult", "aetheredge"),
                "coin_quality_mult": _get_joined_value(row, "quality_mult", "coin"),
                "ae_quality_mult": _get_joined_value(row, "quality_mult", "aetheredge"),
                "coin_micro_context_available": _get_joined_value(row, "micro_context_available", "coin"),
                "ae_micro_context_available": _get_joined_value(row, "micro_context_available", "aetheredge"),
                "coin_micro_filter_action": _get_joined_value(row, "micro_filter_action", "coin"),
                "ae_micro_filter_action": _get_joined_value(row, "micro_filter_action", "aetheredge"),
                "coin_micro_entry_risk_scale": _get_joined_value(row, "micro_entry_risk_scale", "coin"),
                "ae_micro_entry_risk_scale": _get_joined_value(row, "micro_entry_risk_scale", "aetheredge"),
                "open": _get_unsuffixed_or_joined_value(row, "open"),
                "high": _get_unsuffixed_or_joined_value(row, "high"),
                "low": _get_unsuffixed_or_joined_value(row, "low"),
                "close": _get_unsuffixed_or_joined_value(row, "close"),
                "volume": _get_unsuffixed_or_joined_value(row, "volume"),
                "coin_atr": _get_joined_value(row, "atr", "coin"),
                "ae_atr": _get_joined_value(row, "atr", "aetheredge"),
                "coin_atr_pct": _get_joined_value(row, "atr_pct", "coin"),
                "ae_atr_pct": _get_joined_value(row, "atr_pct", "aetheredge"),
                "coin_adx": _get_joined_value(row, "adx", "coin"),
                "ae_adx": _get_joined_value(row, "adx", "aetheredge"),
                "coin_momentum_long_exit_channel": _get_joined_value(row, "momentum_long_exit_channel", "coin"),
                "ae_momentum_long_exit_channel": _get_joined_value(row, "momentum_long_exit_channel", "aetheredge"),
                "coin_momentum_short_exit_channel": _get_joined_value(row, "momentum_short_exit_channel", "coin"),
                "ae_momentum_short_exit_channel": _get_joined_value(row, "momentum_short_exit_channel", "aetheredge"),
                "coin_bear_short_exit_channel": _get_joined_value(row, "bear_short_exit_channel", "coin"),
                "ae_bear_short_exit_channel": _get_joined_value(row, "bear_short_exit_channel", "aetheredge"),
                "coin_bull_long_exit_channel": _get_joined_value(row, "bull_long_exit_channel", "coin"),
                "ae_bull_long_exit_channel": _get_joined_value(row, "bull_long_exit_channel", "aetheredge"),
            }
        )
    context = pd.DataFrame(rows, columns=columns)
    if "warmup_invalid" in context.columns:
        context["warmup_invalid"] = context["warmup_invalid"].astype(object)
    return context


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay AetherEdge V9C signals and compare with an exported signal_audit.csv.")
    parser.add_argument("--coin-audit", required=True, type=Path, help="Path to exported signal_audit.csv.")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help=f"Output directory. Default: {DEFAULT_OUT_DIR}")
    parser.add_argument("--fail-on-mismatch", action="store_true", help="Exit with code 1 when any mismatch is found.")
    parser.add_argument("--tolerance", type=float, default=1e-9, help="Floating-point comparison tolerance. Default: 1e-9")
    parser.add_argument("--skip-warmup-bars", type=int, default=250, help="Rows to skip before comparing. Default: 250")
    parser.add_argument(
        "--auto-skip-feature-warmup",
        action="store_true",
        help="Automatically raise skip_warmup_bars to the first row where AetherEdge replay features are valid.",
    )
    parser.add_argument("--max-rows", type=int, default=None, help="Replay only the first N rows. Default: all rows")
    parser.add_argument("--log-every-rows", type=int, default=500, help="Print replay/compare progress every N rows. Default: 500")
    parser.add_argument("--quiet", action="store_true", help="Only print warnings/errors and final JSON summary.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(verbose=not args.quiet)
    logger.info(
        "V9C signal parity started | coin_audit=%s out_dir=%s skip_warmup_bars=%s auto_skip_feature_warmup=%s tolerance=%s max_rows=%s fail_on_mismatch=%s",
        args.coin_audit,
        args.out_dir,
        args.skip_warmup_bars,
        args.auto_skip_feature_warmup,
        args.tolerance,
        args.max_rows,
        args.fail_on_mismatch,
    )
    try:
        logger.info("Loading CoinBacktest audit | path=%s", args.coin_audit)
        coin_df = pd.read_csv(args.coin_audit)
        logger.info("CoinBacktest audit loaded | rows=%s columns=%s", len(coin_df), len(coin_df.columns))
        if args.max_rows is not None:
            coin_df = coin_df.head(args.max_rows)
            logger.info("CoinBacktest audit truncated | max_rows=%s rows=%s", args.max_rows, len(coin_df))
        logger.info("Validating CoinBacktest audit columns | required=%s", len(REQUIRED_COLUMNS))
        validate_coin_audit_columns(coin_df)
        logger.info("CoinBacktest audit columns validated | missing=0")
        logger.info("AetherEdge V9C replay started | rows=%s", len(coin_df))
        replay_started = time.perf_counter()
        aetheredge_df = replay_aetheredge_signal_audit(
            coin_df,
            log_every_rows=args.log_every_rows,
        )
        logger.info(
            "AetherEdge V9C replay completed | rows=%s duration_sec=%.3f",
            len(aetheredge_df),
            time.perf_counter() - replay_started,
        )
        warmup_diag = detect_feature_warmup(aetheredge_df)
        effective_skip_warmup_bars = args.skip_warmup_bars
        if args.auto_skip_feature_warmup:
            recommended = warmup_diag.get("recommended_skip_warmup_bars")
            if recommended is not None:
                effective_skip_warmup_bars = max(args.skip_warmup_bars, int(recommended))
        logger.info(
            "Feature warmup diagnostics | first_valid_index=%s first_valid_timestamp=%s recommended_skip=%s requested_skip=%s effective_skip=%s auto_skip=%s",
            warmup_diag["first_valid_ae_feature_index"],
            warmup_diag["first_valid_ae_feature_timestamp"],
            warmup_diag["recommended_skip_warmup_bars"],
            args.skip_warmup_bars,
            effective_skip_warmup_bars,
            args.auto_skip_feature_warmup,
        )
        logger.info(
            "Comparing signal audits | coin_rows=%s aetheredge_rows=%s skip_warmup_bars=%s tolerance=%s",
            len(coin_df),
            len(aetheredge_df),
            effective_skip_warmup_bars,
            args.tolerance,
        )
        compare_started = time.perf_counter()
        compare_result = compare_signal_audits(
            coin_df,
            aetheredge_df,
            tolerance=args.tolerance,
            skip_warmup_bars=effective_skip_warmup_bars,
            log_every_rows=args.log_every_rows,
        )
        logger.info(
            "Signal audit compare completed | joined_rows=%s compared_rows=%s mismatch_count=%s mismatch_fields=%s duration_sec=%.3f",
            compare_result.joined_rows,
            compare_result.compared_rows,
            compare_result.mismatch_count,
            compare_result.mismatch_fields,
            time.perf_counter() - compare_started,
        )
        logger.info("Writing parity outputs | out_dir=%s", args.out_dir)
        summary = write_outputs(
            coin_audit_path=args.coin_audit,
            out_dir=args.out_dir,
            coin_df=coin_df,
            aetheredge_df=aetheredge_df,
            compare_result=compare_result,
            tolerance=args.tolerance,
            requested_skip_warmup_bars=args.skip_warmup_bars,
            effective_skip_warmup_bars=effective_skip_warmup_bars,
            auto_skip_feature_warmup=args.auto_skip_feature_warmup,
            feature_warmup=warmup_diag,
            fingerprint=strategy_fingerprint(),
        )
        logger.info(
            "Parity outputs written | replay_audit=%s mismatches=%s mismatch_context=%s summary=%s fingerprint=%s",
            args.out_dir / REPLAY_AUDIT_FILENAME,
            args.out_dir / MISMATCH_FILENAME,
            args.out_dir / MISMATCH_CONTEXT_FILENAME,
            args.out_dir / SUMMARY_FILENAME,
            args.out_dir / FINGERPRINT_FILENAME,
        )
    except Exception as exc:
        logger.exception("V9C signal parity failed with exception")
        print(str(exc), file=sys.stderr)
        return 2

    if summary["passed"]:
        logger.info("V9C signal parity passed | compared_rows=%s mismatch_count=0", summary["compared_rows"])
    else:
        logger.warning(
            "V9C signal parity failed | compared_rows=%s mismatch_count=%s mismatch_fields=%s",
            summary["compared_rows"],
            summary["mismatch_count"],
            summary["mismatch_fields"],
        )
    if args.fail_on_mismatch and not summary["passed"]:
        logger.warning("Exiting with code 1 because --fail-on-mismatch is set")
        print(json.dumps(summary, sort_keys=True, default=str))
        return 1
    print(json.dumps(summary, sort_keys=True, default=str))
    return 0


def _audit_row_from_context(input_row: Mapping[str, Any], context: BarReadyContext) -> dict[str, Any]:
    routed = context.routed_signal
    is_flat = routed.side is Side.FLAT
    selected_feature_key = _feature_key_for_engine(None if is_flat else routed.engine)
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
        "atr": _engine_feature_value(context.engine_features, selected_feature_key, "atr"),
        "atr_pct": _engine_feature_value(context.engine_features, selected_feature_key, "atr_pct"),
        "adx": _engine_feature_value(context.engine_features, selected_feature_key, "adx"),
        "momentum_long_exit_channel": _engine_feature_value(context.engine_features, "momentum", "long_exit_channel", fallback=False),
        "momentum_short_exit_channel": _engine_feature_value(context.engine_features, "momentum", "short_exit_channel", fallback=False),
        "bear_short_exit_channel": _engine_feature_value(context.engine_features, "bear", "short_exit_channel", fallback=False),
        "bull_long_exit_channel": _engine_feature_value(context.engine_features, "bull", "long_exit_channel", fallback=False),
    }


def _validate_aetheredge_audit_columns(df: pd.DataFrame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _append_mismatch(
    mismatches: list[dict[str, Any]],
    mismatch_fields: dict[str, int],
    category_mismatch_fields: dict[str, int],
    category: str,
    timestamp: Any,
    field: str,
    coin_value: Any,
    aetheredge_value: Any,
    abs_diff: float | None,
) -> None:
    mismatches.append(
        {
            "timestamp": timestamp,
            "category": category,
            "field": field,
            "coin_value": coin_value,
            "aetheredge_value": aetheredge_value,
            "abs_diff": abs_diff,
        }
    )
    mismatch_fields[field] = mismatch_fields.get(field, 0) + 1
    category_mismatch_fields[field] = category_mismatch_fields.get(field, 0) + 1


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


def _canonical_micro_action(value: Any, *, has_signal: bool) -> Any:
    text = _canonical_strict_value(value)
    if not has_signal and text in {"NEUTRAL", "NO_SIGNAL", "NONE", None}:
        return "NO_SIGNAL_OR_NEUTRAL"
    return text


def _canonical_int(value: Any) -> int:
    if _is_missing(value):
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def field_like_engine_value(value: str) -> bool:
    return value.upper() in {"NONE", "MOMENTUM_V3", "BEAR_V3_ONLY", "BULL_RECLAIM_V2"}


def _engine_signal(row: Mapping[str, Any] | None) -> int:
    if not row:
        return 0
    value = row.get("signal", 0)
    if _is_missing(value):
        return 0
    return int(value)


def _feature_key_for_engine(engine: str | None) -> str | None:
    return {"MOMENTUM_V3": "momentum", "BEAR_V3_ONLY": "bear", "BULL_RECLAIM_V2": "bull"}.get(str(engine or "").upper())


def _engine_feature_value(engine_features: Mapping[str, Mapping[str, Any]], feature_key: str | None, key: str, *, fallback: bool = True) -> Any:
    if feature_key is not None:
        value = engine_features.get(feature_key, {}).get(key)
        if not _is_missing(value):
            return value
    if not fallback:
        return None
    for fallback_feature_key in ("momentum", "bear", "bull"):
        value = engine_features.get(fallback_feature_key, {}).get(key)
        if not _is_missing(value):
            return value
    return None


def _get_joined_value(row: pd.Series, field: str, side: str) -> Any:
    suffixed = f"{field}_{side}"
    if suffixed in row.index:
        return _none_if_missing(row[suffixed])
    if field in row.index:
        return _none_if_missing(row[field])
    return None


def _get_unsuffixed_or_joined_value(row: pd.Series, field: str) -> Any:
    if field in row.index:
        return _none_if_missing(row[field])
    return _get_joined_value(row, field, "coin")


def _none_if_missing(value: Any) -> Any:
    return None if _is_missing(value) else value


def _sum_counts(counts: Mapping[str, int]) -> int:
    return int(sum(counts.values()))


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
