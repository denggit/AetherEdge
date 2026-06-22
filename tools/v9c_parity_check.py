#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Offline historical parity checker for CoinBacktest Portfolio V9C vs AetherEdge V9C.

This tool is intentionally offline and deterministic. It does NOT compare real
exchange fills. It compares strategy/audit outputs built from the same historical
4H bars and range-footprint context.

MVP levels:

1. CoinBacktest audit export: import CoinBacktest's V9C script and generate the
   canonical V9C feature/trade audit using the same defaults as the champion run.
2. AetherEdge config/parameter audit: export live V9C routing, micro context and
   execution parameters from AetherEdge.
3. Parity compare: compare normalized CoinBacktest signal audit against an
   AetherEdge replay audit CSV when provided, and always compare key config
   parity. The AetherEdge replay audit can be produced by a later dedicated
   replay exporter without changing this comparator.

Run examples:

    python tools/v9c_parity_check.py \
        --coinbacktest-root D:/Code_Project/CoinBacktest \
        --start-date 2025-01-01 \
        --end-date 2026-06-20 \
        --out-dir data/reports/parity/v9c_2025_2026

    python tools/v9c_parity_check.py \
        --coinbacktest-root D:/Code_Project/CoinBacktest \
        --aetheredge-audit data/reports/parity/aetheredge_v9c_signal_audit.csv \
        --fail-on-mismatch
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Iterable, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd

from strategies.eth_lf_portfolio_v8.strategy import Strategy, _default_engine_execution_params


DEFAULT_PRIORITY_ORDER = ["BULL_RECLAIM_V2", "MOMENTUM_V3", "BEAR_V3_ONLY"]
DEFAULT_SIGNAL_COMPARE_COLUMNS = [
    "time",
    "signal",
    "side",
    "selected_engine",
    "risk_mult",
    "quality_mult",
    "micro_entry_risk_scale",
    "micro_filter_action",
]


@dataclass(frozen=True)
class ParityConfig:
    symbol: str = "ETH-USDT-SWAP"
    start_date: str = "2023-01-01"
    end_date: str | None = None
    warmup_start_date: str | None = None
    warmup_days: int = 365
    initial_capital: float = 1000.0
    preset: str = "turbo"
    bear_preset: str = "high"
    bull_preset: str = "high"
    priority_mode: str = "reclaim_first"
    global_risk_scale: float = 1.30
    fee_rate: float = 0.00055
    slippage_pct: float = 0.0002
    micro_filter_mode: str = "soft"
    range_pct: float = 0.002
    price_step: float = 1.0
    micro_min_range_bars: int = 5
    micro_contra_imbalance: float = 0.05
    micro_aligned_imbalance: float = 0.05
    micro_bad_close_pos: float = 0.35
    micro_good_close_pos: float = 0.65
    micro_contra_risk_scale: float = 0.50
    micro_not_aligned_risk_scale: float = 0.50


@dataclass(frozen=True)
class CompareResult:
    checked_rows: int
    mismatched_rows: int
    column_mismatches: dict[str, int]
    matched: bool


@dataclass(frozen=True)
class ConfigParityResult:
    matched: bool
    mismatches: dict[str, dict[str, Any]]
    coinbacktest: dict[str, Any]
    aetheredge: dict[str, Any]


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return Decimal(default)
    return Decimal(str(value))


def _to_float(value: Any) -> float:
    if value is None:
        return float("nan")
    try:
        return float(value)
    except Exception:
        return float("nan")


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _timestamp_to_ms(ts: Any) -> int:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    else:
        t = t.tz_convert("UTC")
    return int(t.timestamp() * 1000)


def load_coinbacktest_v9c_module(coinbacktest_root: Path) -> ModuleType:
    coinbacktest_root = coinbacktest_root.resolve()
    script = coinbacktest_root / "backtest" / "lf" / "eth_lf_portfolio_v9c_reclaim_priority_backtest.py"
    if not script.exists():
        raise FileNotFoundError(f"CoinBacktest V9C script not found: {script}")
    if str(coinbacktest_root) not in sys.path:
        sys.path.insert(0, str(coinbacktest_root))
    spec = importlib.util.spec_from_file_location("coinbacktest_v9c_reclaim_priority", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot import CoinBacktest V9C script: {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_coinbacktest_namespace(cfg: ParityConfig, *, range_data_dir: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        symbol=cfg.symbol,
        start_date=cfg.start_date,
        end_date=cfg.end_date or _today(),
        warmup_start_date=cfg.warmup_start_date,
        warmup_days=cfg.warmup_days,
        initial_capital=cfg.initial_capital,
        preset=cfg.preset,
        unit_risk_per_trade=None,
        max_total_notional_mult=None,
        max_units=None,
        min_risk_mult=0.35,
        max_risk_mult=None,
        fee_rate=cfg.fee_rate,
        slippage_pct=cfg.slippage_pct,
        disable_short=False,
        bear_preset=cfg.bear_preset,
        bear_min_risk_mult=0.25,
        bear_standalone_risk_scale=1.0,
        bear_standalone_quality_scale=1.0,
        disable_bear_standalone=False,
        bull_preset=cfg.bull_preset,
        bull_min_risk_mult=0.25,
        bull_reclaim_risk_scale=1.0,
        bull_reclaim_quality_scale=1.0,
        bull_execution_mode="inherit",
        disable_bull_reclaim=False,
        priority_mode=cfg.priority_mode,
        global_risk_scale=cfg.global_risk_scale,
        quality_mult_cap=2.20,
        micro_filter_mode=cfg.micro_filter_mode,
        range_pct=cfg.range_pct,
        price_step=cfg.price_step,
        range_data_dir=range_data_dir,
        disable_footprint_context=False,
        micro_min_range_bars=cfg.micro_min_range_bars,
        micro_contra_imbalance=cfg.micro_contra_imbalance,
        micro_aligned_imbalance=cfg.micro_aligned_imbalance,
        micro_bad_close_pos=cfg.micro_bad_close_pos,
        micro_good_close_pos=cfg.micro_good_close_pos,
        micro_contra_risk_scale=cfg.micro_contra_risk_scale,
        micro_not_aligned_risk_scale=cfg.micro_not_aligned_risk_scale,
        out_dir=None,
    )


def compute_load_start(args: SimpleNamespace) -> str:
    trade_start = pd.Timestamp(args.start_date)
    if args.warmup_start_date:
        load_start = pd.Timestamp(args.warmup_start_date)
    elif args.warmup_days and args.warmup_days > 0:
        load_start = trade_start - pd.Timedelta(days=int(args.warmup_days))
    else:
        load_start = trade_start
    return load_start.strftime("%Y-%m-%d")


def build_coinbacktest_outputs(module: ModuleType, args: SimpleNamespace) -> tuple[pd.DataFrame, list[dict[str, Any]], dict[str, Any]]:
    mom_cfg = module.make_momentum_config(args)
    bear_cfg = module.make_bear_config(args)
    bull_cfg = module.make_bull_config(args)
    exec_cfg = module.make_exec_config(mom_cfg)
    bull_exec_cfg = module.bull_to_exec_config(bull_cfg) if args.bull_execution_mode == "own" else exec_cfg

    load_start_str = compute_load_start(args)
    base = module.load_data(args.symbol, load_start_str, args.end_date, "4H")
    if base.empty:
        raise RuntimeError(f"CoinBacktest loaded no 4H rows for {args.symbol} {load_start_str}->{args.end_date}")

    momentum = module.build_momentum_features(base, mom_cfg)
    bear = module.build_bear_features(base, bear_cfg)
    bull = module.build_bull_features(base, bull_cfg)
    features = module.select_portfolio_signals(momentum, bear, bull, args)
    micro_ctx = module.load_range_footprint_context(args, load_start_str, args.end_date)
    features = module.apply_micro_context_filter(features, micro_ctx, args)

    trade_start = pd.Timestamp(args.start_date)
    features = features.loc[trade_start: pd.Timestamp(args.end_date)].copy()
    trades, equity = module.run_priority_backtest(
        features,
        exec_cfg,
        engine_cfgs={"MOMENTUM_V3": exec_cfg, "BEAR_V3_ONLY": exec_cfg, "BULL_RECLAIM_V2": bull_exec_cfg},
        global_risk_scale=args.global_risk_scale,
    )
    trades = module.attach_engine_to_trades(trades, features)
    summary = module.summarize(trades, equity, exec_cfg.initial_capital)
    summary.update(
        {
            "preset": args.preset,
            "bear_preset": args.bear_preset,
            "bull_preset": args.bull_preset,
            "priority_mode": args.priority_mode,
            "priority_order": module.PRIORITY_MODES[args.priority_mode],
            "global_risk_scale": args.global_risk_scale,
            "micro_filter_mode": args.micro_filter_mode,
            "range_pct": args.range_pct,
            "trade_start_date": args.start_date,
            "warmup_start_date": load_start_str,
        }
    )
    return features, trades, summary


def normalize_coinbacktest_signal_audit(features: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for ts, row in features.iterrows():
        signal = int(row.get("signal", 0) or 0)
        if signal > 0:
            side = "long"
        elif signal < 0:
            side = "short"
        else:
            side = "flat"
        rows.append(
            {
                "time": pd.Timestamp(ts).isoformat(),
                "open_time_ms": _timestamp_to_ms(ts),
                "signal": signal,
                "side": side,
                "selected_engine": str(row.get("selected_engine", "NONE")),
                "selected_priority": int(row.get("selected_priority", 0) or 0),
                "risk_mult": _to_float(row.get("risk_mult", 1.0)),
                "quality_mult": _to_float(row.get("quality_mult", 1.0)),
                "micro_entry_risk_scale": _to_float(row.get("micro_entry_risk_scale", 1.0)),
                "micro_filter_action": str(row.get("micro_filter_action", "NA")),
                "micro_context_available": _bool_value(row.get("micro_context_available", False)),
                "micro_aligned": _bool_value(row.get("micro_aligned", False)),
                "micro_contra": _bool_value(row.get("micro_contra", False)),
                "momentum_signal": int(row.get("momentum_signal", 0) or 0),
                "bear_signal": int(row.get("bear_signal", 0) or 0),
                "bull_signal": int(row.get("bull_signal", 0) or 0),
                "rf_bar_count": int(0 if pd.isna(row.get("rf_bar_count", 0)) else row.get("rf_bar_count", 0)),
                "rf_imbalance": _to_float(row.get("rf_imbalance")),
                "rf_close_pos": _to_float(row.get("rf_close_pos")),
                "atr": _to_float(row.get("atr")),
                "open": _to_float(row.get("open")),
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "close": _to_float(row.get("close")),
            }
        )
    return pd.DataFrame(rows)


def normalize_trades(trades: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for i, trade in enumerate(trades, start=1):
        rows.append(
            {
                "trade_index": i,
                "entry_time": str(trade.get("entry_time", "")),
                "exit_time": str(trade.get("exit_time", "")),
                "side": trade.get("side"),
                "engine": trade.get("engine", "UNKNOWN"),
                "entry_price": _to_float(trade.get("first_entry", trade.get("entry_price"))),
                "avg_entry": _to_float(trade.get("avg_entry")),
                "exit_price": _to_float(trade.get("exit_price")),
                "qty": _to_float(trade.get("qty")),
                "units": int(trade.get("units", 0) or 0),
                "reason": str(trade.get("reason", "")),
                "risk_mult": _to_float(trade.get("risk_mult", 1.0)),
                "micro_entry_risk_scale": _to_float(trade.get("micro_entry_risk_scale", 1.0)),
            }
        )
    return pd.DataFrame(rows)


def aetheredge_config_audit() -> dict[str, Any]:
    strategy = Strategy()
    params = _default_engine_execution_params()
    return {
        "strategy_id": strategy.config.strategy_id,
        "priority_order": list(strategy.config.router.priority_order),
        "global_risk_scale": str(strategy.config.global_risk_scale),
        "micro_context": {
            "mode": strategy.config.micro_context.mode,
            "min_range_bars": strategy.config.micro_context.min_range_bars,
            "contra_imbalance": str(strategy.config.micro_context.contra_imbalance),
            "aligned_imbalance": str(strategy.config.micro_context.aligned_imbalance),
            "contra_risk_scale": str(strategy.config.micro_context.contra_risk_scale),
            "not_aligned_risk_scale": str(strategy.config.micro_context.not_aligned_risk_scale),
        },
        "engine_execution_params": {
            name: {
                "initial_atr_mult": str(p.initial_atr_mult),
                "trailing_atr_mult": str(p.trailing_atr_mult),
                "unit_risk_per_trade": str(p.unit_risk_per_trade),
                "max_total_notional_mult": str(p.max_total_notional_mult),
                "max_units": p.max_units,
                "add_every_r": str(p.add_every_r),
                "max_hold_bars": p.max_hold_bars,
                "cooldown_bars": p.cooldown_bars,
            }
            for name, p in params.items()
        },
    }


def coinbacktest_config_audit(module: ModuleType, args: SimpleNamespace) -> dict[str, Any]:
    mom_cfg = module.make_momentum_config(args)
    bear_cfg = module.make_bear_config(args)
    bull_cfg = module.make_bull_config(args)
    exec_cfg = module.make_exec_config(mom_cfg)
    bull_exec_cfg = module.bull_to_exec_config(bull_cfg) if args.bull_execution_mode == "own" else exec_cfg
    return {
        "strategy_id": module.STRATEGY_NAME,
        "priority_order": list(module.PRIORITY_MODES[args.priority_mode]),
        "global_risk_scale": str(args.global_risk_scale),
        "micro_context": {
            "mode": args.micro_filter_mode,
            "min_range_bars": args.micro_min_range_bars,
            "contra_imbalance": str(args.micro_contra_imbalance),
            "aligned_imbalance": str(args.micro_aligned_imbalance),
            "contra_risk_scale": str(args.micro_contra_risk_scale),
            "not_aligned_risk_scale": str(args.micro_not_aligned_risk_scale),
        },
        "engine_execution_params": {
            "MOMENTUM_V3": _exec_cfg_dict(exec_cfg),
            "BEAR_V3_ONLY": _exec_cfg_dict(module.make_exec_config(mom_cfg) if False else module.make_exec_config(mom_cfg)),
            "BULL_RECLAIM_V2": _exec_cfg_dict(bull_exec_cfg),
        },
        "coinbacktest_presets": {
            "preset": args.preset,
            "bear_preset": args.bear_preset,
            "bull_preset": args.bull_preset,
            "bull_execution_mode": args.bull_execution_mode,
        },
    }


def _exec_cfg_dict(cfg: Any) -> dict[str, Any]:
    return {
        "initial_atr_mult": str(getattr(cfg, "initial_atr_mult")),
        "trailing_atr_mult": str(getattr(cfg, "trailing_atr_mult")),
        "unit_risk_per_trade": str(getattr(cfg, "unit_risk_per_trade")),
        "max_total_notional_mult": str(getattr(cfg, "max_total_notional_mult")),
        "max_units": int(getattr(cfg, "max_units")),
        "add_every_r": str(getattr(cfg, "add_every_r")),
        "max_hold_bars": int(getattr(cfg, "max_hold_bars")),
        "cooldown_bars": int(getattr(cfg, "cooldown_bars")),
    }


def compare_config(coin: Mapping[str, Any], ae: Mapping[str, Any]) -> ConfigParityResult:
    checks = {
        "priority_order": (coin.get("priority_order"), ae.get("priority_order")),
        "global_risk_scale": (str(coin.get("global_risk_scale")), str(ae.get("global_risk_scale"))),
        "micro_context": (coin.get("micro_context"), ae.get("micro_context")),
        "engine_execution_params.MOMENTUM_V3": (
            coin.get("engine_execution_params", {}).get("MOMENTUM_V3"),
            ae.get("engine_execution_params", {}).get("MOMENTUM_V3"),
        ),
        "engine_execution_params.BULL_RECLAIM_V2": (
            coin.get("engine_execution_params", {}).get("BULL_RECLAIM_V2"),
            ae.get("engine_execution_params", {}).get("BULL_RECLAIM_V2"),
        ),
    }
    mismatches: dict[str, dict[str, Any]] = {}
    for key, (left, right) in checks.items():
        if left != right:
            mismatches[key] = {"coinbacktest": left, "aetheredge": right}
    return ConfigParityResult(
        matched=not mismatches,
        mismatches=mismatches,
        coinbacktest=dict(coin),
        aetheredge=dict(ae),
    )


def compare_signal_audits(
    coin: pd.DataFrame,
    ae: pd.DataFrame,
    *,
    columns: list[str] | None = None,
    float_tolerance: float = 1e-9,
) -> tuple[CompareResult, pd.DataFrame]:
    columns = list(columns or DEFAULT_SIGNAL_COMPARE_COLUMNS)
    required = {"time", *columns}
    missing_coin = sorted(required - set(coin.columns))
    missing_ae = sorted(required - set(ae.columns))
    if missing_coin:
        raise ValueError(f"CoinBacktest audit missing columns: {missing_coin}")
    if missing_ae:
        raise ValueError(f"AetherEdge audit missing columns: {missing_ae}")

    left = coin[["time", *[c for c in columns if c != "time"]]].copy()
    right = ae[["time", *[c for c in columns if c != "time"]]].copy()
    merged = left.merge(right, on="time", how="outer", suffixes=("_coin", "_ae"), indicator=True)
    mismatch_rows: list[dict[str, Any]] = []
    column_mismatches = {col: 0 for col in columns if col != "time"}

    for _, row in merged.iterrows():
        row_mismatches: dict[str, Any] = {}
        if row["_merge"] != "both":
            row_mismatches["row_presence"] = row["_merge"]
        for col in columns:
            if col == "time":
                continue
            lval = row.get(f"{col}_coin")
            rval = row.get(f"{col}_ae")
            equal = _values_equal(lval, rval, float_tolerance=float_tolerance)
            if not equal:
                column_mismatches[col] += 1
                row_mismatches[col] = {"coinbacktest": lval, "aetheredge": rval}
        if row_mismatches:
            mismatch_rows.append({"time": row.get("time"), "mismatches": json.dumps(row_mismatches, default=_json_default, ensure_ascii=False)})

    mismatch_df = pd.DataFrame(mismatch_rows)
    return (
        CompareResult(
            checked_rows=len(merged),
            mismatched_rows=len(mismatch_df),
            column_mismatches={k: v for k, v in column_mismatches.items() if v > 0},
            matched=mismatch_df.empty,
        ),
        mismatch_df,
    )


def _values_equal(left: Any, right: Any, *, float_tolerance: float) -> bool:
    if pd.isna(left) and pd.isna(right):
        return True
    try:
        lf = float(left)
        rf = float(right)
        if math.isnan(lf) and math.isnan(rf):
            return True
        return abs(lf - rf) <= float_tolerance
    except Exception:
        return str(left) == str(right)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline historical parity checker for CoinBacktest V9C vs AetherEdge V9C.")
    parser.add_argument("--coinbacktest-root", required=True, help="Path to local CoinBacktest repo root.")
    parser.add_argument("--aetheredge-audit", default=None, help="Optional AetherEdge replay audit CSV to compare against CoinBacktest audit.")
    parser.add_argument("--symbol", default="ETH-USDT-SWAP")
    parser.add_argument("--start-date", default="2023-01-01")
    parser.add_argument("--end-date", default=_today())
    parser.add_argument("--warmup-start-date", default=None)
    parser.add_argument("--warmup-days", type=int, default=365)
    parser.add_argument("--range-data-dir", default=None)
    parser.add_argument("--out-dir", default="data/reports/parity/v9c")
    parser.add_argument("--compare-columns", default=",".join(DEFAULT_SIGNAL_COMPARE_COLUMNS), help="Comma-separated signal audit columns to compare.")
    parser.add_argument("--float-tolerance", type=float, default=1e-9)
    parser.add_argument("--fail-on-mismatch", action="store_true")
    return parser.parse_args()


def main() -> int:
    cli = parse_args()
    out_dir = Path(cli.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg = ParityConfig(
        symbol=cli.symbol,
        start_date=cli.start_date,
        end_date=cli.end_date,
        warmup_start_date=cli.warmup_start_date,
        warmup_days=cli.warmup_days,
    )
    cb_root = Path(cli.coinbacktest_root)
    cb_module = load_coinbacktest_v9c_module(cb_root)
    cb_args = build_coinbacktest_namespace(cfg, range_data_dir=cli.range_data_dir)

    features, trades, cb_summary = build_coinbacktest_outputs(cb_module, cb_args)
    cb_signal_audit = normalize_coinbacktest_signal_audit(features)
    cb_trades = normalize_trades(trades)

    cb_signal_path = out_dir / "coinbacktest_v9c_signal_audit.csv"
    cb_trade_path = out_dir / "coinbacktest_v9c_trades.csv"
    cb_signal_audit.to_csv(cb_signal_path, index=False)
    cb_trades.to_csv(cb_trade_path, index=False)
    _write_json(out_dir / "coinbacktest_v9c_summary.json", cb_summary)

    cb_config = coinbacktest_config_audit(cb_module, cb_args)
    ae_config = aetheredge_config_audit()
    config_result = compare_config(cb_config, ae_config)
    _write_json(out_dir / "config_parity.json", asdict(config_result))

    result_payload: dict[str, Any] = {
        "coinbacktest_signal_audit": str(cb_signal_path),
        "coinbacktest_trades": str(cb_trade_path),
        "config_parity_matched": config_result.matched,
        "config_mismatches": config_result.mismatches,
    }

    signal_result: CompareResult | None = None
    if cli.aetheredge_audit:
        ae_signal_audit = pd.read_csv(cli.aetheredge_audit)
        compare_columns = [c.strip() for c in cli.compare_columns.split(",") if c.strip()]
        signal_result, mismatch_df = compare_signal_audits(
            cb_signal_audit,
            ae_signal_audit,
            columns=compare_columns,
            float_tolerance=cli.float_tolerance,
        )
        mismatch_path = out_dir / "signal_mismatches.csv"
        mismatch_df.to_csv(mismatch_path, index=False)
        result_payload.update(
            {
                "signal_parity_matched": signal_result.matched,
                "signal_checked_rows": signal_result.checked_rows,
                "signal_mismatched_rows": signal_result.mismatched_rows,
                "signal_column_mismatches": signal_result.column_mismatches,
                "signal_mismatch_path": str(mismatch_path),
            }
        )

    _write_json(out_dir / "parity_result.json", result_payload)
    print(json.dumps(result_payload, indent=2, ensure_ascii=False, default=_json_default))

    failed = not config_result.matched or (signal_result is not None and not signal_result.matched)
    if cli.fail_on_mismatch and failed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
