#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.storage.trade_feature_store import (  # noqa: E402
    SqliteTradeFeatureStore,
)
from src.market_data.trade_features.coverage import (  # noqa: E402
    resolve_trade_feature_readiness,
)
from src.market_data.backfill.status_store import now_ms  # noqa: E402
from tools.mf_feature_backfill_worker import run_cycle  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prebuild MF trade-derived feature history until ready."
        )
    )
    parser.add_argument("--symbol", default="ETH-USDT-PERP")
    parser.add_argument("--exchange", default="okx")
    parser.add_argument(
        "--market-db",
        default="data/market_data/aether_market_data.sqlite3",
    )
    parser.add_argument(
        "--raw-root",
        default="data/okx/raw/trades",
    )
    parser.add_argument(
        "--status-path",
        default="data/state/mf_feature_prebuild_status.json",
    )
    parser.add_argument(
        "--global-lock-path",
        default="data/state/raw_trade_backfill_global.lock",
    )
    parser.add_argument(
        "--global-status-path",
        default=(
            "data/state/raw_trade_backfill_global_status.json"
        ),
    )
    parser.add_argument(
        "--range-footprint-range-pct",
        default="0.002",
    )
    parser.add_argument(
        "--range-footprint-price-step",
        default="1",
    )
    parser.add_argument("--range-footprint-warmup-days", type=int, default=1)
    parser.add_argument("--contract-value", default="0.01")
    parser.add_argument("--price-bucket-size", default="1")
    parser.add_argument("--large-trade-threshold", default="10000")
    parser.add_argument("--target-days", type=int, default=120)
    parser.add_argument(
        "--large-share-min-samples",
        type=int,
        default=43_200,
    )
    parser.add_argument(
        "--large-share-window-days",
        type=int,
        default=90,
    )
    parser.add_argument(
        "--max-minutes-per-cycle",
        type=int,
        default=4320,
    )
    parser.add_argument("--max-days-per-cycle", type=int, default=3)
    parser.add_argument(
        "--max-trades-per-cycle",
        type=int,
        default=2_000_000,
    )
    parser.add_argument(
        "--max-seconds-per-cycle",
        type=float,
        default=600.0,
    )
    parser.add_argument("--max-cycles", type=int, default=200)
    parser.add_argument("--max-seconds", type=float, default=0.0)
    parser.add_argument("--max-failures", type=int, default=3)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--chunk-sleep-seconds", type=float, default=0.0)
    parser.add_argument(
        "--download",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    return parser


def run_prebuild(args: argparse.Namespace) -> int:
    started_at_ms = now_ms()
    started_monotonic = time.monotonic()
    status_path = Path(args.status_path)
    requested_minutes = max(1, int(args.target_days)) * 1440
    required_minutes = max(
        requested_minutes,
        max(1, int(args.large_share_min_samples)),
        max(1, int(args.large_share_window_days)) * 1_440,
    )
    cycles = 0
    consecutive_failures = 0
    last_result: Mapping[str, Any] | None = None
    last_readiness: Mapping[str, Any] | None = None
    status = {
        "running": True,
        "started_at_ms": started_at_ms,
        "updated_at_ms": started_at_ms,
        "symbol": args.symbol,
        "exchange": args.exchange,
        "market_db": args.market_db,
        "target_days": int(args.target_days),
        "requested_minutes": requested_minutes,
        "effective_required_minutes": required_minutes,
        "large_share_min_samples": int(
            args.large_share_min_samples
        ),
        "large_share_window_days": int(
            args.large_share_window_days
        ),
        "cycles": 0,
        "ready": False,
        "last_result": None,
        "last_readiness": None,
        "error": False,
    }
    _write_status(status_path, status)

    try:
        last_readiness = _readiness_audit(
            args,
            required_minutes=required_minutes,
        )
        if _is_ready(last_readiness):
            return _complete(
                status_path=status_path,
                status=status,
                cycles=0,
                readiness=last_readiness,
                started_monotonic=started_monotonic,
                market_db=args.market_db,
            )

        max_cycles = max(1, int(args.max_cycles))
        max_failures = max(1, int(args.max_failures))
        for cycle in range(1, max_cycles + 1):
            elapsed = time.monotonic() - started_monotonic
            if args.max_seconds > 0 and elapsed >= args.max_seconds:
                return _fail(
                    status_path=status_path,
                    status=status,
                    cycles=cycles,
                    result=last_result,
                    readiness=last_readiness,
                    error_detail="max_seconds_reached",
                )

            cycles = cycle
            try:
                last_result = dict(
                    run_cycle(
                        symbol=args.symbol,
                        exchange=args.exchange,
                        market_db=args.market_db,
                        raw_root=args.raw_root,
                        status_path=args.status_path,
                        global_lock_path=args.global_lock_path,
                        global_status_path=args.global_status_path,
                        mode="prebuild",
                        direction="oldest-to-recent",
                        max_minutes_per_cycle=max(
                            1, int(args.max_minutes_per_cycle)
                        ),
                        max_days_per_cycle=max(
                            1, int(args.max_days_per_cycle)
                        ),
                        max_trades_per_cycle=max(
                            1, int(args.max_trades_per_cycle)
                        ),
                        max_seconds_per_cycle=max(
                            0.0,
                            float(args.max_seconds_per_cycle),
                        ),
                        chunk_sleep_seconds=max(
                            0.0,
                            float(args.chunk_sleep_seconds),
                        ),
                        no_download=not bool(args.download),
                        save_raw_trades=False,
                        contract_value=Decimal(args.contract_value),
                        large_trade_threshold=Decimal(
                            args.large_trade_threshold
                        ),
                        price_bucket_size=Decimal(
                            args.price_bucket_size
                        ),
                        range_footprint_range_pct=Decimal(
                            args.range_footprint_range_pct
                        ),
                        range_footprint_price_step=Decimal(
                            args.range_footprint_price_step
                        ),
                        range_footprint_warmup_days=max(
                            0,
                            int(args.range_footprint_warmup_days),
                        ),
                        required_minutes=required_minutes,
                    )
                )
            except Exception as exc:
                last_result = {
                    "status": "error",
                    "reason": "run_cycle_exception",
                    "error": f"{type(exc).__name__}: {exc}",
                }

            if _cycle_failed(last_result):
                consecutive_failures += 1
            else:
                consecutive_failures = 0

            last_readiness = _readiness_audit(
                args,
                required_minutes=required_minutes,
            )
            elapsed = time.monotonic() - started_monotonic
            _print_progress(
                cycle=cycle,
                result=last_result,
                readiness=last_readiness,
                elapsed=elapsed,
            )
            status.update(
                {
                    "running": True,
                    "updated_at_ms": now_ms(),
                    "cycles": cycle,
                    "ready": _is_ready(last_readiness),
                    "last_result": last_result,
                    "last_readiness": last_readiness,
                    "error": False,
                }
            )
            _write_status(status_path, status)

            if _is_ready(last_readiness):
                return _complete(
                    status_path=status_path,
                    status=status,
                    cycles=cycle,
                    readiness=last_readiness,
                    started_monotonic=started_monotonic,
                    market_db=args.market_db,
                )
            if consecutive_failures >= max_failures:
                return _fail(
                    status_path=status_path,
                    status=status,
                    cycles=cycle,
                    result=last_result,
                    readiness=last_readiness,
                    error_detail="max_failures_reached",
                )
            if args.max_seconds > 0 and elapsed >= args.max_seconds:
                return _fail(
                    status_path=status_path,
                    status=status,
                    cycles=cycle,
                    result=last_result,
                    readiness=last_readiness,
                    error_detail="max_seconds_reached",
                )
            if args.sleep_seconds > 0:
                time.sleep(max(0.0, float(args.sleep_seconds)))

        return _fail(
            status_path=status_path,
            status=status,
            cycles=cycles,
            result=last_result,
            readiness=last_readiness,
            error_detail="max_cycles_reached",
        )
    except Exception as exc:
        return _fail(
            status_path=status_path,
            status=status,
            cycles=cycles,
            result=last_result,
            readiness=last_readiness,
            error_detail=f"{type(exc).__name__}: {exc}",
        )


def _readiness_audit(
    args: argparse.Namespace,
    *,
    required_minutes: int,
) -> dict[str, Any]:
    store = SqliteTradeFeatureStore(path=args.market_db)
    readiness = resolve_trade_feature_readiness(
        symbol=args.symbol,
        exchange=args.exchange,
        store=store,
        required_minutes=required_minutes,
        range_pct=args.range_footprint_range_pct,
        price_step=args.range_footprint_price_step,
    )
    audit = dict(readiness.audit())
    audit["ready"] = _is_ready(audit)
    return audit


def _is_ready(readiness: Mapping[str, Any]) -> bool:
    return bool(
        readiness.get("tradebar_ready", False)
        and readiness.get("fixed_time_footprint_ready", False)
        and readiness.get("range_footprint_ready", False)
        and readiness.get("coverage_ready", False)
        and not readiness.get("degraded_footprint", False)
    )


def _cycle_failed(result: Mapping[str, Any]) -> bool:
    if str(result.get("status", "")).lower() in {
        "error",
        "failed",
        "launch_failed",
    }:
        return True
    if str(result.get("reason", "")).lower() in {
        "download_failures",
        "subprocess_error",
        "run_cycle_exception",
    }:
        return True
    return bool(result.get("failed_downloads"))


def _print_progress(
    *,
    cycle: int,
    result: Mapping[str, Any],
    readiness: Mapping[str, Any],
    elapsed: float,
) -> None:
    print(
        "[prebuild-mf] "
        f"cycle={cycle} "
        f"status={result.get('status', 'unknown')} "
        f"reason={result.get('reason', 'unknown')} "
        f"ready={_is_ready(readiness)} "
        f"tradebar={readiness.get('tradebar_ready', False)} "
        "footprint="
        f"{readiness.get('fixed_time_footprint_ready', False)} "
        f"range_fp={readiness.get('range_footprint_ready', False)} "
        f"target={result.get('target_end_ms', 'pending')} "
        f"elapsed={elapsed:.1f}",
        flush=True,
    )


def _complete(
    *,
    status_path: Path,
    status: dict[str, Any],
    cycles: int,
    readiness: Mapping[str, Any],
    started_monotonic: float,
    market_db: str,
) -> int:
    elapsed = time.monotonic() - started_monotonic
    status.update(
        {
            "running": False,
            "updated_at_ms": now_ms(),
            "cycles": cycles,
            "ready": True,
            "last_readiness": dict(readiness),
            "error": False,
        }
    )
    _write_status(status_path, status)
    print(
        "[prebuild-mf] complete "
        f"ready=True cycles={cycles} elapsed={elapsed:.1f} "
        f"market_db={market_db}",
        flush=True,
    )
    return 0


def _fail(
    *,
    status_path: Path,
    status: dict[str, Any],
    cycles: int,
    result: Mapping[str, Any] | None,
    readiness: Mapping[str, Any] | None,
    error_detail: str,
) -> int:
    status.update(
        {
            "running": False,
            "updated_at_ms": now_ms(),
            "cycles": cycles,
            "ready": False,
            "last_result": result,
            "last_readiness": readiness,
            "error": True,
            "error_detail": error_detail,
        }
    )
    _write_status(status_path, status)
    print(
        "[prebuild-mf] failed "
        f"cycles={cycles} reason={error_detail}",
        flush=True,
    )
    return 1


def _write_status(
    path: str | Path,
    payload: Mapping[str, Any],
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    temporary.write_text(
        json.dumps(
            dict(payload),
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    os.replace(temporary, target)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.target_days <= 0:
        print("[prebuild-mf] failed reason=target_days_must_be_positive")
        return 1
    if args.large_share_min_samples <= 0:
        print(
            "[prebuild-mf] failed "
            "reason=large_share_min_samples_must_be_positive"
        )
        return 1
    if args.large_share_window_days <= 0:
        print(
            "[prebuild-mf] failed "
            "reason=large_share_window_days_must_be_positive"
        )
        return 1
    return run_prebuild(args)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "build_parser",
    "main",
    "run_prebuild",
]
