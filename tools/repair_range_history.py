from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.models import RangeBar, RangeCoverageStatus, TimeRange
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.storage import SqliteRangeBarStore, SqliteTradeStore
from src.platform.exchanges.models import ExchangeName
from src.platform.markets import get_market_profile


POLLUTION_CUTOFF_MS = 1_640_995_200_000  # 2022-01-01T00:00:00Z
DEFAULT_CONTRACT_VALUE = Decimal("0.01")


@dataclass(frozen=True)
class CountMinMax:
    count: int = 0
    min: int | None = None
    max: int | None = None


@dataclass
class RepairSummary:
    symbol: str
    exchange: str
    raw_symbol: str
    range_pct: str
    bucket_interval: str
    bucket_ms: int
    start_ms: int
    end_ms: int
    bucket_count_target: int
    trade_coverage_complete_buckets: int = 0
    missing_trade_coverage_buckets: int = 0
    missing_trade_coverage_examples: list[dict[str, int]] = field(default_factory=list)
    trades_exist_but_coverage_missing: int = 0
    trades_exist_but_coverage_missing_examples: list[dict[str, int]] = field(default_factory=list)
    empty_trade_buckets: int = 0
    empty_trade_bucket_examples: list[dict[str, int]] = field(default_factory=list)
    trades_loaded: int = 0
    range_bars_before_count: int = 0
    range_bars_rebuilt_count: int = 0
    range_bars_written_count: int = 0
    aggregates_before_count: int = 0
    aggregates_before_min: int | None = None
    aggregates_before_max: int | None = None
    aggregates_built_count: int = 0
    aggregates_written_count: int = 0
    aggregates_after_count: int = 0
    aggregates_after_min: int | None = None
    aggregates_after_max: int | None = None
    min_buckets: int = 100
    enough_for_range_speed: bool = False
    dry_run: bool = False
    backup_paths: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    legacy_or_test_polluted_completed_aggregates_detected: bool = False
    polluted_completed_aggregates_deleted: int = 0
    deleted_existing_aggregates: int = 0
    deleted_existing_range_bar_buckets: int = 0
    repair_range_bars: bool = False
    rebuild_aggregates: bool = True
    delete_existing_aggregates: bool = False
    delete_existing_range_bars: bool = False
    contract_value: str = str(DEFAULT_CONTRACT_VALUE)
    contract_value_source: str = "fallback"
    builder_mode: str = "bucket_isolated"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline repair tool for local range-speed history. No exchange APIs are called."
    )
    parser.add_argument("--symbol", default="ETH-USDT-PERP")
    parser.add_argument("--exchange", default="okx")
    parser.add_argument("--raw-symbol", default="ETH-USDT-SWAP")
    parser.add_argument("--range-pct", default="0.002")
    parser.add_argument("--bucket-interval", default="4h")
    parser.add_argument("--market-db", default="data/market_data/aether_market_data.sqlite3")
    parser.add_argument("--checkpoint-db", default="data/state/range_builder_checkpoint.sqlite3")
    parser.add_argument("--start-ms", type=int, default=None)
    parser.add_argument("--end-ms", type=int, default=None)
    parser.add_argument("--min-buckets", type=int, default=100)
    parser.add_argument("--contract-value", default=None)
    parser.add_argument("--repair-range-bars", nargs="?", const=True, default=False, type=_bool)
    parser.add_argument("--rebuild-aggregates", nargs="?", const=True, default=True, type=_bool)
    parser.add_argument("--delete-existing-aggregates", nargs="?", const=True, default=False, type=_bool)
    parser.add_argument("--delete-existing-range-bars", nargs="?", const=True, default=False, type=_bool)
    parser.add_argument("--backup", nargs="?", const=True, default=True, type=_bool)
    parser.add_argument("--dry-run", nargs="?", const=True, default=False, type=_bool)
    parser.add_argument("--fail-under-min", nargs="?", const=True, default=False, type=_bool)
    parser.add_argument("--json-output", default=None)
    parser.add_argument(
        "--builder-mode",
        choices=("bucket_isolated", "continuous"),
        default="bucket_isolated",
        help="Rebuild bars per bucket by default; continuous still never marks incomplete buckets COMPLETE.",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace, *, now_ms: int | None = None) -> tuple[RepairSummary, int]:
    bucket_ms = _parse_interval_ms(args.bucket_interval)
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    current_bucket_start_ms = (now // bucket_ms) * bucket_ms
    end_ms = int(args.end_ms) if args.end_ms is not None else current_bucket_start_ms - 1
    end_ms = min(end_ms, current_bucket_start_ms - 1)
    start_ms = int(args.start_ms) if args.start_ms is not None else current_bucket_start_ms - int(args.min_buckets) * bucket_ms
    target_range = TimeRange(start_ms, end_ms)
    bucket_starts = _complete_bucket_starts(start_ms, end_ms, bucket_ms)
    range_pct = _decimal_text(args.range_pct)
    exchange = str(args.exchange).strip().lower()
    market_db = _resolve_path(args.market_db)
    checkpoint_db = _resolve_path(args.checkpoint_db)
    contract_value, contract_source, contract_warning = _resolve_contract_value(
        symbol=args.symbol,
        exchange=exchange,
        explicit=args.contract_value,
    )

    summary = RepairSummary(
        symbol=args.symbol,
        exchange=exchange,
        raw_symbol=args.raw_symbol,
        range_pct=range_pct,
        bucket_interval=args.bucket_interval,
        bucket_ms=bucket_ms,
        start_ms=start_ms,
        end_ms=end_ms,
        bucket_count_target=len(bucket_starts),
        min_buckets=int(args.min_buckets),
        dry_run=bool(args.dry_run),
        repair_range_bars=bool(args.repair_range_bars),
        rebuild_aggregates=bool(args.rebuild_aggregates),
        delete_existing_aggregates=bool(args.delete_existing_aggregates),
        delete_existing_range_bars=bool(args.delete_existing_range_bars),
        contract_value=str(contract_value),
        contract_value_source=contract_source,
        builder_mode=args.builder_mode,
    )
    if contract_warning:
        summary.warnings.append(contract_warning)
    if end_ms < start_ms:
        raise ValueError("end-ms must be greater than or equal to start-ms after excluding the current bucket")

    trade_store = SqliteTradeStore(market_db)
    range_store = SqliteRangeBarStore(market_db)
    checkpoint_store = SqliteRangeCheckpointStore(checkpoint_db)

    summary.range_bars_before_count = _range_bar_count(
        market_db,
        symbol=args.symbol,
        range_pct=range_pct,
        time_range=target_range,
    )
    before = _completed_count_min_max(
        checkpoint_db,
        exchange=exchange,
        symbol=args.symbol,
        range_pct=range_pct,
    )
    summary.aggregates_before_count = before.count
    summary.aggregates_before_min = before.min
    summary.aggregates_before_max = before.max
    summary.legacy_or_test_polluted_completed_aggregates_detected = _polluted_count(
        checkpoint_db,
        exchange=exchange,
        symbol=args.symbol,
        range_pct=range_pct,
    ) > 0
    if summary.legacy_or_test_polluted_completed_aggregates_detected and not args.delete_existing_aggregates:
        summary.warnings.append("polluted_completed_aggregates_remain_use_delete_existing_aggregates")

    complete_bucket_starts, missing_bucket_starts = _classify_trade_coverage(
        trade_store.coverage_ranges(symbol=args.symbol, time_range=target_range, source="historical"),
        bucket_starts=bucket_starts,
        bucket_ms=bucket_ms,
    )
    summary.trade_coverage_complete_buckets = len(complete_bucket_starts)
    summary.missing_trade_coverage_buckets = len(missing_bucket_starts)
    summary.missing_trade_coverage_examples = _bucket_examples(missing_bucket_starts, bucket_ms)
    if missing_bucket_starts:
        summary.trades_exist_but_coverage_missing_examples = _trades_exist_examples(
            market_db,
            symbol=args.symbol,
            bucket_starts=missing_bucket_starts,
            bucket_ms=bucket_ms,
        )
        summary.trades_exist_but_coverage_missing = len(summary.trades_exist_but_coverage_missing_examples)
        if summary.trades_exist_but_coverage_missing:
            summary.warnings.append("trades_exist_but_coverage_missing")

    write_requested = bool(args.repair_range_bars or args.rebuild_aggregates or args.delete_existing_aggregates)
    if args.backup and not args.dry_run and write_requested:
        summary.backup_paths = _backup_databases(market_db=market_db, checkpoint_db=checkpoint_db, now_ms=now)

    empty_bucket_starts: list[int] = []
    if args.repair_range_bars:
        rebuilt, written, trades_loaded, empty_buckets = _repair_range_bars(
            trade_store=trade_store,
            range_store=range_store,
            symbol=args.symbol,
            range_pct=range_pct,
            bucket_starts=complete_bucket_starts,
            bucket_ms=bucket_ms,
            contract_value=contract_value,
            delete_existing=bool(args.delete_existing_range_bars),
            dry_run=bool(args.dry_run),
            builder_mode=args.builder_mode,
        )
        summary.range_bars_rebuilt_count = rebuilt
        summary.range_bars_written_count = written
        summary.trades_loaded = trades_loaded
        summary.empty_trade_buckets = len(empty_buckets)
        summary.empty_trade_bucket_examples = _bucket_examples(empty_buckets, bucket_ms)
        empty_bucket_starts = empty_buckets
        if args.delete_existing_range_bars and not args.dry_run:
            summary.deleted_existing_range_bar_buckets = len(complete_bucket_starts)

    if args.rebuild_aggregates:
        if args.delete_existing_aggregates:
            target_deleted, polluted_deleted = _delete_existing_aggregates(
                checkpoint_db,
                exchange=exchange,
                symbol=args.symbol,
                range_pct=range_pct,
                time_range=target_range,
                dry_run=bool(args.dry_run),
            )
            summary.deleted_existing_aggregates = target_deleted
            summary.polluted_completed_aggregates_deleted = polluted_deleted
        aggregates = RangeBarAggregator().aggregate(
            range_store.load(symbol=args.symbol, range_pct=range_pct, time_range=target_range),
            bucket_ms=bucket_ms,
        )
        complete_starts = set(complete_bucket_starts) - set(empty_bucket_starts)
        eligible = [
            aggregate
            for aggregate in aggregates
            if aggregate.bucket_start_ms in complete_starts
            and aggregate.bar_count > 0
            and aggregate.bucket_end_ms <= end_ms
        ]
        summary.aggregates_built_count = len(eligible)
        if not args.dry_run:
            for aggregate in eligible:
                checkpoint_store.save_completed_aggregate(
                    exchange=exchange,
                    aggregate=aggregate,
                    coverage_status=RangeCoverageStatus.COMPLETE.value,
                    missing_gap_ms=0,
                    completed_at_ms=now,
                )
            summary.aggregates_written_count = len(eligible)

    after = _completed_count_min_max(
        checkpoint_db,
        exchange=exchange,
        symbol=args.symbol,
        range_pct=range_pct,
    )
    summary.aggregates_after_count = after.count
    summary.aggregates_after_min = after.min
    summary.aggregates_after_max = after.max
    summary.enough_for_range_speed = summary.aggregates_after_count >= summary.min_buckets
    if not summary.enough_for_range_speed:
        summary.warnings.append("WARNING insufficient_complete_range_history_for_min_periods")

    _print_summary(summary)
    if args.json_output:
        _write_json_output(_resolve_path(args.json_output), summary, dry_run=bool(args.dry_run))

    exit_code = 2 if args.fail_under_min and not summary.enough_for_range_speed else 0
    return summary, exit_code


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    _, exit_code = run(args)
    return exit_code


def _repair_range_bars(
    *,
    trade_store: SqliteTradeStore,
    range_store: SqliteRangeBarStore,
    symbol: str,
    range_pct: str,
    bucket_starts: Sequence[int],
    bucket_ms: int,
    contract_value: Decimal,
    delete_existing: bool,
    dry_run: bool,
    builder_mode: str,
) -> tuple[int, int, int, list[int]]:
    rebuilt_rows: list[RangeBar] = []
    written = 0
    trades_loaded = 0
    empty_buckets: list[int] = []
    builder = RangeBarBuilder(range_pct=range_pct, contract_value=contract_value)
    for bucket_start in bucket_starts:
        bucket_range = TimeRange(bucket_start, bucket_start + bucket_ms - 1)
        trades = sorted(
            trade_store.load(symbol=symbol, time_range=bucket_range),
            key=lambda trade: (
                trade.trade_time_ms if trade.trade_time_ms is not None else trade.event_time_ms or 0,
                trade.trade_id or "",
            ),
        )
        trades_loaded += len(trades)
        if not trades:
            empty_buckets.append(bucket_start)
            if delete_existing and not dry_run:
                range_store.replace_range(symbol=symbol, range_pct=range_pct, time_range=bucket_range, rows=[])
            continue
        bucket_rows: list[RangeBar] = []
        for trade in trades:
            for row in builder.on_trade(trade):
                if bucket_range.start_time_ms <= row.end_time_ms <= bucket_range.end_time_ms:
                    bucket_rows.append(row)
        if builder_mode == "bucket_isolated":
            builder.discard_active_bar()
        rebuilt_rows.extend(bucket_rows)
        if not dry_run:
            if delete_existing:
                written += range_store.replace_range(
                    symbol=symbol,
                    range_pct=range_pct,
                    time_range=bucket_range,
                    rows=bucket_rows,
                )
            elif bucket_rows:
                written += range_store.save(bucket_rows)
    return len(rebuilt_rows), written, trades_loaded, empty_buckets


def _classify_trade_coverage(
    coverage_ranges: Sequence[TimeRange],
    *,
    bucket_starts: Sequence[int],
    bucket_ms: int,
) -> tuple[list[int], list[int]]:
    complete: list[int] = []
    missing: list[int] = []
    for bucket_start in bucket_starts:
        bucket_end = bucket_start + bucket_ms - 1
        if any(item.start_time_ms <= bucket_start and item.end_time_ms >= bucket_end for item in coverage_ranges):
            complete.append(bucket_start)
        else:
            missing.append(bucket_start)
    return complete, missing


def _complete_bucket_starts(start_ms: int, end_ms: int, bucket_ms: int) -> list[int]:
    first_start = start_ms - (start_ms % bucket_ms)
    if first_start < start_ms:
        first_start += bucket_ms
    last_start = end_ms - (end_ms % bucket_ms)
    starts: list[int] = []
    current = first_start
    while current <= last_start and current + bucket_ms - 1 <= end_ms:
        starts.append(current)
        current += bucket_ms
    return starts


def _range_bar_count(db_path: Path, *, symbol: str, range_pct: str, time_range: TimeRange) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM range_bars
            WHERE symbol = ? AND range_pct = ? AND end_time_ms BETWEEN ? AND ?
            """,
            (symbol, range_pct, time_range.start_time_ms, time_range.end_time_ms),
        ).fetchone()
    return int(row[0] or 0)


def _completed_count_min_max(db_path: Path, *, exchange: str, symbol: str, range_pct: str) -> CountMinMax:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*), MIN(bucket_end_ms), MAX(bucket_end_ms)
            FROM completed_range_aggregates
            WHERE exchange = ? AND symbol = ? AND range_pct = ? AND coverage_status = ?
            """,
            (exchange, symbol, range_pct, RangeCoverageStatus.COMPLETE.value),
        ).fetchone()
    return CountMinMax(
        count=int(row[0] or 0),
        min=None if row[1] is None else int(row[1]),
        max=None if row[2] is None else int(row[2]),
    )


def _polluted_count(db_path: Path, *, exchange: str, symbol: str, range_pct: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM completed_range_aggregates
            WHERE exchange = ? AND symbol = ? AND range_pct = ? AND bucket_end_ms < ?
            """,
            (exchange, symbol, range_pct, POLLUTION_CUTOFF_MS),
        ).fetchone()
    return int(row[0] or 0)


def _delete_existing_aggregates(
    db_path: Path,
    *,
    exchange: str,
    symbol: str,
    range_pct: str,
    time_range: TimeRange,
    dry_run: bool,
) -> tuple[int, int]:
    with sqlite3.connect(db_path) as conn:
        target_count = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM completed_range_aggregates
                WHERE exchange = ? AND symbol = ? AND range_pct = ? AND bucket_end_ms BETWEEN ? AND ?
                """,
                (exchange, symbol, range_pct, time_range.start_time_ms, time_range.end_time_ms),
            ).fetchone()[0]
            or 0
        )
        polluted_count = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM completed_range_aggregates
                WHERE exchange = ? AND symbol = ? AND range_pct = ? AND bucket_end_ms < ?
                """,
                (exchange, symbol, range_pct, POLLUTION_CUTOFF_MS),
            ).fetchone()[0]
            or 0
        )
        if not dry_run:
            conn.execute(
                """
                DELETE FROM completed_range_aggregates
                WHERE exchange = ? AND symbol = ? AND range_pct = ? AND bucket_end_ms BETWEEN ? AND ?
                """,
                (exchange, symbol, range_pct, time_range.start_time_ms, time_range.end_time_ms),
            )
            conn.execute(
                """
                DELETE FROM completed_range_aggregates
                WHERE exchange = ? AND symbol = ? AND range_pct = ? AND bucket_end_ms < ?
                """,
                (exchange, symbol, range_pct, POLLUTION_CUTOFF_MS),
            )
    return target_count, polluted_count


def _trades_exist_examples(
    db_path: Path,
    *,
    symbol: str,
    bucket_starts: Sequence[int],
    bucket_ms: int,
    limit: int = 10,
) -> list[dict[str, int]]:
    examples: list[dict[str, int]] = []
    with sqlite3.connect(db_path) as conn:
        for bucket_start in bucket_starts:
            bucket_end = bucket_start + bucket_ms - 1
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM trades
                WHERE symbol = ? AND COALESCE(trade_time_ms, event_time_ms) BETWEEN ? AND ?
                """,
                (symbol, bucket_start, bucket_end),
            ).fetchone()
            count = int(row[0] or 0)
            if count:
                examples.append({"bucket_start_ms": bucket_start, "bucket_end_ms": bucket_end, "trade_count": count})
                if len(examples) >= limit:
                    break
    return examples


def _bucket_examples(bucket_starts: Sequence[int], bucket_ms: int, limit: int = 10) -> list[dict[str, int]]:
    return [
        {"bucket_start_ms": bucket_start, "bucket_end_ms": bucket_start + bucket_ms - 1}
        for bucket_start in list(bucket_starts)[:limit]
    ]


def _backup_databases(*, market_db: Path, checkpoint_db: Path, now_ms: int) -> list[str]:
    backups: list[str] = []
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now_ms / 1000))
    for db_path in (market_db, checkpoint_db):
        if not db_path.exists():
            continue
        backup_path = db_path.with_name(f"{db_path.name}.{stamp}.bak")
        shutil.copy2(db_path, backup_path)
        backups.append(str(backup_path))
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(db_path) + suffix)
            if sidecar.exists():
                sidecar_backup = Path(str(backup_path) + suffix)
                shutil.copy2(sidecar, sidecar_backup)
                backups.append(str(sidecar_backup))
    return backups


def _resolve_contract_value(*, symbol: str, exchange: str, explicit: str | None) -> tuple[Decimal, str, str | None]:
    if explicit is not None:
        value = Decimal(str(explicit))
        if value <= 0:
            raise ValueError("contract-value must be positive")
        return value, "argument", None
    try:
        value = get_market_profile(symbol).contract_value(ExchangeName(exchange))
    except Exception as exc:
        return DEFAULT_CONTRACT_VALUE, "fallback", f"contract_value_profile_lookup_failed_using_0.01: {exc}"
    if value is None or value <= 0:
        return DEFAULT_CONTRACT_VALUE, "fallback", "contract_value_missing_using_0.01"
    return Decimal(str(value)), "market_profile", None


def _write_json_output(path: Path, summary: RepairSummary, *, dry_run: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _print_summary(summary: RepairSummary) -> None:
    data = asdict(summary)
    print("range history repair summary")
    for key in (
        "symbol",
        "exchange",
        "range_pct",
        "start_ms",
        "end_ms",
        "bucket_count_target",
        "trade_coverage_complete_buckets",
        "missing_trade_coverage_buckets",
        "trades_loaded",
        "range_bars_before_count",
        "range_bars_rebuilt_count",
        "range_bars_written_count",
        "aggregates_before_count",
        "aggregates_before_min",
        "aggregates_before_max",
        "aggregates_built_count",
        "aggregates_written_count",
        "aggregates_after_count",
        "aggregates_after_min",
        "aggregates_after_max",
        "min_buckets",
        "enough_for_range_speed",
        "dry_run",
        "backup_paths",
        "warnings",
    ):
        print(f"{key}: {data[key]}")


def _parse_interval_ms(value: str) -> int:
    raw = str(value).strip().lower()
    units = (("ms", 1), ("h", 60 * 60_000), ("m", 60_000), ("s", 1000), ("d", 24 * 60 * 60_000))
    for suffix, multiplier in units:
        if raw.endswith(suffix):
            amount = int(raw[: -len(suffix)])
            if amount <= 0:
                raise ValueError("bucket interval must be positive")
            return amount * multiplier
    raise ValueError(f"unsupported bucket interval: {value!r}")


def _resolve_path(value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path


def _decimal_text(value: Decimal | str | float) -> str:
    return format(Decimal(str(value)).normalize(), "f")


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid boolean value: {value!r}")


if __name__ == "__main__":
    raise SystemExit(main())
