from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Callable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.models import RangeBar, RangeCoverageStatus, TimeRange
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.storage import SqliteRangeBarStore, SqliteTradeStore
from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from src.platform.markets import get_market_profile


POLLUTION_CUTOFF_MS = 1_640_995_200_000  # 2022-01-01T00:00:00Z
DEFAULT_CONTRACT_VALUE = Decimal("0.01")

# ---------------------------------------------------------------------------
# Live process detection
# ---------------------------------------------------------------------------

LIVE_PID_FILES = (
    REPO_ROOT / "data" / "run" / "aether_live.pid",
    REPO_ROOT / "data" / "run" / "aether_watchdog.pid",
)

LIVE_PROCESS_NAMES = (
    "run_live.py",
    "watchdog_live.py",
)


class LiveDetector:
    """Checks whether an AetherEdge live process is currently running."""

    def __init__(
        self,
        *,
        pid_files: tuple[Path, ...] = LIVE_PID_FILES,
        process_names: tuple[str, ...] = LIVE_PROCESS_NAMES,
    ) -> None:
        self._pid_files = pid_files
        self._process_names = process_names

    def is_live(self) -> bool:
        if self._pid_file_live():
            return True
        if self._process_list_live():
            return True
        return False

    def _pid_file_live(self) -> bool:
        for pid_file in self._pid_files:
            if not pid_file.exists():
                continue
            raw = ""
            try:
                raw = pid_file.read_text(encoding="utf-8").strip()
            except OSError:
                continue
            if not raw:
                continue
            try:
                pid = int(raw)
            except ValueError:
                continue
            if self._is_pid_alive(pid):
                return True
        return False

    @staticmethod
    def _is_pid_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        if os.name == "nt":
            return LiveDetector._is_windows_pid_alive(pid)
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False

    @staticmethod
    def _is_windows_pid_alive(pid: int) -> bool:
        import ctypes

        process_query_limited_information = 0x1000
        try:
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        except Exception:
            return False
        handle = kernel32.OpenProcess(process_query_limited_information, False, int(pid))
        if not handle:
            return False
        kernel32.CloseHandle(handle)
        return True

    def _process_list_live(self) -> bool:
        try:
            if os.name == "nt":
                return self._windows_process_list_live()
            return self._posix_process_list_live()
        except Exception:
            return False

    def _windows_process_list_live(self) -> bool:
        import subprocess

        completed = subprocess.run(
            ("tasklist", "/FO", "CSV", "/NH"),
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if completed.returncode != 0:
            return False
        output = completed.stdout or ""
        for name in self._process_names:
            if name in output:
                return True
        return False

    def _posix_process_list_live(self) -> bool:
        import subprocess

        completed = subprocess.run(
            ("ps", "-eo", "comm,args"),
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if completed.returncode != 0:
            return False
        output = completed.stdout or ""
        for name in self._process_names:
            if name in output:
                return True
        return False


# ---------------------------------------------------------------------------
# OKX historical trades downloader (public REST, no API key)
# ---------------------------------------------------------------------------

OKX_HISTORY_TRADES_PATH = "/api/v5/market/history-trades"
OKX_RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
OKX_TOO_MANY_REQUESTS_CODE = "50011"


@dataclass(frozen=True)
class _OkxRawTrade:
    trade_id: str
    price: str
    size: str
    side: str
    ts: str


class OkxHistoricalTradeDownloader:
    """Download public historical trades from OKX REST API.

    Uses the public ``GET /api/v5/market/history-trades`` endpoint.
    No API key is required.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://www.okx.com",
        timeout_seconds: int = 20,
        max_retries: int = 5,
        sleep_seconds: float = 0.2,
    ) -> None:
        self._base_url = str(base_url).rstrip("/")
        self._timeout = int(timeout_seconds)
        self._max_retries = int(max_retries)
        self._sleep_seconds = float(sleep_seconds)

    def fetch_page(
        self,
        raw_symbol: str,
        *,
        after: str | None = None,
        limit: int = 100,
    ) -> tuple[list[_OkxRawTrade], str | None]:
        params = f"instId={urllib.parse.quote(str(raw_symbol))}&limit={min(max(1, int(limit)), 100)}"
        if after:
            params += f"&after={urllib.parse.quote(str(after))}"
        url = f"{self._base_url}{OKX_HISTORY_TRADES_PATH}?{params}"

        last_error = ""
        for attempt in range(1, self._max_retries + 1):
            try:
                req = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "AetherEdge/repair-tool",
                        "Accept": "application/json",
                    },
                )
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                payload = json.loads(body)
                code = str(payload.get("code", ""))
                if code != "0":
                    if code == OKX_TOO_MANY_REQUESTS_CODE and attempt < self._max_retries:
                        time.sleep(self._retry_sleep(attempt))
                        continue
                    raise RuntimeError(
                        f"OKX API error code={code} msg={payload.get('msg', '')}"
                    )
                raw_data = payload.get("data") or []
                trades = [
                    _OkxRawTrade(
                        trade_id=str(item.get("tradeId", "")),
                        price=str(item.get("px", "0")),
                        size=str(item.get("sz", "0")),
                        side=str(item.get("side", "")),
                        ts=str(item.get("ts", "0")),
                    )
                    for item in raw_data
                    if item.get("tradeId")
                ]
                next_after = trades[-1].trade_id if trades else None
                return trades, next_after
            except urllib.error.HTTPError as exc:
                detail = ""
                try:
                    detail = exc.read().decode("utf-8", errors="replace")[:300]
                except Exception:
                    pass
                last_error = f"HTTP {exc.code}: {detail}"
                if exc.code in OKX_RETRYABLE_HTTP_CODES and attempt < self._max_retries:
                    time.sleep(self._retry_sleep(attempt))
                    continue
                raise RuntimeError(f"OKX request failed: {last_error}") from exc
            except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
                last_error = repr(exc)
                if attempt < self._max_retries:
                    time.sleep(self._retry_sleep(attempt))
                    continue
                raise RuntimeError(f"OKX request failed: {last_error}") from exc

        raise RuntimeError(f"OKX request failed after {self._max_retries} attempts: {last_error}")

    def _retry_sleep(self, attempt: int) -> float:
        return min(self._sleep_seconds * (2 ** max(0, attempt - 1)), 30.0)

    def download_bucket_trades(
        self,
        raw_symbol: str,
        bucket_start_ms: int,
        bucket_end_ms: int,
        *,
        limit: int = 100,
        max_pages: int | None = None,
    ) -> tuple[list[MarketTrade], int, bool]:
        all_raw: list[_OkxRawTrade] = []
        after: str | None = None
        pages = 0
        complete = False

        while True:
            if max_pages is not None and pages >= max_pages:
                break
            page, next_after = self.fetch_page(
                raw_symbol, after=after, limit=limit
            )
            pages += 1
            if not page:
                break

            in_range = [
                t for t in page
                if bucket_start_ms <= int(t.ts) <= bucket_end_ms
            ]
            all_raw.extend(in_range)

            oldest_ts = min(int(t.ts) for t in page)
            if oldest_ts < bucket_start_ms:
                complete = True
                break

            if next_after is None:
                break
            after = next_after

            if self._sleep_seconds > 0:
                time.sleep(self._sleep_seconds)

        trades = [
            MarketTrade(
                exchange=ExchangeName.OKX,
                symbol="",
                raw_symbol=raw_symbol,
                price=Decimal(r.price),
                quantity=Decimal(r.size),
                side=TradeSide.BUY if r.side.lower() == "buy" else TradeSide.SELL,
                trade_id=r.trade_id,
                event_time_ms=int(r.ts),
                trade_time_ms=int(r.ts),
                source=MarketDataSource.REST,
                raw={
                    "tradeId": r.trade_id,
                    "px": r.price,
                    "sz": r.size,
                    "side": r.side,
                    "ts": r.ts,
                },
            )
            for r in all_raw
        ]
        return trades, pages, complete


# ---------------------------------------------------------------------------
# Download coordination
# ---------------------------------------------------------------------------

@dataclass
class DownloadResult:
    requested_buckets: int = 0
    downloaded_buckets: int = 0
    failed_buckets: int = 0
    skipped_buckets: int = 0
    downloaded_trade_count: int = 0
    coverage_validated_buckets: int = 0
    coverage_validation_failed_buckets: int = 0
    coverage_validation_failed_examples: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    # Dry-run stats (counts what WOULD be done)
    would_download_buckets: int = 0
    would_download_trade_count: int = 0


DownloadFunc = Callable[[str, int, int, int], tuple[list[MarketTrade], int, bool]]


def _download_missing_trades(
    *,
    raw_symbol: str,
    symbol: str,
    missing_bucket_starts: list[int],
    bucket_ms: int,
    current_bucket_start_ms: int,
    download_func: DownloadFunc,
    trade_store: SqliteTradeStore,
    limit: int,
    coverage_edge_tolerance_ms: int,
    coverage_max_gap_ms: int,
    max_pages_per_bucket: int | None,
    dry_run: bool = False,
    dry_run_download_network: bool = False,
) -> DownloadResult:
    """Download trades for missing-coverage buckets, save, and mark coverage.

    When *dry_run* is True:
      - If *dry_run_download_network* is False (default): skip all network
        calls; only populate ``would_download_*`` counters.
      - If *dry_run_download_network* is True: make network calls to validate
        the download logic but NEVER persist trades or coverage.
      - In either case, **never** calls ``trade_store.save()`` or
        ``trade_store.mark_coverage()``.
    """
    result = DownloadResult(requested_buckets=len(missing_bucket_starts))
    ordered = sorted(missing_bucket_starts, reverse=True)

    for bucket_start in ordered:
        bucket_end = bucket_start + bucket_ms - 1

        if bucket_start >= current_bucket_start_ms:
            result.skipped_buckets += 1
            continue

        # --- dry-run without network: report what WOULD be attempted ---
        if dry_run and not dry_run_download_network:
            result.would_download_buckets += 1
            result.would_download_trade_count += 0  # unknown without network
            continue

        try:
            trades, pages, complete = download_func(
                raw_symbol, bucket_start, bucket_end, limit
            )
        except Exception as exc:
            result.failed_buckets += 1
            result.errors.append(
                f"download_failed bucket_start_ms={bucket_start} error={exc}"
            )
            continue

        if not trades:
            result.failed_buckets += 1
            result.errors.append(
                f"no_trades_returned bucket_start_ms={bucket_start}"
            )
            continue

        # --- dry-run with network: validate but DO NOT persist ---
        if dry_run:
            result.would_download_buckets += 1
            result.would_download_trade_count += len(trades)
            # Simulate coverage validation without persisting.
            # We can't run _validate_bucket_trade_coverage against the real DB
            # because trades were never saved.  Just count as "would cover".
            continue

        # --- normal (non-dry-run) path: persist ---
        for t in trades:
            object.__setattr__(t, "symbol", symbol)

        trade_store.save(trades)
        result.downloaded_trade_count += len(trades)

        ok, reason = _validate_bucket_trade_coverage(
            db_path=trade_store.path,
            symbol=symbol,
            bucket_start_ms=bucket_start,
            bucket_end_ms=bucket_end,
            edge_tolerance_ms=coverage_edge_tolerance_ms,
            max_gap_ms=coverage_max_gap_ms,
        )
        if ok:
            trade_store.mark_coverage(
                symbol=symbol,
                time_range=TimeRange(bucket_start, bucket_end),
                source="historical",
            )
            result.downloaded_buckets += 1
            result.coverage_validated_buckets += 1
        else:
            result.failed_buckets += 1
            result.coverage_validation_failed_buckets += 1
            result.coverage_validation_failed_examples.append({
                "bucket_start_ms": bucket_start,
                "bucket_end_ms": bucket_end,
                "trades_downloaded": len(trades),
                "pages": pages,
                "reason": reason,
            })

    return result


def _validate_bucket_trade_coverage(
    *,
    db_path: Path,
    symbol: str,
    bucket_start_ms: int,
    bucket_end_ms: int,
    edge_tolerance_ms: int,
    max_gap_ms: int,
) -> tuple[bool, str]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*),
                   MIN(COALESCE(trade_time_ms, event_time_ms)),
                   MAX(COALESCE(trade_time_ms, event_time_ms))
            FROM trades
            WHERE symbol = ?
              AND COALESCE(trade_time_ms, event_time_ms) BETWEEN ? AND ?
            """,
            (symbol, bucket_start_ms, bucket_end_ms),
        ).fetchone()
    count = int(row[0] or 0)
    earliest = row[1]
    latest = row[2]

    if count == 0:
        return False, "no trades in bucket"

    if earliest is None or latest is None:
        return False, "no timestamps in bucket trades"

    earliest_ms = int(earliest)
    latest_ms = int(latest)

    if earliest_ms > bucket_start_ms + edge_tolerance_ms:
        return False, (
            f"earliest trade {earliest_ms} too far from bucket_start {bucket_start_ms} "
            f"(gap={earliest_ms - bucket_start_ms}ms > tolerance={edge_tolerance_ms}ms)"
        )

    if latest_ms < bucket_end_ms - edge_tolerance_ms:
        return False, (
            f"latest trade {latest_ms} too far from bucket_end {bucket_end_ms} "
            f"(gap={bucket_end_ms - latest_ms}ms > tolerance={edge_tolerance_ms}ms)"
        )

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(trade_time_ms, event_time_ms)
            FROM trades
            WHERE symbol = ?
              AND COALESCE(trade_time_ms, event_time_ms) BETWEEN ? AND ?
            ORDER BY COALESCE(trade_time_ms, event_time_ms) ASC
            """,
            (symbol, bucket_start_ms, bucket_end_ms),
        ).fetchall()

    prev: int | None = None
    max_gap_found = 0
    for (ts,) in rows:
        ts_int = int(ts)
        if prev is not None:
            gap = ts_int - prev
            if gap > max_gap_found:
                max_gap_found = gap
        prev = ts_int

    if max_gap_found > max_gap_ms:
        return False, (
            f"max inter-trade gap {max_gap_found}ms exceeds threshold {max_gap_ms}ms"
        )

    return True, "ok"


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

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
    # Trade-coverage classification
    trade_coverage_complete_buckets: int = 0
    missing_trade_coverage_buckets: int = 0
    missing_trade_coverage_examples: list[dict[str, int]] = field(default_factory=list)
    trades_exist_but_coverage_missing: int = 0
    trades_exist_but_coverage_missing_examples: list[dict[str, int]] = field(default_factory=list)
    empty_trade_buckets: int = 0
    empty_trade_bucket_examples: list[dict[str, int]] = field(default_factory=list)
    trades_loaded: int = 0
    # Range bars
    range_bars_before_count: int = 0
    range_bars_rebuilt_count: int = 0
    range_bars_written_count: int = 0
    # Aggregates
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
    # Pollution
    legacy_or_test_polluted_completed_aggregates_detected: bool = False
    pollution_rows_deleted: int = 0
    # Mode & scope
    mode: str = "incremental"
    force_rebuild_window: bool = False
    clean_pollution: bool = False
    deleted_existing_aggregates: int = 0
    deleted_existing_range_bar_buckets: int = 0
    repair_range_bars: bool = False
    rebuild_aggregates: bool = True
    delete_existing_aggregates: bool = False
    delete_existing_range_bars: bool = False
    # Incremental bucket tracking
    buckets_already_complete: int = 0
    buckets_downloaded: int = 0
    buckets_repaired: int = 0
    buckets_aggregate_upserted: int = 0
    buckets_skipped_existing_complete: int = 0
    # Contract
    contract_value: str = str(DEFAULT_CONTRACT_VALUE)
    contract_value_source: str = "fallback"
    builder_mode: str = "bucket_isolated"
    # Download
    download_missing_trades: bool = False
    dry_run_download_network: bool = False
    download_requested_buckets: int = 0
    downloaded_buckets: int = 0
    download_failed_buckets: int = 0
    download_skipped_buckets: int = 0
    downloaded_trade_count: int = 0
    would_download_buckets: int = 0
    would_download_trade_count: int = 0
    coverage_validated_buckets: int = 0
    coverage_validation_failed_buckets: int = 0
    coverage_validation_failed_examples: list[dict] = field(default_factory=list)
    download_errors: list[str] = field(default_factory=list)
    # Live protection
    live_running_detected: bool = False
    live_db_write_allowed: bool = False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline repair tool for local range-speed history."
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
    # Mode & scope
    parser.add_argument(
        "--mode",
        choices=("incremental", "rebuild-window"),
        default="incremental",
        help="Repair mode: incremental (bucket-scoped, default) or rebuild-window (full window).",
    )
    parser.add_argument(
        "--force-rebuild-window",
        nargs="?", const=True, default=False, type=_bool,
        help="When set, allows window-level deletion of aggregates / range_bars. Requires --mode rebuild-window for "
             "full effect.",
    )
    parser.add_argument(
        "--clean-pollution",
        nargs="?", const=True, default=False, type=_bool,
        help="Delete pollution rows (bucket_end_ms < 2022-01-01) without touching valid history.",
    )
    # Delete flags (behaviour depends on --mode)
    parser.add_argument(
        "--delete-existing-aggregates",
        nargs="?", const=True, default=False, type=_bool,
        help="In incremental mode: only delete aggregates for repaired buckets before upsert. "
             "In rebuild-window mode with --force-rebuild-window: delete all aggregates in the window.",
    )
    parser.add_argument(
        "--delete-existing-range-bars",
        nargs="?", const=True, default=False, type=_bool,
        help="In incremental mode: replace range bars per repaired bucket. "
             "In rebuild-window mode with --force-rebuild-window: replace all range bars in the window.",
    )
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
    # Download
    parser.add_argument(
        "--download-missing-trades",
        nargs="?", const=True, default=False, type=_bool,
        help="Download missing OKX historical trades before repair.",
    )
    parser.add_argument(
        "--dry-run-download-network",
        nargs="?", const=True, default=False, type=_bool,
        help="When --dry-run is set, allow real OKX network requests for download validation "
             "(DB writes are still forbidden). Default: false.",
    )
    parser.add_argument("--okx-base-url", default="https://www.okx.com")
    parser.add_argument("--download-limit", type=int, default=100)
    parser.add_argument("--download-sleep-seconds", type=float, default=0.2)
    parser.add_argument("--download-max-retries", type=int, default=5)
    parser.add_argument("--download-timeout-seconds", type=int, default=20)
    parser.add_argument("--download-lookback-buckets", type=int, default=None)
    parser.add_argument("--download-max-pages", type=int, default=None)
    parser.add_argument(
        "--skip-download-if-live", nargs="?", const=True, default=True, type=_bool,
    )
    parser.add_argument(
        "--allow-live-db-write", nargs="?", const=True, default=False, type=_bool,
    )
    parser.add_argument("--coverage-edge-tolerance-ms", type=int, default=300_000)
    parser.add_argument("--coverage-max-gap-ms", type=int, default=1_800_000)
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def run(
    args: argparse.Namespace,
    *,
    now_ms: int | None = None,
    download_func: DownloadFunc | None = None,
    live_detector: LiveDetector | None = None,
) -> tuple[RepairSummary, int]:
    bucket_ms = _parse_interval_ms(args.bucket_interval)
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    current_bucket_start_ms = (now // bucket_ms) * bucket_ms
    end_ms = int(args.end_ms) if args.end_ms is not None else current_bucket_start_ms - 1
    end_ms = min(end_ms, current_bucket_start_ms - 1)

    if args.start_ms is not None:
        start_ms = int(args.start_ms)
    elif args.download_lookback_buckets is not None:
        start_ms = current_bucket_start_ms - int(args.download_lookback_buckets) * bucket_ms
    else:
        start_ms = current_bucket_start_ms - int(args.min_buckets) * bucket_ms

    target_range = TimeRange(start_ms, end_ms)
    bucket_starts = _complete_bucket_starts(start_ms, end_ms, bucket_ms)
    range_pct = _decimal_text(args.range_pct)
    exchange = str(args.exchange).strip().lower()
    market_db = _resolve_path(args.market_db)
    checkpoint_db = _resolve_path(args.checkpoint_db)
    contract_value, contract_source, contract_warning = _resolve_contract_value(
        symbol=args.symbol, exchange=exchange, explicit=args.contract_value,
    )

    mode = str(args.mode)
    force_rebuild = bool(args.force_rebuild_window)
    clean_pollution = bool(args.clean_pollution)
    download_missing = bool(args.download_missing_trades)
    allow_live_write = bool(args.allow_live_db_write)
    dry_run = bool(args.dry_run)
    dry_run_dl_net = bool(args.dry_run_download_network)

    # ---- will_write_db: any operation that mutates the database ----
    will_write_db = not dry_run and bool(
        download_missing
        or args.repair_range_bars
        or args.rebuild_aggregates
        or args.delete_existing_aggregates
        or args.delete_existing_range_bars
        or clean_pollution
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
        dry_run=dry_run,
        repair_range_bars=bool(args.repair_range_bars),
        rebuild_aggregates=bool(args.rebuild_aggregates),
        delete_existing_aggregates=bool(args.delete_existing_aggregates),
        delete_existing_range_bars=bool(args.delete_existing_range_bars),
        contract_value=str(contract_value),
        contract_value_source=contract_source,
        builder_mode=args.builder_mode,
        download_missing_trades=download_missing,
        dry_run_download_network=dry_run_dl_net,
        live_db_write_allowed=allow_live_write,
        mode=mode,
        force_rebuild_window=force_rebuild,
        clean_pollution=clean_pollution,
    )
    if contract_warning:
        summary.warnings.append(contract_warning)
    if end_ms < start_ms:
        raise ValueError(
            "end-ms must be greater than or equal to start-ms after excluding the current bucket"
        )

    # ---- live detection (uses will_write_db) ----
    detector = live_detector if live_detector is not None else LiveDetector()
    live_running = detector.is_live()
    summary.live_running_detected = live_running

    if live_running and will_write_db and not allow_live_write:
        _print_summary(summary)
        print(
            "\n[REPAIR-ABORT] Live process detected and --allow-live-db-write not set. "
            "Exiting (exit code 3).\n"
            "  Rerun with --allow-live-db-write to force writes, or stop the live process first.\n"
            "  Dry-run is always allowed even when live.",
            file=sys.stderr,
        )
        return summary, 3

    if live_running and will_write_db and allow_live_write:
        summary.warnings.append(
            "WARNING live_running_detected_allow_live_db_write_active"
        )

    trade_store = SqliteTradeStore(market_db)
    range_store = SqliteRangeBarStore(market_db)
    checkpoint_store = SqliteRangeCheckpointStore(checkpoint_db)

    # ---- read before state ----
    summary.range_bars_before_count = _range_bar_count(
        market_db, symbol=args.symbol, range_pct=range_pct, time_range=target_range,
    )
    before = _completed_count_min_max(
        checkpoint_db, exchange=exchange, symbol=args.symbol, range_pct=range_pct,
    )
    summary.aggregates_before_count = before.count
    summary.aggregates_before_min = before.min
    summary.aggregates_before_max = before.max

    pollution_detected = _polluted_count(
        checkpoint_db, exchange=exchange, symbol=args.symbol, range_pct=range_pct,
    ) > 0
    summary.legacy_or_test_polluted_completed_aggregates_detected = pollution_detected

    # ---- classify trade coverage ----
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
            market_db, symbol=args.symbol, bucket_starts=missing_bucket_starts, bucket_ms=bucket_ms,
        )
        summary.trades_exist_but_coverage_missing = len(summary.trades_exist_but_coverage_missing_examples)
        if summary.trades_exist_but_coverage_missing:
            summary.warnings.append("trades_exist_but_coverage_missing")

    # ---- find buckets that already have COMPLETE aggregates ----
    already_complete_buckets: set[int] = _find_buckets_with_complete_aggregates(
        checkpoint_db, exchange=exchange, symbol=args.symbol, range_pct=range_pct,
        bucket_starts=complete_bucket_starts,
    )
    summary.buckets_already_complete = len(already_complete_buckets)
    summary.buckets_skipped_existing_complete = len(already_complete_buckets)

    # ---- determine which buckets need repair ----
    if mode == "incremental" and not force_rebuild:
        needs_repair_coverage = [b for b in complete_bucket_starts if b not in already_complete_buckets]
        needs_repair_set = set(needs_repair_coverage)
    else:
        needs_repair_coverage = list(complete_bucket_starts)
        needs_repair_set = set(needs_repair_coverage)

    # =====================================================================
    # BACKUP must happen BEFORE any DB writes (download, repair, etc.)
    # =====================================================================
    if args.backup and will_write_db:
        summary.backup_paths = _backup_databases(market_db=market_db, checkpoint_db=checkpoint_db, now_ms=now)

    # ---- download missing trades (AFTER backup) ----
    original_missing_set = set(missing_bucket_starts)
    newly_downloaded: set[int] = set()

    if download_missing and missing_bucket_starts:
        if live_running and bool(args.skip_download_if_live):
            summary.warnings.append("download_skipped_live_running")
        else:
            func: DownloadFunc
            if download_func is not None:
                func = download_func
            elif dry_run and not dry_run_dl_net:
                # Dry-run without network: use a no-op func to avoid real HTTP.
                def _noop_downloader(
                    _rs: str, _bs: int, _be: int, _lim: int,
                ) -> tuple[list[MarketTrade], int, bool]:
                    return [], 0, False

                func = _noop_downloader
            else:
                dl = OkxHistoricalTradeDownloader(
                    base_url=args.okx_base_url,
                    timeout_seconds=int(args.download_timeout_seconds),
                    max_retries=int(args.download_max_retries),
                    sleep_seconds=float(args.download_sleep_seconds),
                )
                func = dl.download_bucket_trades

            dl_result = _download_missing_trades(
                raw_symbol=args.raw_symbol,
                symbol=args.symbol,
                missing_bucket_starts=list(missing_bucket_starts),
                bucket_ms=bucket_ms,
                current_bucket_start_ms=current_bucket_start_ms,
                download_func=func,
                trade_store=trade_store,
                limit=min(int(args.download_limit), 100),
                coverage_edge_tolerance_ms=int(args.coverage_edge_tolerance_ms),
                coverage_max_gap_ms=int(args.coverage_max_gap_ms),
                max_pages_per_bucket=args.download_max_pages,
                dry_run=dry_run,
                dry_run_download_network=dry_run_dl_net,
            )
            summary.download_requested_buckets = dl_result.requested_buckets
            summary.downloaded_buckets = dl_result.downloaded_buckets
            summary.download_failed_buckets = dl_result.failed_buckets
            summary.download_skipped_buckets = dl_result.skipped_buckets
            summary.downloaded_trade_count = dl_result.downloaded_trade_count
            summary.would_download_buckets = dl_result.would_download_buckets
            summary.would_download_trade_count = dl_result.would_download_trade_count
            summary.coverage_validated_buckets = dl_result.coverage_validated_buckets
            summary.coverage_validation_failed_buckets = dl_result.coverage_validation_failed_buckets
            summary.coverage_validation_failed_examples = dl_result.coverage_validation_failed_examples
            summary.download_errors = dl_result.errors

            if dl_result.errors:
                summary.warnings.append("download_errors_occurred")
            if dl_result.coverage_validation_failed_buckets:
                summary.warnings.append("downloaded_trades_failed_coverage_validation")

            # Re-read coverage after download (only when NOT dry-run, since
            # dry-run does not persist).
            if not dry_run:
                complete_bucket_starts, missing_bucket_starts = _classify_trade_coverage(
                    trade_store.coverage_ranges(
                        symbol=args.symbol, time_range=target_range, source="historical"
                    ),
                    bucket_starts=bucket_starts,
                    bucket_ms=bucket_ms,
                )
                summary.trade_coverage_complete_buckets = len(complete_bucket_starts)
                summary.missing_trade_coverage_buckets = len(missing_bucket_starts)
                summary.missing_trade_coverage_examples = _bucket_examples(missing_bucket_starts, bucket_ms)

                newly_downloaded = set(complete_bucket_starts) & original_missing_set
                needs_repair_set |= newly_downloaded
                needs_repair_coverage = [b for b in complete_bucket_starts if b in needs_repair_set]

    summary.buckets_downloaded = len(newly_downloaded & needs_repair_set)

    # ---- clean pollution (independent of mode) ----
    if clean_pollution or (args.delete_existing_aggregates and mode == "incremental" and pollution_detected):
        pollution_deleted = _delete_pollution_rows(
            checkpoint_db,
            exchange=exchange,
            symbol=args.symbol,
            range_pct=range_pct,
            dry_run=dry_run,
        )
        summary.pollution_rows_deleted = pollution_deleted

    # ---- repair range bars (bucket-scoped) ----
    buckets_to_repair_range: list[int] = []
    empty_bucket_starts: list[int] = []

    if args.repair_range_bars:
        buckets_to_repair_range = sorted(needs_repair_set & set(complete_bucket_starts))

        rebuilt, written, trades_loaded, empty_buckets = _repair_range_bars(
            trade_store=trade_store,
            range_store=range_store,
            symbol=args.symbol,
            range_pct=range_pct,
            bucket_starts=buckets_to_repair_range,
            bucket_ms=bucket_ms,
            contract_value=contract_value,
            delete_existing=bool(args.delete_existing_range_bars) or mode == "incremental",
            dry_run=dry_run,
            builder_mode=args.builder_mode,
        )
        summary.range_bars_rebuilt_count = rebuilt
        summary.range_bars_written_count = written
        summary.trades_loaded = trades_loaded
        summary.empty_trade_buckets = len(empty_buckets)
        summary.empty_trade_bucket_examples = _bucket_examples(empty_buckets, bucket_ms)
        empty_bucket_starts = empty_buckets
        summary.buckets_repaired = len(buckets_to_repair_range)
        if args.delete_existing_range_bars and not dry_run:
            summary.deleted_existing_range_bar_buckets = len(buckets_to_repair_range)

    # ---- rebuild aggregates (bucket-scoped upsert) ----
    if args.rebuild_aggregates:
        agg_candidate_set = needs_repair_set if mode == "incremental" else set(complete_bucket_starts)

        if args.delete_existing_aggregates and force_rebuild and mode == "rebuild-window":
            window_deleted, _ = _delete_aggregates_window(
                checkpoint_db,
                exchange=exchange,
                symbol=args.symbol,
                range_pct=range_pct,
                time_range=target_range,
                dry_run=dry_run,
            )
            summary.deleted_existing_aggregates = window_deleted
        elif args.delete_existing_aggregates and not dry_run:
            for bucket_start in sorted(agg_candidate_set):
                _delete_one_aggregate(
                    checkpoint_db,
                    exchange=exchange,
                    symbol=args.symbol,
                    range_pct=range_pct,
                    bucket_start_ms=bucket_start,
                    bucket_ms=bucket_ms,
                )

        aggregates = RangeBarAggregator().aggregate(
            range_store.load(symbol=args.symbol, range_pct=range_pct, time_range=target_range),
            bucket_ms=bucket_ms,
        )
        complete_starts = set(complete_bucket_starts) - set(empty_bucket_starts)
        eligible = [
            aggregate
            for aggregate in aggregates
            if aggregate.bucket_start_ms in (agg_candidate_set & complete_starts)
            and aggregate.bar_count > 0
            and aggregate.bucket_end_ms <= end_ms
        ]
        summary.aggregates_built_count = len(eligible)
        if not dry_run:
            for aggregate in eligible:
                checkpoint_store.save_completed_aggregate(
                    exchange=exchange,
                    aggregate=aggregate,
                    coverage_status=RangeCoverageStatus.COMPLETE.value,
                    missing_gap_ms=0,
                    completed_at_ms=now,
                )
            summary.aggregates_written_count = len(eligible)
        summary.buckets_aggregate_upserted = len(
            set(a.bucket_start_ms for a in eligible)
        )

    # ---- read after state ----
    after = _completed_count_min_max(
        checkpoint_db, exchange=exchange, symbol=args.symbol, range_pct=range_pct,
    )
    summary.aggregates_after_count = after.count
    summary.aggregates_after_min = after.min
    summary.aggregates_after_max = after.max
    summary.enough_for_range_speed = summary.aggregates_after_count >= summary.min_buckets
    if not summary.enough_for_range_speed:
        summary.warnings.append("WARNING insufficient_complete_range_history_for_min_periods")

    _print_summary(summary)
    if args.json_output:
        _write_json_output(_resolve_path(args.json_output), summary, dry_run=dry_run)

    exit_code = 2 if args.fail_under_min and not summary.enough_for_range_speed else 0
    return summary, exit_code


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    _, exit_code = run(args)
    return exit_code


# ---------------------------------------------------------------------------
# Repair helpers
# ---------------------------------------------------------------------------

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


def _find_buckets_with_complete_aggregates(
    db_path: Path,
    *,
    exchange: str,
    symbol: str,
    range_pct: str,
    bucket_starts: Sequence[int],
) -> set[int]:
    if not bucket_starts:
        return set()
    placeholders = ",".join("?" for _ in bucket_starts)
    pct = _decimal_text(range_pct)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT bucket_start_ms
            FROM completed_range_aggregates
            WHERE exchange = ? AND symbol = ? AND range_pct = ?
              AND coverage_status = ?
              AND bucket_start_ms IN ({placeholders})
            """,
            (exchange, symbol, pct, RangeCoverageStatus.COMPLETE.value, *bucket_starts),
        ).fetchall()
    return {int(row[0]) for row in rows}


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


def _delete_pollution_rows(
    db_path: Path,
    *,
    exchange: str,
    symbol: str,
    range_pct: str,
    dry_run: bool,
) -> int:
    with sqlite3.connect(db_path) as conn:
        count = int(
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
        if not dry_run and count > 0:
            conn.execute(
                """
                DELETE FROM completed_range_aggregates
                WHERE exchange = ? AND symbol = ? AND range_pct = ? AND bucket_end_ms < ?
                """,
                (exchange, symbol, range_pct, POLLUTION_CUTOFF_MS),
            )
    return count


def _delete_aggregates_window(
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
            if target_count > 0:
                conn.execute(
                    """
                    DELETE FROM completed_range_aggregates
                    WHERE exchange = ? AND symbol = ? AND range_pct = ? AND bucket_end_ms BETWEEN ? AND ?
                    """,
                    (exchange, symbol, range_pct, time_range.start_time_ms, time_range.end_time_ms),
                )
            if polluted_count > 0:
                conn.execute(
                    """
                    DELETE FROM completed_range_aggregates
                    WHERE exchange = ? AND symbol = ? AND range_pct = ? AND bucket_end_ms < ?
                    """,
                    (exchange, symbol, range_pct, POLLUTION_CUTOFF_MS),
                )
    return target_count, polluted_count


def _delete_one_aggregate(
    db_path: Path,
    *,
    exchange: str,
    symbol: str,
    range_pct: str,
    bucket_start_ms: int,
    bucket_ms: int,
) -> None:
    bucket_end_ms = bucket_start_ms + bucket_ms - 1
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            DELETE FROM completed_range_aggregates
            WHERE exchange = ? AND symbol = ? AND range_pct = ? AND bucket_start_ms = ? AND bucket_end_ms = ?
            """,
            (exchange, symbol, _decimal_text(range_pct), bucket_start_ms, bucket_end_ms),
        )


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
        "mode",
        "start_ms",
        "end_ms",
        "bucket_count_target",
        "buckets_already_complete",
        "buckets_skipped_existing_complete",
        "buckets_downloaded",
        "buckets_repaired",
        "buckets_aggregate_upserted",
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
        "pollution_rows_deleted",
        "min_buckets",
        "enough_for_range_speed",
        "dry_run",
        "dry_run_download_network",
        "download_missing_trades",
        "download_requested_buckets",
        "downloaded_buckets",
        "download_failed_buckets",
        "download_skipped_buckets",
        "downloaded_trade_count",
        "would_download_buckets",
        "would_download_trade_count",
        "coverage_validated_buckets",
        "coverage_validation_failed_buckets",
        "live_running_detected",
        "live_db_write_allowed",
        "force_rebuild_window",
        "clean_pollution",
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
