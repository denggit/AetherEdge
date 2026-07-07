from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import pytest

from tools import prebuild_mf_feature_history as tool


def _readiness(ready: bool) -> dict[str, bool]:
    return {
        "tradebar_ready": ready,
        "fixed_time_footprint_ready": ready,
        "range_footprint_ready": ready,
        "coverage_ready": ready,
        "degraded_footprint": False,
        "ready": ready,
    }


def _args(tmp_path, *extra: str):
    return tool.build_parser().parse_args(
        [
            "--status-path",
            str(tmp_path / "status.json"),
            "--market-db",
            str(tmp_path / "market.sqlite3"),
            "--sleep-seconds",
            "0",
            *extra,
        ]
    )


def test_already_ready_exits_without_run_cycle(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: _readiness(True),
    )
    calls = []
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: calls.append(kwargs),
    )

    result = tool.run_prebuild(_args(tmp_path))

    assert result == 0
    assert calls == []


def test_partial_cycle_then_ready_exits_zero(
    tmp_path,
    monkeypatch,
) -> None:
    readiness = iter((_readiness(False), _readiness(True)))
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: next(readiness),
    )
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: {
            "status": "partial",
            "reason": "cycle_limit_reached",
            "target_end_ms": 2,
        },
    )

    result = tool.run_prebuild(_args(tmp_path))

    assert result == 0


def test_repeated_cycle_failures_exit_nonzero(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: _readiness(False),
    )
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: {
            "status": "error",
            "reason": "download_failures",
        },
    )

    result = tool.run_prebuild(
        _args(tmp_path, "--max-failures", "2")
    )

    assert result != 0
    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["error"] is True
    assert status["error_detail"] == "max_failures_reached"


def test_max_cycles_not_ready_exits_nonzero(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: _readiness(False),
    )
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: {
            "status": "partial",
            "reason": "cycle_limit_reached",
        },
    )

    result = tool.run_prebuild(
        _args(tmp_path, "--max-cycles", "2")
    )

    assert result != 0
    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["cycles"] == 2
    assert status["running"] is False


def test_status_file_is_written_with_atomic_replace(
    tmp_path,
    monkeypatch,
) -> None:
    calls = []
    real_replace = os.replace

    def capture(source, target):
        calls.append((source, target))
        real_replace(source, target)

    monkeypatch.setattr(tool.os, "replace", capture)
    target = tmp_path / "nested" / "status.json"

    tool._write_status(target, {"running": True})

    assert calls == [
        (target.with_name("status.json.tmp"), target)
    ]
    assert not target.with_name("status.json.tmp").exists()
    assert json.loads(target.read_text())["running"] is True


def test_no_download_passes_true_to_worker(
    tmp_path,
    monkeypatch,
) -> None:
    readiness = iter((_readiness(False), _readiness(True)))
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: next(readiness),
    )
    captured = {}

    def cycle(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "reason": "cycle_complete"}

    monkeypatch.setattr(tool, "run_cycle", cycle)

    result = tool.run_prebuild(
        _args(tmp_path, "--no-download")
    )

    assert result == 0
    assert captured["no_download"] is True
    assert captured["archive_publish_lag_hours"] == 8.0


def test_effective_required_minutes_covers_large_share_window(
    tmp_path,
    monkeypatch,
) -> None:
    readiness_calls = []
    readiness = iter((_readiness(False), _readiness(True)))

    def capture_readiness(_args, *, required_minutes):
        readiness_calls.append(required_minutes)
        return next(readiness)

    monkeypatch.setattr(tool, "_readiness_audit", capture_readiness)
    captured = {}
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: captured.update(kwargs)
        or {"status": "ok", "reason": "cycle_complete"},
    )

    result = tool.run_prebuild(
        _args(tmp_path, "--target-days", "3")
    )

    assert result == 0
    assert readiness_calls == [129_600, 129_600]
    assert captured["required_minutes"] == 129_600
    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["requested_minutes"] == 4_320
    assert status["effective_required_minutes"] == 129_600


def test_default_command_arguments() -> None:
    args = tool.build_parser().parse_args([])

    assert args.symbol == "ETH-USDT-PERP"
    assert args.exchange == "okx"
    assert args.target_days == 95
    assert args.max_minutes_per_cycle == 4320
    assert args.max_days_per_cycle == 3
    assert args.max_trades_per_cycle == 20_000_000
    assert args.max_seconds_per_cycle == 1800
    assert args.max_cycles == 200
    assert args.max_seconds == 0
    assert args.download is True
    assert args.large_share_min_samples == 43_200
    assert args.large_share_window_days == 90
    assert args.archive_publish_lag_hours == 8.0


def test_format_okx_time_uses_utc_plus_8() -> None:
    timestamp_ms = int(
        datetime(
            2026,
            4,
            13,
            12,
            40,
            59,
            tzinfo=UTC,
        ).timestamp()
        * 1_000
    )

    assert tool._format_okx_time(timestamp_ms) == (
        "2026-04-13 20:40:59+08"
    )


def test_progress_snapshot_calculates_days_percentage_and_eta() -> None:
    required_start_ms = 1_700_000_000_000
    safe_end_ms = required_start_ms + 95 * 86_400_000 - 1
    processed_through_ms = (
        required_start_ms + 10 * 86_400_000 - 1
    )

    progress = tool._progress_snapshot(
        result={
            "target_end_ms": processed_through_ms,
            "safe_archive_end_ms": safe_end_ms,
            "processed_through_ms": processed_through_ms,
            "elapsed_seconds": 476.0,
            "total_bars_written": 4_320,
            "total_footprints_written": 4_320,
            "range_footprints_written": 123,
            "coverage_after": {
                "complete_minutes": 14_400,
                "missing_minutes": 122_400,
            },
            "cycle_truncated": True,
        },
        readiness=_readiness(False),
        target_days=95,
        elapsed_seconds=400.0,
    )

    assert progress["completed_days"] == pytest.approx(10.0)
    assert progress["progress_pct"] == pytest.approx(
        10 / 95 * 100
    )
    assert progress["remaining_days"] == pytest.approx(85.0)
    assert progress["avg_seconds_per_day"] == pytest.approx(40.0)
    assert progress["eta_seconds"] == pytest.approx(3_400.0)
    assert progress["eta"] != "unknown"
    assert progress["coverage_complete_minutes"] == 14_400
    assert progress["coverage_missing_minutes"] == 122_400


def test_progress_never_reports_100_percent_with_missing_coverage() -> None:
    safe_end_ms = 1_700_000_000_000 + 95 * 86_400_000 - 1

    progress = tool._progress_snapshot(
        result={
            "target_end_ms": safe_end_ms,
            "safe_archive_end_ms": safe_end_ms,
            "processed_through_ms": safe_end_ms,
            "coverage_after": {
                "complete_minutes": 95 * 1_440,
                "missing_minutes": 1,
            },
        },
        readiness=_readiness(False),
        target_days=95,
        elapsed_seconds=10.0,
    )

    assert progress["progress_pct"] < 100.0
    assert progress["remaining_days"] > 0


def test_progress_summary_contains_cycle_status_and_ready(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    readiness = iter((_readiness(False), _readiness(True)))
    target_days = 120
    required_start_ms = 1_700_000_000_000
    safe_end_ms = (
        required_start_ms + target_days * 86_400_000 - 1
    )
    processed_through_ms = (
        required_start_ms + 12 * 86_400_000 - 1
    )
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: next(readiness),
    )
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: {
            "status": "ok",
            "reason": "cycle_complete",
            "target_end_ms": processed_through_ms,
            "safe_archive_end_ms": safe_end_ms,
            "processed_through_ms": processed_through_ms,
            "elapsed_seconds": 60.0,
            "total_bars_written": 4_320,
            "total_footprints_written": 4_320,
            "range_footprints_written": 10,
            "coverage_after": {
                "complete_minutes": 17_280,
                "missing_minutes": 155_520,
            },
        },
    )

    assert tool.run_prebuild(_args(tmp_path, "--target-days", "120")) == 0

    output = capsys.readouterr().out
    assert "[prebuild-mf] cycle=1" in output
    assert "status=ok" in output
    assert "ready=True" in output
    assert "target_okx=" in output
    assert "progress=12.00/120.00d" in output
    assert "remaining_days=108.00" in output
    assert "eta=" in output
    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["progress"] == "12.00/120.00d"
    assert status["progress_pct"] == pytest.approx(10.0)
    assert status["completed_days"] == pytest.approx(12.0)
    assert status["remaining_days"] == pytest.approx(108.0)
    assert status["eta_seconds"] is not None
    assert status["target_okx"] != "unknown"
    assert status["safe_end_okx"] != "unknown"
    assert status["required_start_okx"] != "unknown"


# ---------------------------------------------------------------------------
# End-to-end tests with real SQLite store (R011)
# ---------------------------------------------------------------------------

def test_resolve_trade_feature_readiness_reads_real_sqlite_data(
    tmp_path, monkeypatch,
) -> None:
    """resolve_trade_feature_readiness reads from a real SQLite store and
    correctly reports readiness based on actual data present (or absent)."""
    from decimal import Decimal
    from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
    from src.market_data.trade_features.coverage import resolve_trade_feature_readiness

    db_path = tmp_path / "market.sqlite3"
    store = SqliteTradeFeatureStore(path=db_path)

    # Start with empty store — readiness must be False
    readiness = resolve_trade_feature_readiness(
        symbol="ETH-USDT-PERP", exchange="okx", store=store,
        required_minutes=10, reference_end_ms=1_749_999_960_000,
    )
    audit = dict(readiness.audit())
    audit["ready"] = tool._is_ready(audit)
    assert audit["ready"] is False

    # Write a small amount of data — still not enough
    from src.market_data.models import FixedTimeTradeBar

    store.upsert_tradebars_many(
        [
            FixedTimeTradeBar(
                exchange="okx", symbol="ETH-USDT-PERP", timeframe="1m",
                open_time_ms=1_749_999_900_000,
                close_time_ms=1_749_999_959_999,
                available_time_ms=1_749_999_959_999,
                open=Decimal("3000"), high=Decimal("3005"),
                low=Decimal("2995"), close=Decimal("3002"),
                volume=Decimal("10"), buy_volume=Decimal("6"),
                sell_volume=Decimal("4"), buy_notional=Decimal("18000"),
                sell_notional=Decimal("12000"), delta_volume=Decimal("2"),
                delta_notional=Decimal("6000"), abs_delta_notional=Decimal("6000"),
                trade_count=5, large_trade_share=Decimal("0.05"),
                quality="COMPLETE",
            )
        ]
    )

    readiness = resolve_trade_feature_readiness(
        symbol="ETH-USDT-PERP", exchange="okx", store=store,
        required_minutes=10, reference_end_ms=1_749_999_960_000,
    )
    audit2 = dict(readiness.audit())
    audit2["ready"] = tool._is_ready(audit2)
    assert audit2["ready"] is False  # still not enough


def test_resolve_trade_feature_readiness_with_insufficient_sqlite_data(
    tmp_path, monkeypatch,
) -> None:
    """resolve_trade_feature_readiness with a sparse SQLite store returns
    ready=False when coverage is insufficient."""
    from decimal import Decimal
    from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
    from src.market_data.models import FixedTimeTradeBar
    from src.market_data.trade_features.coverage import resolve_trade_feature_readiness

    db_path = tmp_path / "market.sqlite3"
    store = SqliteTradeFeatureStore(path=db_path)
    bucket_ms = 60_000
    ref_end = 1_749_999_960_000

    import src.market_data.trade_features.coverage as cov
    monkeypatch.setattr(
        cov,
        "safe_okx_archive_end_ms",
        lambda now_ms=None, **kwargs: ref_end,
    )

    # Write only 10 bars, but require 100
    bars = []
    for i in range(10):
        open_ms = ref_end - (10 - i - 1) * bucket_ms
        close_ms = open_ms + bucket_ms - 1
        bars.append(
            FixedTimeTradeBar(
                exchange="okx", symbol="ETH-USDT-PERP", timeframe="1m",
                open_time_ms=open_ms, close_time_ms=close_ms,
                available_time_ms=close_ms,
                open=Decimal("3000"), high=Decimal("3005"),
                low=Decimal("2995"), close=Decimal("3002"),
                volume=Decimal("10"), buy_volume=Decimal("6"),
                sell_volume=Decimal("4"), buy_notional=Decimal("18000"),
                sell_notional=Decimal("12000"), delta_volume=Decimal("2"),
                delta_notional=Decimal("6000"), abs_delta_notional=Decimal("6000"),
                trade_count=5, large_trade_share=Decimal("0.05"),
                quality="COMPLETE",
            )
        )
    store.upsert_tradebars_many(bars)

    readiness = resolve_trade_feature_readiness(
        symbol="ETH-USDT-PERP", exchange="okx", store=store,
        required_minutes=100, reference_end_ms=ref_end,
    )
    audit = dict(readiness.audit())
    audit["ready"] = tool._is_ready(audit)

    assert audit["ready"] is False


def test_run_prebuild_exits_nonzero_when_target_days_invalid(tmp_path) -> None:
    result = tool.main(
        [
            "--target-days", "0",
            "--status-path", str(tmp_path / "status.json"),
        ]
    )
    assert result == 1


def test_run_prebuild_no_download_flag_passed_to_worker(
    tmp_path,
    monkeypatch,
) -> None:
    """--no-download causes no_download=True in run_cycle kwargs."""
    readiness = iter((_readiness(False), _readiness(True)))
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: next(readiness),
    )
    captured = {}

    def cycle(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "reason": "cycle_complete"}

    monkeypatch.setattr(tool, "run_cycle", cycle)

    result = tool.run_prebuild(
        _args(tmp_path, "--no-download")
    )

    assert result == 0
    assert captured.get("no_download") is True


def test_run_prebuild_failed_cycle_counts_as_failure(
    tmp_path,
    monkeypatch,
) -> None:
    """A cycle with status='error' counts as a failure and eventually exits
    non-zero after max_failures."""
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: _readiness(False),
    )
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: {
            "status": "error",
            "reason": "run_cycle_exception",
        },
    )

    result = tool.run_prebuild(
        _args(tmp_path, "--max-failures", "2")
    )

    assert result != 0
    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["ready"] is False
    assert status["error"] is True


def test_run_prebuild_respects_max_seconds(
    tmp_path,
    monkeypatch,
) -> None:
    """When max_seconds is reached, prebuild exits non-zero."""
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: _readiness(False),
    )
    # Simulate time passage
    elapsed = [0.0]

    real_monotonic = tool.time.monotonic

    def fake_monotonic():
        result = real_monotonic()
        if elapsed[0] > 0:
            return result + 999_999  # way past max_seconds
        return result

    monkeypatch.setattr(tool.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: elapsed.__setitem__(0, 1.0)
        or {"status": "partial", "reason": "cycle_limit_reached"},
    )

    result = tool.run_prebuild(
        _args(tmp_path, "--max-seconds", "30")
    )

    assert result != 0
    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["error"] is True


def test_run_prebuild_reaches_ready_with_real_sqlite_feature_coverage(
    tmp_path,
    monkeypatch,
) -> None:
    from decimal import Decimal
    import src.market_data.trade_features.coverage as coverage_module
    from src.market_data.models import (
        FixedTimeTradeBar,
        RangeFootprintFeature,
        TradeFootprintFeature,
    )
    from src.market_data.storage.trade_feature_store import (
        SqliteTradeFeatureStore,
    )

    db_path = tmp_path / "market.sqlite3"
    minute_ms = 60_000
    required_minutes = 9
    minutes_per_cycle = required_minutes // 3
    safe_end_ms = 1_749_999_959_999
    window_start_ms = safe_end_ms - required_minutes * minute_ms + 1
    cycles: list[int] = []
    monkeypatch.setattr(
        tool,
        "_required_windows",
        lambda args: (required_minutes, required_minutes),
    )
    monkeypatch.setattr(
        coverage_module,
        "safe_okx_archive_end_ms",
        lambda now_ms=None, **kwargs: safe_end_ms,
    )

    def data_writing_run_cycle(**kwargs) -> dict[str, object]:
        cycle = len(cycles)
        segment_start_ms = (
            window_start_ms + cycle * minutes_per_cycle * minute_ms
        )
        open_times = [
            segment_start_ms + index * minute_ms
            for index in range(minutes_per_cycle)
        ]
        store = SqliteTradeFeatureStore(path=str(db_path))
        store.upsert_tradebars_many(
            [
                FixedTimeTradeBar(
                    exchange="okx",
                    symbol="ETH-USDT-PERP",
                    timeframe="1m",
                    open_time_ms=open_ms,
                    close_time_ms=open_ms + minute_ms - 1,
                    available_time_ms=open_ms + minute_ms - 1,
                    open=Decimal("3000"),
                    high=Decimal("3005"),
                    low=Decimal("2995"),
                    close=Decimal("3002"),
                    volume=Decimal("10"),
                    buy_volume=Decimal("6"),
                    sell_volume=Decimal("4"),
                    buy_notional=Decimal("18000"),
                    sell_notional=Decimal("12000"),
                    delta_volume=Decimal("2"),
                    delta_notional=Decimal("6000"),
                    abs_delta_notional=Decimal("6000"),
                    trade_count=5,
                    large_trade_share=Decimal("0.05"),
                    quality="COMPLETE",
                )
                for open_ms in open_times
            ]
        )
        store.upsert_footprints_many(
            [
                TradeFootprintFeature(
                    exchange="okx",
                    symbol="ETH-USDT-PERP",
                    timeframe="1m",
                    open_time_ms=open_ms,
                    close_time_ms=open_ms + minute_ms - 1,
                    available_time_ms=open_ms + minute_ms - 1,
                    delta_notional=Decimal("6000"),
                    abs_delta_notional=Decimal("6000"),
                    taker_buy_ratio=Decimal("0.6"),
                    close_pos=Decimal("0.5"),
                    range_pct=Decimal("0.002"),
                    return_pct=Decimal("0.001"),
                    fp_max_bucket_abs_delta_pressure=Decimal("0.8"),
                    context_available=True,
                    quality="COMPLETE",
                )
                for open_ms in open_times
            ]
        )
        store.upsert_range_footprints_many(
            [
                RangeFootprintFeature(
                    exchange="okx",
                    symbol="ETH-USDT-PERP",
                    range_pct=Decimal("0.002"),
                    price_step=Decimal("1"),
                    range_bar_id=cycle + 1,
                    range_start_ms=segment_start_ms - 1_000,
                    range_end_ms=segment_start_ms,
                    available_time_ms=segment_start_ms,
                    fp_max_bucket_abs_delta_pressure=Decimal("0.8"),
                    fp_low_bucket_delta_pressure=Decimal("-0.2"),
                    fp_high_bucket_delta_pressure=Decimal("0.4"),
                    fp_delta_pressure=Decimal("0.1"),
                    bucket_count=3,
                    trade_count=5,
                    context_available=True,
                    quality="COMPLETE",
                )
            ]
        )
        store.mark_range_footprint_coverage(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            range_pct=Decimal("0.002"),
            price_step=Decimal("1"),
            start_ms=segment_start_ms,
            end_ms=open_times[-1] + minute_ms - 1,
            complete=True,
        )
        cycles.append(cycle + 1)
        return {
            "status": "partial" if len(cycles) < 3 else "ok",
            "reason": (
                "cycle_limit_reached"
                if len(cycles) < 3
                else "cycle_complete"
            ),
            "total_bars_written": len(open_times),
        }

    monkeypatch.setattr(tool, "run_cycle", data_writing_run_cycle)

    result = tool.run_prebuild(
        _args(
            tmp_path,
            "--target-days", "1",
            "--large-share-min-samples", "1",
            "--large-share-window-days", "1",
            "--max-cycles", "5",
        )
    )

    assert result == 0
    assert cycles == [1, 2, 3]
    store = SqliteTradeFeatureStore(path=str(db_path))
    with store._connect() as conn:
        counts = {
            table: conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE symbol=?",
                ("ETH-USDT-PERP",),
            ).fetchone()[0]
            for table in (
                "tradebar_1m_features",
                "trade_footprint_1m_features",
                "range_footprint_features",
                "range_footprint_backfill_coverage",
            )
        }
    assert counts == {
        "tradebar_1m_features": required_minutes,
        "trade_footprint_1m_features": required_minutes,
        "range_footprint_features": 3,
        "range_footprint_backfill_coverage": 3,
    }

    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["ready"] is True
    assert status["cycles"] == 3
    assert status["error"] is False
    assert status["last_readiness"]["coverage_ready"] is True


def test_run_prebuild_status_json_ready_false_on_incomplete(
    tmp_path,
    monkeypatch,
) -> None:
    """Status JSON shows ready=false when max_cycles reached without readiness."""
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: _readiness(False),
    )
    monkeypatch.setattr(
        tool,
        "run_cycle",
        lambda **kwargs: {
            "status": "partial",
            "reason": "cycle_limit_reached",
        },
    )

    result = tool.run_prebuild(
        _args(tmp_path, "--max-cycles", "1")
    )

    assert result != 0
    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["ready"] is False
    assert status["error"] is True


def test_deferred_archive_does_not_trigger_max_failures(
    tmp_path,
    monkeypatch,
) -> None:
    calls = []
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: _readiness(False),
    )

    def deferred_cycle(**kwargs):
        calls.append(kwargs)
        return {
            "status": "deferred",
            "reason": "archive_not_published_yet",
            "archive_not_published_days": ["2026-07-06"],
        }

    monkeypatch.setattr(tool, "run_cycle", deferred_cycle)

    result = tool.run_prebuild(
        _args(
            tmp_path,
            "--max-failures",
            "1",
            "--max-cycles",
            "2",
        )
    )

    assert result != 0
    assert len(calls) == 2
    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["error_detail"] == "max_cycles_reached"
    assert status["error_detail"] != "max_failures_reached"


def test_status_includes_archive_lag_and_both_safe_edges(
    tmp_path,
    monkeypatch,
) -> None:
    safe_end = 1_700_000_000_000
    calendar_safe_end = safe_end + 86_400_000
    readiness = {
        **_readiness(True),
        "archive_publish_lag_hours": 8.0,
        "coverage": {
            "latest_complete_close_time_ms": safe_end,
            "complete_minutes": 1_440,
            "missing_minutes": 0,
            "extra": {
                "archive_publish_lag_hours": 8.0,
                "safe_archive_end_ms": safe_end,
                "calendar_safe_archive_end_ms": calendar_safe_end,
            },
        },
    }
    monkeypatch.setattr(
        tool,
        "_readiness_audit",
        lambda *args, **kwargs: readiness,
    )

    assert tool.run_prebuild(_args(tmp_path)) == 0

    status = json.loads(
        (tmp_path / "status.json").read_text(encoding="utf-8")
    )
    assert status["archive_publish_lag_hours"] == 8.0
    assert status["safe_end_okx"] != "unknown"
    assert status["calendar_safe_end_okx"] != "unknown"
