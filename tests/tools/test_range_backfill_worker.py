from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.market_data.backfill.models import RangeBackfillSummary
from src.market_data.backfill.status_store import RangeBackfillStatusStore
from tools import range_backfill_worker as worker
from src.market_data.historical_trades.okx_archive import (
    okx_archive_date_from_utc_ms,
)


def test_worker_cli_defaults_parse() -> None:
    args = worker.build_parser().parse_args([])
    request = worker.request_from_args(args)

    assert request.mode == "live"
    assert request.direction == "recent-to-oldest"
    assert request.required_buckets == 100
    assert request.save_raw_trades is False
    assert request.chunk_sleep_seconds == 0.1
    assert request.max_seconds_per_cycle == 30
    assert request.max_trades_per_cycle == 300_000


def test_worker_all_modes_default_to_no_raw_trade_persistence() -> None:
    live_request = worker.request_from_args(
        worker.build_parser().parse_args(["--mode", "live"])
    )
    prebuild_request = worker.request_from_args(
        worker.build_parser().parse_args(["--mode", "prebuild"])
    )

    assert live_request.save_raw_trades is False
    assert prebuild_request.save_raw_trades is False


def test_worker_raw_trade_persistence_requires_explicit_flag() -> None:
    request = worker.request_from_args(
        worker.build_parser().parse_args(["--save-raw-trades"])
    )

    assert request.save_raw_trades is True


def test_live_worker_default_once_false_and_prebuild_once_true() -> None:
    live_args = worker.build_parser().parse_args(["--mode", "live"])
    prebuild_args = worker.build_parser().parse_args(["--mode", "prebuild"])

    assert worker.resolve_once(live_args) is False
    assert worker.resolve_once(prebuild_args) is True


def test_worker_accepts_max_target_end_ms() -> None:
    args = worker.build_parser().parse_args(["--max-target-end-ms", "1782777599999"])
    request = worker.request_from_args(args)

    assert request.max_target_end_ms == 1782777599999


def test_worker_retry_defaults_are_conservative() -> None:
    args = worker.build_parser().parse_args([])

    assert args.failure_cooldown_seconds == 3600
    assert args.archive_not_ready_cooldown_seconds == 21600
    assert args.daily_retry_after_utc_hour == 1


def test_worker_low_priority_skips_os_nice_on_windows(monkeypatch) -> None:
    called = False

    def fake_nice(value):
        nonlocal called
        called = True

    monkeypatch.setattr(worker.platform, "system", lambda: "Windows")
    monkeypatch.setattr(worker.os, "nice", fake_nice, raising=False)

    worker.maybe_lower_priority(True)

    assert called is False


def test_worker_check_only_does_not_request_writes(monkeypatch) -> None:
    args = worker.build_parser().parse_args(["--check-only"])
    request = worker.request_from_args(args)

    assert request.dry_run is False
    assert args.check_only is True


def _paths(tmp_path) -> list[str]:
    return [
        "--status-path",
        str(tmp_path / "status.json"),
        "--lock-path",
        str(tmp_path / "range.lock"),
        "--global-lock-path",
        str(tmp_path / "global_raw_trade.lock"),
        "--global-status-path",
        str(tmp_path / "global_raw_trade_status.json"),
        "--market-db",
        str(tmp_path / "market.sqlite3"),
        "--checkpoint-db",
        str(tmp_path / "checkpoint.sqlite3"),
        "--raw-root",
        str(tmp_path / "raw"),
    ]


def test_worker_loops_until_missing_after_zero(tmp_path, monkeypatch) -> None:
    calls = 0

    class FakeService:
        def __init__(self, request) -> None:
            pass

        def run_once(self, **kwargs):
            assert kwargs["acquire_lock"] is False
            assert kwargs["mark_process_finished_on_summary"] is False
            nonlocal calls
            calls += 1
            missing = 1 if calls == 1 else 0
            return RangeBackfillSummary(
                symbol="ETH-USDT-PERP",
                exchange="okx",
                range_pct="0.002",
                bucket_interval="4h",
                target_buckets=3,
                complete_before=1,
                complete_after=3 - missing,
                missing_before=2,
                missing_after=missing,
                status="ok",
            )

    monkeypatch.setattr(worker, "RangeBackfillService", FakeService)
    monkeypatch.setattr(worker.time, "sleep", lambda value: None)

    assert worker.main(["--mode", "live", "--sleep-seconds", "0", *_paths(tmp_path)]) == 0
    assert calls == 2


def test_worker_exits_zero_when_cycle_reaches_required_coverage(tmp_path, monkeypatch) -> None:
    class FakeService:
        def __init__(self, request) -> None:
            pass

        def run_once(self, **kwargs):
            return RangeBackfillSummary(
                symbol="ETH-USDT-PERP",
                exchange="okx",
                range_pct="0.002",
                bucket_interval="4h",
                target_buckets=3,
                complete_before=2,
                complete_after=3,
                missing_before=1,
                missing_after=0,
                status="ok",
            )

    monkeypatch.setattr(worker, "RangeBackfillService", FakeService)

    assert worker.main(["--mode", "live", *_paths(tmp_path)]) == 0
    status = RangeBackfillStatusStore(tmp_path / "status.json").read()
    assert status is not None
    assert status["running"] is False
    assert status["phase"] == "completed"
    assert status["exit_code"] == 0


def test_live_worker_status_running_during_cycle_sleep(tmp_path, monkeypatch) -> None:
    captured: list[dict] = []

    class FakeService:
        def __init__(self, request) -> None:
            self.request = request

        def run_once(self, **kwargs):
            RangeBackfillStatusStore(self.request.status_path).patch(
                running=True,
                phase="sleeping",
                heartbeat_ms=1,
                missing_after=1,
            )
            return RangeBackfillSummary(
                symbol="ETH-USDT-PERP",
                exchange="okx",
                range_pct="0.002",
                bucket_interval="4h",
                target_buckets=3,
                complete_before=1,
                complete_after=2,
                missing_before=2,
                missing_after=1,
                status="ok",
            )

    def fake_sleep(value: float) -> None:
        status = RangeBackfillStatusStore(tmp_path / "status.json").read()
        assert status is not None
        captured.append(status)
        raise RuntimeError("stop after observing sleep status")

    monkeypatch.setattr(worker, "RangeBackfillService", FakeService)
    monkeypatch.setattr(worker.time, "sleep", fake_sleep)

    with pytest.raises(RuntimeError):
        worker.main(["--mode", "live", "--sleep-seconds", "30", *_paths(tmp_path)])

    assert captured[0]["running"] is True
    assert captured[0]["phase"] == "sleeping"


def test_live_worker_no_progress_exits_once_and_releases_lock(tmp_path, monkeypatch) -> None:
    calls = 0

    class FakeService:
        def __init__(self, request) -> None:
            pass

        def run_once(self, **kwargs):
            nonlocal calls
            calls += 1
            return RangeBackfillSummary(
                symbol="ETH-USDT-PERP",
                exchange="okx",
                range_pct="0.002",
                bucket_interval="4h",
                target_buckets=100,
                complete_before=96,
                complete_after=96,
                missing_before=4,
                missing_after=4,
                status="no_progress",
                missing_raw_days=("2026-06-29",),
                failed_downloads=("https://example.test/2026-06-29.zip",),
            )

    monkeypatch.setattr(worker, "RangeBackfillService", FakeService)
    monkeypatch.setattr(
        worker.time,
        "sleep",
        lambda value: (_ for _ in ()).throw(AssertionError("must not sleep")),
    )

    assert worker.main(["--mode", "live", "--no-once", *_paths(tmp_path)]) == 0

    status = RangeBackfillStatusStore(tmp_path / "status.json").read()
    assert calls == 1
    assert status is not None
    assert status["running"] is False
    assert status["phase"] == "no_progress"
    assert status["exit_code"] == 0
    assert status["range_speed_reason"] == "archive_gap_no_progress"
    assert status["next_retry_after_ms"] > status["finished_at_ms"]
    assert status["missing_raw_days"] == ["2026-06-29"]
    assert not (tmp_path / "range.lock").exists()


def test_live_worker_current_day_archive_missing_defers_until_next_utc_day(
    tmp_path,
    monkeypatch,
) -> None:
    today = okx_archive_date_from_utc_ms(
        int(datetime.now(UTC).timestamp() * 1000)
    ).isoformat()

    class FakeService:
        def __init__(self, request) -> None:
            pass

        def run_once(self, **kwargs):
            return RangeBackfillSummary(
                symbol="ETH-USDT-PERP",
                exchange="okx",
                range_pct="0.002",
                bucket_interval="4h",
                target_buckets=100,
                complete_before=96,
                complete_after=96,
                missing_before=4,
                missing_after=4,
                status="no_progress",
                missing_raw_days=(today,),
                failed_downloads=(f"https://example.test/{today}.zip",),
            )

    monkeypatch.setattr(worker, "RangeBackfillService", FakeService)

    assert worker.main(["--mode", "live", "--no-once", *_paths(tmp_path)]) == 0

    status = RangeBackfillStatusStore(tmp_path / "status.json").read()
    assert status is not None
    assert status["range_speed_reason"] == "current_day_archive_not_ready"
    retry_at = datetime.fromtimestamp(status["next_retry_after_ms"] / 1000, tz=UTC)
    assert retry_at.date() > datetime.now(UTC).date()


def test_partial_cycle_with_only_current_day_missing_also_exits(
    tmp_path,
    monkeypatch,
) -> None:
    today = okx_archive_date_from_utc_ms(
        int(datetime.now(UTC).timestamp() * 1000)
    ).isoformat()
    calls = 0

    class FakeService:
        def __init__(self, request) -> None:
            pass

        def run_once(self, **kwargs):
            nonlocal calls
            calls += 1
            return RangeBackfillSummary(
                symbol="ETH-USDT-PERP",
                exchange="okx",
                range_pct="0.002",
                bucket_interval="4h",
                target_buckets=100,
                complete_before=95,
                complete_after=96,
                missing_before=5,
                missing_after=4,
                aggregates_written=1,
                status="partial",
                missing_raw_days=(today,),
            )

    monkeypatch.setattr(worker, "RangeBackfillService", FakeService)
    monkeypatch.setattr(
        worker.time,
        "sleep",
        lambda value: (_ for _ in ()).throw(AssertionError("must not sleep")),
    )

    assert worker.main(["--mode", "live", "--no-once", *_paths(tmp_path)]) == 0
    assert calls == 1


def test_partial_cycle_without_writes_exits_to_cooldown(tmp_path, monkeypatch) -> None:
    calls = 0

    class FakeService:
        def __init__(self, request) -> None:
            pass

        def run_once(self, **kwargs):
            nonlocal calls
            calls += 1
            return RangeBackfillSummary(
                symbol="ETH-USDT-PERP",
                exchange="okx",
                range_pct="0.002",
                bucket_interval="4h",
                target_buckets=100,
                complete_before=96,
                complete_after=96,
                missing_before=4,
                missing_after=4,
                aggregates_written=0,
                range_bars_written=0,
                status="partial",
            )

    monkeypatch.setattr(worker, "RangeBackfillService", FakeService)
    monkeypatch.setattr(
        worker.time,
        "sleep",
        lambda value: (_ for _ in ()).throw(AssertionError("must not sleep")),
    )

    assert worker.main(["--mode", "live", "--no-once", *_paths(tmp_path)]) == 0

    status = RangeBackfillStatusStore(tmp_path / "status.json").read()
    assert calls == 1
    assert status is not None
    assert status["running"] is False
    assert status["phase"] == "no_progress"
    assert status["range_speed_reason"] == "archive_gap_partial_no_progress"
    assert status["next_retry_after_ms"] > status["finished_at_ms"]
    assert status["worker_heartbeat_ms"] == status["heartbeat_ms"]
    assert not (tmp_path / "range.lock").exists()


def test_partial_cycle_with_progress_continues(tmp_path, monkeypatch) -> None:
    calls = 0

    class FakeService:
        def __init__(self, request) -> None:
            pass

        def run_once(self, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                return RangeBackfillSummary(
                    symbol="ETH-USDT-PERP",
                    exchange="okx",
                    range_pct="0.002",
                    bucket_interval="4h",
                    target_buckets=100,
                    complete_before=96,
                    complete_after=97,
                    missing_before=4,
                    missing_after=3,
                    aggregates_written=1,
                    status="partial",
                )
            return RangeBackfillSummary(
                symbol="ETH-USDT-PERP",
                exchange="okx",
                range_pct="0.002",
                bucket_interval="4h",
                target_buckets=100,
                complete_before=97,
                complete_after=100,
                missing_before=3,
                missing_after=0,
                aggregates_written=3,
                status="ok",
            )

    monkeypatch.setattr(worker, "RangeBackfillService", FakeService)
    monkeypatch.setattr(worker.time, "sleep", lambda value: None)

    assert worker.main(["--mode", "live", "--no-once", *_paths(tmp_path)]) == 0
    assert calls == 2
