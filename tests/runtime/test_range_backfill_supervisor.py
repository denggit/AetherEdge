from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from src.market_data.backfill.models import BucketGap
from src.runtime.range_backfill_supervisor import (
    RangeBackfillSupervisor,
    RangeBackfillSupervisorConfig,
    _archive_complete_max_target_end_ms,
)


def test_archive_complete_boundary_uses_okx_utc_plus_8_day() -> None:
    assert _archive_complete_max_target_end_ms(
        1782835199999, exchange="okx"
    ) == 1782748799999
    assert _archive_complete_max_target_end_ms(
        1782835200000, exchange="okx"
    ) == 1782835199999


def test_archive_complete_boundary_keeps_non_okx_utc_behavior() -> None:
    assert _archive_complete_max_target_end_ms(
        1782835200000, exchange="binance"
    ) == 1782777599999


class FakeProcess:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.pid = 4321
        self.terminated = False

    def poll(self):
        return None

    def terminate(self) -> None:
        self.terminated = True

    def wait(self, timeout=None) -> None:
        return None


def test_supervisor_starts_when_history_insufficient(tmp_path, monkeypatch) -> None:
    started: list[FakeProcess] = []

    def fake_popen(*args, **kwargs):
        process = FakeProcess(*args, **kwargs)
        started.append(process)
        return process

    monkeypatch.setattr("subprocess.Popen", fake_popen)
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "range.lock",
            repo_root=Path.cwd(),
            market_db_path=tmp_path / "market.sqlite3",
            checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        )
    )

    assert supervisor.start_if_needed(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        complete_history=6,
        min_periods=100,
    )
    assert started
    command = started[0].args[0]
    assert "tools/range_backfill_worker.py" in command
    assert "--no-once" in command
    assert "--no-save-raw-trades" in command
    assert "--failure-cooldown-seconds" in command
    assert "--archive-not-ready-cooldown-seconds" in command
    assert "--daily-retry-after-utc-hour" in command


def test_supervisor_never_enables_raw_trade_persistence(tmp_path) -> None:
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "range.lock",
            save_raw_trades=True,
        )
    )

    command = supervisor._build_command(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
    )

    assert "--no-save-raw-trades" in command
    assert "--save-raw-trades" not in command


def test_supervisor_does_not_start_when_history_available(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("subprocess.Popen", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not start")))
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(status_path=tmp_path / "status.json", lock_path=tmp_path / "range.lock")
    )

    assert not supervisor.start_if_needed(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        complete_history=100,
        min_periods=100,
    )


def test_supervisor_stop_terminates_process(tmp_path, monkeypatch) -> None:
    process = FakeProcess()
    monkeypatch.setattr("subprocess.Popen", lambda *args, **kwargs: process)
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(status_path=tmp_path / "status.json", lock_path=tmp_path / "range.lock", repo_root=Path.cwd())
    )
    supervisor.start_if_needed(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        complete_history=1,
        min_periods=100,
    )

    supervisor.stop()

    assert process.terminated


@pytest.mark.asyncio
async def test_supervisor_monitor_starts_worker_when_coverage_insufficient(tmp_path, monkeypatch) -> None:
    started: list[FakeProcess] = []

    def fake_popen(*args, **kwargs):
        process = FakeProcess(*args, **kwargs)
        started.append(process)
        return process

    monkeypatch.setattr("subprocess.Popen", fake_popen)
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "range.lock",
            repo_root=Path.cwd(),
            monitor_seconds=1,
            restart_cooldown_seconds=0,
        )
    )
    monkeypatch.setattr(
        "src.runtime.range_backfill_supervisor._archive_complete_max_target_end_ms",
        lambda **kwargs: 1782777599999,
    )
    monkeypatch.setattr(
        supervisor,
        "_scan_coverage",
        lambda **kwargs: type(
            "Coverage",
            (),
            {
                "available": False,
                "required_window_complete_count": 1,
                "required_buckets": 3,
                "required_window_missing_count": 1,
                "required_window_missing_buckets": (BucketGap(1782763200000, 1782777599999),),
                "current_closed_bucket_end_ms": 1782863999999,
            },
        )(),
    )
    stop_event = asyncio.Event()

    supervisor.start_monitor(
        stop_event=stop_event,
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
    )
    await asyncio.sleep(0.05)
    stop_event.set()
    await supervisor.stop_async()

    assert started
    assert "--max-target-end-ms" in started[0].args[0]
    assert "1782777599999" in started[0].args[0]


@pytest.mark.asyncio
async def test_supervisor_current_day_gap_only_writes_status_without_starting(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("subprocess.Popen", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not start")))
    monkeypatch.setattr(
        "src.runtime.range_backfill_supervisor._archive_complete_max_target_end_ms",
        lambda **kwargs: 1782777599999,
    )
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "range.lock",
            repo_root=Path.cwd(),
            monitor_seconds=1,
            restart_cooldown_seconds=0,
        )
    )
    monkeypatch.setattr(
        supervisor,
        "_scan_coverage",
        lambda **kwargs: type(
            "Coverage",
            (),
            {
                "available": False,
                "required_window_complete_count": 99,
                "required_buckets": 100,
                "required_window_missing_count": 1,
                "required_window_missing_buckets": (BucketGap(1782849600000, 1782863999999),),
                "current_closed_bucket_end_ms": 1782863999999,
            },
        )(),
    )
    stop_event = asyncio.Event()

    supervisor.start_monitor(
        stop_event=stop_event,
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
    )
    await asyncio.sleep(0.05)
    stop_event.set()
    await supervisor.stop_async()

    status = supervisor.status_store.read()
    assert status is not None
    assert status["range_speed_available"] is False
    assert status["range_speed_reason"] == "current_day_gap_too_large"


def test_supervisor_persisted_no_progress_retry_blocks_restart(tmp_path, monkeypatch) -> None:
    fixed_now_ms = 1782835200000
    monkeypatch.setattr(
        "src.runtime.range_backfill_supervisor.now_ms",
        lambda: fixed_now_ms,
    )
    monkeypatch.setattr(
        "subprocess.Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("worker must remain in cooldown")
        ),
    )
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "range.lock",
            repo_root=Path.cwd(),
            restart_cooldown_seconds=0,
        )
    )
    supervisor.status_store.patch(
        running=False,
        phase="no_progress",
        range_speed_reason="archive_gap_no_progress",
        next_retry_after_ms=fixed_now_ms + 3_600_000,
    )

    assert not supervisor.start_if_needed(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        complete_history=96,
        min_periods=100,
        max_target_end_ms=1782777599999,
    )


def test_supervisor_allows_restart_after_persisted_retry_deadline(tmp_path, monkeypatch) -> None:
    fixed_now_ms = 1782835200000
    started: list[FakeProcess] = []
    monkeypatch.setattr(
        "src.runtime.range_backfill_supervisor.now_ms",
        lambda: fixed_now_ms,
    )

    def fake_popen(*args, **kwargs):
        process = FakeProcess(*args, **kwargs)
        started.append(process)
        return process

    monkeypatch.setattr("subprocess.Popen", fake_popen)
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "range.lock",
            repo_root=Path.cwd(),
            restart_cooldown_seconds=0,
        )
    )
    supervisor.status_store.patch(
        running=False,
        phase="no_progress",
        range_speed_reason="current_day_archive_not_ready",
        next_retry_after_ms=fixed_now_ms - 1,
    )

    assert supervisor.start_if_needed(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        complete_history=96,
        min_periods=100,
        max_target_end_ms=1782777599999,
    )
    assert started


def test_available_coverage_clears_retry_deadline(tmp_path) -> None:
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "range.lock",
        )
    )
    supervisor.status_store.patch(
        running=False,
        range_speed_reason="archive_gap_no_progress",
        next_retry_after_ms=9999999999999,
    )
    coverage = type(
        "Coverage",
        (),
        {
            "available": True,
            "required_window_complete_count": 100,
            "required_buckets": 100,
            "required_window_missing_count": 0,
            "required_window_missing_buckets": (),
            "current_closed_bucket_end_ms": 1782863999999,
        },
    )()

    reason = supervisor._coverage_reason(
        coverage,
        archive_max_target_end_ms=1782777599999,
    )
    supervisor._write_coverage_status(
        coverage,
        reason=reason,
        archive_max_target_end_ms=1782777599999,
    )

    status = supervisor.status_store.read()
    assert status is not None
    assert status["range_speed_available"] is True
    assert status["range_speed_reason"] == "available"
    assert status["next_retry_after_ms"] is None


def test_missing_worker_pid_is_cleared_and_does_not_block_restart(
    tmp_path,
    monkeypatch,
) -> None:
    fixed_now_ms = 1782835200000
    started: list[FakeProcess] = []
    monkeypatch.setattr(
        "src.runtime.range_backfill_supervisor.now_ms",
        lambda: fixed_now_ms,
    )
    monkeypatch.setattr(
        "src.runtime.range_backfill_supervisor.process_id_exists",
        lambda pid: False,
    )

    def fake_popen(*args, **kwargs):
        process = FakeProcess(*args, **kwargs)
        started.append(process)
        return process

    monkeypatch.setattr("subprocess.Popen", fake_popen)
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "range.lock",
            repo_root=Path.cwd(),
            restart_cooldown_seconds=0,
        )
    )
    supervisor.status_store.patch(
        running=True,
        pid=909230,
        phase="sleeping",
        worker_heartbeat_ms=fixed_now_ms,
        range_speed_reason="archive_gap_backfilling",
    )

    assert supervisor._status_shows_running_worker() is False
    stale = supervisor.status_store.read()
    assert stale is not None
    assert stale["running"] is False
    assert stale["pid"] is None
    assert stale["phase"] == "stale_worker_missing"
    assert stale["range_speed_reason"] == "stale_worker_missing"
    assert stale["exit_code"] == 0
    assert stale["last_error"] == "stale worker pid not found"

    assert supervisor.start_if_needed(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        complete_history=96,
        min_periods=100,
        max_target_end_ms=1782777599999,
    )
    assert started


def test_existing_worker_pid_with_fresh_worker_heartbeat_blocks_restart(
    tmp_path,
    monkeypatch,
) -> None:
    fixed_now_ms = 1782835200000
    monkeypatch.setattr(
        "src.runtime.range_backfill_supervisor.now_ms",
        lambda: fixed_now_ms,
    )
    monkeypatch.setattr(
        "src.runtime.range_backfill_supervisor.process_id_exists",
        lambda pid: True,
    )
    monkeypatch.setattr(
        "subprocess.Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("second worker must not start")
        ),
    )
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "range.lock",
            repo_root=Path.cwd(),
        )
    )
    supervisor.status_store.patch(
        running=True,
        pid=4321,
        worker_heartbeat_ms=fixed_now_ms,
    )

    assert supervisor._status_shows_running_worker() is True
    assert not supervisor.start_if_needed(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        complete_history=96,
        min_periods=100,
    )


def test_existing_worker_supports_legacy_heartbeat_field(tmp_path, monkeypatch) -> None:
    fixed_now_ms = 1782835200000
    monkeypatch.setattr(
        "src.runtime.range_backfill_supervisor.now_ms",
        lambda: fixed_now_ms,
    )
    monkeypatch.setattr(
        "src.runtime.range_backfill_supervisor.process_id_exists",
        lambda pid: True,
    )
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "range.lock",
        )
    )
    supervisor.status_store.patch(
        running=True,
        pid=4321,
        heartbeat_ms=fixed_now_ms,
    )

    assert supervisor._status_shows_running_worker() is True


def test_coverage_status_only_updates_supervisor_heartbeat(tmp_path, monkeypatch) -> None:
    fixed_now_ms = 1782835200000
    monkeypatch.setattr(
        "src.runtime.range_backfill_supervisor.now_ms",
        lambda: fixed_now_ms,
    )
    supervisor = RangeBackfillSupervisor(
        RangeBackfillSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "range.lock",
        )
    )
    supervisor.status_store.patch(
        running=True,
        pid=909230,
        worker_heartbeat_ms=111,
        heartbeat_ms=222,
    )
    coverage = type(
        "Coverage",
        (),
        {
            "required_window_complete_count": 96,
            "required_window_missing_count": 4,
            "required_buckets": 100,
            "current_closed_bucket_end_ms": 1782863999999,
        },
    )()

    supervisor._write_coverage_status(
        coverage,
        reason="archive_gap_backfilling",
        archive_max_target_end_ms=1782777599999,
    )

    status = supervisor.status_store.read()
    assert status is not None
    assert status["supervisor_heartbeat_ms"] == fixed_now_ms
    assert status["worker_heartbeat_ms"] == 111
    assert status["heartbeat_ms"] == 222
