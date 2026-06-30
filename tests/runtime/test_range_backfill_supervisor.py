from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from src.market_data.backfill.models import BucketGap
from src.runtime.range_backfill_supervisor import RangeBackfillSupervisor, RangeBackfillSupervisorConfig


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
        lambda: 1782777599999,
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
        lambda: 1782777599999,
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
