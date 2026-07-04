from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace

import pytest

from src.market_data.backfill.coordinator import (
    LF_RANGE_BACKFILL_PRIORITY,
    MF_FEATURE_BACKFILL_PRIORITY,
    RawTradeBackfillCoordinator,
)
from tools import range_backfill_worker


def test_mf_priority_higher_than_lf() -> None:
    assert MF_FEATURE_BACKFILL_PRIORITY > LF_RANGE_BACKFILL_PRIORITY


def test_acquire_and_release(tmp_path: Path) -> None:
    lock_path = tmp_path / "global.lock"
    status_path = tmp_path / "global_status.json"

    coordinator = RawTradeBackfillCoordinator(lock_path=lock_path, status_path=status_path)
    assert coordinator.try_acquire(
        owner="mf_test", priority=MF_FEATURE_BACKFILL_PRIORITY, symbol="ETH-USDT",
    ) is True
    assert coordinator.is_held is True
    assert lock_path.exists()
    assert status_path.exists()

    coordinator.release()
    assert coordinator.is_held is False


def test_mf_blocks_lf(tmp_path: Path) -> None:
    lock_path = tmp_path / "global.lock"
    status_path = tmp_path / "global_status.json"

    mf = RawTradeBackfillCoordinator(lock_path=lock_path, status_path=status_path)
    assert mf.try_acquire(owner="mf", priority=MF_FEATURE_BACKFILL_PRIORITY, symbol="ETH") is True

    lf = RawTradeBackfillCoordinator(lock_path=lock_path, status_path=status_path)
    assert lf.try_acquire(owner="lf", priority=LF_RANGE_BACKFILL_PRIORITY, symbol="ETH") is False

    mf.release()
    assert lf.try_acquire(owner="lf", priority=LF_RANGE_BACKFILL_PRIORITY, symbol="ETH") is True
    lf.release()


def test_higher_priority_blocks_lower(tmp_path: Path) -> None:
    lock_path = tmp_path / "global.lock"
    status_path = tmp_path / "global_status.json"

    lf = RawTradeBackfillCoordinator(lock_path=lock_path, status_path=status_path)
    assert lf.try_acquire(owner="lf", priority=LF_RANGE_BACKFILL_PRIORITY, symbol="ETH") is True

    mf = RawTradeBackfillCoordinator(lock_path=lock_path, status_path=status_path)
    assert mf.try_acquire(owner="mf", priority=MF_FEATURE_BACKFILL_PRIORITY, symbol="ETH") is False

    lf.release()


def test_stale_lock_is_evicted(tmp_path: Path) -> None:
    lock_path = tmp_path / "global.lock"
    status_path = tmp_path / "global_status.json"

    payload = {
        "pid": 99999,
        "owner": "stale_worker",
        "priority": LF_RANGE_BACKFILL_PRIORITY,
        "symbol": "ETH",
        "raw_days": 1,
        "started_at_ms": int(time.time() * 1000) - 3600_000,
    }
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(json.dumps(payload))

    status_payload = {
        "version": 1,
        "pid": 99999,
        "owner": "stale_worker",
        "priority": LF_RANGE_BACKFILL_PRIORITY,
        "running": False,
        "worker_heartbeat_ms": int(time.time() * 1000) - 3600_000,
    }
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(status_payload))

    mf = RawTradeBackfillCoordinator(
        lock_path=lock_path, status_path=status_path, stale_after_seconds=1,
    )
    assert mf.try_acquire(owner="mf", priority=MF_FEATURE_BACKFILL_PRIORITY, symbol="ETH") is True
    mf.release()


def test_status_contains_required_fields(tmp_path: Path) -> None:
    lock_path = tmp_path / "global.lock"
    status_path = tmp_path / "global_status.json"

    coordinator = RawTradeBackfillCoordinator(lock_path=lock_path, status_path=status_path)
    coordinator.try_acquire(
        owner="test", priority=MF_FEATURE_BACKFILL_PRIORITY, symbol="ETH", raw_days=3,
    )

    status = coordinator.status()
    assert status is not None
    for field in ("pid", "owner", "priority", "symbol", "raw_days"):
        assert field in status
    assert status["priority"] == MF_FEATURE_BACKFILL_PRIORITY

    coordinator.release()


def test_context_manager_releases_lock(tmp_path: Path) -> None:
    lock_path = tmp_path / "global.lock"
    status_path = tmp_path / "global_status.json"

    with RawTradeBackfillCoordinator(lock_path=lock_path, status_path=status_path) as coordinator:
        coordinator.try_acquire(owner="ctx_test", priority=MF_FEATURE_BACKFILL_PRIORITY, symbol="ETH")
        assert lock_path.exists()


def test_heartbeat_updates_status(tmp_path: Path) -> None:
    lock_path = tmp_path / "global.lock"
    status_path = tmp_path / "global_status.json"

    coordinator = RawTradeBackfillCoordinator(lock_path=lock_path, status_path=status_path)
    coordinator.try_acquire(owner="hb_test", priority=MF_FEATURE_BACKFILL_PRIORITY, symbol="ETH")

    initial_status = coordinator.status()
    assert initial_status is not None

    time.sleep(0.02)
    coordinator.heartbeat()

    updated = coordinator.status()
    assert updated is not None
    assert updated["worker_heartbeat_ms"] >= initial_status["worker_heartbeat_ms"]

    coordinator.release()


def test_range_worker_acquires_and_releases_global_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    global_lock = tmp_path / "global.lock"
    observed = {"held_during_run": False}

    class FakeService:
        def __init__(self, request) -> None:
            self.request = request

        def run_once(self, **kwargs):
            observed["held_during_run"] = global_lock.exists()
            return SimpleNamespace(
                status="ok",
                complete_after=1,
                missing_after=0,
                aggregates_written=1,
                target_bucket_start_ms=1,
                target_bucket_end_ms=2,
                selected_archive_dates=(),
                per_file_min_trade_time_ms={},
                per_file_max_trade_time_ms={},
                target_trade_count=0,
                candidate_range_bars=0,
                candidate_aggregates=0,
                filtered_reason_if_zero=None,
                last_error=None,
                missing_raw_days=(),
                failed_downloads=(),
            )

    monkeypatch.setattr(range_backfill_worker, "RangeBackfillService", FakeService)
    exit_code = range_backfill_worker.main(
        [
            "--once",
            "--mode",
            "prebuild",
            "--symbol",
            "ETH-USDT-PERP",
            "--status-path",
            str(tmp_path / "range_status.json"),
            "--lock-path",
            str(tmp_path / "range.lock"),
            "--global-lock-path",
            str(global_lock),
            "--global-status-path",
            str(tmp_path / "global_status.json"),
            "--market-db",
            str(tmp_path / "market.sqlite3"),
            "--checkpoint-db",
            str(tmp_path / "checkpoint.sqlite3"),
            "--raw-root",
            str(tmp_path / "raw"),
        ]
    )

    assert exit_code == 0
    assert observed["held_during_run"] is True
    assert not global_lock.exists()


def test_simultaneous_acquire_has_exactly_one_winner(tmp_path: Path) -> None:
    lock_path = tmp_path / "global.lock"
    status_path = tmp_path / "global_status.json"
    coordinators = [
        RawTradeBackfillCoordinator(
            lock_path=lock_path, status_path=status_path
        )
        for _ in range(2)
    ]
    barrier = Barrier(2)

    def acquire(index: int) -> bool:
        barrier.wait()
        return coordinators[index].try_acquire(
            owner=f"worker_{index}",
            priority=MF_FEATURE_BACKFILL_PRIORITY,
            symbol="ETH",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(acquire, range(2)))

    try:
        assert sum(results) == 1
    finally:
        for coordinator in coordinators:
            coordinator.release()
