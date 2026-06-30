from __future__ import annotations

from src.market_data.backfill.lock import RangeBackfillLock
from src.market_data.backfill.status_store import RangeBackfillStatusStore


def test_lock_is_mutually_exclusive_and_released(tmp_path) -> None:
    lock_path = tmp_path / "state" / "range.lock"
    first = RangeBackfillLock(lock_path, status_path=None)
    second = RangeBackfillLock(lock_path, status_path=None)

    assert first.acquire(mode="test")
    assert not second.acquire(mode="test")
    first.release()
    assert second.acquire(mode="test")


def test_lock_force_can_replace_stale_status(tmp_path) -> None:
    lock_path = tmp_path / "range.lock"
    status_path = tmp_path / "status.json"
    RangeBackfillStatusStore(status_path).write({"running": False, "heartbeat_ms": 1})
    lock_path.write_text("stale", encoding="utf-8")

    lock = RangeBackfillLock(lock_path, status_path=status_path, stale_after_seconds=1)

    assert lock.acquire(mode="test", force=True)
