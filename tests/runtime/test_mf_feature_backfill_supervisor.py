from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path

from src.market_data.models import FixedTimeTradeBar
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from src.runtime.mf_feature_backfill_supervisor import MfFeatureBackfillSupervisor

_MINUTE = 60_000


def _now_aligned() -> int:
    now = int(time.time() * 1000)
    return now - (now % _MINUTE)


def _make_bar(open_time_ms: int, close_time_ms: int | None = None) -> FixedTimeTradeBar:
    if close_time_ms is None:
        close_time_ms = open_time_ms + _MINUTE - 1
    return FixedTimeTradeBar(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        timeframe="1m",
        open_time_ms=open_time_ms,
        close_time_ms=close_time_ms,
        available_time_ms=close_time_ms,
        open=Decimal("1000"),
        high=Decimal("1005"),
        low=Decimal("995"),
        close=Decimal("1002"),
        volume=Decimal("10"),
        buy_volume=Decimal("6"),
        sell_volume=Decimal("4"),
        buy_notional=Decimal("6000"),
        sell_notional=Decimal("4000"),
        delta_volume=Decimal("2"),
        delta_notional=Decimal("2000"),
        abs_delta_notional=Decimal("2000"),
        trade_count=5,
    )


def test_supervisor_scan_coverage_returns_dict(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite3"
    status_path = tmp_path / "status.json"
    lock_path = tmp_path / "lock.lock"
    global_lock = tmp_path / "global.lock"
    global_status = tmp_path / "global_status.json"
    log_path = tmp_path / "worker.out"

    store = SqliteTradeFeatureStore(path=db_path)
    base = _now_aligned() - 3600_000
    store.upsert_many([
        _make_bar(base + i * 60_000)
        for i in range(5)
    ])

    supervisor = MfFeatureBackfillSupervisor(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        market_db=str(db_path),
        status_path=str(status_path),
        lock_path=str(lock_path),
        global_lock_path=str(global_lock),
        global_status_path=str(global_status),
        worker_log_path=str(log_path),
        required_minutes=5,
    )
    coverage = supervisor.scan_coverage()
    assert isinstance(coverage, dict)
    assert "coverage_ready" in coverage
    assert "mf_signal_ready" in coverage
    assert coverage["mf_signal_ready"] is False


def test_supervisor_check_and_launch_when_coverage_complete(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite3"
    status_path = tmp_path / "status.json"
    lock_path = tmp_path / "lock.lock"
    global_lock = tmp_path / "global.lock"
    global_status = tmp_path / "global_status.json"
    log_path = tmp_path / "worker.out"

    store = SqliteTradeFeatureStore(path=db_path)
    base = _now_aligned() - 3600_000
    store.upsert_many([
        _make_bar(base + i * 60_000)
        for i in range(10)
    ])

    supervisor = MfFeatureBackfillSupervisor(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        market_db=str(db_path),
        status_path=str(status_path),
        lock_path=str(lock_path),
        global_lock_path=str(global_lock),
        global_status_path=str(global_status),
        worker_log_path=str(log_path),
        required_minutes=10,
    )
    result = supervisor.check_and_launch()
    assert result["action"] == "none"
    assert result["reason"] == "coverage_complete"


def test_supervisor_worker_running_detection(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite3"
    status_path = tmp_path / "status.json"
    lock_path = tmp_path / "lock.lock"
    global_lock = tmp_path / "global.lock"
    global_status = tmp_path / "global_status.json"
    log_path = tmp_path / "worker.out"

    status_payload = {
        "version": 1,
        "pid": 99999,
        "running": True,
        "worker_heartbeat_ms": int(time.time() * 1000),
    }
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(status_payload))

    supervisor = MfFeatureBackfillSupervisor(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        market_db=str(db_path),
        status_path=str(status_path),
        lock_path=str(lock_path),
        global_lock_path=str(global_lock),
        global_status_path=str(global_status),
        worker_log_path=str(log_path),
    )
    # The PID 99999 doesn't exist, so worker should be detected as not running
    assert supervisor._worker_running() is False


def test_supervisor_reports_mf_signal_ready_false_always(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite3"
    status_path = tmp_path / "status.json"
    lock_path = tmp_path / "lock.lock"
    global_lock = tmp_path / "global.lock"
    global_status = tmp_path / "global_status.json"
    log_path = tmp_path / "worker.out"

    supervisor = MfFeatureBackfillSupervisor(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        market_db=str(db_path),
        status_path=str(status_path),
        lock_path=str(lock_path),
        global_lock_path=str(global_lock),
        global_status_path=str(global_status),
        worker_log_path=str(log_path),
    )
    coverage = supervisor.scan_coverage()
    assert coverage["mf_signal_ready"] is False
