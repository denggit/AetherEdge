from __future__ import annotations

import asyncio

import pytest

from src.market_data.range_checkpoint import MICRO_REPAIR_FAILED
from src.runtime.range_micro_repair_supervisor import (
    RangeMicroRepairSupervisor,
    RangeMicroRepairSupervisorConfig,
)


class _Process:
    def __init__(self, command, **kwargs) -> None:
        self.command = command
        self.kwargs = kwargs
        self.pid = 1234
        self.returncode = None

    def poll(self):
        return self.returncode


def _start(supervisor: RangeMicroRepairSupervisor) -> bool:
    return supervisor.start_startup_recovery(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=1_780_000_000_000,
        bucket_end_ms=1_780_000_009_999,
        coverage_status="RECOVERED_DEGRADED_MINOR",
        missing_gap_ms=500,
    )


def test_supervisor_builds_independent_worker_command(tmp_path, monkeypatch) -> None:
    processes = []

    def fake_popen(command, **kwargs):
        process = _Process(command, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr("subprocess.Popen", fake_popen)
    supervisor = RangeMicroRepairSupervisor(
        RangeMicroRepairSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "repair.lock",
            checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
            market_db_path=tmp_path / "market.sqlite3",
            journal_db_path=tmp_path / "journal.sqlite3",
            repo_root=tmp_path,
        )
    )

    assert _start(supervisor) is True
    assert processes
    command = processes[0].command
    assert "tools/range_micro_repair_worker.py" in command
    assert "--bucket-start-ms" in command
    assert "1780000000000" in command
    assert "--bucket-end-ms" in command
    assert "--journal-db" in command
    assert "--max-gap-ms" in command
    assert "--missing-bucket-grace-seconds" in command
    assert supervisor.status_store.read() is None


def test_supervisor_reads_failed_status_and_emits_warning_callback(
    tmp_path,
) -> None:
    failures = []
    supervisor = RangeMicroRepairSupervisor(
        RangeMicroRepairSupervisorConfig(
            status_path=tmp_path / "status.json",
            checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
            market_db_path=tmp_path / "market.sqlite3",
            repo_root=tmp_path,
        ),
        on_failure=failures.append,
    )
    process = _Process([])
    process.returncode = 1
    supervisor.process = process
    supervisor.status_store.write(
        {
            "running": False,
            "repair_status": MICRO_REPAIR_FAILED,
            "failure_reason": "REST timeout",
        }
    )

    supervisor._refresh_finished_process()

    assert failures == ["REST timeout"]
    assert supervisor.process is None


@pytest.mark.asyncio
async def test_monitor_only_reads_worker_status_and_never_launches_repair(
    tmp_path, monkeypatch
) -> None:
    processes = []
    monkeypatch.setattr(
        "subprocess.Popen",
        lambda command, **kwargs: processes.append(_Process(command, **kwargs))
        or processes[-1],
    )
    supervisor = RangeMicroRepairSupervisor(
        RangeMicroRepairSupervisorConfig(
            monitor_seconds=1,
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "repair.lock",
            checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
            market_db_path=tmp_path / "market.sqlite3",
            repo_root=tmp_path,
        )
    )
    stop = asyncio.Event()
    supervisor.start_monitor(stop_event=stop)
    await asyncio.sleep(0.05)
    assert processes == []

    stop.set()
    await supervisor.stop_async()
