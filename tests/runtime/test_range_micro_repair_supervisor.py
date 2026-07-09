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


# ── recoverable failed job retry ───────────────────────────────────────

from src.market_data.range_checkpoint import (
    MICRO_REPAIR_FAILED,
    MICRO_REPAIR_PARTIAL,
    MICRO_REPAIR_PENDING,
    MIN_VALID_COMPLETED_AGGREGATE_MS,
    RETRY_MARKER,
    RangeMicroRepairJob,
    SqliteRangeCheckpointStore,
    failure_reason_is_recoverable,
)
from src.market_data.range_repair import (
    JOURNAL_FINALIZED,
    RangeRepairJournalState,
    SqliteRangeRepairJournalStore,
)
from src.market_data.models import RangeBarAggregate
from decimal import Decimal


def _seed_failed_job(
    checkpoint_db,
    *,
    last_error: str,
    symbol: str = "ETH-USDT-PERP",
    bucket_start_ms: int | None = None,
) -> SqliteRangeCheckpointStore:
    store = SqliteRangeCheckpointStore(checkpoint_db)
    start = bucket_start_ms or (MIN_VALID_COMPLETED_AGGREGATE_MS + 500_000)
    job = RangeMicroRepairJob(
        exchange="okx",
        symbol=symbol,
        range_pct="0.002",
        bucket_start_ms=start,
        bucket_end_ms=start + 9_999,
        checkpoint_last_trade_id="cp10",
        checkpoint_last_trade_ts_ms=start + 100,
        builder_state={"version": 1},
        coverage_status="RECOVERED_DEGRADED_MINOR",
        missing_gap_ms=500,
        status=MICRO_REPAIR_FAILED,
        created_at_ms=start,
        updated_at_ms=start + 9_999,
        last_error=last_error,
    )
    store.enqueue_micro_repair(job)
    # Seed a non-COMPLETE aggregate so the COMPLETE guard doesn't block
    store.save_completed_aggregate(
        exchange="okx",
        aggregate=RangeBarAggregate(
            symbol=symbol,
            range_pct=Decimal("0.002"),
            bucket_start_ms=start,
            bucket_end_ms=start + 9_999,
            bar_count=1,
            first_open=Decimal("100"),
            last_close=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("100"),
            buy_notional_sum=Decimal("1"),
            sell_notional_sum=Decimal("0"),
            delta_notional_sum=Decimal("1"),
            notional_sum=Decimal("1"),
        ),
        coverage_status="RECOVERED_DEGRADED_MINOR",
        missing_gap_ms=500,
        completed_at_ms=start + 9_999,
    )
    return store


def _seed_journal_for_failed_job(
    journal_db,
    *,
    bucket_start_ms: int | None = None,
    finalized: bool = True,
    dropped_trades: int = 0,
    writer_failures: int = 0,
    journal_status: str = JOURNAL_FINALIZED,
) -> SqliteRangeRepairJournalStore:
    journal = SqliteRangeRepairJournalStore(journal_db)
    start = bucket_start_ms or (MIN_VALID_COMPLETED_AGGREGATE_MS + 500_000)
    journal.open_bucket(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=start,
        bucket_end_ms=start + 9_999,
        checkpoint_last_trade_ts_ms=start + 100,
        checkpoint_last_trade_id="cp10",
        updated_at_ms=start,
    )
    journal.record_first_live_trade(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=start,
        trade_time_ms=start + 200,
        trade_id="live1",
        recorded_at_ms=start + 200,
    )
    if dropped_trades > 0 or writer_failures > 0:
        from src.market_data.range_repair import (
            JOURNAL_INVALID_DROPPED_TRADE,
            JOURNAL_INVALID_WRITER_ERROR,
        )
        inv_status = (
            JOURNAL_INVALID_DROPPED_TRADE
            if dropped_trades > 0
            else JOURNAL_INVALID_WRITER_ERROR
        )
        journal.invalidate(
            exchange="okx",
            symbol="ETH-USDT-PERP",
            range_pct="0.002",
            bucket_start_ms=start,
            status=inv_status,
            last_error="test invalid",
            dropped_trades=dropped_trades,
            writer_failures=writer_failures,
        )
    if finalized:
        journal.finalize(
            exchange="okx",
            symbol="ETH-USDT-PERP",
            range_pct="0.002",
            bucket_start_ms=start,
            finalized_at_ms=start + 10_000,
        )
    return journal


def test_recoverable_failed_job_is_retried_by_supervisor(
    tmp_path, monkeypatch
) -> None:
    """Old pagination-limit FAILED job → marked PENDING → worker launched."""
    processes = []

    def fake_popen(command, **kwargs):
        p = _Process(command, **kwargs)
        processes.append(p)
        return p

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    journal_db = tmp_path / "journal.sqlite3"
    _seed_failed_job(
        checkpoint_db,
        last_error="ExchangeApiError:OKX history-trades pagination limit reached before older_trade_id coverage",
    )
    _seed_journal_for_failed_job(journal_db)

    supervisor = RangeMicroRepairSupervisor(
        RangeMicroRepairSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "repair.lock",
            checkpoint_db_path=checkpoint_db,
            market_db_path=tmp_path / "market.sqlite3",
            journal_db_path=journal_db,
            repo_root=tmp_path,
        )
    )

    supervisor._retry_recoverable_failed_jobs()

    assert len(processes) == 1, "worker should have been launched"
    command = processes[0].command
    assert "tools/range_micro_repair_worker.py" in command

    # Verify job was marked PENDING with retry marker
    ck_store = SqliteRangeCheckpointStore(checkpoint_db)
    job = ck_store.load_micro_repair_job(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=MIN_VALID_COMPLETED_AGGREGATE_MS + 500_000,
    )
    assert job is not None
    assert job.status == MICRO_REPAIR_PENDING
    assert RETRY_MARKER in (job.last_error or "")


def test_non_pagination_failed_job_not_retried(
    tmp_path, monkeypatch
) -> None:
    """Non-recoverable error → _retry_recoverable_failed_jobs does nothing."""
    processes = []

    def fake_popen(command, **kwargs):
        p = _Process(command, **kwargs)
        processes.append(p)
        return p

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    journal_db = tmp_path / "journal.sqlite3"
    _seed_failed_job(
        checkpoint_db,
        last_error="RuntimeError:REST unavailable",
    )
    _seed_journal_for_failed_job(journal_db)

    supervisor = RangeMicroRepairSupervisor(
        RangeMicroRepairSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "repair.lock",
            checkpoint_db_path=checkpoint_db,
            market_db_path=tmp_path / "market.sqlite3",
            journal_db_path=journal_db,
            repo_root=tmp_path,
        )
    )

    supervisor._retry_recoverable_failed_jobs()

    assert len(processes) == 0, (
        "non-recoverable failure must not launch worker"
    )


def test_already_complete_bucket_not_retried(
    tmp_path, monkeypatch
) -> None:
    """Recoverable error but aggregate already COMPLETE → skip."""
    processes = []

    def fake_popen(command, **kwargs):
        p = _Process(command, **kwargs)
        processes.append(p)
        return p

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    journal_db = tmp_path / "journal.sqlite3"
    start = MIN_VALID_COMPLETED_AGGREGATE_MS + 600_000
    _seed_failed_job(
        checkpoint_db,
        bucket_start_ms=start,
        last_error="ExchangeApiError:OKX history-trades pagination limit reached",
    )
    _seed_journal_for_failed_job(journal_db, bucket_start_ms=start)

    # Overwrite aggregate to COMPLETE
    ck_store = SqliteRangeCheckpointStore(checkpoint_db)
    ck_store.save_completed_aggregate(
        exchange="okx",
        aggregate=RangeBarAggregate(
            symbol="ETH-USDT-PERP",
            range_pct=Decimal("0.002"),
            bucket_start_ms=start,
            bucket_end_ms=start + 9_999,
            bar_count=1,
            first_open=Decimal("100"),
            last_close=Decimal("100"),
            high=Decimal("100"),
            low=Decimal("100"),
            buy_notional_sum=Decimal("1"),
            sell_notional_sum=Decimal("0"),
            delta_notional_sum=Decimal("1"),
            notional_sum=Decimal("1"),
        ),
        coverage_status="COMPLETE",
        missing_gap_ms=0,
        completed_at_ms=start + 9_999,
    )

    supervisor = RangeMicroRepairSupervisor(
        RangeMicroRepairSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "repair.lock",
            checkpoint_db_path=checkpoint_db,
            market_db_path=tmp_path / "market.sqlite3",
            journal_db_path=journal_db,
            repo_root=tmp_path,
        )
    )

    supervisor._retry_recoverable_failed_jobs()

    assert len(processes) == 0, "COMPLETE bucket must not be retried"


def test_journal_not_finalized_skips_retry(
    tmp_path, monkeypatch
) -> None:
    """Journal not finalized → skip retry."""
    processes = []

    def fake_popen(command, **kwargs):
        p = _Process(command, **kwargs)
        processes.append(p)
        return p

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    journal_db = tmp_path / "journal.sqlite3"
    start = MIN_VALID_COMPLETED_AGGREGATE_MS + 700_000
    _seed_failed_job(
        checkpoint_db,
        bucket_start_ms=start,
        last_error="ExchangeApiError:REST pagination exhausted",
    )
    # Journal opened but NOT finalized
    journal = SqliteRangeRepairJournalStore(journal_db)
    journal.open_bucket(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=start,
        bucket_end_ms=start + 9_999,
        checkpoint_last_trade_ts_ms=start + 100,
        checkpoint_last_trade_id="cp10",
        updated_at_ms=start,
    )
    journal.record_first_live_trade(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=start,
        trade_time_ms=start + 200,
        trade_id="live1",
        recorded_at_ms=start + 200,
    )
    # NOT finalized

    supervisor = RangeMicroRepairSupervisor(
        RangeMicroRepairSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "repair.lock",
            checkpoint_db_path=checkpoint_db,
            market_db_path=tmp_path / "market.sqlite3",
            journal_db_path=journal_db,
            repo_root=tmp_path,
        )
    )

    supervisor._retry_recoverable_failed_jobs()

    assert len(processes) == 0, (
        "non-finalized journal must not trigger retry"
    )


def test_journal_not_valid_for_repair_skips_retry(
    tmp_path, monkeypatch
) -> None:
    """Journal finalized but invalid (dropped trades) → skip retry."""
    processes = []

    def fake_popen(command, **kwargs):
        p = _Process(command, **kwargs)
        processes.append(p)
        return p

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    journal_db = tmp_path / "journal.sqlite3"
    start = MIN_VALID_COMPLETED_AGGREGATE_MS + 800_000
    _seed_failed_job(
        checkpoint_db,
        bucket_start_ms=start,
        last_error="ExchangeApiError:OKX history-trades pagination limit reached",
    )
    _seed_journal_for_failed_job(
        journal_db,
        bucket_start_ms=start,
        dropped_trades=1,
    )

    supervisor = RangeMicroRepairSupervisor(
        RangeMicroRepairSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "repair.lock",
            checkpoint_db_path=checkpoint_db,
            market_db_path=tmp_path / "market.sqlite3",
            journal_db_path=journal_db,
            repo_root=tmp_path,
        )
    )

    supervisor._retry_recoverable_failed_jobs()

    assert len(processes) == 0, (
        "invalid journal must not trigger retry"
    )


def test_supervisor_suppresses_notification_for_first_recoverable_failure(
    tmp_path,
) -> None:
    """Recoverable failure WITHOUT retry marker → no callback (first attempt)."""
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
            "failure_reason": "ExchangeApiError:OKX history-trades pagination limit reached before older_trade_id coverage",
        }
    )

    supervisor._refresh_finished_process()

    assert failures == [], (
        "first recoverable failure (no retry marker) must not trigger callback"
    )
    assert supervisor.process is None


def test_supervisor_still_notifies_for_non_recoverable_failure(
    tmp_path,
) -> None:
    """Non-recoverable failure still triggers callback (existing behavior)."""
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
            "failure_reason": "RuntimeError:database connection lost",
        }
    )

    supervisor._refresh_finished_process()

    assert failures == ["RuntimeError:database connection lost"], (
        "non-recoverable failure must still trigger on_failure callback"
    )
    assert supervisor.process is None


def test_supervisor_notifies_for_recoverable_failure_with_retry_marker(
    tmp_path,
) -> None:
    """Recoverable failure WITH retry marker → callback fires (already retried)."""
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
            "failure_reason": (
                "[micro_repair_retried] ExchangeApiError:OKX history-trades "
                "pagination limit reached before older_trade_id coverage"
            ),
        }
    )

    supervisor._refresh_finished_process()

    assert len(failures) == 1, (
        "recoverable failure WITH retry marker must trigger on_failure callback"
    )
    assert "pagination limit" in failures[0]
    assert supervisor.process is None


def test_supervisor_suppresses_notification_for_partial_status(
    tmp_path,
) -> None:
    """PARTIAL repair status → no failure callback (resumable, not failed)."""
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
    process.returncode = 0
    supervisor.process = process
    supervisor.status_store.write(
        {
            "running": False,
            "repair_status": MICRO_REPAIR_PARTIAL,
            "failure_reason": None,
        }
    )

    supervisor._refresh_finished_process()

    assert failures == [], (
        "PARTIAL status must not trigger on_failure callback"
    )
    assert supervisor.process is None


def test_supervisor_suppresses_notification_for_pending_status(
    tmp_path,
) -> None:
    """PENDING repair status → no failure callback (resumable, not failed)."""
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
    process.returncode = 0
    supervisor.process = process
    supervisor.status_store.write(
        {
            "running": False,
            "repair_status": MICRO_REPAIR_PENDING,
            "failure_reason": None,
        }
    )

    supervisor._refresh_finished_process()

    assert failures == [], (
        "PENDING status must not trigger on_failure callback"
    )
    assert supervisor.process is None


def test_recoverable_failed_job_not_retried_twice_in_same_session(
    tmp_path, monkeypatch
) -> None:
    """Second call to _retry_recoverable_failed_jobs is a no-op for same job."""
    processes = []

    def fake_popen(command, **kwargs):
        p = _Process(command, **kwargs)
        processes.append(p)
        return p

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    journal_db = tmp_path / "journal.sqlite3"
    _seed_failed_job(
        checkpoint_db,
        last_error="ExchangeApiError:OKX history-trades pagination limit reached",
    )
    _seed_journal_for_failed_job(journal_db)

    supervisor = RangeMicroRepairSupervisor(
        RangeMicroRepairSupervisorConfig(
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "repair.lock",
            checkpoint_db_path=checkpoint_db,
            market_db_path=tmp_path / "market.sqlite3",
            journal_db_path=journal_db,
            repo_root=tmp_path,
        )
    )

    # First call: worker launched
    supervisor._retry_recoverable_failed_jobs()
    assert len(processes) == 1

    # Simulate worker running (so self.running is True)
    # Second call: should be no-op
    supervisor._retry_recoverable_failed_jobs()
    assert len(processes) == 1, "should not launch while worker is running"

    # "Finish" the worker and reset the job back to FAILED for re-test
    supervisor.process = None
    ck_store = SqliteRangeCheckpointStore(checkpoint_db)
    ck_store.mark_micro_repair_status(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=MIN_VALID_COMPLETED_AGGREGATE_MS + 500_000,
        status=MICRO_REPAIR_FAILED,
        updated_at_ms=MIN_VALID_COMPLETED_AGGREGATE_MS + 600_000,
        last_error="ExchangeApiError:OKX history-trades pagination limit reached",
    )

    # Third call: should NOT launch because _retried_job_keys already has it
    supervisor._retry_recoverable_failed_jobs()
    assert len(processes) == 1, (
        "same-session re-retry must be suppressed"
    )
