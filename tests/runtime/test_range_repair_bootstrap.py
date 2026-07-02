from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from src.market_data.models import RangeCoverageStatus
from src.market_data.range_checkpoint import (
    RangeBuilderCheckpoint,
    RangeCheckpointRecovery,
)
from src.runtime.range_repair_bootstrap import (
    RangeRepairBootstrapService,
)


BUCKET_START = 1_780_000_000_000
BUCKET_END = BUCKET_START + 14_400_000 - 1
CHECKPOINT_TS = BUCKET_START + 1_000


class _CheckpointStore:
    def __init__(self) -> None:
        self.jobs = []

    def enqueue_micro_repair(self, job) -> None:
        self.jobs.append(job)


class _JournalStore:
    pass


class _Writer:
    def __init__(self, store, **kwargs) -> None:
        self.store = store
        self.kwargs = kwargs
        self.started = 0
        self.open_calls = []

    def start(self) -> None:
        self.started += 1

    def submit_open(self, **kwargs) -> bool:
        self.open_calls.append(kwargs)
        return True


class _Supervisor:
    def __init__(self, config, *, on_failure=None) -> None:
        self.config = config
        self.on_failure = on_failure
        self.launches = []

    def start_startup_recovery(self, **kwargs) -> bool:
        self.launches.append(kwargs)
        return True


def _config(tmp_path):
    return SimpleNamespace(
        range_micro_repair_enabled=True,
        range_repair_journal_enabled=True,
        range_repair_journal_db=str(tmp_path / "journal.sqlite3"),
        range_repair_journal_writer_max_pending=20_000,
        range_repair_journal_flush_interval_ms=500,
        range_repair_journal_batch_size=1_000,
        range_repair_journal_retention_hours=12,
        range_micro_repair_monitor_seconds=30.0,
        range_micro_repair_status_path=str(tmp_path / "status.json"),
        range_micro_repair_lock_path=str(tmp_path / "repair.lock"),
        range_checkpoint_db_path=str(tmp_path / "checkpoint.sqlite3"),
        market_data_db_path=str(tmp_path / "market.sqlite3"),
        range_micro_repair_max_gap_ms=600_000,
        range_micro_repair_page_limit=100,
        range_micro_repair_max_pages=20,
        range_micro_repair_max_seconds=30.0,
        range_micro_repair_missing_bucket_grace_seconds=120,
    )


def _checkpoint() -> RangeBuilderCheckpoint:
    return RangeBuilderCheckpoint(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_start_ms=BUCKET_START,
        bucket_end_ms=BUCKET_END,
        last_trade_id="checkpoint-trade",
        last_trade_ts_ms=CHECKPOINT_TS,
        last_ws_recv_ts_ms=CHECKPOINT_TS,
        range_bar_count=0,
        aggregate={},
        builder_state={"version": 1},
        coverage_status=RangeCoverageStatus.COMPLETE.value,
        missing_gap_ms=0,
        checkpoint_updated_at_ms=CHECKPOINT_TS,
    )


def _service(tmp_path, checkpoint_store, clock_ms):
    return RangeRepairBootstrapService(
        runtime_config=_config(tmp_path),
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        closed_bar_interval_ms=14_400_000,
        checkpoint_store=checkpoint_store,
        emit_alert=lambda alert: None,
        journal_store_factory=lambda path: _JournalStore(),
        journal_writer_factory=_Writer,
        micro_repair_supervisor_factory=_Supervisor,
        clock_ms=clock_ms,
        repo_root=tmp_path,
    )


@pytest.mark.asyncio
async def test_bootstrap_does_not_start_without_degraded_bucket(
    tmp_path,
) -> None:
    checkpoint_store = _CheckpointStore()
    service = _service(tmp_path, checkpoint_store, lambda: 1)
    recovery = RangeCheckpointRecovery(
        coverage_status=RangeCoverageStatus.COMPLETE.value,
        checkpoint=None,
        checkpoint_age_ms=None,
        missing_gap_ms=0,
        recovered_from_checkpoint=False,
    )

    result = service.start_if_needed(
        recovery,
        initial_bucket_ms=BUCKET_START,
    )

    assert result.journal_store is None
    assert result.journal_writer is None
    assert result.micro_repair_supervisor is None
    assert result.micro_repair_started is False
    assert checkpoint_store.jobs == []


@pytest.mark.asyncio
async def test_bootstrap_starts_journal_and_supervisor_with_runtime_config(
    tmp_path,
) -> None:
    timestamps = iter([1_001, 1_002])
    checkpoint_store = _CheckpointStore()
    service = _service(
        tmp_path,
        checkpoint_store,
        lambda: next(timestamps),
    )
    checkpoint = _checkpoint()
    recovery = RangeCheckpointRecovery(
        coverage_status=(
            RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value
        ),
        checkpoint=checkpoint,
        checkpoint_age_ms=100,
        missing_gap_ms=500,
        recovered_from_checkpoint=True,
    )

    result = service.start_if_needed(
        recovery,
        initial_bucket_ms=BUCKET_START,
    )

    assert result.journal_store is not None
    assert isinstance(result.journal_writer, _Writer)
    assert isinstance(result.micro_repair_supervisor, _Supervisor)
    assert result.micro_repair_started is True
    assert result.journal_bucket_start_ms == BUCKET_START
    assert result.checkpoint_last_trade_ts_ms == CHECKPOINT_TS
    assert result.journal_writer.started == 1
    assert result.journal_writer.open_calls == [
        {
            "exchange": "okx",
            "symbol": "ETH-USDT-PERP",
            "range_pct": "0.002",
            "bucket_start_ms": BUCKET_START,
            "bucket_end_ms": BUCKET_END,
            "checkpoint_last_trade_ts_ms": CHECKPOINT_TS,
            "checkpoint_last_trade_id": "checkpoint-trade",
            "updated_at_ms": 1_002,
        }
    ]
    assert len(checkpoint_store.jobs) == 1
    assert checkpoint_store.jobs[0].created_at_ms == 1_001
    assert checkpoint_store.jobs[0].updated_at_ms == 1_001
    supervisor = result.micro_repair_supervisor
    assert supervisor.config.status_path == tmp_path / "status.json"
    assert supervisor.config.lock_path == tmp_path / "repair.lock"
    assert supervisor.config.checkpoint_db_path == (
        tmp_path / "checkpoint.sqlite3"
    )
    assert supervisor.config.market_db_path == tmp_path / "market.sqlite3"
    assert supervisor.config.journal_db_path == tmp_path / "journal.sqlite3"
    assert supervisor.config.max_gap_ms == 600_000
    assert supervisor.config.page_limit == 100
    assert supervisor.config.max_pages == 20
    assert supervisor.config.max_seconds == 30.0
    assert supervisor.config.missing_bucket_grace_seconds == 120
    assert supervisor.config.repo_root == Path(tmp_path)
    assert supervisor.launches == [
        {
            "exchange": "okx",
            "symbol": "ETH-USDT-PERP",
            "range_pct": "0.002",
            "bucket_start_ms": BUCKET_START,
            "bucket_end_ms": BUCKET_END,
            "coverage_status": (
                RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value
            ),
            "missing_gap_ms": 500,
        }
    ]


def test_bootstrap_module_has_no_exchange_adapter_dependency() -> None:
    text = Path(
        "src/runtime/range_repair_bootstrap.py"
    ).read_text(encoding="utf-8")

    assert "src.platform.exchanges.okx.client" not in text
    assert "src.platform.exchanges.binance.client" not in text
    assert "/api/v5" not in text
    assert "fapi" not in text
    assert "dapi" not in text
