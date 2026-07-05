from __future__ import annotations

import json
import os
import time
from pathlib import Path

from src.market_data.trade_features.backfill_supervisor import (
    TradeFeatureBackfillConfig,
    TradeFeatureBackfillSupervisor,
)


def _config(tmp_path: Path) -> TradeFeatureBackfillConfig:
    return TradeFeatureBackfillConfig(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        worker_script=tmp_path / "worker.py",
        repository_root=tmp_path,
        market_db=str(tmp_path / "market.sqlite3"),
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "worker.lock",
        global_lock_path=tmp_path / "global.lock",
        global_status_path=tmp_path / "global-status.json",
        worker_log_path=tmp_path / "worker.out",
        restart_cooldown_seconds=0,
        failure_cooldown_seconds=0,
    )


def test_complete_coverage_does_not_launch(tmp_path, monkeypatch) -> None:
    supervisor = TradeFeatureBackfillSupervisor(
        config=_config(tmp_path),
        coverage_reader=lambda: {"coverage_ready": True},
    )
    launches = []
    monkeypatch.setattr(
        supervisor,
        "_launch_worker",
        lambda: launches.append(True),
    )

    result = supervisor.check_and_launch()

    assert result["reason"] == "coverage_complete"
    assert launches == []


def test_coverage_gap_launches_injected_worker(tmp_path, monkeypatch) -> None:
    supervisor = TradeFeatureBackfillSupervisor(
        config=_config(tmp_path),
        coverage_reader=lambda: {"coverage_ready": False},
    )
    monkeypatch.setattr(
        supervisor,
        "_launch_worker",
        lambda: True,
    )

    result = supervisor.check_and_launch()

    assert result["action"] == "launched"
    assert result["reason"] == "coverage_gap"


def test_running_worker_prevents_duplicate_launch(
    tmp_path,
    monkeypatch,
) -> None:
    config = _config(tmp_path)
    config.status_path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "running": True,
                "worker_heartbeat_ms": int(time.time() * 1000),
            }
        ),
        encoding="utf-8",
    )
    supervisor = TradeFeatureBackfillSupervisor(
        config=config,
        coverage_reader=lambda: {"coverage_ready": False},
    )
    launches = []
    monkeypatch.setattr(
        supervisor,
        "_launch_worker",
        lambda: launches.append(True),
    )

    result = supervisor.check_and_launch()

    assert result["reason"] == "worker_already_running"
    assert launches == []


def test_scan_coverage_returns_detached_mapping(tmp_path) -> None:
    source = {"coverage_ready": False}
    supervisor = TradeFeatureBackfillSupervisor(
        config=_config(tmp_path),
        coverage_reader=lambda: source,
    )

    result = supervisor.scan_coverage()
    result["coverage_ready"] = True

    assert source["coverage_ready"] is False
