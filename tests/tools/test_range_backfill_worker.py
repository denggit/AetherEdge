from __future__ import annotations

import json
from pathlib import Path

import src.market_data.backfill.worker as worker_module
from src.market_data.backfill.worker import RangeBackfillWorker, WorkerLock
import tools.range_backfill_worker as cli


def test_cli_has_repo_root_bootstrap() -> None:
    text = Path("tools/range_backfill_worker.py").read_text(encoding="utf-8")
    assert "REPO_ROOT = Path(__file__).resolve().parents[1]" in text
    assert "sys.path.insert(0, str(REPO_ROOT))" in text


def test_mode_once_outputs_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    class Lock:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    class Worker:
        def __init__(self, **_kwargs):
            pass

        def acquire_single_instance(self):
            return Lock()

        def run_once(self):
            return {
                "range_speed_ready": True,
                "missing_bucket_count": 0,
                "plan": {"continuous_complete_buckets_from_latest": 100},
                "result": {"processed_buckets": 0, "locked": False, "tail_fetch_failed_buckets": [1], "archive_errors": ["x"]},
            }

    monkeypatch.setattr(cli, "RangeBackfillWorker", Worker)

    assert cli.main(["--mode", "once", "--pid-file", str(tmp_path / "pid"), "--lock-file", str(tmp_path / "lock")]) == 0
    out = capsys.readouterr().out
    assert '"range_speed_ready": true' in out
    assert '"archive_errors_count": 1' in out
    assert '"tail_fetch_failed_buckets": [1]' in out


def test_daemon_survives_run_once_exception_and_writes_status(tmp_path: Path) -> None:
    class FailingWorker(RangeBackfillWorker):
        def run_once(self):
            raise RuntimeError("boom")

    status = tmp_path / "status.json"
    worker = FailingWorker(json_status=status, cycle_sleep_seconds=0, warning_interval_seconds=0)

    assert worker.run_daemon(stop_after_cycles=1) == 0
    data = json.loads(status.read_text(encoding="utf-8"))
    assert data["range_speed_ready"] is False
    assert data["result"]["errors"] == ["RuntimeError: boom"]


def test_single_instance_lock_detects_live_pid(tmp_path: Path, monkeypatch) -> None:
    pid_file = tmp_path / "worker.pid"
    lock_file = tmp_path / "worker.lock"
    pid_file.write_text("999999", encoding="utf-8")
    monkeypatch.setattr(worker_module, "_pid_alive", lambda pid: pid == 999999)

    assert WorkerLock.acquire(lock_file, pid_file) is None
