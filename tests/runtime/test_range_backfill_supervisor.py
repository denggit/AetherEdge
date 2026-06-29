from __future__ import annotations

import json
import os
from pathlib import Path

from src.runtime.range_backfill_supervisor import RangeBackfillSupervisor


def test_existing_worker_pid_is_not_started_twice(tmp_path: Path, monkeypatch) -> None:
    pid_file = tmp_path / "worker.pid"
    pid_file.write_text(str(os.getpid()), encoding="utf-8")
    supervisor = RangeBackfillSupervisor(project_root=tmp_path, pid_file=pid_file)

    called = False
    monkeypatch.setattr("subprocess.Popen", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("should not start")))

    assert supervisor.start() == os.getpid()
    assert called is False


def test_autostart_writes_log_and_pid(tmp_path: Path, monkeypatch) -> None:
    class Proc:
        pid = 12345

    monkeypatch.setattr("subprocess.Popen", lambda *_args, **_kwargs: Proc())
    supervisor = RangeBackfillSupervisor(
        project_root=tmp_path,
        script_path=tmp_path / "tools" / "range_backfill_worker.py",
        pid_file=tmp_path / "run" / "worker.pid",
        log_file=tmp_path / "logs" / "range_backfill_worker.out",
    )

    assert supervisor.start(args=["--required-buckets", "1"]) == 12345
    assert (tmp_path / "run" / "worker.pid").read_text(encoding="utf-8") == "12345"
    assert (tmp_path / "logs" / "range_backfill_worker.out").exists()


def test_status_json_is_readable(tmp_path: Path) -> None:
    status = tmp_path / "status.json"
    status.write_text(json.dumps({"range_speed_ready": True}), encoding="utf-8")
    supervisor = RangeBackfillSupervisor(project_root=tmp_path, status_json=status)

    assert supervisor.read_status() == {"range_speed_ready": True}
