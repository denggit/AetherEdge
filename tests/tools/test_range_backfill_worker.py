from __future__ import annotations

from pathlib import Path

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
                "result": {"processed_buckets": 0, "locked": False},
            }

    monkeypatch.setattr(cli, "RangeBackfillWorker", Worker)

    assert cli.main(["--mode", "once", "--pid-file", str(tmp_path / "pid"), "--lock-file", str(tmp_path / "lock")]) == 0
    assert '"range_speed_ready": true' in capsys.readouterr().out
