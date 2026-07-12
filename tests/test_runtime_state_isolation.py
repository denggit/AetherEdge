from __future__ import annotations

import hashlib
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.range_repair.store import SqliteRangeRepairJournalStore
from src.market_data.storage import SqliteRangeBarStore
from src.order_management import SqliteOrderJournalStore, SqlitePositionPlanStore
from src.platform.state.sqlite_store import SqliteStateStore
from tests._support.runtime_manifest import build_manifest
from tests._support.runtime_state_guard import BLOCKED_MESSAGE


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOTS = ("data/state", "data/market_data", "data/reports", "logs")


@pytest.mark.parametrize(
    "target",
    (
        Path("data/state/aether_position_plan.sqlite3"),
        REPO_ROOT / "data/state/aether_order_journal.sqlite3",
        REPO_ROOT / "data/market_data/aether_market_data.sqlite3",
        REPO_ROOT / "data/state/aether_position_plan.sqlite3-wal",
        REPO_ROOT / "data/state/aether_position_plan.sqlite3-shm",
    ),
)
def test_repository_runtime_sqlite_writes_are_blocked(target, monkeypatch) -> None:
    monkeypatch.chdir(REPO_ROOT)
    with pytest.raises(RuntimeError, match=BLOCKED_MESSAGE):
        sqlite3.connect(target)


def test_default_position_and_journal_store_paths_are_blocked(monkeypatch) -> None:
    monkeypatch.chdir(REPO_ROOT)
    with pytest.raises(RuntimeError, match=BLOCKED_MESSAGE):
        SqlitePositionPlanStore("data/state/aether_position_plan.sqlite3")
    with pytest.raises(RuntimeError, match=BLOCKED_MESSAGE):
        SqliteOrderJournalStore("data/state/aether_order_journal.sqlite3")


@pytest.mark.parametrize("mode", ("rw", "rwc"))
def test_writable_sqlite_uri_is_blocked(mode: str) -> None:
    target = (REPO_ROOT / "data/state/uri-guard-test.sqlite3").as_uri()
    with pytest.raises(RuntimeError, match=BLOCKED_MESSAGE):
        sqlite3.connect(f"{target}?mode={mode}", uri=True)


def test_read_only_sqlite_uri_is_not_classified_as_write() -> None:
    target = (REPO_ROOT / "data/state/nonexistent-read-only.sqlite3").as_uri()
    with pytest.raises(sqlite3.OperationalError):
        sqlite3.connect(f"{target}?mode=ro", uri=True)


def test_repository_absolute_path_is_blocked_from_other_working_directory(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(RuntimeError, match=BLOCKED_MESSAGE):
        sqlite3.connect(REPO_ROOT / "data/state/aether_position_plan.sqlite3")


def test_memory_and_pytest_temporary_databases_remain_usable(tmp_path) -> None:
    with sqlite3.connect(":memory:") as connection:
        assert connection.execute("SELECT 1").fetchone() == (1,)

    state = SqliteStateStore(tmp_path / "state.sqlite3")
    journal = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    plans = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    checkpoint = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    repair = SqliteRangeRepairJournalStore(tmp_path / "repair.sqlite3")
    market = SqliteRangeBarStore(tmp_path / "market.sqlite3")

    assert Path(state.path).is_file()
    assert Path(journal.path).is_file()
    assert Path(plans.path).is_file()
    assert Path(checkpoint.path).is_file()
    assert Path(repair.path).is_file()
    assert Path(market.path).is_file()


def test_system_temp_database_outside_this_pytest_basetemp_is_blocked() -> None:
    allowed = Path(os.environ["AETHER_PYTEST_ALLOWED_TEMP_ROOT"]).resolve()
    target = Path(os.environ["AETHER_PYTEST_SYSTEM_TEMP_ROOT"]).resolve() / "aether-outside-basetemp.sqlite3"
    assert not target.is_relative_to(allowed)
    with pytest.raises(
        RuntimeError,
        match="pytest blocked write access outside the temporary directory",
    ):
        sqlite3.connect(target)


def test_subprocess_blocks_system_temp_database_outside_pytest_basetemp() -> None:
    target = Path(os.environ["AETHER_PYTEST_SYSTEM_TEMP_ROOT"]).resolve() / "aether-child-outside-basetemp.sqlite3"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            f"import sqlite3; sqlite3.connect({str(target)!r})",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0
    assert "pytest blocked write access outside the temporary directory" in (
        result.stdout + result.stderr
    )


@pytest.mark.parametrize("use_other_cwd", (False, True))
def test_python_subprocess_inherits_repository_sqlite_guard(
    tmp_path,
    use_other_cwd: bool,
) -> None:
    target = REPO_ROOT / "data/state/aether_position_plan.sqlite3"
    script_target = str(target) if use_other_cwd else "data/state/aether_position_plan.sqlite3"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sqlite3; "
                f"sqlite3.connect({script_target!r}); "
                "raise SystemExit('guard did not run')"
            ),
        ],
        cwd=tmp_path if use_other_cwd else REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert BLOCKED_MESSAGE in output


def test_fake_repository_sentinel_and_real_runtime_manifest_remain_unchanged(
    tmp_path,
) -> None:
    from tests.runtime import test_live_runtime_glue as glue

    fake_repo = tmp_path / "fake_repo"
    sentinel = fake_repo / "data/state/sentinel.sqlite3"
    sentinel.parent.mkdir(parents=True)
    with sqlite3.connect(sentinel) as connection:
        connection.execute("CREATE TABLE sentinel(value TEXT NOT NULL)")
        connection.execute("INSERT INTO sentinel VALUES ('unchanged')")
    before_sentinel = _file_fingerprint(sentinel)
    before_real = build_manifest(repo_root=REPO_ROOT, roots=RUNTIME_ROOTS)

    runner = glue._runner(
        glue.FeatureStrategy(),
        services={
            "recovery_service": glue.FakeRecoveryService(),
            "snapshot": glue._snapshot(),
        },
        dry_run=True,
    )
    runner._get_reconciliation_service()
    assert runner._get_position_plan_store().list_active_positions() == ()

    pytest_root = Path(os.environ["AETHER_PYTEST_STATE_ROOT"]).resolve()
    paths = (
        runner._get_position_plan_store().path,
        runner._get_order_journal().path,
        runner._get_range_checkpoint_store().path,
        runner._range_repair_journal_store.path,
        runner._range_bar_store.path,
        runner._heartbeat_service.store.db_path,
    )
    for path in paths:
        Path(path).resolve().relative_to(pytest_root)

    assert _file_fingerprint(sentinel) == before_sentinel
    assert build_manifest(repo_root=REPO_ROOT, roots=RUNTIME_ROOTS) == before_real


def _file_fingerprint(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    return hashlib.sha256(path.read_bytes()).hexdigest(), stat.st_size, stat.st_mtime_ns
