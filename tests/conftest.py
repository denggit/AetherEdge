from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from tests._support.runtime_manifest import build_manifest
from tests._support.runtime_state_guard import install_sqlite_guard, uninstall_sqlite_guard


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOTS = ("data/state", "data/market_data", "data/reports", "logs")
_DEFAULT_MF_STORE_PATH = Path("data/market_data/aether_market_data.sqlite3")
_PATH_ENV = {
    "AETHER_STATE_DB": "state/aether_state.sqlite3",
    "AETHER_ORDER_JOURNAL_DB": "state/aether_order_journal.sqlite3",
    "AETHER_POSITION_PLAN_DB": "state/aether_position_plan.sqlite3",
    "AETHER_RANGE_CHECKPOINT_DB": "state/range_builder_checkpoint.sqlite3",
    "AETHER_RANGE_REPAIR_JOURNAL_DB": "state/range_repair_trade_journal.sqlite3",
    "AETHER_MARKET_DATA_DB": "market_data/aether_market_data.sqlite3",
    "AETHER_RANGE_MICRO_REPAIR_STATUS_PATH": "state/range_micro_repair_status.json",
    "AETHER_RANGE_MICRO_REPAIR_LOCK_PATH": "state/range_micro_repair.lock",
    "AETHER_RANGE_BACKFILL_STATUS_PATH": "state/range_backfill_status.json",
    "AETHER_RANGE_BACKFILL_LOCK_PATH": "state/range_backfill.lock",
    "AETHER_RANGE_BACKFILL_RAW_ROOT": "raw/trades",
    "AETHER_RAW_TRADE_BACKFILL_GLOBAL_LOCK_PATH": "state/raw_trade_backfill_global.lock",
    "AETHER_RAW_TRADE_BACKFILL_GLOBAL_STATUS_PATH": "state/raw_trade_backfill_global_status.json",
    "AETHER_LIVE_PREFLIGHT_REPORT": "reports/live_preflight.json",
    "AETHER_LIVE_SMOKE_REPORT": "reports/live_smoke.json",
    "LOG_DIR": "logs",
}
_saved_environment: dict[str, str | None] = {}
_session_root: Path | None = None
_before_manifest: dict[str, dict[str, int | str]] = {}
_saved_tempdir: str | None = None


@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config) -> None:
    """Install isolation after pytest's TempPathFactory exists, before collection."""

    global _session_root, _saved_tempdir
    factory = config._tmp_path_factory  # pytest test infrastructure API; unavailable in production code.
    system_temp_root = Path(tempfile.gettempdir()).resolve()
    allowed_temp_root = factory.getbasetemp().resolve()
    _session_root = factory.mktemp("aether-runtime-state", numbered=True)
    keys = tuple(_PATH_ENV) + (
        "AETHER_PYTEST_SQLITE_GUARD",
        "AETHER_PYTEST_REPO_ROOT",
        "AETHER_PYTEST_STATE_ROOT",
        "AETHER_PYTEST_ALLOWED_TEMP_ROOT",
        "AETHER_PYTEST_SYSTEM_TEMP_ROOT",
        "AETHER_PYTEST_RUNTIME_HEARTBEAT_DB",
        "PYTHONPATH",
    )
    for key in keys:
        _saved_environment[key] = os.environ.get(key)
    _saved_tempdir = tempfile.tempdir
    os.environ["AETHER_PYTEST_ALLOWED_TEMP_ROOT"] = str(allowed_temp_root)
    os.environ["AETHER_PYTEST_SYSTEM_TEMP_ROOT"] = str(system_temp_root)
    tempfile.tempdir = str(allowed_temp_root)
    _apply_isolated_environment(_session_root)
    install_sqlite_guard(repo_root=REPO_ROOT, pytest_root=allowed_temp_root)
    from src.platform.config import reset_project_env_config_for_tests

    reset_project_env_config_for_tests()


def pytest_sessionstart(session: pytest.Session) -> None:
    global _before_manifest
    _before_manifest = build_manifest(repo_root=REPO_ROOT, roots=RUNTIME_ROOTS)


@pytest.fixture(autouse=True)
def _isolate_runtime_state_per_test(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    test_root = tmp_path / "aether-runtime"
    test_root.mkdir(parents=True, exist_ok=True)
    _apply_isolated_environment(test_root)
    from src.platform.config import reset_project_env_config_for_tests
    from src.runtime.heartbeat import RuntimeHeartbeatService, RuntimeHeartbeatStore
    import src.runtime.runner as runner_module
    from strategies.eth_portfolio_v1.domain.mf_data import MfDataBuffer, MfDataReadiness

    reset_project_env_config_for_tests()

    def isolated_heartbeat_service(*, store=None, interval_seconds: float = 15.0):
        resolved = store or RuntimeHeartbeatStore(
            os.environ["AETHER_PYTEST_RUNTIME_HEARTBEAT_DB"]
        )
        return RuntimeHeartbeatService(store=resolved, interval_seconds=interval_seconds)

    monkeypatch.setattr(
        runner_module,
        "RuntimeHeartbeatService",
        isolated_heartbeat_service,
    )
    original_mf_buffer_init = MfDataBuffer.__init__

    def isolated_mf_buffer_init(self, *args, **kwargs):
        raw_store_path = kwargs.get("store_path")
        if raw_store_path is None or Path(raw_store_path) == _DEFAULT_MF_STORE_PATH:
            kwargs["store_path"] = os.environ["AETHER_MARKET_DATA_DB"]
        return original_mf_buffer_init(self, *args, **kwargs)

    monkeypatch.setattr(MfDataBuffer, "__init__", isolated_mf_buffer_init)
    original_mf_readiness_init = MfDataReadiness.__init__

    def isolated_mf_readiness_init(self, *args, **kwargs):
        raw_store_path = kwargs.get("store_path")
        if raw_store_path is None or Path(raw_store_path) == _DEFAULT_MF_STORE_PATH:
            kwargs["store_path"] = os.environ["AETHER_MARKET_DATA_DB"]
        return original_mf_readiness_init(self, *args, **kwargs)

    monkeypatch.setattr(MfDataReadiness, "__init__", isolated_mf_readiness_init)
    yield
    reset_project_env_config_for_tests()
    if _session_root is not None:
        _apply_isolated_environment(_session_root)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    global _saved_tempdir
    after = build_manifest(repo_root=REPO_ROOT, roots=RUNTIME_ROOTS)
    if after != _before_manifest:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        if reporter is not None:
            reporter.write_sep("!", "repository runtime manifest changed during pytest")
            reporter.write_line(f"before={_before_manifest}")
            reporter.write_line(f"after={after}")
    uninstall_sqlite_guard()
    tempfile.tempdir = _saved_tempdir
    for key, original in _saved_environment.items():
        if original is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original


def _apply_isolated_environment(root: Path) -> None:
    root = root.resolve()
    for key, relative in _PATH_ENV.items():
        target = root / relative
        if key.endswith("_ROOT") or key.endswith("_DIR") or key == "LOG_DIR":
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
        os.environ[key] = str(target)
    heartbeat = root / "state" / "aether_runtime_heartbeat.sqlite3"
    heartbeat.parent.mkdir(parents=True, exist_ok=True)
    os.environ["AETHER_PYTEST_RUNTIME_HEARTBEAT_DB"] = str(heartbeat)
    os.environ["AETHER_PYTEST_SQLITE_GUARD"] = "1"
    os.environ["AETHER_PYTEST_REPO_ROOT"] = str(REPO_ROOT)
    os.environ["AETHER_PYTEST_STATE_ROOT"] = str(root)
    support = str(REPO_ROOT / "tests" / "_support" / "sqlite_guard")
    current = os.environ.get("PYTHONPATH", "")
    parts = [part for part in current.split(os.pathsep) if part and part != support]
    os.environ["PYTHONPATH"] = os.pathsep.join((support, *parts))
