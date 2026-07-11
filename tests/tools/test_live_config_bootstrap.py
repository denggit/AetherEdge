from __future__ import annotations

import argparse
import os

import pytest

from src.platform.config import (
    get_project_env_config,
    reset_project_env_config_for_tests,
)
from src.runtime.live_smoke import BootstrapFailureReport


@pytest.fixture(autouse=True)
def _reset_project_env_config():
    reset_project_env_config_for_tests()
    yield
    reset_project_env_config_for_tests()


class _Provider:
    def __init__(self, report):
        self.report = report

    async def run(self):
        return self.report


class _Strategy:
    def __init__(self, captured, report):
        self._captured = captured
        self._report = report

    def live_preflight_provider(self, **kwargs):
        self._captured.update(kwargs)
        return _Provider(self._report)

    def live_smoke_provider(self, **kwargs):
        self._captured.update(kwargs)
        return _Provider(self._report)


def _write_project_files(tmp_path):
    example = tmp_path / ".env.example"
    env = tmp_path / ".env"
    example.write_text(
        "AETHER_EXAMPLE_ONLY=not-runtime-config\n"
        "OKX_API_KEY=placeholder-api-key\n",
        encoding="utf-8",
    )
    env.write_text(
        "AETHER_MARKET=from-file\n"
        "AETHER_EXCHANGES=okx\n"
        "AETHER_DATA_EXCHANGE=okx\n"
        "AETHER_STRATEGY=strategies.eth_portfolio_v1:Strategy\n",
        encoding="utf-8",
    )
    return env


@pytest.mark.asyncio
async def test_live_preflight_entry_uses_process_env_without_example(
    tmp_path,
    monkeypatch,
):
    import tools.live_preflight_check as preflight
    import tools.live_server_smoke as smoke

    env = _write_project_files(tmp_path)
    report_path = tmp_path / "preflight.json"
    report = BootstrapFailureReport(
        verdict="pass",
        exit_code=0,
    )
    captured = {}
    strategy = _Strategy(captured, report)
    args = argparse.Namespace(
        strategy="strategies.eth_portfolio_v1:Strategy",
        defaults=tmp_path / "missing-defaults.json",
        env_file=env,
        report=str(report_path),
        apply_reconcile=False,
        skip_api=True,
        skip_kline=True,
    )
    monkeypatch.setattr(preflight, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(preflight, "parse_args", lambda: args)
    monkeypatch.setattr(preflight, "load_strategy", lambda _path: strategy)
    monkeypatch.setattr(smoke, "load_strategy", lambda _path: strategy)
    monkeypatch.setenv("AETHER_MARKET", "from-process")
    process_env_before = dict(os.environ)

    exit_code = await preflight.main()

    project_env = captured["project_env"]
    assert exit_code == 0
    assert project_env.get("AETHER_MARKET") == "from-process"
    assert "AETHER_EXAMPLE_ONLY" not in project_env.values
    assert "OKX_API_KEY" not in project_env.values
    assert project_env.source_files == (str(env),)
    assert project_env.example_file is None
    assert get_project_env_config() is project_env
    assert dict(os.environ) == process_env_before


@pytest.mark.asyncio
async def test_live_server_smoke_entry_uses_process_env_without_example(
    tmp_path,
    monkeypatch,
):
    import tools.live_server_smoke as smoke

    env = _write_project_files(tmp_path)
    report = BootstrapFailureReport(
        verdict="pass",
        exit_code=0,
    )
    captured = {}
    strategy = _Strategy(captured, report)
    monkeypatch.setattr(smoke, "load_strategy", lambda _path: strategy)
    monkeypatch.setenv("AETHER_MARKET", "from-process")
    process_env_before = dict(os.environ)

    result = await smoke.run_server_smoke(
        defaults_path=tmp_path / "missing-defaults.json",
        env_file=env,
        strategy_name="strategies.eth_portfolio_v1:Strategy",
        repo_root=tmp_path,
    )

    project_env = captured["project_env"]
    assert result is report
    assert project_env.get("AETHER_MARKET") == "from-process"
    assert "AETHER_EXAMPLE_ONLY" not in project_env.values
    assert "OKX_API_KEY" not in project_env.values
    assert project_env.source_files == (str(env),)
    assert project_env.example_file is None
    assert get_project_env_config() is project_env
    assert dict(os.environ) == process_env_before
