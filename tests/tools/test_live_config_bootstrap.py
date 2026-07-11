from __future__ import annotations

import argparse
import json
import os

import pytest

from src.platform.config import (
    get_project_env_config,
    reset_project_env_config_for_tests,
    set_project_env_config,
)
from src.platform import config as platform_config
from src.runtime.live_smoke import BootstrapFailureReport


@pytest.fixture(autouse=True)
def _reset_project_env_config():
    previous = platform_config._PROJECT_ENV_CONFIG
    reset_project_env_config_for_tests()
    yield
    if previous is None:
        reset_project_env_config_for_tests()
    else:
        set_project_env_config(previous)


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
    monkeypatch.setenv("AETHER_TEST_ENV_SENTINEL", "sentinel-before-load")
    monkeypatch.setenv("AETHER_MARKET", "from-process")

    exit_code = await preflight.main()

    project_env = captured["project_env"]
    assert exit_code == 0
    assert project_env.get("AETHER_MARKET") == "from-process"
    assert "AETHER_EXAMPLE_ONLY" not in project_env.values
    assert "OKX_API_KEY" not in project_env.values
    assert project_env.source_files == (str(env),)
    assert project_env.example_file is None
    assert get_project_env_config() is project_env
    assert os.environ["AETHER_TEST_ENV_SENTINEL"] == "sentinel-before-load"
    assert os.environ["AETHER_MARKET"] == "from-process"


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
    monkeypatch.setenv("AETHER_TEST_ENV_SENTINEL", "sentinel-before-load")
    monkeypatch.setenv("AETHER_MARKET", "from-process")

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
    assert os.environ["AETHER_TEST_ENV_SENTINEL"] == "sentinel-before-load"
    assert os.environ["AETHER_MARKET"] == "from-process"


@pytest.mark.asyncio
async def test_live_preflight_rejects_env_example_before_strategy_or_api(
    tmp_path,
    monkeypatch,
):
    import tools.live_preflight_check as preflight

    fake_secret = "FAKE_PREFLIGHT_SECRET_MUST_NOT_APPEAR"
    example = tmp_path / ".env.example"
    example.write_text(
        f"OKX_API_KEY={fake_secret}\n"
        f"BINANCE_SECRET_KEY={fake_secret}\n",
        encoding="utf-8",
    )
    report_path = tmp_path / "preflight-failure.json"
    args = argparse.Namespace(
        strategy="strategies.eth_portfolio_v1:Strategy",
        defaults=tmp_path / "missing-defaults.json",
        env_file=example,
        report=str(report_path),
        apply_reconcile=False,
        skip_api=False,
        skip_kline=False,
    )
    monkeypatch.setattr(preflight, "parse_args", lambda: args)

    def forbidden_call(*_args, **_kwargs):
        pytest.fail("strategy or API initialization ran after config rejection")

    monkeypatch.setattr(preflight, "load_strategy", forbidden_call)
    monkeypatch.setattr(preflight, "create_account_client", forbidden_call)
    monkeypatch.setattr(preflight, "create_execution_client", forbidden_call)

    exit_code = await preflight.main()

    payload_text = report_path.read_text(encoding="utf-8")
    payload = json.loads(payload_text)
    assert exit_code == preflight.EXIT_FAIL_CONFIG
    assert payload["verdict"] == "fail_config"
    assert payload["checks"][0]["error"] == "config_load_failed:ValueError"
    assert fake_secret not in payload_text
    assert platform_config._PROJECT_ENV_CONFIG is None


@pytest.mark.asyncio
async def test_live_server_smoke_classifies_env_example_as_fail_config(
    tmp_path,
    monkeypatch,
):
    import tools.live_server_smoke as smoke

    fake_secret = "FAKE_SMOKE_SECRET_MUST_NOT_APPEAR"
    example = tmp_path / ".env.example"
    example.write_text(
        f"OKX_API_KEY={fake_secret}\n",
        encoding="utf-8",
    )

    def forbidden_load_strategy(_path):
        pytest.fail("provider initialization ran after config rejection")

    monkeypatch.setattr(smoke, "load_strategy", forbidden_load_strategy)

    result = await smoke.run_server_smoke(
        defaults_path=tmp_path / "missing-defaults.json",
        env_file=example,
        strategy_name="strategies.eth_portfolio_v1:Strategy",
        repo_root=tmp_path,
    )

    report_text = result.to_json()
    assert result.verdict == "fail_config"
    assert result.exit_code == 1
    assert result.issues == ["live_smoke_config_load_failed:ValueError"]
    assert fake_secret not in report_text
    assert platform_config._PROJECT_ENV_CONFIG is None
