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


FAKE_PROCESS_CREDENTIALS = {
    "OKX_API_KEY": "fake_process_okx_key",
    "OKX_SECRET_KEY": "fake_process_okx_secret",
    "OKX_PASSPHRASE": "fake_process_okx_passphrase",
    "BINANCE_API_KEY": "fake_process_binance_key",
    "BINANCE_SECRET_KEY": "fake_process_binance_secret",
}


def _set_fake_process_credentials(monkeypatch) -> None:
    for key, value in FAKE_PROCESS_CREDENTIALS.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("AETHER_EXAMPLE_ONLY", raising=False)


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
        "AETHER_EXAMPLE_ONLY=example-only-value\n"
        "OKX_API_KEY=fake_example_placeholder_okx_key\n"
        "BINANCE_API_KEY=fake_example_placeholder_binance_key\n",
        encoding="utf-8",
    )
    env.write_text(
        "AETHER_MARKET=from-file\n"
        "AETHER_EXCHANGES=okx\n"
        "AETHER_DATA_EXCHANGE=okx\n"
        "AETHER_STRATEGY=strategies.eth_portfolio_v1:Strategy\n"
        "OKX_API_KEY=fake_file_okx_key\n"
        "BINANCE_API_KEY=fake_file_binance_key\n",
        encoding="utf-8",
    )
    return env


def _write_private_env(tmp_path, *, exchange: str, credentials: str):
    env = tmp_path / f"{exchange}.env"
    env.write_text(
        "AETHER_MARKET=ETH-USDT-PERP\n"
        f"AETHER_EXCHANGES={exchange}\n"
        f"AETHER_DATA_EXCHANGE={exchange}\n"
        "AETHER_STRATEGY=strategies.eth_portfolio_v1:Strategy\n"
        + credentials,
        encoding="utf-8",
    )
    return env


def _clear_private_environment(monkeypatch) -> None:
    for key in FAKE_PROCESS_CREDENTIALS:
        monkeypatch.delenv(key, raising=False)


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
    _set_fake_process_credentials(monkeypatch)
    monkeypatch.setenv("AETHER_TEST_ENV_SENTINEL", "sentinel-before-load")
    monkeypatch.setenv("AETHER_MARKET", "from-process")

    exit_code = await preflight.main()

    project_env = captured["project_env"]
    missing = object()
    assert exit_code == 0
    assert project_env.get("AETHER_MARKET") == "from-process"
    assert project_env.get("OKX_API_KEY") == "fake_process_okx_key"
    assert project_env.get("OKX_SECRET_KEY") == "fake_process_okx_secret"
    assert project_env.get("OKX_PASSPHRASE") == "fake_process_okx_passphrase"
    assert project_env.get("BINANCE_API_KEY") == "fake_process_binance_key"
    assert project_env.get("BINANCE_SECRET_KEY") == "fake_process_binance_secret"
    assert project_env.get("OKX_API_KEY") != "fake_example_placeholder_okx_key"
    assert project_env.get("BINANCE_API_KEY") != "fake_example_placeholder_binance_key"
    assert project_env.get("AETHER_EXAMPLE_ONLY", missing) is missing
    assert project_env.source_files == (str(env),)
    assert project_env.example_file is None
    assert id(get_project_env_config()) == id(project_env)
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
    _set_fake_process_credentials(monkeypatch)
    monkeypatch.setenv("AETHER_TEST_ENV_SENTINEL", "sentinel-before-load")
    monkeypatch.setenv("AETHER_MARKET", "from-process")

    result = await smoke.run_server_smoke(
        defaults_path=tmp_path / "missing-defaults.json",
        env_file=env,
        strategy_name="strategies.eth_portfolio_v1:Strategy",
        repo_root=tmp_path,
    )

    project_env = captured["project_env"]
    missing = object()
    assert result is report
    assert project_env.get("AETHER_MARKET") == "from-process"
    assert project_env.get("OKX_API_KEY") == "fake_process_okx_key"
    assert project_env.get("OKX_SECRET_KEY") == "fake_process_okx_secret"
    assert project_env.get("OKX_PASSPHRASE") == "fake_process_okx_passphrase"
    assert project_env.get("BINANCE_API_KEY") == "fake_process_binance_key"
    assert project_env.get("BINANCE_SECRET_KEY") == "fake_process_binance_secret"
    assert project_env.get("OKX_API_KEY") != "fake_example_placeholder_okx_key"
    assert project_env.get("BINANCE_API_KEY") != "fake_example_placeholder_binance_key"
    assert project_env.get("AETHER_EXAMPLE_ONLY", missing) is missing
    assert project_env.source_files == (str(env),)
    assert project_env.example_file is None
    assert id(get_project_env_config()) == id(project_env)
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
    assert id(platform_config._PROJECT_ENV_CONFIG) == id(None)


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
    assert id(platform_config._PROJECT_ENV_CONFIG) == id(None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exchange", "credentials", "expected_field", "canary"),
    (
        (
            "okx",
            "OKX_API_KEY=你的_okx_api_key\n"
            "OKX_SECRET_KEY=canary_okx_secret\n"
            "OKX_PASSPHRASE=canary_okx_passphrase\n",
            "api_key",
            "canary_okx_secret",
        ),
        (
            "binance",
            "BINANCE_API_KEY=canary_binance_key\n"
            "BINANCE_SECRET_KEY=${BINANCE_SECRET_KEY}\n",
            "api_secret",
            "canary_binance_key",
        ),
    ),
)
async def test_live_server_smoke_rejects_invalid_credentials_before_provider(
    tmp_path,
    monkeypatch,
    exchange,
    credentials,
    expected_field,
    canary,
):
    import tools.live_server_smoke as smoke

    _clear_private_environment(monkeypatch)
    env = _write_private_env(
        tmp_path,
        exchange=exchange,
        credentials=credentials,
    )

    def forbidden_provider_load(_path):
        pytest.fail("provider loaded with invalid private credentials")

    monkeypatch.setattr(smoke, "load_strategy", forbidden_provider_load)

    result = await smoke.run_server_smoke(
        defaults_path=tmp_path / "missing-defaults.json",
        env_file=env,
        strategy_name="strategies.eth_portfolio_v1:Strategy",
        repo_root=tmp_path,
        provider_kwargs={"skip_api": False},
    )

    report_text = result.to_json()
    assert result.verdict == "fail_config"
    assert result.issues[0].startswith("placeholder_private_credentials")
    assert f"exchange={exchange}" in result.issues[0]
    assert f"placeholder_fields={expected_field}" in result.issues[0]
    assert canary not in result.issues[0]
    assert canary not in report_text


@pytest.mark.asyncio
async def test_live_preflight_reports_invalid_credentials_without_provider_or_secret(
    tmp_path,
    monkeypatch,
):
    import tools.live_preflight_check as preflight
    import tools.live_server_smoke as smoke

    _clear_private_environment(monkeypatch)
    env = _write_private_env(
        tmp_path,
        exchange="okx",
        credentials=(
            "OKX_API_KEY=canary_okx_key\n"
            "OKX_SECRET_KEY=canary_okx_secret\n"
            "OKX_PASSPHRASE=<OKX_PASSPHRASE>\n"
        ),
    )
    report_path = tmp_path / "preflight-invalid-credentials.json"
    args = argparse.Namespace(
        strategy="strategies.eth_portfolio_v1:Strategy",
        defaults=tmp_path / "missing-defaults.json",
        env_file=env,
        report=str(report_path),
        apply_reconcile=False,
        skip_api=False,
        skip_kline=True,
    )
    monkeypatch.setattr(preflight, "parse_args", lambda: args)

    def forbidden_strategy_load(_path):
        pytest.fail("strategy loaded with invalid private credentials")

    monkeypatch.setattr(preflight, "load_strategy", forbidden_strategy_load)
    monkeypatch.setattr(smoke, "load_strategy", forbidden_strategy_load)

    exit_code = await preflight.main()

    report_text = report_path.read_text(encoding="utf-8")
    assert exit_code == preflight.EXIT_FAIL_CONFIG
    assert "fail_config" in report_text
    assert "placeholder_fields=passphrase" in report_text
    assert "canary_okx_key" not in report_text
    assert "canary_okx_secret" not in report_text


@pytest.mark.asyncio
async def test_live_server_smoke_skip_api_allows_offline_provider_without_credentials(
    tmp_path,
    monkeypatch,
):
    import tools.live_server_smoke as smoke

    _clear_private_environment(monkeypatch)
    env = _write_private_env(tmp_path, exchange="okx", credentials="")
    report = BootstrapFailureReport(verdict="pass", exit_code=0)
    captured = {}
    strategy = _Strategy(captured, report)
    monkeypatch.setattr(smoke, "load_strategy", lambda _path: strategy)

    result = await smoke.run_server_smoke(
        defaults_path=tmp_path / "missing-defaults.json",
        env_file=env,
        strategy_name="strategies.eth_portfolio_v1:Strategy",
        repo_root=tmp_path,
        provider_kwargs={"skip_api": True},
    )

    assert result is report
    assert captured["project_env"].get("OKX_API_KEY", "") == ""
