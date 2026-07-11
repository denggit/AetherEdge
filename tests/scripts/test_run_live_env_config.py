from __future__ import annotations

import asyncio
import importlib
import os
import sys
from contextlib import contextmanager
from unittest.mock import AsyncMock, Mock

import pytest

import src.app.factory as app_factory
from src.platform.data.websocket.connector import WebsocketsConnector
from src.platform.exchanges.http import RequestsHttpClient, StdlibHttpClient
from src.platform import config as platform_config
from src.platform.config import get_project_env_config, load_project_env_config
from src.runtime import RuntimeMode
from src.runtime.runner import LiveRuntimeError


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


@contextmanager
def _isolated_run_live_import(tmp_path):
    module_name = "scripts.run_live"
    previous_module = sys.modules.pop(module_name, None)
    previous_project_env = platform_config._PROJECT_ENV_CONFIG
    real_loader = platform_config.load_project_env_config
    import_env = tmp_path / "isolated-import.env"
    import_env.write_text("", encoding="utf-8")
    import_config = real_loader(
        env_file=import_env,
        process_env={},
    )
    imported_module = None

    def isolated_import_loader(*_args, **_kwargs):
        return import_config

    platform_config.load_project_env_config = isolated_import_loader
    try:
        imported_module = importlib.import_module(module_name)
        platform_config.load_project_env_config = real_loader
        imported_module.load_project_env_config = real_loader
        yield imported_module, import_config
    finally:
        platform_config.load_project_env_config = real_loader
        if imported_module is not None:
            imported_module.load_project_env_config = real_loader
            imported_module.PROJECT_ENV_CONFIG = import_config
        if previous_project_env is None:
            platform_config.reset_project_env_config_for_tests()
        else:
            platform_config.set_project_env_config(previous_project_env)
        sys.modules.pop(module_name, None)
        if previous_module is not None:
            sys.modules[module_name] = previous_module


def test_bootstrap_live_process_config_uses_process_env_without_example(
    tmp_path,
    monkeypatch,
):
    previous_module = sys.modules.get("scripts.run_live")
    previous_module_config = (
        getattr(previous_module, "PROJECT_ENV_CONFIG", None)
        if previous_module is not None
        else None
    )
    previous_module_loader = (
        getattr(previous_module, "load_project_env_config", None)
        if previous_module is not None
        else None
    )
    previous_platform_loader = platform_config.load_project_env_config
    previous_project_env = platform_config._PROJECT_ENV_CONFIG
    project_root = tmp_path / "runtime-project"
    project_root.mkdir()
    (project_root / ".env.example").write_text(
        "AETHER_EXAMPLE_ONLY=example-only-value\n"
        "OKX_API_KEY=fake_example_placeholder_okx_key\n"
        "BINANCE_API_KEY=fake_example_placeholder_binance_key\n",
        encoding="utf-8",
    )
    (project_root / ".env").write_text(
        "AETHER_LIVE_TRADING=false\n"
        "AETHER_EXCHANGES=okx,binance\n"
        "OKX_API_KEY=fake_file_okx_key\n"
        "BINANCE_API_KEY=fake_file_binance_key\n",
        encoding="utf-8",
    )
    _set_fake_process_credentials(monkeypatch)
    monkeypatch.setenv("AETHER_TEST_ENV_SENTINEL", "sentinel-before-load")
    monkeypatch.setenv("AETHER_LIVE_TRADING", "true")

    with _isolated_run_live_import(tmp_path) as (run_live, import_config):
        assert id(run_live.PROJECT_ENV_CONFIG) == id(import_config)

        config = run_live.bootstrap_live_process_config(project_root)
        missing = object()

        assert config.get("AETHER_LIVE_TRADING") == "true"
        assert config.get("AETHER_EXCHANGES") == "okx,binance"
        assert config.get("OKX_API_KEY") == "fake_process_okx_key"
        assert config.get("OKX_SECRET_KEY") == "fake_process_okx_secret"
        assert config.get("OKX_PASSPHRASE") == "fake_process_okx_passphrase"
        assert config.get("BINANCE_API_KEY") == "fake_process_binance_key"
        assert config.get("BINANCE_SECRET_KEY") == "fake_process_binance_secret"
        assert config.get("OKX_API_KEY") != "fake_example_placeholder_okx_key"
        assert config.get("BINANCE_API_KEY") != "fake_example_placeholder_binance_key"
        assert config.get("AETHER_EXAMPLE_ONLY", missing) is missing
        assert config.source_files == (str(project_root / ".env"),)
        assert config.example_file is None
        assert id(get_project_env_config()) == id(config)
        assert id(run_live.PROJECT_ENV_CONFIG) == id(config)
        assert os.environ["AETHER_TEST_ENV_SENTINEL"] == "sentinel-before-load"
        assert os.environ["AETHER_LIVE_TRADING"] == "true"

    assert id(platform_config._PROJECT_ENV_CONFIG) == id(previous_project_env)
    assert platform_config.load_project_env_config is previous_platform_loader
    assert sys.modules.get("scripts.run_live") is previous_module
    if previous_module is not None:
        assert id(previous_module.PROJECT_ENV_CONFIG) == id(previous_module_config)
        assert previous_module.load_project_env_config is previous_module_loader


def _live_runtime_project_env(tmp_path, *, dry_run: bool):
    env = tmp_path / "direct-live.env"
    env.write_text("", encoding="utf-8")
    return load_project_env_config(
        env_file=env,
        process_env={
            "AETHER_RUNTIME_MODE": "live_runtime",
            "AETHER_LIVE_TRADING": "true",
            "AETHER_DRY_RUN": str(dry_run).lower(),
            "AETHER_MARKET": "ETH-USDT-PERP",
            "AETHER_EXCHANGES": "okx,binance",
            "AETHER_DATA_EXCHANGE": "okx",
            "AETHER_STRATEGY": "strategies.eth_portfolio_v1:Strategy",
            "AETHER_REQUIRED_LIVE_STRATEGY": "eth_portfolio_v1",
            "OKX_API_KEY": "你的_okx_api_key",
            "OKX_SECRET_KEY": "canary_okx_secret",
            "OKX_PASSPHRASE": "canary_okx_passphrase",
            "BINANCE_API_KEY": "canary_binance_key",
            "BINANCE_SECRET_KEY": "canary_binance_secret",
        },
    )


def _legacy_project_env(
    tmp_path,
    *,
    dry_run: bool,
    runtime_mode: str | None = "legacy_app",
):
    env = tmp_path / "legacy-app.env"
    env.write_text("", encoding="utf-8")
    process_env = {
        "AETHER_LIVE_TRADING": "true",
        "AETHER_DRY_RUN": str(dry_run).lower(),
        "AETHER_MARKET": "ETH-USDT-PERP",
        "AETHER_EXCHANGES": "okx",
        "AETHER_DATA_EXCHANGE": "okx",
        "AETHER_STRATEGY": "strategies.eth_portfolio_v1:Strategy",
        "OKX_API_KEY": "your_okx_api_key",
        "OKX_SECRET_KEY": "canary_legacy_okx_secret",
        "OKX_PASSPHRASE": "canary_legacy_okx_passphrase",
    }
    if runtime_mode is not None:
        process_env["AETHER_RUNTIME_MODE"] = runtime_mode
    return load_project_env_config(
        env_file=env,
        process_env=process_env,
    )


def test_direct_live_rejects_placeholder_credentials_before_app_context(
    tmp_path,
    monkeypatch,
):
    with _isolated_run_live_import(tmp_path) as (run_live, _import_config):
        project_env = _live_runtime_project_env(tmp_path, dry_run=False)
        platform_config.set_project_env_config(project_env)
        run_live.PROJECT_ENV_CONFIG = project_env
        monkeypatch.setattr(sys, "argv", ["run_live.py"])
        build_calls: list[object] = []

        def forbidden_build(*_args, **_kwargs):
            build_calls.append(object())
            raise AssertionError("app context built with invalid credentials")

        monkeypatch.setattr(run_live, "build_app_context", forbidden_build)

        with pytest.raises(LiveRuntimeError) as exc_info:
            asyncio.run(run_live.main())

    text = str(exc_info.value)
    assert "placeholder_private_credentials" in text
    assert "exchange=okx" in text
    assert "placeholder_fields=api_key" in text
    assert "canary_okx_secret" not in text
    assert "canary_okx_passphrase" not in text
    assert build_calls == []


def test_dry_run_does_not_require_private_credentials_at_live_validation_layer(
    tmp_path,
    monkeypatch,
):
    class BuildReached(RuntimeError):
        pass

    with _isolated_run_live_import(tmp_path) as (run_live, _import_config):
        project_env = _live_runtime_project_env(tmp_path, dry_run=True)
        platform_config.set_project_env_config(project_env)
        run_live.PROJECT_ENV_CONFIG = project_env
        monkeypatch.setattr(sys, "argv", ["run_live.py"])

        def stop_after_validation(*_args, **_kwargs):
            raise BuildReached("build_app_context_reached")

        monkeypatch.setattr(run_live, "build_app_context", stop_after_validation)

        with pytest.raises(BuildReached, match="build_app_context_reached"):
            asyncio.run(run_live.main())


def test_legacy_direct_live_rejects_placeholder_credentials_before_startup(
    tmp_path,
    monkeypatch,
):
    with _isolated_run_live_import(tmp_path) as (run_live, _import_config):
        project_env = _legacy_project_env(tmp_path, dry_run=False)
        platform_config.set_project_env_config(project_env)
        run_live.PROJECT_ENV_CONFIG = project_env
        monkeypatch.setattr(sys, "argv", ["run_live.py"])

        build_context = Mock(
            side_effect=AssertionError("app context built with invalid credentials")
        )
        load_strategy = Mock(
            side_effect=AssertionError("strategy loaded with invalid credentials")
        )
        create_execution_client = Mock(
            side_effect=AssertionError("client created with invalid credentials")
        )
        app_runner = Mock(
            side_effect=AssertionError("AppRunner created with invalid credentials")
        )
        live_runtime_runner = Mock(
            side_effect=AssertionError(
                "LiveRuntimeRunner created with invalid credentials"
            )
        )
        requests_http = AsyncMock(
            side_effect=AssertionError("HTTP called with invalid credentials")
        )
        stdlib_http = AsyncMock(
            side_effect=AssertionError("HTTP called with invalid credentials")
        )
        websocket_connect = AsyncMock(
            side_effect=AssertionError("WebSocket called with invalid credentials")
        )
        monkeypatch.setattr(run_live, "build_app_context", build_context)
        monkeypatch.setattr(run_live, "AppRunner", app_runner)
        monkeypatch.setattr(run_live, "LiveRuntimeRunner", live_runtime_runner)
        monkeypatch.setattr(app_factory, "load_strategy", load_strategy)
        monkeypatch.setattr(
            app_factory,
            "create_execution_client",
            create_execution_client,
        )
        monkeypatch.setattr(RequestsHttpClient, "request", requests_http)
        monkeypatch.setattr(StdlibHttpClient, "request", stdlib_http)
        monkeypatch.setattr(WebsocketsConnector, "connect", websocket_connect)

        with pytest.raises(LiveRuntimeError) as exc_info:
            asyncio.run(run_live.main())

    text = str(exc_info.value)
    assert "placeholder_private_credentials" in text
    assert "exchange=okx" in text
    assert "placeholder_fields=api_key" in text
    assert "canary_legacy_okx_secret" not in text
    assert "canary_legacy_okx_passphrase" not in text
    build_context.assert_not_called()
    load_strategy.assert_not_called()
    create_execution_client.assert_not_called()
    app_runner.assert_not_called()
    live_runtime_runner.assert_not_called()
    requests_http.assert_not_awaited()
    stdlib_http.assert_not_awaited()
    websocket_connect.assert_not_awaited()


def test_legacy_dry_run_reaches_app_context_with_placeholder_credentials(
    tmp_path,
    monkeypatch,
):
    class BuildReached(RuntimeError):
        pass

    with _isolated_run_live_import(tmp_path) as (run_live, _import_config):
        project_env = _legacy_project_env(tmp_path, dry_run=True)
        platform_config.set_project_env_config(project_env)
        run_live.PROJECT_ENV_CONFIG = project_env
        monkeypatch.setattr(sys, "argv", ["run_live.py"])
        build_calls = []

        def stop_at_app_context(config):
            build_calls.append(config)
            raise BuildReached("legacy_build_app_context_reached")

        monkeypatch.setattr(run_live, "build_app_context", stop_at_app_context)

        with pytest.raises(BuildReached, match="legacy_build_app_context_reached"):
            asyncio.run(run_live.main())

    assert len(build_calls) == 1
    assert build_calls[0].dry_run is True
    assert build_calls[0].strategy == "strategies.eth_portfolio_v1:Strategy"


def test_default_legacy_mode_rejects_direct_live_before_app_context(
    tmp_path,
    monkeypatch,
):
    with _isolated_run_live_import(tmp_path) as (run_live, _import_config):
        project_env = _legacy_project_env(
            tmp_path,
            dry_run=False,
            runtime_mode=None,
        )
        defaults_path = tmp_path / "missing-defaults.json"
        platform_config.set_project_env_config(project_env)
        run_live.PROJECT_ENV_CONFIG = project_env
        monkeypatch.setattr(
            sys,
            "argv",
            ["run_live.py", "--defaults", str(defaults_path)],
        )
        build_context = Mock(
            side_effect=AssertionError("default legacy app context built")
        )
        monkeypatch.setattr(run_live, "build_app_context", build_context)

        assert (
            run_live.runtime_mode_from_env(defaults_path=defaults_path)
            is RuntimeMode.LEGACY_APP
        )
        with pytest.raises(
            LiveRuntimeError,
            match="placeholder_private_credentials exchange=okx",
        ):
            asyncio.run(run_live.main())

    build_context.assert_not_called()
