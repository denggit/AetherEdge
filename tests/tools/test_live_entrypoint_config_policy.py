from __future__ import annotations

import asyncio
import json
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import src.app.factory as app_factory
from src.platform import config as platform_config
from src.platform.config import load_project_env_config
from src.platform.data.websocket.connector import WebsocketsConnector
from src.platform.exchanges.errors import ExchangeConfigError
from src.platform.exchanges.http import RequestsHttpClient
import tools.exchange_connectivity_smoke as connectivity_smoke
import tools.preflight_check_v10b as preflight_v10b
import tools.run_live as tool_run_live
import tools.run_runtime_skeleton as runtime_skeleton
import tools.smoke_private_readonly as private_smoke
import tools.v8_live_preflight_check as preflight_v8


class StartupBoundaryReached(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def _restore_project_env_config():
    previous = platform_config._PROJECT_ENV_CONFIG
    try:
        yield
    finally:
        if previous is None:
            platform_config.reset_project_env_config_for_tests()
        else:
            platform_config.set_project_env_config(previous)


def _project_env(tmp_path, values):
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    return load_project_env_config(
        env_file=env_file,
        process_env=values,
    )


def _app_env(
    exchange: str = "okx",
    *,
    dry_run: bool = False,
    live_trading: bool = True,
    valid_credentials: bool = False,
    strategy: str = "strategies.empty_strategy:Strategy",
):
    values = {
        "AETHER_LIVE_TRADING": str(live_trading).lower(),
        "AETHER_DRY_RUN": str(dry_run).lower(),
        "AETHER_MARKET": "ETH-USDT-PERP",
        "AETHER_EXCHANGES": exchange,
        "AETHER_DATA_EXCHANGE": exchange,
        "AETHER_STRATEGY": strategy,
    }
    if exchange == "okx":
        values.update(
            {
                "OKX_API_KEY": (
                    "valid_fake_okx_api_key"
                    if valid_credentials
                    else "your_okx_api_key"
                ),
                "OKX_SECRET_KEY": "canary_secret",
                "OKX_PASSPHRASE": "canary_passphrase",
            }
        )
    elif valid_credentials:
        values.update(
            {
                "BINANCE_API_KEY": "valid_fake_binance_api_key",
                "BINANCE_SECRET_KEY": "canary_secret",
            }
        )
    return values


@pytest.mark.parametrize(
    ("exchange", "error_code", "field_name"),
    (
        ("okx", "placeholder_private_credentials", "api_key"),
        ("binance", "missing_private_credentials", "api_key"),
    ),
)
def test_tool_run_live_rejects_direct_live_before_startup_boundaries(
    tmp_path,
    monkeypatch,
    caplog,
    exchange,
    error_code,
    field_name,
):
    project_env = _project_env(tmp_path, _app_env(exchange))
    build_context = Mock()
    create_execution_client = Mock()
    requests_http = AsyncMock()
    websocket_connect = AsyncMock()
    monkeypatch.setattr(
        tool_run_live,
        "load_project_env_config",
        lambda **_kwargs: project_env,
    )
    monkeypatch.setattr(tool_run_live, "build_app_context", build_context)
    monkeypatch.setattr(
        app_factory,
        "create_execution_client",
        create_execution_client,
    )
    monkeypatch.setattr(RequestsHttpClient, "request", requests_http)
    monkeypatch.setattr(WebsocketsConnector, "connect", websocket_connect)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_live.py", "--defaults", str(tmp_path / "missing.json")],
    )

    with pytest.raises(ExchangeConfigError) as exc_info:
        asyncio.run(tool_run_live.main())

    text = str(exc_info.value)
    assert error_code in text
    assert f"exchange={exchange}" in text
    assert field_name in text
    assert "canary_secret" not in text
    assert "canary_passphrase" not in text
    assert "canary_secret" not in repr(exc_info.value)
    assert "canary_secret" not in caplog.text
    build_context.assert_not_called()
    create_execution_client.assert_not_called()
    requests_http.assert_not_awaited()
    websocket_connect.assert_not_awaited()


def test_tool_run_live_dry_run_uses_process_override_and_ignores_example(
    tmp_path,
    monkeypatch,
):
    (tmp_path / ".env").write_text(
        "AETHER_LIVE_TRADING=true\n"
        "AETHER_DRY_RUN=false\n"
        "AETHER_EXCHANGES=okx\n"
        "AETHER_DATA_EXCHANGE=okx\n"
        "OKX_API_KEY=your_okx_api_key\n",
        encoding="utf-8",
    )
    (tmp_path / ".env.example").write_text(
        "AETHER_DRY_RUN=false\n"
        "AETHER_EXAMPLE_ONLY=must_not_load\n"
        "OKX_SECRET_KEY=canary_example_secret\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AETHER_DRY_RUN", "true")
    monkeypatch.setenv("AETHER_STRATEGY", "strategies.empty_strategy:Strategy")
    monkeypatch.setenv("AETHER_MARKET", "ETH-USDT-PERP")
    monkeypatch.setattr(tool_run_live, "REPO_ROOT", tmp_path)
    built_configs = []

    def stop_at_build(config):
        built_configs.append(config)
        raise StartupBoundaryReached("build_app_context_reached")

    monkeypatch.setattr(tool_run_live, "build_app_context", stop_at_build)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_live.py", "--defaults", str(tmp_path / "missing.json")],
    )

    with pytest.raises(StartupBoundaryReached, match="build_app_context_reached"):
        asyncio.run(tool_run_live.main())

    snapshot = platform_config.get_project_env_config()
    assert built_configs[0].dry_run is True
    assert snapshot.get("AETHER_DRY_RUN") == "true"
    assert snapshot.get("AETHER_EXAMPLE_ONLY", "") == ""
    assert snapshot.example_file is None
    assert snapshot.source_files == (str(tmp_path / ".env"),)


def test_private_readonly_rejects_before_clients_and_network(
    tmp_path,
    monkeypatch,
    caplog,
):
    project_env = _project_env(tmp_path, _app_env("okx"))
    account_client = Mock()
    execution_client = Mock()
    snapshot = AsyncMock()
    http = AsyncMock()
    websocket = AsyncMock()
    monkeypatch.setattr(
        private_smoke,
        "load_project_env_config",
        lambda **_kwargs: project_env,
    )
    monkeypatch.setattr(private_smoke, "create_account_client", account_client)
    monkeypatch.setattr(
        private_smoke,
        "create_execution_client",
        execution_client,
    )
    monkeypatch.setattr(private_smoke, "fetch_platform_snapshot", snapshot)
    monkeypatch.setattr(RequestsHttpClient, "request", http)
    monkeypatch.setattr(WebsocketsConnector, "connect", websocket)
    monkeypatch.setattr(sys, "argv", ["smoke_private_readonly.py", "okx"])

    with pytest.raises(ExchangeConfigError) as exc_info:
        asyncio.run(private_smoke.main())

    text = str(exc_info.value)
    assert "placeholder_private_credentials" in text
    assert "exchange=okx" in text
    assert "placeholder_fields=api_key" in text
    assert "canary_secret" not in text
    assert "canary_passphrase" not in text
    assert "canary_secret" not in repr(exc_info.value)
    assert "canary_secret" not in caplog.text
    account_client.assert_not_called()
    execution_client.assert_not_called()
    snapshot.assert_not_awaited()
    http.assert_not_awaited()
    websocket.assert_not_awaited()


def test_private_readonly_reuses_validated_config_for_both_clients(
    tmp_path,
    monkeypatch,
):
    project_env = _project_env(
        tmp_path,
        _app_env("okx", valid_credentials=True),
    )
    account_client = Mock(return_value=object())
    execution_client = Mock(return_value=object())
    snapshot = AsyncMock(
        side_effect=StartupBoundaryReached("snapshot_reached")
    )
    monkeypatch.setattr(
        private_smoke,
        "load_project_env_config",
        lambda **_kwargs: project_env,
    )
    monkeypatch.setattr(private_smoke, "create_account_client", account_client)
    monkeypatch.setattr(
        private_smoke,
        "create_execution_client",
        execution_client,
    )
    monkeypatch.setattr(private_smoke, "fetch_platform_snapshot", snapshot)
    monkeypatch.setattr(sys, "argv", ["smoke_private_readonly.py", "okx"])

    with pytest.raises(StartupBoundaryReached, match="snapshot_reached"):
        asyncio.run(private_smoke.main())

    account_config = account_client.call_args.kwargs["config"]
    execution_config = execution_client.call_args.kwargs["config"]
    assert account_config is execution_config
    snapshot.assert_awaited_once()


def test_connectivity_smoke_validates_before_public_or_private_calls(
    tmp_path,
    monkeypatch,
):
    project_env = _project_env(tmp_path, _app_env("okx", dry_run=True))
    data_feed = Mock()
    account_client = Mock()
    execution_client = Mock()
    monkeypatch.setattr(
        connectivity_smoke,
        "load_project_env_config",
        lambda **_kwargs: project_env,
    )
    monkeypatch.setattr(connectivity_smoke, "create_market_data_feed", data_feed)
    monkeypatch.setattr(connectivity_smoke, "create_account_client", account_client)
    monkeypatch.setattr(
        connectivity_smoke,
        "create_execution_client",
        execution_client,
    )
    monkeypatch.setattr(sys, "argv", ["exchange_connectivity_smoke.py"])

    with pytest.raises(ExchangeConfigError, match="placeholder_private_credentials"):
        asyncio.run(connectivity_smoke.main())

    data_feed.assert_not_called()
    account_client.assert_not_called()
    execution_client.assert_not_called()


@pytest.mark.parametrize(
    "extra_args",
    ((), ("--live", "--skip-order-test")),
)
def test_connectivity_smoke_valid_dry_preview_preserves_order_flags(
    tmp_path,
    monkeypatch,
    extra_args,
):
    project_env = _project_env(
        tmp_path,
        _app_env("okx", dry_run=True, valid_credentials=True),
    )
    feed = SimpleNamespace(
        fetch_ticker=AsyncMock(
            return_value=SimpleNamespace(price=Decimal("2000"))
        )
    )
    account = SimpleNamespace(
        fetch_balance=AsyncMock(return_value=None),
        fetch_positions=AsyncMock(return_value=[]),
        fetch_position_mode=AsyncMock(return_value=None),
        set_position_mode=AsyncMock(return_value=None),
        set_margin_mode=AsyncMock(return_value=None),
        set_leverage=AsyncMock(return_value=None),
        fetch_leverage=AsyncMock(return_value=None),
    )
    coordinator = Mock()
    monkeypatch.setattr(
        connectivity_smoke,
        "load_project_env_config",
        lambda **_kwargs: project_env,
    )
    monkeypatch.setattr(
        connectivity_smoke,
        "create_market_data_feed",
        Mock(return_value=feed),
    )
    monkeypatch.setattr(
        connectivity_smoke,
        "create_account_client",
        Mock(return_value=account),
    )
    monkeypatch.setattr(
        connectivity_smoke,
        "create_execution_client",
        Mock(return_value=object()),
    )
    monkeypatch.setattr(
        connectivity_smoke,
        "MultiExchangeOrderCoordinator",
        coordinator,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["exchange_connectivity_smoke.py", *extra_args],
    )

    assert asyncio.run(connectivity_smoke.main()) == 0
    coordinator.assert_not_called()


def test_runtime_skeleton_validates_before_context_build(tmp_path, monkeypatch):
    project_env = _project_env(tmp_path, _app_env("okx"))
    build_context = Mock()
    runtime = Mock()
    monkeypatch.setattr(
        runtime_skeleton,
        "load_project_env_config",
        lambda **_kwargs: project_env,
    )
    monkeypatch.setattr(runtime_skeleton, "build_runtime_context", build_context)
    monkeypatch.setattr(runtime_skeleton, "PlatformRuntime", runtime)
    monkeypatch.setattr(sys, "argv", ["run_runtime_skeleton.py", "okx"])

    with pytest.raises(ExchangeConfigError, match="placeholder_private_credentials"):
        asyncio.run(runtime_skeleton.main())

    build_context.assert_not_called()
    runtime.assert_not_called()


@pytest.mark.parametrize("skip_api", (False, True))
def test_v8_preflight_private_validation_respects_skip_api(
    tmp_path,
    monkeypatch,
    skip_api,
):
    project_env = _project_env(
        tmp_path,
        {
            **_app_env(
                "okx",
                dry_run=True,
                strategy="strategies.eth_lf_portfolio_v8:Strategy",
            ),
            "AETHER_RUNTIME_MODE": "live_runtime",
        },
    )
    monkeypatch.setattr(
        preflight_v8,
        "load_project_env_config",
        lambda **_kwargs: project_env,
    )
    monkeypatch.setattr(preflight_v8, "load_strategy", Mock(return_value=object()))
    monkeypatch.setattr(
        preflight_v8,
        "resolve_strategy_runtime_requirements",
        Mock(return_value=object()),
    )
    for name in (
        "_check_runtime_config",
        "_check_strategy_identity",
        "_check_range_exit_config",
        "_check_local_writable",
        "_check_local_rangebar_builder",
    ):
        monkeypatch.setattr(preflight_v8, name, Mock())
    account_client = Mock()
    execution_client = Mock()
    monkeypatch.setattr(preflight_v8, "create_account_client", account_client)
    monkeypatch.setattr(preflight_v8, "create_execution_client", execution_client)
    report_path = tmp_path / f"v8-{skip_api}.json"
    argv = [
        "v8_live_preflight_check.py",
        "--defaults",
        str(tmp_path / "missing.json"),
        "--report",
        str(report_path),
        "--skip-kline",
    ]
    if skip_api:
        argv.append("--skip-api")
    monkeypatch.setattr(sys, "argv", argv)

    code = asyncio.run(preflight_v8.main())
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert code == (0 if skip_api else 1)
    assert "canary_secret" not in json.dumps(payload)
    if skip_api:
        assert not any(
            item["name"] == "private_credentials"
            for item in payload["checks"]
        )
    else:
        credential_check = next(
            item
            for item in payload["checks"]
            if item["name"] == "private_credentials"
        )
        assert credential_check["status"] == "fail"
        assert "placeholder_private_credentials" in credential_check["error"]
    account_client.assert_not_called()
    execution_client.assert_not_called()


def test_v10b_private_api_validates_all_credentials_before_clients(
    tmp_path,
    monkeypatch,
):
    from src.platform.account import factory as account_factory
    from src.platform.execution import factory as execution_factory
    from src.platform import snapshot as snapshot_module

    account_client = Mock()
    execution_client = Mock()
    fetch_snapshot = AsyncMock()
    monkeypatch.setattr(account_factory, "create_account_client", account_client)
    monkeypatch.setattr(
        execution_factory,
        "create_execution_client",
        execution_client,
    )
    monkeypatch.setattr(snapshot_module, "fetch_platform_snapshot", fetch_snapshot)
    report = preflight_v10b.PreflightReport()
    env = {
        **_app_env(
            "okx",
            strategy=preflight_v10b.EXPECTED_STRATEGY,
        ),
    }

    asyncio.run(
        preflight_v10b._check_api_position_safety(
            report,
            env=env,
            repo_root=Path(__file__).resolve().parents[2],
        )
    )

    check = next(item for item in report.checks if item.name == "api_position_check")
    assert check.status == "FAIL"
    assert "placeholder_private_credentials" in check.message
    assert "canary_secret" not in check.message
    account_client.assert_not_called()
    execution_client.assert_not_called()
    fetch_snapshot.assert_not_awaited()


def test_indirect_live_entrypoints_target_safe_runner_without_secrets(
    monkeypatch,
):
    import scripts.watchdog_live as watchdog

    monkeypatch.delenv("LIVE_SCRIPT", raising=False)
    monkeypatch.delenv("LIVE_ARGS", raising=False)
    command = watchdog.build_command()
    assert Path(command[2]).resolve() == (
        Path(__file__).resolve().parents[2] / "scripts" / "run_live.py"
    ).resolve()

    root = Path(__file__).resolve().parents[2]
    paths = (
        root / "scripts" / "run_live.sh",
        root / "scripts" / "watchdog_live.py",
        root / "scripts" / "watchdog_live.sh",
        root / "scripts" / "start_live_watchdog.sh",
    )
    combined = "\n".join(path.read_text(encoding="utf-8") for path in paths)
    assert "scripts/run_live.py" in combined
    assert ".env.example" not in combined
    for marker in ("API_KEY", "SECRET_KEY", "PASSPHRASE"):
        assert marker not in combined
