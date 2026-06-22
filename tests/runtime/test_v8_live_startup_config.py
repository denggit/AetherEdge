from __future__ import annotations

import json
from pathlib import Path

from src.app import AppConfig, build_app_context
from src.platform import ExchangeName
from src.runtime import RuntimeMode, live_runtime_config_from_app, runtime_mode_from_env
from strategies.eth_lf_portfolio_v8.strategy import Strategy


def test_aether_defaults_point_to_v8_live_runtime_with_safe_order_switches() -> None:
    defaults = json.loads(Path("config/aether_defaults.json").read_text(encoding="utf-8"))

    assert defaults["runtime_mode"] == "live_runtime"
    assert defaults["strategy"] == "strategies.eth_lf_portfolio_v8:Strategy"
    assert defaults["data_streams"] == ["trades"]
    assert defaults["dry_run"] is True
    assert defaults["exchanges"] == ["okx", "binance"]
    assert defaults["data_exchange"] == "okx"


def test_v8_live_env_loads_strategy_and_runtime_roles(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")
    environ = {
        "AETHER_RUNTIME_MODE": "live_runtime",
        "AETHER_MARKET": "ETH-USDT-PERP",
        "AETHER_STRATEGY": "strategies.eth_lf_portfolio_v8:Strategy",
        "AETHER_EXCHANGES": "okx,binance",
        "AETHER_DATA_EXCHANGE": "okx",
        "AETHER_MASTER_EXCHANGE": "okx",
        "AETHER_FOLLOWER_EXCHANGES": "binance",
        "AETHER_DATA_STREAMS": "trades",
        "AETHER_DRY_RUN": "true",
        "AETHER_LIVE_TRADING": "false",
    }

    app = AppConfig.from_env(env_file=env_file, environ=environ)
    runtime_mode = runtime_mode_from_env(env_file=env_file, environ=environ)
    runtime = live_runtime_config_from_app(app, env_file=env_file, environ=environ)
    context = build_app_context(app)

    assert runtime_mode is RuntimeMode.LIVE_RUNTIME
    assert app.strategy == "strategies.eth_lf_portfolio_v8:Strategy"
    assert app.data_streams == ("trades",)
    assert isinstance(context.strategy, Strategy)
    assert runtime.master_follower_policy is not None
    assert runtime.master_follower_policy.master_exchange is ExchangeName.OKX
    assert runtime.master_follower_policy.follower_exchanges == (ExchangeName.BINANCE,)
    req = context.strategy.runtime_requirements()
    assert req["order_book"]["enabled"] is False
    assert req["trades"]["stream_enabled"] is True
    assert req["trades"]["warmup_enabled"] is True
    assert req["range_bars"]["enabled"] is True
    assert req["private_account_stream"]["enabled"] is True
