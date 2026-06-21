from __future__ import annotations

from src.app import AppConfig
from src.platform import ExchangeName
from src.runtime import RuntimeMode, live_runtime_config_from_app, runtime_mode_from_env


def _app_config() -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="unused",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )


def test_runtime_mode_defaults_to_legacy_app(tmp_path):
    assert runtime_mode_from_env(defaults_path=tmp_path / "missing.json", environ={}) is RuntimeMode.LEGACY_APP


def test_runtime_mode_can_be_enabled_from_environment(tmp_path):
    assert runtime_mode_from_env(defaults_path=tmp_path / "missing.json", environ={"AETHER_RUNTIME_MODE": "live_runtime"}) is RuntimeMode.LIVE_RUNTIME


def test_live_runtime_config_wraps_existing_app_config(tmp_path):
    cfg = live_runtime_config_from_app(
        _app_config(),
        defaults_path=tmp_path / "missing.json",
        environ={"AETHER_RUNTIME_MODE": "live_runtime", "AETHER_BACKGROUND_QUEUE_MAXSIZE": "7"},
    )

    assert cfg.mode is RuntimeMode.LIVE_RUNTIME
    assert cfg.symbol == "ETH-USDT-PERP"
    assert cfg.background_queue_maxsize == 7
