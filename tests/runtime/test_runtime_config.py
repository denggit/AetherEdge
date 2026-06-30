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


def test_live_runtime_config_loads_range_repair_cooldowns(tmp_path):
    cfg = live_runtime_config_from_app(
        _app_config(),
        defaults_path=tmp_path / "missing.json",
        environ={
            "AETHER_RANGE_REPAIR_FAILURE_COOLDOWN_SECONDS": "1234",
            "AETHER_RANGE_REPAIR_ARCHIVE_NOT_READY_COOLDOWN_SECONDS": "5678",
            "AETHER_RANGE_REPAIR_DAILY_RETRY_AFTER_UTC_HOUR": "2",
        },
    )

    assert cfg.range_repair_failure_cooldown_seconds == 1234
    assert cfg.range_repair_archive_not_ready_cooldown_seconds == 5678
    assert cfg.range_repair_daily_retry_after_utc_hour == 2


def test_live_runtime_config_injected_environ_ignores_project_master_follower_env(tmp_path, monkeypatch):
    def fake_load_env_config(env_file=None, *, environ=None):
        values = {"AETHER_FOLLOWER_EXCHANGES": "binance"}
        values.update({str(key): str(value) for key, value in (environ or {}).items()})
        return values

    monkeypatch.setattr("src.runtime.config.load_env_config", fake_load_env_config)

    cfg = live_runtime_config_from_app(
        _app_config(),
        defaults_path=tmp_path / "missing.json",
        environ={"AETHER_RUNTIME_MODE": "live_runtime"},
    )

    assert cfg.master_follower_policy is not None
    assert cfg.master_follower_policy.master_exchange is ExchangeName.OKX
    assert cfg.master_follower_policy.follower_exchanges == ()
