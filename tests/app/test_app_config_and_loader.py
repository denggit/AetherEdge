from pathlib import Path

from src.app import AppConfig
from src.platform import ExchangeName
from src.strategy import load_strategy


def test_app_config_uses_defaults_and_env_for_runtime_choices(tmp_path):
    defaults = tmp_path / "defaults.json"
    defaults.write_text(
        '{"symbol":"ETH-USDT-PERP","exchanges":["okx"],"data_exchange":"okx",'
        '"strategy":"strategies.empty_strategy:Strategy","data_streams":["trades"],'
        '"state_db_path":"data/state.sqlite3","market_queue_maxsize":10,'
        '"signal_queue_maxsize":5,"alert_queue_maxsize":3,"dry_run":true,'
        '"enable_email_alerts":false}',
        encoding="utf-8",
    )

    config = AppConfig.from_env(
        defaults_path=defaults,
        environ={
            "AETHER_MARKET": "BTC-USDT-PERP",
            "AETHER_EXCHANGES": "okx,binance",
            "AETHER_DATA_EXCHANGE": "binance",
            "AETHER_DRY_RUN": "false",
            "AETHER_ENABLE_EMAIL_ALERT": "true",
        },
    )

    assert config.symbol == "BTC-USDT-PERP"
    assert config.exchanges == (ExchangeName.OKX, ExchangeName.BINANCE)
    assert config.data_exchange is ExchangeName.BINANCE
    assert config.dry_run is False
    assert config.enable_email_alerts is True


def test_strategy_loader_loads_empty_strategy_plugin():
    strategy = load_strategy("strategies.empty_strategy:Strategy")
    assert callable(strategy.on_trade)
