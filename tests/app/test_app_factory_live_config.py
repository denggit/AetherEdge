from __future__ import annotations

from src.app import AppConfig
from src.app.factory import build_app_context
from src.platform import ExchangeName


class FakeStrategy:
    async def on_start(self, snapshot):
        return []

    async def on_kline(self, kline):
        return []

    async def on_ticker(self, ticker):
        return []

    async def on_trade(self, trade):
        return []

    async def on_order_book(self, order_book):
        return []

    async def on_account_event(self, event):
        return []


def _config(exchange: ExchangeName) -> AppConfig:
    return AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(exchange,),
        data_exchange=exchange,
        strategy="unused",
        data_streams=("trades",),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )


def test_app_factory_passes_okx_env_exchange_config_to_data_feed(monkeypatch):
    captured = {}

    def fake_data_feed(exchange, *, symbol, config, **kwargs):
        captured["exchange"] = exchange
        captured["sandbox"] = config.sandbox
        return object()

    monkeypatch.setenv("OKX_SANDBOX", "true")
    monkeypatch.setattr("src.app.factory.create_market_data_feed", fake_data_feed)
    monkeypatch.setattr("src.app.factory.create_execution_client", lambda *args, **kwargs: object())
    monkeypatch.setattr("src.app.factory.load_strategy", lambda path: FakeStrategy())
    monkeypatch.setattr("src.app.factory.SqliteStateStore", lambda path: object())

    build_app_context(_config(ExchangeName.OKX))

    assert captured == {"exchange": ExchangeName.OKX, "sandbox": True}


def test_app_factory_passes_binance_env_exchange_config_to_data_feed(monkeypatch):
    captured = {}

    def fake_data_feed(exchange, *, symbol, config, **kwargs):
        captured["exchange"] = exchange
        captured["sandbox"] = config.sandbox
        return object()

    monkeypatch.setenv("BINANCE_SANDBOX", "true")
    monkeypatch.setattr("src.app.factory.create_market_data_feed", fake_data_feed)
    monkeypatch.setattr("src.app.factory.create_execution_client", lambda *args, **kwargs: object())
    monkeypatch.setattr("src.app.factory.load_strategy", lambda path: FakeStrategy())
    monkeypatch.setattr("src.app.factory.SqliteStateStore", lambda path: object())

    build_app_context(_config(ExchangeName.BINANCE))

    assert captured == {"exchange": ExchangeName.BINANCE, "sandbox": True}
