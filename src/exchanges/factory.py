from __future__ import annotations

from src.exchanges.errors import UnsupportedExchangeError
from src.exchanges.http import StdlibHttpClient
from src.exchanges.models import ExchangeConfig, ExchangeName
from src.exchanges.ports import ExchangeClient, HttpClient


def normalize_exchange_name(exchange: ExchangeName | str) -> ExchangeName:
    if isinstance(exchange, ExchangeName):
        return exchange
    try:
        return ExchangeName(str(exchange).strip().lower())
    except ValueError as exc:
        raise UnsupportedExchangeError(f"Unsupported exchange: {exchange!r}") from exc


def create_exchange_client(
    exchange: ExchangeName | str,
    config: ExchangeConfig | None = None,
    *,
    http_client: HttpClient | None = None,
) -> ExchangeClient:
    """Create a unified OKX/Binance exchange client.

    Business code should call this factory and then use the returned
    ExchangeClient protocol. That keeps adapter imports out of strategy/runtime.
    """

    exchange_name = normalize_exchange_name(exchange)
    cfg = config or ExchangeConfig()
    http = http_client or StdlibHttpClient()

    if exchange_name == ExchangeName.OKX:
        from src.exchanges.okx.client import OkxExchangeClient

        return OkxExchangeClient(config=cfg, http_client=http)

    if exchange_name == ExchangeName.BINANCE:
        from src.exchanges.binance.client import BinanceExchangeClient

        return BinanceExchangeClient(config=cfg, http_client=http)

    raise UnsupportedExchangeError(f"Unsupported exchange: {exchange_name.value}")
