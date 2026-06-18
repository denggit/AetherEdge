from __future__ import annotations

from typing import Any, Mapping, Protocol

from src.exchanges.models import (
    Balance,
    CancelOrderRequest,
    ExchangeName,
    Kline,
    Order,
    OrderRequest,
    Position,
    Ticker,
)


class HttpClient(Protocol):
    """Small HTTP port so adapters can be tested without real network calls."""

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_seconds: float | None = None,
    ) -> Any:
        ...


class ExchangeClient(Protocol):
    """Unified exchange interface used by business code.

    Business modules should depend on this protocol only. They should not import
    OKX/Binance adapters directly and should never build exchange REST payloads.
    """

    @property
    def exchange(self) -> ExchangeName:
        ...

    async def get_server_time_ms(self) -> int:
        ...

    async def fetch_klines(
        self,
        symbol: str,
        *,
        interval: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[Kline]:
        ...

    async def fetch_ticker(self, symbol: str) -> Ticker:
        ...

    async def fetch_balance(self, asset: str = "USDT") -> Balance:
        ...

    async def fetch_positions(self, symbol: str | None = None) -> list[Position]:
        ...

    async def place_order(self, request: OrderRequest) -> Order:
        ...

    async def cancel_order(self, request: CancelOrderRequest) -> Order:
        ...
