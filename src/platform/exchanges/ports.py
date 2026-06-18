from __future__ import annotations

from typing import Any, Mapping, Protocol

from src.platform.exchanges.models import (
    AmendOrderRequest,
    Balance,
    CancelOrderRequest,
    ExchangeName,
    InstrumentRule,
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


class ExchangeIdentity(Protocol):
    @property
    def exchange(self) -> ExchangeName:
        ...


class ExchangeMarketDataClient(ExchangeIdentity, Protocol):
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
        oldest_first: bool = False,
    ) -> list[Kline]:
        ...

    async def fetch_ticker(self, symbol: str) -> Ticker:
        ...

    async def fetch_instrument_rule(self, symbol: str) -> InstrumentRule:
        ...


class ExchangeExecutionClient(ExchangeIdentity, Protocol):
    async def place_order(self, request: OrderRequest) -> Order:
        ...

    async def cancel_order(self, request: CancelOrderRequest) -> Order:
        ...

    async def amend_order(self, request: AmendOrderRequest) -> Order:
        ...


class ExchangeAccountClient(ExchangeIdentity, Protocol):
    async def fetch_balance(self, asset: str = "USDT") -> Balance:
        ...

    async def fetch_positions(self, symbol: str | None = None) -> list[Position]:
        ...


class ExchangeClient(ExchangeMarketDataClient, ExchangeExecutionClient, ExchangeAccountClient, Protocol):
    """Full exchange adapter port.

    Application code should usually depend on one of the narrower facades:
    data_feed.MarketDataFeed, execution.ExecutionClient, or account.AccountClient.
    """
