from __future__ import annotations

from typing import Any, Mapping, Protocol

from src.platform.exchanges.models import (
    AmendOrderRequest,
    Balance,
    CancelOrderRequest,
    CancelStopOrderRequest,
    ExchangeName,
    InstrumentRule,
    Kline,
    LeverageInfo,
    LeverageRequest,
    MarginMode,
    Order,
    OrderQuery,
    OrderRequest,
    Position,
    PositionMode,
    StopMarketOrderRequest,
    StopOrderQuery,
    Ticker,
    Trade,
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

    async def fetch_trades(
        self,
        symbol: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
        oldest_first: bool = True,
        max_pages: int | None = None,
    ) -> list[Trade]:
        ...

    async def fetch_trades_between_ids(
        self,
        symbol: str,
        *,
        newer_trade_id: str,
        older_trade_id: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 100,
        max_pages: int = 20,
        oldest_first: bool = True,
    ) -> list[Trade]:
        ...

    async def fetch_instrument_rule(self, symbol: str) -> InstrumentRule:
        ...


class ExchangeExecutionClient(ExchangeIdentity, Protocol):
    async def place_order(self, request: OrderRequest) -> Order:
        ...

    async def place_stop_market_order(self, request: StopMarketOrderRequest) -> Order:
        ...

    async def cancel_order(self, request: CancelOrderRequest) -> Order:
        ...

    async def cancel_all_orders(self, symbol: str) -> list[Order]:
        ...

    async def amend_order(self, request: AmendOrderRequest) -> Order:
        ...

    async def fetch_order_status(self, query: OrderQuery) -> Order:
        ...

    async def fetch_open_orders(self, symbol: str) -> list[Order]:
        ...

    async def fetch_stop_order_status(self, query: StopOrderQuery) -> Order:
        ...

    async def fetch_open_stop_orders(self, symbol: str) -> list[Order]:
        ...

    async def cancel_stop_order(self, request: CancelStopOrderRequest) -> Order:
        ...

    async def cancel_all_stop_orders(self, symbol: str) -> list[Order]:
        ...


class ExchangeAccountClient(ExchangeIdentity, Protocol):
    async def fetch_balance(self, asset: str = "USDT") -> Balance:
        ...

    async def fetch_positions(self, symbol: str | None = None) -> list[Position]:
        ...

    async def fetch_leverage(self, symbol: str, *, margin_mode: MarginMode = MarginMode.CROSS) -> LeverageInfo:
        ...

    async def set_leverage(self, request: LeverageRequest) -> LeverageInfo:
        ...

    async def set_margin_mode(self, symbol: str, margin_mode: MarginMode) -> Mapping[str, Any]:
        ...

    async def fetch_position_mode(self) -> PositionMode:
        ...

    async def set_position_mode(self, mode: PositionMode) -> PositionMode:
        ...


class ExchangeClient(ExchangeMarketDataClient, ExchangeExecutionClient, ExchangeAccountClient, Protocol):
    """Full exchange adapter port.

    Application code should usually depend on one of the narrower facades:
    data.MarketDataFeed, execution.ExecutionClient, or account.AccountClient.
    """
