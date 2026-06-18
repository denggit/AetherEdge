from __future__ import annotations

from src.platform.execution.risk import ExecutionRiskGate, ExecutionRiskLimits, LiveTradingBlocked
from src.platform.execution.rules import normalize_amend_order_request, normalize_order_request
from src.platform.exchanges.models import AmendOrderRequest, CancelOrderRequest, ExchangeName, InstrumentRule, Order, OrderQuery, OrderRequest
from src.platform.exchanges.ports import ExchangeExecutionClient
from src.platform.markets import MarketProfile


class ExchangeExecutionService:
    """Execution facade bound to one exchange + one canonical market symbol."""

    def __init__(
        self,
        exchange_client: ExchangeExecutionClient,
        *,
        symbol: str,
        market_profile: MarketProfile,
        risk_limits: ExecutionRiskLimits | None = None,
        validate_orders: bool = True,
        sandbox: bool = False,
        live_trading_enabled: bool = False,
    ) -> None:
        self._exchange_client = exchange_client
        self._symbol = symbol
        self._market_profile = market_profile
        self._risk_gate = ExecutionRiskGate(risk_limits)
        self._validate_orders = validate_orders
        self._sandbox = sandbox
        self._live_trading_enabled = live_trading_enabled

    @property
    def exchange(self) -> ExchangeName:
        return self._exchange_client.exchange

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def market_profile(self) -> MarketProfile:
        return self._market_profile

    async def place_order(self, request: OrderRequest) -> Order:
        self._ensure_live_write_allowed("place_order")
        self._ensure_bound_symbol(request.symbol)
        rule = await self._fetch_rule_if_available(request.symbol)
        normalized = normalize_order_request(request, rule)
        if self._validate_orders:
            self._risk_gate.validate_order(normalized, rule)
        return await self._exchange_client.place_order(normalized)

    async def cancel_order(self, request: CancelOrderRequest) -> Order:
        self._ensure_bound_symbol(request.symbol)
        return await self._exchange_client.cancel_order(request)

    async def amend_order(self, request: AmendOrderRequest) -> Order:
        self._ensure_live_write_allowed("amend_order")
        self._ensure_bound_symbol(request.symbol)
        rule = await self._fetch_rule_if_available(request.symbol)
        normalized = normalize_amend_order_request(request, rule)
        if self._validate_orders:
            self._risk_gate.validate_amend(normalized, rule)
        return await self._exchange_client.amend_order(normalized)


    async def fetch_order_status(self, query: OrderQuery) -> Order:
        self._ensure_bound_symbol(query.symbol)
        return await self._exchange_client.fetch_order_status(query)

    async def fetch_open_orders(self) -> list[Order]:
        return await self._exchange_client.fetch_open_orders(self._symbol)

    async def replace_order(self, cancel_request: CancelOrderRequest, new_order: OrderRequest) -> Order:
        self._ensure_live_write_allowed("replace_order")
        await self.cancel_order(cancel_request)
        return await self.place_order(new_order)

    async def _fetch_rule_if_available(self, symbol: str) -> InstrumentRule | None:
        fetch_rule = getattr(self._exchange_client, "fetch_instrument_rule", None)
        if fetch_rule is None:
            return None
        rule = await fetch_rule(symbol)
        if rule.contract_value is None:
            profile_contract_value = self._market_profile.contract_value(self.exchange)
            if profile_contract_value is not None:
                return InstrumentRule(
                    exchange=rule.exchange,
                    symbol=rule.symbol,
                    raw_symbol=rule.raw_symbol,
                    price_tick=rule.price_tick,
                    quantity_step=rule.quantity_step,
                    min_quantity=rule.min_quantity,
                    min_notional=rule.min_notional,
                    max_quantity=rule.max_quantity,
                    contract_value=profile_contract_value,
                    raw=rule.raw,
                )
        return rule

    def _ensure_bound_symbol(self, symbol: str) -> None:
        if symbol != self._symbol:
            raise ValueError(f"execution client is bound to {self._symbol}, got {symbol}")

    def _ensure_live_write_allowed(self, action: str) -> None:
        if not self._sandbox and not self._live_trading_enabled:
            raise LiveTradingBlocked(
                f"{action} blocked: live trading requires AETHER_LIVE_TRADING=true or sandbox=true"
            )
