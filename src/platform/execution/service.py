from __future__ import annotations

from decimal import Decimal

from src.platform.execution.risk import ExecutionRiskGate, ExecutionRiskLimits, LiveTradingBlocked
from src.platform.execution.rules import (
    normalize_amend_order_request,
    normalize_order_request,
    normalize_stop_market_order_request,
)
from src.platform.exchanges.models import (
    AmendOrderRequest,
    CancelOrderRequest,
    CancelStopOrderRequest,
    ExchangeName,
    InstrumentRule,
    Order,
    OrderQuery,
    OrderRequest,
    OrderSide,
    Position,
    PositionMode,
    PositionSide,
    StopMarketOrderRequest,
    StopOrderQuery,
    TriggerPriceType,
)
from src.platform.exchanges.ports import ExchangeExecutionClient
from src.platform.markets import MarketProfile
from src.utils.log import get_logger

logger = get_logger(__name__)


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
        logger.info(
            "Placing order | exchange=%s symbol=%s side=%s quantity=%s type=%s",
            self.exchange.value,
            normalized.symbol,
            normalized.side.value,
            normalized.quantity,
            normalized.order_type.value,
        )
        return await self._exchange_client.place_order(normalized)

    async def place_stop_market_order(self, request: StopMarketOrderRequest) -> Order:
        self._ensure_live_write_allowed("place_stop_market_order")
        self._ensure_bound_symbol(request.symbol)
        rule = await self._fetch_rule_if_available(request.symbol)
        normalized = normalize_stop_market_order_request(request, rule)
        if self._validate_orders:
            self._risk_gate.validate_stop_market(normalized, rule)
        logger.info(
            "Placing stop market order | exchange=%s symbol=%s side=%s quantity=%s trigger_price=%s",
            self.exchange.value,
            normalized.symbol,
            normalized.side.value,
            normalized.quantity,
            normalized.trigger_price,
        )
        return await self._exchange_client.place_stop_market_order(normalized)

    async def place_stop_loss_for_position(
        self,
        position: Position,
        *,
        trigger_price: Decimal,
        client_order_id: str | None = None,
        trigger_price_type: TriggerPriceType = TriggerPriceType.LAST,
    ) -> Order:
        self._ensure_bound_symbol(position.symbol)
        quantity = abs(position.quantity)
        if quantity <= 0:
            raise ValueError("position quantity is zero; cannot place stop loss")
        close_side = _close_side_for_position(position)
        position_side = None if position.side == PositionSide.BOTH else position.side
        close_position = self.exchange == ExchangeName.BINANCE
        return await self.place_stop_market_order(
            StopMarketOrderRequest(
                symbol=position.symbol,
                side=close_side,
                trigger_price=trigger_price,
                quantity=None if close_position else quantity,
                client_order_id=client_order_id,
                reduce_only=not close_position,
                position_side=position_side,
                trigger_price_type=trigger_price_type,
                close_position=close_position,
            )
        )

    async def cancel_order(self, request: CancelOrderRequest) -> Order:
        self._ensure_live_write_allowed("cancel_order")
        self._ensure_bound_symbol(request.symbol)
        logger.info("Canceling order | exchange=%s symbol=%s order_id=%s client_order_id=%s", self.exchange.value, request.symbol, request.order_id, request.client_order_id)
        return await self._exchange_client.cancel_order(request)

    async def cancel_all_orders(self) -> list[Order]:
        self._ensure_live_write_allowed("cancel_all_orders")
        logger.info("Canceling all orders | exchange=%s symbol=%s", self.exchange.value, self._symbol)
        return await self._exchange_client.cancel_all_orders(self._symbol)

    async def cancel_stop_order(self, request: CancelStopOrderRequest) -> Order:
        self._ensure_live_write_allowed("cancel_stop_order")
        self._ensure_bound_symbol(request.symbol)
        logger.info("Canceling stop order | exchange=%s symbol=%s stop_order_id=%s client_order_id=%s", self.exchange.value, request.symbol, request.stop_order_id, request.client_order_id)
        return await self._exchange_client.cancel_stop_order(request)

    async def cancel_all_stop_orders(self) -> list[Order]:
        self._ensure_live_write_allowed("cancel_all_stop_orders")
        logger.info("Canceling all stop orders | exchange=%s symbol=%s", self.exchange.value, self._symbol)
        return await self._exchange_client.cancel_all_stop_orders(self._symbol)

    async def amend_order(self, request: AmendOrderRequest) -> Order:
        self._ensure_live_write_allowed("amend_order")
        self._ensure_bound_symbol(request.symbol)
        rule = await self._fetch_rule_if_available(request.symbol)
        normalized = normalize_amend_order_request(request, rule)
        if self._validate_orders:
            self._risk_gate.validate_amend(normalized, rule)
        logger.info("Amending order | exchange=%s symbol=%s order_id=%s client_order_id=%s", self.exchange.value, normalized.symbol, normalized.order_id, normalized.client_order_id)
        return await self._exchange_client.amend_order(normalized)

    async def fetch_order_status(self, query: OrderQuery) -> Order:
        self._ensure_bound_symbol(query.symbol)
        return await self._exchange_client.fetch_order_status(query)

    async def fetch_open_orders(self) -> list[Order]:
        return await self._exchange_client.fetch_open_orders(self._symbol)

    async def fetch_stop_order_status(self, query: StopOrderQuery) -> Order:
        self._ensure_bound_symbol(query.symbol)
        return await self._exchange_client.fetch_stop_order_status(query)

    async def fetch_open_stop_orders(self) -> list[Order]:
        return await self._exchange_client.fetch_open_stop_orders(self._symbol)

    async def fetch_positions(self) -> list[Position]:
        fetch_positions = getattr(self._exchange_client, "fetch_positions", None)
        if fetch_positions is None:
            return []
        return await fetch_positions(self._symbol)

    async def fetch_position_mode(self) -> PositionMode:
        fetch_position_mode = getattr(self._exchange_client, "fetch_position_mode", None)
        if fetch_position_mode is None:
            return PositionMode.ONE_WAY
        return await fetch_position_mode()

    async def fetch_instrument_rule(self) -> InstrumentRule | None:
        """Expose the bound instrument rule through the execution facade."""

        return await self._fetch_rule_if_available(self._symbol)

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
            logger.warning("Live write blocked | exchange=%s symbol=%s action=%s", self.exchange.value, self._symbol, action)
            raise LiveTradingBlocked(
                f"{action} blocked: live trading requires AETHER_LIVE_TRADING=true or sandbox=true"
            )


def _close_side_for_position(position: Position) -> OrderSide:
    if position.side == PositionSide.LONG:
        return OrderSide.SELL
    if position.side == PositionSide.SHORT:
        return OrderSide.BUY
    if position.quantity > 0:
        return OrderSide.SELL
    if position.quantity < 0:
        return OrderSide.BUY
    raise ValueError("position quantity is zero; cannot infer close side")
