from __future__ import annotations

from src.order_management.safety import (
    ExitSafetyGuard,
    is_exit_action,
    normalize_exit_request_for_exchange,
)
from src.platform.execution import ExecutionClient
from src.platform.exchanges.models import (
    ExchangeName,
    OrderRequest,
    PositionMode,
    PositionSide,
    StopMarketOrderRequest,
)
from src.signals.models import SignalAction
from src.utils.log import get_logger

logger = get_logger(__name__)


from src.order_management.coordinator.support import (
    _client_market_profile,
    _client_positions,
    _log_exchange_exit_normalization,
    _with_position_side_for_mode,
)


class OrderSafetyValidator:
    def __init__(self, *, exit_safety_guard: ExitSafetyGuard) -> None:
        self.exit_safety_guard = exit_safety_guard
        self._position_mode_cache: dict[ExchangeName, PositionMode] = {}

    async def normalize_order(self, client: ExecutionClient, action: SignalAction, request: OrderRequest) -> OrderRequest:
        position_mode = await self._position_mode_for_client(client)
        if is_exit_action(action) and self._client_supports_exit_safety(client):
            profile = _client_market_profile(client)
            assert profile is not None
            positions = await _client_positions(client)
            normalized, report = self.exit_safety_guard.normalize_order(
                exchange=client.exchange,
                action=action,
                request=request,
                position_mode=position_mode,
                positions=positions,
                market_profile=profile,
            )
            if report is not None:
                logger.info("Exit safety approved order | %s", report.as_log_fields())
            exchange_normalized = normalize_exit_request_for_exchange(
                exchange=client.exchange,
                action=action,
                request=normalized,
                position_mode=position_mode,
                safety_report=report,
            )
            if exchange_normalized.metadata:
                _log_exchange_exit_normalization(exchange_normalized.metadata)
            return exchange_normalized.request  # type: ignore[return-value]
        return _with_position_side_for_mode(request, action=action, exchange=client.exchange, position_mode=position_mode)

    async def normalize_stop(self, client: ExecutionClient, action: SignalAction, request: StopMarketOrderRequest) -> StopMarketOrderRequest:
        position_mode = await self._position_mode_for_client(client)
        if is_exit_action(action) and self._client_supports_exit_safety(client):
            profile = _client_market_profile(client)
            assert profile is not None
            positions = await _client_positions(client)
            normalized, report = self.exit_safety_guard.normalize_stop_market(
                exchange=client.exchange,
                action=action,
                request=request,
                position_mode=position_mode,
                positions=positions,
                market_profile=profile,
            )
            if report is not None:
                logger.info("Exit safety approved stop order | %s", report.as_log_fields())
            exchange_normalized = normalize_exit_request_for_exchange(
                exchange=client.exchange,
                action=action,
                request=normalized,
                position_mode=position_mode,
                safety_report=report,
            )
            if exchange_normalized.metadata:
                _log_exchange_exit_normalization(exchange_normalized.metadata)
            return exchange_normalized.request  # type: ignore[return-value]
        return _with_position_side_for_mode(request, action=action, exchange=client.exchange, position_mode=position_mode)

    async def _position_mode_for_client(self, client: ExecutionClient) -> PositionMode:
        cached = self._position_mode_cache.get(client.exchange)
        if cached is not None:
            return cached
        fetch_position_mode = getattr(client, "fetch_position_mode", None)
        if callable(fetch_position_mode):
            mode = await fetch_position_mode()
            if not isinstance(mode, PositionMode):
                mode = PositionMode(str(mode).strip().lower())
            self._position_mode_cache[client.exchange] = mode
            return mode
        positions = await _client_positions(client)
        mode = PositionMode.HEDGE if any(position.side in {PositionSide.LONG, PositionSide.SHORT} for position in positions) else PositionMode.ONE_WAY
        self._position_mode_cache[client.exchange] = mode
        return mode

    def _client_supports_exit_safety(self, client: ExecutionClient) -> bool:
        return _client_market_profile(client) is not None and callable(getattr(client, "fetch_positions", None))

    async def _normalize_order_for_client(
        self,
        client: ExecutionClient,
        action: SignalAction,
        request: OrderRequest,
    ) -> OrderRequest:
        return await self.normalize_order(client, action, request)

    async def _normalize_stop_for_client(
        self,
        client: ExecutionClient,
        action: SignalAction,
        request: StopMarketOrderRequest,
    ) -> StopMarketOrderRequest:
        return await self.normalize_stop(client, action, request)


__all__ = ["OrderSafetyValidator"]
