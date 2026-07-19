from __future__ import annotations

from dataclasses import replace
from typing import Sequence

from src.order_management.models import ExchangeOrderResult, OrderIntent
from src.order_management.ports import ExecutionResultRecorderPort
from src.order_management.quantity import (
    NativeQuantityConverter,
    resolve_executable_base_quantity,
)
from src.platform.execution import ExecutionClient
from src.signals.models import SignalAction


from src.order_management.coordinator.support import (
    _client_market_profile,
    _optional_decimal,
    _signal_exchange_quantity,
)


class OrderIntentPlanner:
    def __init__(
        self,
        *,
        clients: Sequence[ExecutionClient],
        quantity_converter: NativeQuantityConverter,
        result_recorder: ExecutionResultRecorderPort,
    ) -> None:
        self.clients = tuple(clients)
        self.quantity_converter = quantity_converter
        self._result_recorder = result_recorder

    async def normalize_recovery_topup_intent(
        self, intent: OrderIntent
    ) -> tuple[OrderIntent, list[ExchangeOrderResult] | None]:
        """Normalize follower top-ups before planning and turn dust into a no-op."""
    
        signal = intent.signal
        purpose = str(
            signal.metadata.get("execution_purpose", "")
            if signal.metadata
            else ""
        ).strip().lower()
        if purpose != "follower_recovery_topup" or signal.action not in {
            SignalAction.OPEN_LONG,
            SignalAction.OPEN_SHORT,
        }:
            return intent, None
    
        client_by_exchange = {client.exchange: client for client in self.clients}
        reference_price = _optional_decimal(
            signal.metadata.get("reference_price") if signal.metadata else None
        )
        resolutions = {}
        for exchange in intent.target_exchanges:
            client = client_by_exchange.get(exchange)
            profile = _client_market_profile(client) if client is not None else None
            if client is None or profile is None:
                return intent, None
            fetch_rule = getattr(client, "fetch_instrument_rule", None)
            rule = await fetch_rule() if callable(fetch_rule) else None
            raw_quantity = _signal_exchange_quantity(
                signal, exchange, fallback=signal.quantity
            )
            resolutions[exchange] = resolve_executable_base_quantity(
                exchange=exchange,
                symbol=signal.symbol,
                raw_base_quantity=raw_quantity,
                market_profile=profile,
                instrument_rule=rule,
                reference_price=reference_price,
                quantity_converter=self.quantity_converter,
            )
    
        executable = {
            exchange: resolution
            for exchange, resolution in resolutions.items()
            if resolution.executable
        }
        if not executable:
            return intent, self._result_recorder.record_skipped_recovery_topup(
                intent=intent,
                resolutions=resolutions,
            )
    
        # Recovery top-ups are normally single-venue.  Still preserve exact
        # per-exchange quantities if a caller supplies more than one target.
        normalized_quantities = {
            exchange.value: str(resolution.normalized_base_quantity)
            for exchange, resolution in executable.items()
        }
        metadata = {
            **dict(signal.metadata or {}),
            "exchange_quantities_base": normalized_quantities,
            "target_exchanges": [exchange.value for exchange in executable],
            "coordinator_quantity_normalized": True,
        }
        normalized_signal = replace(
            signal,
            quantity=(
                next(iter(executable.values())).normalized_base_quantity
                if len(executable) == 1
                else signal.quantity
            ),
            metadata=metadata,
        )
        return replace(
            intent,
            signal=normalized_signal,
            target_exchanges=tuple(executable),
        ), None

    async def _normalize_recovery_topup_intent(
        self,
        intent: OrderIntent,
    ) -> tuple[OrderIntent, list[ExchangeOrderResult] | None]:
        """Compatibility alias for callers predating service composition."""

        return await self.normalize_recovery_topup_intent(intent)


__all__ = ["OrderIntentPlanner"]
