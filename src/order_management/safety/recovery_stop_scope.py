"""Unambiguous legacy stop scope resolution for recovery and preflight.

This module answers one question: *does a real exchange stop order belong to a
specific logical position scope when the local PositionPlan has no stop IDs?*

It is shared by preflight (read-only) and runtime recovery (can write back).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Sequence

from src.order_management.quantity import NativeQuantityConverter
from src.order_management.safety.recovery_exit_validator import (
    RecoveryExitOrderValidator,
    is_bot_owned_order,
)
from src.order_management.safety.scoped_stop_recovery import (
    filter_orders_for_position_scope,
    order_matches_position_scope,
)
from src.platform.exchanges.models import (
    ExchangeName,
    InstrumentRule,
    Order,
    OrderStatus,
    PositionMode,
    PositionSide,
)
from src.platform.markets import MarketProfile

_ACTIVE_STATUSES = {OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED}


class StopScopeResolutionStatus(str, Enum):
    EXACT = "exact"
    ADOPTABLE_LEGACY = "adoptable_legacy"
    MISSING = "missing"
    INVALID = "invalid"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True)
class StopScopeResolution:
    """The result of resolving a stop order's ownership to a position scope."""

    status: StopScopeResolutionStatus
    exchange: str
    position_id: str
    order: Order | None = None
    order_id: str | None = None
    client_order_id: str | None = None
    effective_stop_price: Decimal | None = None
    canonical_theoretical_stop_price: Decimal | None = None
    price_tick: Decimal | None = None
    detail: dict[str, Any] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()

    @property
    def is_adoptable(self) -> bool:
        return self.status in {
            StopScopeResolutionStatus.EXACT,
            StopScopeResolutionStatus.ADOPTABLE_LEGACY,
        }

    @property
    def is_blocking(self) -> bool:
        return self.status in {
            StopScopeResolutionStatus.AMBIGUOUS,
            StopScopeResolutionStatus.INVALID,
        }

    def audit_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "exchange": self.exchange,
            "position_id": self.position_id,
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "effective_stop_price": (
                None
                if self.effective_stop_price is None
                else _decimal_text(self.effective_stop_price)
            ),
            "canonical_theoretical_stop_price": (
                None
                if self.canonical_theoretical_stop_price is None
                else _decimal_text(self.canonical_theoretical_stop_price)
            ),
            "price_tick": (
                None
                if self.price_tick is None
                else _decimal_text(self.price_tick)
            ),
            "warnings": list(self.warnings),
        }


class RecoveryStopScopeResolver:
    """Resolve whether a real exchange stop belongs to a position scope.

    This is the **only** place that decides whether a stop order with no
    matching local PositionPlan stop IDs can be safely adopted.  Both
    preflight and runtime recovery use the same resolver so the safety
    rules are applied consistently.
    """

    def __init__(
        self,
        *,
        validator: RecoveryExitOrderValidator | None = None,
        converter: NativeQuantityConverter | None = None,
    ) -> None:
        self._validator = validator or RecoveryExitOrderValidator(
            quantity_converter=converter or NativeQuantityConverter(),
        )
        self._converter = converter or NativeQuantityConverter()

    # ── Public API ──────────────────────────────────────────────────────

    def resolve(
        self,
        *,
        exchange: ExchangeName | str,
        symbol: str,
        strategy_id: str,
        position_id: str,
        position_side: PositionSide,
        position_mode: PositionMode,
        current_position_native_quantity: Decimal,
        canonical_stop_price: Decimal,
        open_stop_orders: Sequence[Order],
        known_stop_order_ids: tuple[str | None, ...] = (),
        market_profile: MarketProfile,
        instrument_rule: InstrumentRule | None = None,
        active_plan_count_same_exchange_side: int = 1,
        open_positions_on_exchange: Sequence[Any] = (),
    ) -> StopScopeResolution:
        """Resolve stop scope ownership.

        Returns a ``StopScopeResolution`` whose ``status`` indicates whether
        the stop is exactly matched, adoptable as legacy, missing, invalid,
        or ambiguous.
        """
        exchange_name = (
            exchange
            if isinstance(exchange, ExchangeName)
            else ExchangeName(str(exchange).strip().lower())
        )

        # ── Step 1: Exact match via known IDs ──────────────────────────
        exact_matches = filter_orders_for_position_scope(
            open_stop_orders,
            position_id=position_id,
            known_order_ids=known_stop_order_ids,
        )

        if exact_matches:
            # If exactly one bot-owned order matches exactly, it's EXACT.
            bot_exact = [
                o
                for o in exact_matches
                if is_bot_owned_order(
                    order=o,
                    strategy_id=strategy_id,
                    position_id=position_id,
                )
            ]
            if len(bot_exact) == 1:
                order = bot_exact[0]
                return StopScopeResolution(
                    status=StopScopeResolutionStatus.EXACT,
                    exchange=exchange_name.value,
                    position_id=position_id,
                    order=order,
                    order_id=order.order_id,
                    client_order_id=order.client_order_id,
                    effective_stop_price=order.price,
                    canonical_theoretical_stop_price=canonical_stop_price,
                    price_tick=(
                        None
                        if instrument_rule is None
                        else instrument_rule.price_tick
                    ),
                    detail={
                        "match_method": "exact_known_ids",
                        "bot_owned": True,
                    },
                )
            if len(bot_exact) > 1:
                return self._ambiguous(
                    exchange_name=exchange_name,
                    position_id=position_id,
                    canonical_stop_price=canonical_stop_price,
                    instrument_rule=instrument_rule,
                    reason="multiple_exact_bot_matches",
                    candidate_count=len(bot_exact),
                )
            # Exact match exists but is not bot-owned → treat as missing
            # (we never adopt manual orders).
            return self._missing(
                exchange_name=exchange_name,
                position_id=position_id,
                canonical_stop_price=canonical_stop_price,
                instrument_rule=instrument_rule,
                reason="exact_match_not_bot_owned",
            )

        # ── Step 2: Check if we should attempt legacy adoption ─────────
        has_known_ids = any(
            oid is not None and str(oid).strip()
            for oid in known_stop_order_ids
        )
        if has_known_ids:
            # Known IDs exist but didn't match anything on the exchange.
            return self._missing(
                exchange_name=exchange_name,
                position_id=position_id,
                canonical_stop_price=canonical_stop_price,
                instrument_rule=instrument_rule,
                reason="known_stop_ids_not_found_on_exchange",
            )

        # ── Step 3: Legacy adoption candidate scan ─────────────────────
        return self._resolve_legacy(
            exchange_name=exchange_name,
            symbol=symbol,
            strategy_id=strategy_id,
            position_id=position_id,
            position_side=position_side,
            position_mode=position_mode,
            current_position_native_quantity=current_position_native_quantity,
            canonical_stop_price=canonical_stop_price,
            open_stop_orders=open_stop_orders,
            market_profile=market_profile,
            instrument_rule=instrument_rule,
            active_plan_count_same_exchange_side=active_plan_count_same_exchange_side,
            open_positions_on_exchange=open_positions_on_exchange,
        )

    # ── Legacy resolution ──────────────────────────────────────────────

    def _resolve_legacy(
        self,
        *,
        exchange_name: ExchangeName,
        symbol: str,
        strategy_id: str,
        position_id: str,
        position_side: PositionSide,
        position_mode: PositionMode,
        current_position_native_quantity: Decimal,
        canonical_stop_price: Decimal,
        open_stop_orders: Sequence[Order],
        market_profile: MarketProfile,
        instrument_rule: InstrumentRule | None,
        active_plan_count_same_exchange_side: int,
        open_positions_on_exchange: Sequence[Any],
    ) -> StopScopeResolution:
        # ── Filter to only stop orders on the target exchange ────────
        exchange_orders = [
            o
            for o in open_stop_orders
            if (
                getattr(o, "exchange", None) == exchange_name
                or str(o.raw.get("exchange", "")).lower() == exchange_name.value
            )
            and getattr(o, "symbol", None) == symbol
        ]

        if not exchange_orders:
            return self._missing(
                exchange_name=exchange_name,
                position_id=position_id,
                canonical_stop_price=canonical_stop_price,
                instrument_rule=instrument_rule,
                reason="no_stop_orders_on_exchange",
            )

        # ── Separate bot-owned from manual ───────────────────────────
        bot_orders = [
            o
            for o in exchange_orders
            if is_bot_owned_order(
                order=o,
                strategy_id=strategy_id,
                position_id=position_id,
            )
        ]
        manual_orders = [o for o in exchange_orders if o not in bot_orders]

        # ── Never adopt if there are manual orders on the exchange ──
        #     (could be the user's own stop — we must not touch it)
        if manual_orders:
            return self._missing(
                exchange_name=exchange_name,
                position_id=position_id,
                canonical_stop_price=canonical_stop_price,
                instrument_rule=instrument_rule,
                reason="manual_orders_present_on_exchange",
                manual_order_count=len(manual_orders),
            )

        if not bot_orders:
            return self._missing(
                exchange_name=exchange_name,
                position_id=position_id,
                canonical_stop_price=canonical_stop_price,
                instrument_rule=instrument_rule,
                reason="no_bot_owned_stops_on_exchange",
            )

        # ── Validate each bot-owned candidate ────────────────────────
        valid_candidates: list[Order] = []
        invalid_details: list[dict[str, Any]] = []

        for order in bot_orders:
            if order.status not in _ACTIVE_STATUSES:
                invalid_details.append({
                    "order_id": order.order_id,
                    "client_order_id": order.client_order_id,
                    "reason": "order_not_active",
                    "status": order.status.value if order.status else None,
                })
                continue

            validation = self._validator.validate_stop_orders(
                exchange=exchange_name,
                symbol=symbol,
                strategy_id=strategy_id,
                position_id=position_id,
                position_side=position_side,
                position_mode=position_mode,
                current_position_native_quantity=current_position_native_quantity,
                canonical_stop_price=canonical_stop_price,
                open_stop_orders=(order,),
                open_orders=(),
                market_profile=market_profile,
                instrument_rule=instrument_rule,
            )

            if validation.should_keep_existing_stop and validation.valid_bot_owned_orders:
                valid_candidates.append(order)
            else:
                invalid_details.append({
                    "order_id": order.order_id,
                    "client_order_id": order.client_order_id,
                    "reason": validation.primary_invalid_reason,
                    "detail_reason": validation.primary_invalid_detail_reason,
                })

        # ── Safety constraint: only one active plan for this exchange/side ──
        if active_plan_count_same_exchange_side > 1:
            return self._ambiguous(
                exchange_name=exchange_name,
                position_id=position_id,
                canonical_stop_price=canonical_stop_price,
                instrument_rule=instrument_rule,
                reason="multiple_active_plans_same_exchange_side",
                active_plan_count=active_plan_count_same_exchange_side,
                valid_candidate_count=len(valid_candidates),
            )

        # ── Safety constraint: no opposite-side position ─────────────
        opposite_side = (
            PositionSide.SHORT
            if position_side is PositionSide.LONG
            else PositionSide.LONG
        )
        has_opposite = any(
            getattr(p, "side", None) == opposite_side
            and getattr(p, "quantity", Decimal("0")) != Decimal("0")
            for p in open_positions_on_exchange
        )
        if has_opposite:
            return self._ambiguous(
                exchange_name=exchange_name,
                position_id=position_id,
                canonical_stop_price=canonical_stop_price,
                instrument_rule=instrument_rule,
                reason="opposite_side_position_on_exchange",
                position_side=position_side.value,
                opposite_side=opposite_side.value,
            )

        # ── Decision matrix ──────────────────────────────────────────
        if len(valid_candidates) == 1 and not invalid_details:
            order = valid_candidates[0]
            return StopScopeResolution(
                status=StopScopeResolutionStatus.ADOPTABLE_LEGACY,
                exchange=exchange_name.value,
                position_id=position_id,
                order=order,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                effective_stop_price=order.price,
                canonical_theoretical_stop_price=canonical_stop_price,
                price_tick=(
                    None
                    if instrument_rule is None
                    else instrument_rule.price_tick
                ),
                detail={
                    "match_method": "legacy_adoption",
                    "valid_candidate_count": 1,
                    "total_bot_orders": len(bot_orders),
                    "active_plan_count": active_plan_count_same_exchange_side,
                },
                warnings=(
                    "legacy_stop_scope_will_be_adopted_during_runtime_recovery",
                ),
            )

        if len(valid_candidates) > 1:
            return self._ambiguous(
                exchange_name=exchange_name,
                position_id=position_id,
                canonical_stop_price=canonical_stop_price,
                instrument_rule=instrument_rule,
                reason="multiple_valid_bot_stops",
                valid_candidate_count=len(valid_candidates),
            )

        if invalid_details and not valid_candidates:
            return StopScopeResolution(
                status=StopScopeResolutionStatus.INVALID,
                exchange=exchange_name.value,
                position_id=position_id,
                canonical_theoretical_stop_price=canonical_stop_price,
                price_tick=(
                    None
                    if instrument_rule is None
                    else instrument_rule.price_tick
                ),
                detail={
                    "match_method": "legacy_scan",
                    "valid_candidate_count": 0,
                    "invalid_candidate_count": len(invalid_details),
                    "invalid_reasons": invalid_details,
                },
            )

        # No valid, no invalid → effectively missing
        return self._missing(
            exchange_name=exchange_name,
            position_id=position_id,
            canonical_stop_price=canonical_stop_price,
            instrument_rule=instrument_rule,
            reason="no_valid_or_invalid_bot_stops",
        )

    # ── Helpers ────────────────────────────────────────────────────────

    def _missing(
        self,
        *,
        exchange_name: ExchangeName,
        position_id: str,
        canonical_stop_price: Decimal,
        instrument_rule: InstrumentRule | None,
        reason: str,
        **extra: Any,
    ) -> StopScopeResolution:
        return StopScopeResolution(
            status=StopScopeResolutionStatus.MISSING,
            exchange=exchange_name.value,
            position_id=position_id,
            canonical_theoretical_stop_price=canonical_stop_price,
            price_tick=(
                None
                if instrument_rule is None
                else instrument_rule.price_tick
            ),
            detail={"reason": reason, **extra},
        )

    def _ambiguous(
        self,
        *,
        exchange_name: ExchangeName,
        position_id: str,
        canonical_stop_price: Decimal,
        instrument_rule: InstrumentRule | None,
        reason: str,
        **extra: Any,
    ) -> StopScopeResolution:
        return StopScopeResolution(
            status=StopScopeResolutionStatus.AMBIGUOUS,
            exchange=exchange_name.value,
            position_id=position_id,
            canonical_theoretical_stop_price=canonical_stop_price,
            price_tick=(
                None
                if instrument_rule is None
                else instrument_rule.price_tick
            ),
            detail={"reason": reason, **extra},
        )


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


__all__ = [
    "RecoveryStopScopeResolver",
    "StopScopeResolution",
    "StopScopeResolutionStatus",
]
