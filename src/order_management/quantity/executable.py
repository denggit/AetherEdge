from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from src.order_management.quantity.converter import NativeQuantityConverter
from src.platform.execution.rules import round_to_step
from src.platform.exchanges.models import ExchangeName, InstrumentRule
from src.platform.markets import MarketProfile


@dataclass(frozen=True)
class ExecutableQuantityResolution:
    """Resolve a base-asset quantity exactly as an exchange order will be sized."""

    exchange: ExchangeName
    raw_base_quantity: Decimal
    normalized_base_quantity: Decimal
    raw_native_quantity: Decimal
    normalized_native_quantity: Decimal
    quantity_step: Decimal | None
    min_quantity: Decimal | None
    min_notional: Decimal | None
    normalized_notional: Decimal | None
    executable: bool
    reason: str

    def metadata(self) -> dict[str, str | bool | None]:
        return {
            "raw_base_quantity": _decimal_text(self.raw_base_quantity),
            "normalized_base_quantity": _decimal_text(
                self.normalized_base_quantity
            ),
            "raw_native_quantity": _decimal_text(self.raw_native_quantity),
            "normalized_native_quantity": _decimal_text(
                self.normalized_native_quantity
            ),
            "quantity_step": _optional_decimal_text(self.quantity_step),
            "min_quantity": _optional_decimal_text(self.min_quantity),
            "min_notional": _optional_decimal_text(self.min_notional),
            "normalized_notional": _optional_decimal_text(
                self.normalized_notional
            ),
            "executable": self.executable,
            "reason": self.reason,
        }


def resolve_executable_base_quantity(
    *,
    exchange: ExchangeName | str,
    symbol: str,
    raw_base_quantity: Decimal,
    market_profile: MarketProfile,
    instrument_rule: InstrumentRule | None,
    reference_price: Decimal | None = None,
    quantity_converter: NativeQuantityConverter | None = None,
) -> ExecutableQuantityResolution:
    """Normalize a recovery/open quantity through the live execution rules.

    Strategy quantities are base-asset quantities, while an instrument rule is
    expressed in venue-native units.  This function converts to native units,
    applies the same ``ROUND_DOWN`` step primitive used by live execution, then
    converts the result back to base units for a canonical ``TradeSignal``.
    """

    exchange_name = (
        exchange
        if isinstance(exchange, ExchangeName)
        else ExchangeName(str(exchange).strip().lower())
    )
    converter = quantity_converter or NativeQuantityConverter()
    raw_base = max(Decimal("0"), raw_base_quantity)
    if raw_base > 0:
        raw_native = converter.convert_quantity(
            exchange=exchange_name,
            symbol=symbol,
            base_quantity=raw_base,
            market_profile=market_profile,
        ).native_quantity
    else:
        raw_native = Decimal("0")

    quantity_step = (
        None if instrument_rule is None else instrument_rule.quantity_step
    )
    normalized_native = round_to_step(raw_native, quantity_step)
    normalized_base = converter.native_to_base_quantity(
        exchange=exchange_name,
        symbol=symbol,
        native_quantity=normalized_native,
        market_profile=market_profile,
    )

    min_quantity = (
        None if instrument_rule is None else instrument_rule.min_quantity
    )
    if min_quantity is None:
        profile_min_base = market_profile.min_quantity(exchange_name)
        if profile_min_base is not None and profile_min_base > 0:
            min_quantity = converter.convert_quantity(
                exchange=exchange_name,
                symbol=symbol,
                base_quantity=profile_min_base,
                market_profile=market_profile,
            ).native_quantity

    min_notional = (
        None if instrument_rule is None else instrument_rule.min_notional
    )
    normalized_notional = (
        normalized_base * reference_price
        if reference_price is not None and reference_price > 0
        else None
    )

    if raw_base <= 0:
        executable = False
        reason = "non_positive_quantity"
    elif normalized_native <= 0 or normalized_base <= 0:
        executable = False
        reason = "normalized_to_zero"
    elif min_quantity is not None and normalized_native < min_quantity:
        executable = False
        reason = "below_min_quantity"
    elif (
        min_notional is not None
        and normalized_notional is not None
        and normalized_notional < min_notional
    ):
        executable = False
        reason = "below_min_notional"
    else:
        executable = True
        reason = "executable"

    return ExecutableQuantityResolution(
        exchange=exchange_name,
        raw_base_quantity=raw_base,
        normalized_base_quantity=normalized_base,
        raw_native_quantity=raw_native,
        normalized_native_quantity=normalized_native,
        quantity_step=quantity_step,
        min_quantity=min_quantity,
        min_notional=min_notional,
        normalized_notional=normalized_notional,
        executable=executable,
        reason=reason,
    )


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _optional_decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else _decimal_text(value)
