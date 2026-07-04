from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Mapping

from src.market_data.models import TradeFootprintFeature, TradeFeatureQuality
from src.platform.data.models import MarketTrade, TradeSide

_ONE_MINUTE_MS = 60_000
_DEFAULT_ETH_PRICE_BUCKET_SIZE = Decimal("1")


@dataclass
class _PriceBucketAccum:
    buy_notional: Decimal = Decimal("0")
    sell_notional: Decimal = Decimal("0")


@dataclass
class _FootprintAccum:
    symbol: str = ""
    exchange: str = ""
    open_time_ms: int = 0
    close_time_ms: int = 0
    open: Decimal = Decimal("0")
    high: Decimal = Decimal("0")
    low: Decimal = Decimal("0")
    close: Decimal = Decimal("0")
    buy_notional: Decimal = Decimal("0")
    sell_notional: Decimal = Decimal("0")
    trade_count: int = 0
    available_time_ms: int = 0
    bucket_size: Decimal = Decimal("0")
    price_buckets: dict[Decimal, _PriceBucketAccum] = field(default_factory=dict)


@dataclass
class _FootprintStats:
    features_closed: int = 0
    invalid: int = 0
    out_of_order: int = 0


class TradeFootprintBuilder:
    """Build closed 1m price-bucket footprint pressure from every trade.

    For each price bucket, pressure is::

        abs(buy_notional - sell_notional)
        / (buy_notional + sell_notional)

    The feature value is the maximum pressure across all populated price
    buckets in the minute. This is the CoinBacktest range-footprint formula,
    applied to a fixed-time 1m aggregate.
    """

    _MAX_PENDING_CLOSED = 8

    def __init__(
        self,
        *,
        price_bucket_size: Decimal | str | float | None = None,
        price_bucket_pct: Decimal | str | float | None = None,
        tick_size: Decimal | str | float | None = None,
        contract_value: Decimal | str | float = Decimal("1"),
    ) -> None:
        self.contract_value = Decimal(str(contract_value))
        if self.contract_value <= 0:
            raise ValueError("contract_value must be positive")
        self.price_bucket_size = _optional_positive_decimal(
            price_bucket_size, name="price_bucket_size"
        )
        self.price_bucket_pct = _optional_positive_decimal(
            price_bucket_pct, name="price_bucket_pct"
        )
        self.tick_size = _optional_positive_decimal(tick_size, name="tick_size")
        if self.price_bucket_size is not None and self.price_bucket_pct is not None:
            raise ValueError(
                "price_bucket_size and price_bucket_pct are mutually exclusive"
            )

        self._active: _FootprintAccum | None = None
        self._pending_closed: list[TradeFootprintFeature] = []
        self._stats = _FootprintStats()

    def on_trade(self, trade: MarketTrade) -> tuple[TradeFootprintFeature, ...]:
        """Feed one normalized trade and return newly closed minute features."""
        time_ms = _trade_time_ms(trade)
        if time_ms is None or time_ms <= 0:
            self._stats.invalid += 1
            return ()
        if trade.price <= 0 or trade.quantity <= 0:
            self._stats.invalid += 1
            return ()

        bucket_start = _bucket_start_ms(time_ms)
        exchange = (
            trade.exchange.value
            if hasattr(trade.exchange, "value")
            else str(trade.exchange)
        )

        if self._active is not None and bucket_start > self._active.open_time_ms:
            self._active.available_time_ms = max(
                self._active.available_time_ms, time_ms
            )
            self._flush(self._close())
            result = tuple(self._drain())
            self._active = self._new_accum(
                symbol=trade.symbol,
                exchange=exchange,
                bucket_start=bucket_start,
            )
            self._add_trade(self._active, trade, time_ms)
            return result

        if self._active is None:
            self._active = self._new_accum(
                symbol=trade.symbol,
                exchange=exchange,
                bucket_start=bucket_start,
            )
            self._add_trade(self._active, trade, time_ms)
            return ()

        if bucket_start < self._active.open_time_ms:
            self._stats.out_of_order += 1
            return ()

        self._add_trade(self._active, trade, time_ms)
        return ()

    def drain_closed_only(self) -> tuple[TradeFootprintFeature, ...]:
        """Return pending closed features without closing the active minute."""
        return tuple(self._drain())

    def drain_completed_through(
        self, watermark_ms: int
    ) -> tuple[TradeFootprintFeature, ...]:
        """Close only a minute proven complete by an archive watermark."""
        result: list[TradeFootprintFeature] = list(self._drain())
        if (
            self._active is not None
            and self._active.trade_count > 0
            and self._active.close_time_ms <= int(watermark_ms)
        ):
            self._active.available_time_ms = max(
                self._active.available_time_ms, self._active.close_time_ms
            )
            self._flush(self._close())
            result.extend(self._drain())
        return tuple(result)

    def discard_active(self) -> None:
        """Drop the in-progress minute without writing it."""
        self._active = None

    def drain(self) -> tuple[TradeFootprintFeature, ...]:
        """Force-close active state; backfill workers use drain_closed_only."""
        result: list[TradeFootprintFeature] = list(self._drain())
        if self._active is not None and self._active.trade_count > 0:
            self._active.available_time_ms = max(
                self._active.available_time_ms, self._active.close_time_ms
            )
            self._flush(self._close())
            result.extend(self._drain())
        return tuple(result)

    def snapshot_state(self) -> Mapping[str, Any]:
        active = None
        if self._active is not None:
            active = {
                "symbol": self._active.symbol,
                "exchange": self._active.exchange,
                "open_time_ms": self._active.open_time_ms,
                "close_time_ms": self._active.close_time_ms,
                "open": str(self._active.open),
                "high": str(self._active.high),
                "low": str(self._active.low),
                "close": str(self._active.close),
                "buy_notional": str(self._active.buy_notional),
                "sell_notional": str(self._active.sell_notional),
                "trade_count": self._active.trade_count,
                "available_time_ms": self._active.available_time_ms,
                "bucket_size": str(self._active.bucket_size),
                "price_buckets": {
                    str(price): {
                        "buy_notional": str(bucket.buy_notional),
                        "sell_notional": str(bucket.sell_notional),
                    }
                    for price, bucket in self._active.price_buckets.items()
                },
            }
        return {
            "version": 3,
            "contract_value": str(self.contract_value),
            "price_bucket_size": (
                None if self.price_bucket_size is None else str(self.price_bucket_size)
            ),
            "price_bucket_pct": (
                None if self.price_bucket_pct is None else str(self.price_bucket_pct)
            ),
            "tick_size": None if self.tick_size is None else str(self.tick_size),
            "active": active,
        }

    @classmethod
    def restore_state(cls, state: Mapping[str, Any]) -> "TradeFootprintBuilder":
        version = int(state.get("version", 0))
        if version not in (1, 2, 3):
            raise ValueError("unsupported footprint builder checkpoint version")
        builder = cls(
            contract_value=Decimal(str(state.get("contract_value", "1"))),
            price_bucket_size=state.get("price_bucket_size"),
            price_bucket_pct=state.get("price_bucket_pct"),
            tick_size=state.get("tick_size"),
        )
        raw = state.get("active")
        if raw is not None:
            if not isinstance(raw, Mapping):
                raise ValueError("active footprint state must be a mapping")
            price_buckets: dict[Decimal, _PriceBucketAccum] = {}
            raw_buckets = raw.get("price_buckets", {})
            if isinstance(raw_buckets, Mapping):
                for raw_price, raw_bucket in raw_buckets.items():
                    if not isinstance(raw_bucket, Mapping):
                        continue
                    price_buckets[Decimal(str(raw_price))] = _PriceBucketAccum(
                        buy_notional=Decimal(
                            str(raw_bucket.get("buy_notional", "0"))
                        ),
                        sell_notional=Decimal(
                            str(raw_bucket.get("sell_notional", "0"))
                        ),
                    )
            builder._active = _FootprintAccum(
                symbol=str(raw["symbol"]),
                exchange=str(raw["exchange"]),
                open_time_ms=int(raw["open_time_ms"]),
                close_time_ms=int(raw["close_time_ms"]),
                open=Decimal(str(raw["open"])),
                high=Decimal(str(raw["high"])),
                low=Decimal(str(raw["low"])),
                close=Decimal(str(raw["close"])),
                buy_notional=Decimal(str(raw["buy_notional"])),
                sell_notional=Decimal(str(raw["sell_notional"])),
                trade_count=int(raw["trade_count"]),
                available_time_ms=int(raw["available_time_ms"]),
                bucket_size=Decimal(str(raw.get("bucket_size", "0"))),
                price_buckets=price_buckets,
            )
            # v1/v2 checkpoints did not retain price-bucket context. Keeping
            # them would make a later COMPLETE pressure unauditable.
            if version < 3:
                builder._active = None
        return builder

    @property
    def stats(self) -> Mapping[str, int]:
        return {
            "features_closed": self._stats.features_closed,
            "invalid_trades": self._stats.invalid,
            "out_of_order_trades": self._stats.out_of_order,
        }

    def _new_accum(
        self, *, symbol: str, exchange: str, bucket_start: int
    ) -> _FootprintAccum:
        return _FootprintAccum(
            symbol=symbol,
            exchange=exchange,
            open_time_ms=bucket_start,
            close_time_ms=bucket_start + _ONE_MINUTE_MS - 1,
        )

    def _add_trade(
        self, accum: _FootprintAccum, trade: MarketTrade, time_ms: int
    ) -> None:
        price = trade.price
        notional = price * trade.quantity * self.contract_value

        if accum.trade_count == 0:
            accum.open = price
            accum.high = price
            accum.low = price
            accum.close = price
        else:
            accum.high = max(accum.high, price)
            accum.low = min(accum.low, price)
            accum.close = price

        if accum.bucket_size <= 0:
            accum.bucket_size = self._resolve_bucket_size(price)
        price_bucket = _price_bucket(price, accum.bucket_size)
        bucket = accum.price_buckets.setdefault(
            price_bucket, _PriceBucketAccum()
        )

        if trade.side is TradeSide.BUY:
            accum.buy_notional += notional
            bucket.buy_notional += notional
        elif trade.side is TradeSide.SELL:
            accum.sell_notional += notional
            bucket.sell_notional += notional
        accum.trade_count += 1
        accum.available_time_ms = max(accum.available_time_ms, time_ms)

    def _resolve_bucket_size(self, price: Decimal) -> Decimal:
        if self.price_bucket_size is not None:
            return self.price_bucket_size
        if self.price_bucket_pct is None:
            return _DEFAULT_ETH_PRICE_BUCKET_SIZE
        size = price * self.price_bucket_pct
        if self.tick_size is not None:
            ticks = (size / self.tick_size).to_integral_value(
                rounding=ROUND_CEILING
            )
            size = max(Decimal("1"), ticks) * self.tick_size
        return size

    def _close(self) -> TradeFootprintFeature:
        if self._active is None:
            raise RuntimeError("cannot close an empty footprint builder")
        return self._feature_from_accum(self._active)

    def _feature_from_accum(
        self, accum: _FootprintAccum
    ) -> TradeFootprintFeature:
        delta = accum.buy_notional - accum.sell_notional
        total = accum.buy_notional + accum.sell_notional
        taker_buy_ratio = (
            accum.buy_notional / total if total > 0 else Decimal("0")
        )

        span = accum.high - accum.low
        close_pos = (
            (accum.close - accum.low) / span
            if span > 0 and accum.open > 0 and accum.close > 0
            else Decimal("0.5")
        )
        range_pct = (
            (accum.high - accum.low) / accum.open
            if accum.open > 0
            else Decimal("0")
        )
        return_pct = (
            accum.close / accum.open - Decimal("1")
            if accum.open > 0
            else Decimal("0")
        )

        pressures: list[Decimal] = []
        for bucket in accum.price_buckets.values():
            bucket_total = bucket.buy_notional + bucket.sell_notional
            if bucket_total <= 0:
                continue
            bucket_delta = bucket.buy_notional - bucket.sell_notional
            pressures.append(abs(bucket_delta) / bucket_total)

        context_available = bool(pressures)
        quality = (
            TradeFeatureQuality.COMPLETE.value
            if context_available
            else TradeFeatureQuality.MISSING_FOOTPRINT_CONTEXT.value
        )
        max_pressure = max(pressures) if pressures else Decimal("0")

        feature = TradeFootprintFeature(
            exchange=accum.exchange,
            symbol=accum.symbol,
            timeframe="1m",
            open_time_ms=accum.open_time_ms,
            close_time_ms=accum.close_time_ms,
            available_time_ms=accum.available_time_ms,
            delta_notional=delta,
            abs_delta_notional=abs(delta),
            taker_buy_ratio=taker_buy_ratio,
            close_pos=close_pos,
            range_pct=range_pct,
            return_pct=return_pct,
            fp_max_bucket_abs_delta_pressure=max_pressure,
            context_available=context_available,
            quality=quality,
            source="trade_derived",
        )
        self._stats.features_closed += 1
        return feature

    def _flush(self, feature: TradeFootprintFeature) -> None:
        self._pending_closed.append(feature)
        while len(self._pending_closed) > self._MAX_PENDING_CLOSED:
            self._pending_closed.pop(0)

    def _drain(self) -> list[TradeFootprintFeature]:
        result = list(self._pending_closed)
        self._pending_closed.clear()
        return result


def _trade_time_ms(trade: MarketTrade) -> int | None:
    if trade.trade_time_ms is not None:
        return trade.trade_time_ms
    return trade.event_time_ms


def _bucket_start_ms(time_ms: int) -> int:
    return (time_ms // _ONE_MINUTE_MS) * _ONE_MINUTE_MS


def _optional_positive_decimal(
    value: Decimal | str | float | None, *, name: str
) -> Decimal | None:
    if value is None:
        return None
    result = Decimal(str(value))
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _price_bucket(price: Decimal, bucket_size: Decimal) -> Decimal:
    index = (price / bucket_size).to_integral_value(rounding=ROUND_FLOOR)
    return index * bucket_size
