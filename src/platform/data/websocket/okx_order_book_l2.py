from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from src.platform.data.models import (
    MarketOrderBookL2,
    OrderBookLevel,
)
from src.platform.data.order_book import (
    LocalOrderBook,
    OrderBookSequenceGap,
)
from src.platform.data.websocket.okx import (
    OKX_DEMO_PUBLIC_WS_URL,
    OKX_PUBLIC_WS_URL,
)
from src.platform.data.websocket.okx_public import (
    OkxPublicWebSocketProtocolError,
    OkxSubscriptionError,
    _OkxPublicWebSocketFeed,
    decode_okx_public_message,
)
from src.platform.data.websocket.ports import WebSocketConnector
from src.platform.exchanges.models import ExchangeName
from src.platform.exchanges.symbols import to_exchange_symbol


_CHANNEL = "books"
_DEPTH = 400


class OkxOrderBookL2ProtocolError(OkxPublicWebSocketProtocolError):
    pass


class OkxOrderBookL2WebSocketFeed:
    def __init__(
        self,
        *,
        symbol: str,
        connector: WebSocketConnector,
        sandbox: bool = False,
        reconnect: bool = True,
        reconnect_delay_seconds: float = 1.0,
        max_reconnects: int | None = None,
    ) -> None:
        self._symbol = symbol
        self._raw_symbol = to_exchange_symbol(ExchangeName.OKX, symbol)
        self._book = LocalOrderBook(max_depth=_DEPTH)
        self._last_seq_id: int | None = None
        self._last_event_time_ms: int | None = None
        self._ready = False
        self.heartbeat_count = 0
        self.sequence_gap_count = 0
        self.resync_count = 0
        self._session = _OkxPublicWebSocketFeed[MarketOrderBookL2](
            symbol=symbol,
            raw_symbol=self._raw_symbol,
            channel=_CHANNEL,
            url=(
                OKX_DEMO_PUBLIC_WS_URL
                if sandbox
                else OKX_PUBLIC_WS_URL
            ),
            connector=connector,
            reconnect=reconnect,
            reconnect_delay_seconds=reconnect_delay_seconds,
            max_reconnects=max_reconnects,
        )

    async def stream_order_book_l2(
        self,
    ) -> AsyncIterator[MarketOrderBookL2]:
        self._discard_state()
        async for event in self._session.stream(
            mapper=self._map_message,
            recoverable_exceptions=(
                OrderBookSequenceGap,
                OkxOrderBookL2ProtocolError,
            ),
            on_reconnect=self._handle_reconnect,
        ):
            yield event

    async def close(self) -> None:
        self._discard_state()
        await self._session.close()

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def last_sequence_id(self) -> int | None:
        return self._last_seq_id

    def _handle_reconnect(self, exc: BaseException) -> None:
        self.resync_count += 1
        self._discard_state()

    def _discard_state(self) -> None:
        self._book.clear()
        self._last_seq_id = None
        self._last_event_time_ms = None
        self._ready = False

    def _map_message(
        self,
        message: str | bytes,
    ) -> list[MarketOrderBookL2]:
        try:
            return self._map_message_checked(message)
        except OrderBookSequenceGap:
            self.sequence_gap_count += 1
            self._discard_state()
            raise
        except OkxOrderBookL2ProtocolError:
            self._discard_state()
            raise
        except OkxSubscriptionError:
            self._discard_state()
            raise
        except Exception as exc:
            self._discard_state()
            raise OkxOrderBookL2ProtocolError(
                "invalid OKX books payload | "
                f"exchange=okx channel={_CHANNEL} "
                f"symbol={self._symbol} raw_symbol={self._raw_symbol}"
            ) from exc

    def _map_message_checked(
        self,
        message: str | bytes,
    ) -> list[MarketOrderBookL2]:
        payload = decode_okx_public_message(
            message,
            channel=_CHANNEL,
            symbol=self._symbol,
            raw_symbol=self._raw_symbol,
        )
        if payload is None:
            return []
        action = payload.get("action")
        if action not in {"snapshot", "update"}:
            raise OkxOrderBookL2ProtocolError(
                f"invalid OKX books action: {action!r}"
            )
        rows = payload.get("data")
        if not isinstance(rows, list) or not rows:
            raise OkxOrderBookL2ProtocolError(
                "OKX books data must be a non-empty list"
            )

        parsed = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise OkxOrderBookL2ProtocolError(
                    "OKX books row must be a mapping"
                )
            parsed.append(
                (
                    _required_int(row.get("prevSeqId"), "prevSeqId"),
                    _required_int(row.get("seqId"), "seqId"),
                    _required_int(row.get("ts"), "ts"),
                    _map_ws_levels(row.get("bids"), side="bids"),
                    _map_ws_levels(row.get("asks"), side="asks"),
                    row.get("checksum"),
                )
            )

        events: list[MarketOrderBookL2] = []
        for (
            previous_sequence_id,
            sequence_id,
            event_time_ms,
            bids,
            asks,
            checksum,
        ) in parsed:
            if action == "snapshot":
                if previous_sequence_id != -1:
                    raise self._gap(
                        received_prev_seq_id=previous_sequence_id,
                        received_seq_id=sequence_id,
                    )
                self._book.reset(bids=bids, asks=asks)
                self._last_seq_id = sequence_id
                self._last_event_time_ms = event_time_ms
                self._ready = True
            else:
                if not self._ready or self._last_seq_id is None:
                    raise self._gap(
                        received_prev_seq_id=previous_sequence_id,
                        received_seq_id=sequence_id,
                    )
                if previous_sequence_id != self._last_seq_id:
                    raise self._gap(
                        received_prev_seq_id=previous_sequence_id,
                        received_seq_id=sequence_id,
                    )
                if (
                    not bids
                    and not asks
                    and previous_sequence_id == sequence_id
                ):
                    self.heartbeat_count += 1
                    self._last_event_time_ms = event_time_ms
                    continue
                self._book.apply_updates(bids=bids, asks=asks)
                self._last_seq_id = sequence_id
                self._last_event_time_ms = event_time_ms

            snapshot_bids, snapshot_asks = self._book.snapshot(depth=_DEPTH)
            events.append(
                MarketOrderBookL2(
                    exchange=ExchangeName.OKX,
                    symbol=self._symbol,
                    raw_symbol=self._raw_symbol,
                    bids=snapshot_bids,
                    asks=snapshot_asks,
                    event_time_ms=event_time_ms,
                    sequence_id=sequence_id,
                    previous_sequence_id=previous_sequence_id,
                    depth=_DEPTH,
                    raw={
                        "channel": _CHANNEL,
                        "action": action,
                        "checksum": checksum,
                    },
                )
            )
        return events

    def _gap(
        self,
        *,
        received_prev_seq_id: int,
        received_seq_id: int,
    ) -> OrderBookSequenceGap:
        return OrderBookSequenceGap(
            symbol=self._symbol,
            expected_prev_seq_id=self._last_seq_id,
            received_prev_seq_id=received_prev_seq_id,
            received_seq_id=received_seq_id,
            last_event_time_ms=self._last_event_time_ms,
        )


def _map_ws_levels(
    value: Any,
    *,
    side: str,
) -> tuple[OrderBookLevel, ...]:
    if not isinstance(value, list):
        raise OkxOrderBookL2ProtocolError(
            f"OKX books {side} must be a list"
        )
    levels: list[OrderBookLevel] = []
    for row in value:
        if not isinstance(row, (list, tuple)) or len(row) < 4:
            raise OkxOrderBookL2ProtocolError(
                f"malformed OKX books {side} level: {row!r}"
            )
        try:
            price = Decimal(str(row[0]))
            quantity = Decimal(str(row[1]))
            order_count = _required_int(row[3], "order_count")
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise OkxOrderBookL2ProtocolError(
                f"malformed OKX books {side} level: {row!r}"
            ) from exc
        levels.append(
            OrderBookLevel(
                price=price,
                quantity=quantity,
                order_count=order_count,
            )
        )
    return tuple(levels)


def _required_int(value: Any, field: str) -> int:
    if (
        value is None
        or type(value) is bool
        or not isinstance(value, (int, str))
    ):
        raise OkxOrderBookL2ProtocolError(
            f"missing or invalid OKX books {field}: {value!r}"
        )
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise OkxOrderBookL2ProtocolError(
            f"invalid OKX books {field}: {value!r}"
        ) from exc


__all__ = [
    "OkxOrderBookL2ProtocolError",
    "OkxOrderBookL2WebSocketFeed",
]
