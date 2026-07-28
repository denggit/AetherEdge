from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Sequence
from typing import Any, Generic, Mapping, TypeVar

from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
)

from src.platform.data.websocket.ports import (
    WebSocketConnection,
    WebSocketConnector,
)
from src.utils.log import get_logger


logger = get_logger(__name__)
EventT = TypeVar("EventT")
MessageMapper = Callable[[str | bytes], Sequence[EventT]]
ReconnectHandler = Callable[[BaseException], None]


class OkxPublicWebSocketError(RuntimeError):
    pass


class OkxSubscriptionError(OkxPublicWebSocketError):
    def __init__(
        self,
        *,
        channel: str,
        symbol: str,
        raw_symbol: str,
        code: object,
        message: object,
    ) -> None:
        super().__init__(
            "OKX public websocket subscription failed | "
            "exchange=okx "
            f"channel={channel} "
            f"symbol={symbol} "
            f"raw_symbol={raw_symbol} "
            f"code={code} "
            f"msg={message}"
        )


class OkxPublicWebSocketProtocolError(OkxPublicWebSocketError):
    pass


class _OkxStreamEnded(OSError):
    pass


TRANSIENT_OKX_WS_EXCEPTIONS = (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
    asyncio.TimeoutError,
    OSError,
)


class _OkxPublicWebSocketFeed(Generic[EventT]):
    """Shared connection lifecycle for narrow OKX public channel adapters."""

    def __init__(
        self,
        *,
        symbol: str,
        raw_symbol: str,
        channel: str,
        url: str,
        connector: WebSocketConnector,
        reconnect: bool,
        reconnect_delay_seconds: float,
        max_reconnects: int | None,
    ) -> None:
        self._symbol = symbol
        self._raw_symbol = raw_symbol
        self._channel = channel
        self._url = url
        self._connector = connector
        self._reconnect = reconnect
        self._reconnect_delay_seconds = float(reconnect_delay_seconds)
        self._max_reconnects = max_reconnects
        self._connection: WebSocketConnection | None = None
        self._closed = False

    async def stream(
        self,
        *,
        mapper: MessageMapper[EventT],
        recoverable_exceptions: tuple[type[BaseException], ...] = (),
        on_reconnect: ReconnectHandler | None = None,
    ) -> AsyncIterator[EventT]:
        reconnects = 0
        self._closed = False
        recoverable = TRANSIENT_OKX_WS_EXCEPTIONS + recoverable_exceptions
        while not self._closed:
            connection: WebSocketConnection | None = None
            try:
                connection = await self._connector.connect(self._url)
                self._connection = connection
                await connection.send(
                    _okx_subscribe_message(
                        channel=self._channel,
                        inst_id=self._raw_symbol,
                    )
                )
                logger.info(
                    "OKX websocket subscribed | channel=%s symbol=%s raw_symbol=%s",
                    self._channel,
                    self._symbol,
                    self._raw_symbol,
                )
                async for message in connection:
                    if self._closed:
                        break
                    events = mapper(message)
                    if events:
                        reconnects = 0
                    for event in events:
                        yield event
                if self._closed:
                    break
                raise _OkxStreamEnded(
                    f"OKX public websocket ended | channel={self._channel}"
                )
            except asyncio.CancelledError:
                raise
            except recoverable as exc:
                if on_reconnect is not None:
                    on_reconnect(exc)
                if not self._reconnect:
                    raise
                if (
                    self._max_reconnects is not None
                    and reconnects >= self._max_reconnects
                ):
                    logger.error(
                        "OKX websocket max reconnects exceeded | "
                        "channel=%s symbol=%s raw_symbol=%s reconnect_count=%s "
                        "error=%s",
                        self._channel,
                        self._symbol,
                        self._raw_symbol,
                        reconnects,
                        exc,
                    )
                    raise
                reconnects += 1
                delay = _reconnect_delay(
                    self._reconnect_delay_seconds,
                    reconnects,
                )
                logger.warning(
                    "OKX websocket reconnecting | "
                    "channel=%s symbol=%s raw_symbol=%s reconnect_count=%s "
                    "delay_seconds=%.2f error=%s",
                    self._channel,
                    self._symbol,
                    self._raw_symbol,
                    reconnects,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
            finally:
                self._connection = None
                if connection is not None:
                    try:
                        await connection.close()
                    except Exception as exc:  # pragma: no cover
                        logger.debug(
                            "OKX websocket close failed | channel=%s "
                            "symbol=%s raw_symbol=%s error=%s",
                            self._channel,
                            self._symbol,
                            self._raw_symbol,
                            exc,
                        )

    async def close(self) -> None:
        self._closed = True
        connection = self._connection
        if connection is not None:
            await connection.close()


def decode_okx_public_message(
    message: str | bytes,
    *,
    channel: str,
    symbol: str,
    raw_symbol: str,
) -> Mapping[str, Any] | None:
    if isinstance(message, bytes):
        message = message.decode("utf-8")
    if message.strip().lower() == "pong":
        return None
    try:
        payload = json.loads(message)
    except (TypeError, ValueError) as exc:
        raise OkxPublicWebSocketProtocolError(
            "invalid OKX public websocket JSON | "
            f"channel={channel} symbol={symbol} raw_symbol={raw_symbol}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise OkxPublicWebSocketProtocolError(
            "invalid OKX public websocket payload | "
            f"channel={channel} symbol={symbol} raw_symbol={raw_symbol}"
        )

    event = payload.get("event")
    if event == "error":
        raise OkxSubscriptionError(
            channel=channel,
            symbol=symbol,
            raw_symbol=raw_symbol,
            code=payload.get("code"),
            message=payload.get("msg"),
        )
    if event in {"subscribe", "unsubscribe"}:
        return None
    if event is not None:
        return None

    arg = payload.get("arg")
    if not isinstance(arg, Mapping):
        if "data" not in payload:
            return None
        raise OkxPublicWebSocketProtocolError(
            "OKX public websocket data payload has no arg | "
            f"channel={channel} symbol={symbol} raw_symbol={raw_symbol}"
        )
    if str(arg.get("channel") or "") != channel:
        return None
    if str(arg.get("instId") or "") != raw_symbol:
        return None
    return payload


def _okx_subscribe_message(*, channel: str, inst_id: str) -> str:
    return json.dumps(
        {
            "op": "subscribe",
            "args": [{"channel": channel, "instId": inst_id}],
        },
        separators=(",", ":"),
    )


def _reconnect_delay(
    base_delay_seconds: float,
    reconnect_count: int,
) -> float:
    exponent = min(max(reconnect_count - 1, 0), 6)
    return min(float(base_delay_seconds) * (2 ** exponent), 60.0)


__all__ = [
    "OkxPublicWebSocketError",
    "OkxPublicWebSocketProtocolError",
    "OkxSubscriptionError",
]
