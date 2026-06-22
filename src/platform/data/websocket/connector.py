from __future__ import annotations

from typing import AsyncIterator

from src.utils.log import get_logger

logger = get_logger(__name__)


class WebsocketsConnection:
    def __init__(self, websocket) -> None:
        self._websocket = websocket

    def __aiter__(self) -> AsyncIterator[str | bytes]:
        return self._websocket.__aiter__()

    async def send(self, message: str) -> None:
        await self._websocket.send(message)

    async def close(self) -> None:
        await self._websocket.close()


class WebsocketsConnector:
    """Default WebSocket connector.

    The dependency is imported lazily so unit tests and REST-only deployments do
    not need websockets installed until streaming is used.
    """

    async def connect(self, url: str) -> WebsocketsConnection:
        try:
            import websockets
        except ImportError as exc:  # pragma: no cover - depends on local env
            raise RuntimeError(
                "WebSocket streaming requires the 'websockets' package. Install project dependencies first."
            ) from exc
        logger.info("Opening websocket connection | url=%s", url)
        websocket = await websockets.connect(url, ping_interval=20, ping_timeout=20)
        return WebsocketsConnection(websocket)
