from __future__ import annotations

import asyncio
import json
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.platform.exchanges.errors import ExchangeApiError


class StdlibHttpClient:
    """Dependency-light async HTTP client backed by urllib.

    This keeps the init framework install-free. Later, a faster aiohttp/httpx
    implementation can replace this class without changing exchange adapters.
    """

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_seconds: float | None = None,
    ) -> Any:
        return await asyncio.to_thread(
            self._request_sync,
            method,
            url,
            params,
            json_body,
            headers,
            timeout_seconds,
        )

    @staticmethod
    def _request_sync(
        method: str,
        url: str,
        params: Mapping[str, Any] | None,
        json_body: Mapping[str, Any] | None,
        headers: Mapping[str, str] | None,
        timeout_seconds: float | None,
    ) -> Any:
        method = method.upper()
        headers_dict = dict(headers or {})
        if params:
            query = urlencode({k: v for k, v in params.items() if v is not None})
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}{query}"

        data = None
        if json_body is not None:
            data = json.dumps(json_body, separators=(",", ":")).encode("utf-8")
            headers_dict.setdefault("Content-Type", "application/json")

        req = Request(url=url, data=data, headers=headers_dict, method=method)
        try:
            with urlopen(req, timeout=timeout_seconds or 10.0) as resp:
                text = resp.read().decode("utf-8")
                return json.loads(text) if text else None
        except HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace")
            payload: Any
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                payload = text
            raise ExchangeApiError(
                f"HTTP {exc.code} from exchange API",
                status_code=exc.code,
                payload=payload,
            ) from exc
        except URLError as exc:
            raise ExchangeApiError(f"Network error from exchange API: {exc}") from exc
