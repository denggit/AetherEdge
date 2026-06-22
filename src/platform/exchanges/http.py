from __future__ import annotations

import asyncio
import json
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.platform.exchanges.errors import ExchangeApiError

_DEFAULT_HEADERS = {
    "User-Agent": "AetherEdge/0.1",
    "Accept": "application/json",
}


class RequestsHttpClient:
    """HTTP client backed by requests.

    OKX can reject Python urllib's default client fingerprint on some servers.
    This client keeps the same HttpClient port but uses requests under the hood.
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
        try:
            import requests
        except ImportError as exc:  # pragma: no cover - fallback exists via StdlibHttpClient
            raise ExchangeApiError("requests is required for RequestsHttpClient") from exc

        headers_dict = _merged_headers(headers)
        data = None
        if json_body is not None:
            # For signed APIs such as OKX, the exact JSON body bytes must match
            # the body text used in the signature. ``requests`` serializes
            # ``json=`` with its own spacing, so send the compact JSON string
            # explicitly. This keeps POST signatures consistent with adapters
            # that sign ``json.dumps(..., separators=(",", ":"))``.
            data = json.dumps(json_body, separators=(",", ":"))
            headers_dict.setdefault("Content-Type", "application/json")

        response = requests.request(
            method.upper(),
            url,
            params={k: v for k, v in (params or {}).items() if v is not None} or None,
            data=data,
            headers=headers_dict,
            timeout=timeout_seconds or 10.0,
        )
        if response.status_code >= 400:
            payload = _decode_response_payload(response.text)
            raise ExchangeApiError(
                _error_message(response.status_code, payload),
                status_code=response.status_code,
                payload=payload,
            )
        if not response.text:
            return None
        return _decode_response_payload(response.text)


class StdlibHttpClient:
    """Dependency-light async HTTP client backed by urllib.

    Kept as fallback/test adapter. The default factory uses RequestsHttpClient.
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
        headers_dict = _merged_headers(headers)
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
            payload = _decode_response_payload(text)
            raise ExchangeApiError(
                _error_message(exc.code, payload),
                status_code=exc.code,
                payload=payload,
            ) from exc
        except URLError as exc:
            raise ExchangeApiError(f"Network error from exchange API: {exc}") from exc


def _merged_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    headers_dict = dict(_DEFAULT_HEADERS)
    headers_dict.update(dict(headers or {}))
    return headers_dict


def _decode_response_payload(text: str) -> Any:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _error_message(status_code: int, payload: Any) -> str:
    message = f"HTTP {status_code} from exchange API"
    if payload:
        return f"{message}: {payload}"
    return message
