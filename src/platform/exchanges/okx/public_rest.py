from __future__ import annotations

import asyncio
import os
from typing import Any, Mapping

from src.platform.exchanges.errors import ExchangeApiError
from src.platform.exchanges.models import ExchangeConfig
from src.platform.exchanges.ports import HttpClient


OKX_PROD_REST_URL = "https://www.okx.com"
OKX_DEMO_REST_URL = "https://www.okx.com"
_OKX_TOO_MANY_REQUESTS_CODE = "50011"
_RETRYABLE_PUBLIC_HTTP_STATUS = {429, 500, 502, 503, 504}


class OkxPublicRestRequester:
    """Shared unauthenticated OKX REST transport with bounded retry/backoff."""

    def __init__(
        self,
        *,
        config: ExchangeConfig,
        http_client: HttpClient,
    ) -> None:
        self._config = config
        self._http = http_client
        self._base_url = (
            OKX_DEMO_REST_URL
            if config.sandbox
            else OKX_PROD_REST_URL
        )

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        attempts = max(
            1,
            int(os.getenv("OKX_PUBLIC_REST_RETRY_ATTEMPTS", "5")),
        )
        base_sleep = max(
            0.0,
            float(
                os.getenv(
                    "OKX_PUBLIC_REST_RETRY_BACKOFF_SECONDS",
                    "2",
                )
            ),
        )
        max_sleep = max(
            base_sleep,
            float(
                os.getenv(
                    "OKX_PUBLIC_REST_RETRY_MAX_SLEEP_SECONDS",
                    "30",
                )
            ),
        )
        last_error: ExchangeApiError | None = None
        for attempt in range(1, attempts + 1):
            try:
                return await self._http.request(
                    method,
                    f"{self._base_url}{path}",
                    params=params,
                    timeout_seconds=self._config.timeout_seconds,
                )
            except ExchangeApiError as exc:
                last_error = exc
                if (
                    attempt >= attempts
                    or not _is_retryable_public_error(exc)
                ):
                    raise
                await asyncio.sleep(
                    _retry_sleep_seconds(
                        attempt,
                        base_sleep=base_sleep,
                        max_sleep=max_sleep,
                    )
                )
        assert last_error is not None
        raise last_error


def _is_retryable_public_error(exc: ExchangeApiError) -> bool:
    if exc.status_code in _RETRYABLE_PUBLIC_HTTP_STATUS:
        return True
    payload = exc.payload
    return (
        isinstance(payload, Mapping)
        and str(payload.get("code")) == _OKX_TOO_MANY_REQUESTS_CODE
    )


def _retry_sleep_seconds(
    attempt: int,
    *,
    base_sleep: float,
    max_sleep: float,
) -> float:
    if base_sleep <= 0:
        return 0.0
    return min(
        base_sleep * (2 ** max(0, attempt - 1)),
        max_sleep,
    )


__all__ = [
    "OKX_DEMO_REST_URL",
    "OKX_PROD_REST_URL",
    "OkxPublicRestRequester",
]
