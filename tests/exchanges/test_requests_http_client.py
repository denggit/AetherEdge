from types import SimpleNamespace

import pytest

from src.platform.exchanges import http as http_module
from src.platform.exchanges.errors import ExchangeApiError
from src.platform.exchanges.factory import create_exchange_client
from src.platform.exchanges.http import RequestsHttpClient
from src.platform.exchanges.models import ExchangeConfig


def test_factory_uses_requests_http_client_by_default():
    client = create_exchange_client("okx", ExchangeConfig())
    assert client._http.__class__.__name__ == "RequestsHttpClient"


def test_requests_http_client_sets_default_headers(monkeypatch):
    captured = {}

    def fake_request(method, url, *, params=None, json=None, headers=None, timeout=None):
        captured.update({"method": method, "url": url, "params": params, "json": json, "headers": headers, "timeout": timeout})
        return SimpleNamespace(status_code=200, text='{"ok":true}')

    monkeypatch.setattr(http_module.requests if hasattr(http_module, 'requests') else __import__('requests'), "request", fake_request)
    payload = RequestsHttpClient._request_sync("GET", "https://example.com/api", {"a": 1}, None, None, 1)

    assert payload == {"ok": True}
    assert captured["headers"]["User-Agent"] == "AetherEdge/0.1"
    assert captured["headers"]["Accept"] == "application/json"


def test_requests_http_client_keeps_error_payload(monkeypatch):
    def fake_request(method, url, *, params=None, json=None, headers=None, timeout=None):
        return SimpleNamespace(status_code=403, text='{"code":"403","msg":"forbidden"}')

    monkeypatch.setattr(http_module.requests if hasattr(http_module, 'requests') else __import__('requests'), "request", fake_request)

    with pytest.raises(ExchangeApiError) as exc_info:
        RequestsHttpClient._request_sync("GET", "https://example.com/api", None, None, None, 1)

    assert exc_info.value.status_code == 403
    assert exc_info.value.payload == {"code": "403", "msg": "forbidden"}
    assert "forbidden" in str(exc_info.value)
