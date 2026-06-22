from __future__ import annotations

import pytest

from src.platform.exchanges.http import RequestsHttpClient


class _Response:
    status_code = 200
    text = "{}"


def test_requests_http_client_sends_compact_json_body(monkeypatch):
    captured = {}

    def fake_request(method, url, *, params=None, data=None, json=None, headers=None, timeout=None):
        captured.update({"method": method, "url": url, "params": params, "data": data, "json": json, "headers": headers, "timeout": timeout})
        return _Response()

    import requests

    monkeypatch.setattr(requests, "request", fake_request)
    result = RequestsHttpClient._request_sync(
        "POST",
        "https://example.test/order",
        params=None,
        json_body={"instId": "ETH-USDT-SWAP", "sz": "1"},
        headers={"X-Test": "1"},
        timeout_seconds=1,
    )

    assert result == {}
    assert captured["json"] is None
    assert captured["data"] == '{"instId":"ETH-USDT-SWAP","sz":"1"}'
    assert captured["headers"]["Content-Type"] == "application/json"
