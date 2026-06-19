import io
from urllib.error import HTTPError

import pytest

from src.platform.exchanges.errors import ExchangeApiError
from src.platform.exchanges import http as http_module
from src.platform.exchanges.http import StdlibHttpClient


class FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return b'{"ok":true}'


def test_stdlib_http_client_sets_non_urllib_user_agent(monkeypatch):
    captured = {}

    def fake_urlopen(req, timeout):
        captured["headers"] = {key.lower(): value for key, value in req.header_items()}
        return FakeResponse()

    monkeypatch.setattr(http_module, "urlopen", fake_urlopen)

    payload = StdlibHttpClient._request_sync("GET", "https://example.com/api", None, None, None, 1)

    assert payload == {"ok": True}
    assert captured["headers"]["user-agent"] == "AetherEdge/0.1"
    assert captured["headers"]["accept"] == "application/json"


def test_stdlib_http_client_includes_error_payload_in_exception(monkeypatch):
    def fake_urlopen(req, timeout):
        raise HTTPError(
            url=req.full_url,
            code=403,
            msg="Forbidden",
            hdrs={},
            fp=io.BytesIO(b'{"code":"403","msg":"forbidden"}'),
        )

    monkeypatch.setattr(http_module, "urlopen", fake_urlopen)

    with pytest.raises(ExchangeApiError) as exc_info:
        StdlibHttpClient._request_sync("GET", "https://example.com/api", None, None, None, 1)

    assert exc_info.value.status_code == 403
    assert exc_info.value.payload == {"code": "403", "msg": "forbidden"}
    assert "forbidden" in str(exc_info.value)
