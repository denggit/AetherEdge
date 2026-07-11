import asyncio
from decimal import Decimal

import pytest

from src.platform.exchanges.errors import (
    ExchangeApiError,
    PrivateCredentialValidationError,
)
from src.platform.exchanges import (
    CancelOrderRequest,
    ExchangeConfig,
    ExchangeName,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    create_exchange_client,
)


class FakeHttpClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def request(self, method, url, *, params=None, json_body=None, headers=None, timeout_seconds=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "params": params,
                "json_body": json_body,
                "headers": headers or {},
                "timeout_seconds": timeout_seconds,
            }
        )
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def test_okx_public_klines_preserve_exchange_order_by_default():
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [
                    ["1710000060000", "3010", "3020", "3000", "3015", "13", "130", "39000", "1"],
                    ["1710000000000", "3000", "3010", "2990", "3005", "12", "120", "36000", "1"],
                ],
            }
        ]
    )
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    rows = asyncio.run(client.fetch_klines("ETH-USDT-PERP", interval="1m", limit=2))

    assert http.calls[0]["url"].endswith("/api/v5/market/candles")
    assert http.calls[0]["params"] == {"instId": "ETH-USDT-SWAP", "bar": "1m", "limit": 2}
    assert rows[0].exchange is ExchangeName.OKX
    assert rows[0].symbol == "ETH-USDT-PERP"
    assert rows[0].raw_symbol == "ETH-USDT-SWAP"
    assert [row.open_time_ms for row in rows] == [1710000060000, 1710000000000]
    assert rows[0].close_time_ms == 1710000119999
    assert rows[0].close == Decimal("3015")



def test_okx_public_klines_sets_4h_close_time_from_open_time():
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [["1710000000000", "3000", "3010", "2990", "3005", "12", "120", "36000", "1"]],
            }
        ]
    )
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    rows = asyncio.run(client.fetch_klines("ETH-USDT-PERP", interval="4h", limit=1))

    assert http.calls[0]["params"]["bar"] == "4H"
    assert rows[0].open_time_ms == 1710000000000
    assert rows[0].close_time_ms == 1710014399999


def test_okx_public_klines_maps_normalized_4h_to_exchange_4H():
    http = FakeHttpClient([{"code": "0", "data": []}])
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    rows = asyncio.run(client.fetch_klines("ETH-USDT-PERP", interval="4h", limit=10))

    assert rows == []
    assert http.calls[0]["params"]["bar"] == "4H"


def test_okx_public_klines_can_normalize_oldest_first_when_requested():
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [
                    ["1710000060000", "3010", "3020", "3000", "3015", "13", "130", "39000", "1"],
                    ["1710000000000", "3000", "3010", "2990", "3005", "12", "120", "36000", "1"],
                ],
            }
        ]
    )
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    rows = asyncio.run(client.fetch_klines("ETH-USDT-PERP", interval="1m", limit=2, oldest_first=True))

    assert [row.open_time_ms for row in rows] == [1710000000000, 1710000060000]


def test_okx_public_klines_uses_history_candles_for_time_range():
    start = 1710000000000
    step = 60_000
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [
                    [str(start + step * 3), "3030", "3040", "3020", "3035", "15", "150", "45500", "1"],
                    [str(start + step * 2), "3020", "3030", "3010", "3025", "14", "140", "42350", "1"],
                    [str(start + step), "3010", "3020", "3000", "3015", "13", "130", "39000", "1"],
                    [str(start), "3000", "3010", "2990", "3005", "12", "120", "36000", "1"],
                ],
            }
        ]
    )
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    rows = asyncio.run(
        client.fetch_klines(
            "ETH-USDT-PERP",
            interval="1m",
            limit=100,
            start_time_ms=start,
            end_time_ms=start + step * 2,
            oldest_first=True,
        )
    )

    assert http.calls[0]["url"].endswith("/api/v5/market/history-candles")
    assert http.calls[0]["params"] == {
        "instId": "ETH-USDT-SWAP",
        "bar": "1m",
        "limit": 100,
        "after": start + step * 2 + 1_000,
    }
    assert [row.open_time_ms for row in rows] == [start, start + step, start + step * 2]



def test_okx_fetch_historical_trades_filters_time_range():
    http = FakeHttpClient([
        {
            "code": "0",
            "data": [
                {"tradeId": "3", "px": "100.3", "sz": "1", "side": "buy", "ts": "3000"},
                {"tradeId": "2", "px": "100.2", "sz": "2", "side": "sell", "ts": "2000"},
                {"tradeId": "1", "px": "100.1", "sz": "3", "side": "buy", "ts": "1000"},
            ],
        }
    ])
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    rows = asyncio.run(client.fetch_trades("ETH-USDT-PERP", start_time_ms=1500, end_time_ms=3000, limit=100, oldest_first=True))

    assert http.calls[0]["url"].endswith("/api/v5/market/history-trades")
    assert http.calls[0]["params"]["instId"] == "ETH-USDT-SWAP"
    assert [row.trade_id for row in rows] == ["2", "3"]
    assert rows[0].price == Decimal("100.2")
    assert rows[0].side is OrderSide.SELL


def test_okx_place_and_cancel_order_use_same_business_request_model():
    http = FakeHttpClient(
        [
            {"code": "0", "data": [{"ordId": "okx-1", "clOrdId": "client-1", "sCode": "0"}]},
            {"code": "0", "data": [{"ordId": "okx-1", "clOrdId": "client-1", "sCode": "0"}]},
        ]
    )
    cfg = ExchangeConfig(api_key="key", api_secret="secret", passphrase="pass", sandbox=True)
    client = create_exchange_client("okx", cfg, http_client=http)

    order = asyncio.run(
        client.place_order(
            OrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.01"),
                client_order_id="client-1",
            )
        )
    )
    canceled = asyncio.run(client.cancel_order(CancelOrderRequest(symbol="ETH-USDT-PERP", order_id="okx-1")))

    assert http.calls[0]["url"].endswith("/api/v5/trade/order")
    assert http.calls[0]["json_body"] == {
        "instId": "ETH-USDT-SWAP",
        "tdMode": "cross",
        "side": "buy",
        "ordType": "market",
        "sz": "0.01",
        "clOrdId": "client-1",
    }
    assert http.calls[0]["headers"]["OK-ACCESS-KEY"] == "key"
    assert http.calls[0]["headers"]["x-simulated-trading"] == "1"
    assert order.status is OrderStatus.NEW
    assert canceled.status is OrderStatus.CANCELED


def test_okx_private_requests_include_content_type_header():
    http = FakeHttpClient([{"code": "0", "data": [{"details": [{"ccy": "USDT", "cashBal": "1", "availBal": "1"}]}]}])
    cfg = ExchangeConfig(api_key="k", api_secret="s", passphrase="p")
    client = create_exchange_client("okx", cfg, http_client=http)

    import asyncio

    asyncio.run(client.fetch_balance("USDT"))

    headers = http.calls[0]["headers"]
    assert headers["Content-Type"] == "application/json"
    assert "OK-ACCESS-KEY" in headers


def test_okx_private_request_rejects_placeholder_before_signing_or_http_call():
    http = FakeHttpClient([])
    client = create_exchange_client(
        ExchangeName.OKX,
        ExchangeConfig(
            api_key="canary_okx_key",
            api_secret="你的_okx_secret_key",
            passphrase="canary_okx_passphrase",
        ),
        http_client=http,
    )

    with pytest.raises(PrivateCredentialValidationError) as exc_info:
        asyncio.run(client.fetch_balance("USDT"))

    text = str(exc_info.value)
    assert exc_info.value.code == "placeholder_private_credentials"
    assert "placeholder_fields=api_secret" in text
    assert "canary_okx_key" not in text
    assert "canary_okx_passphrase" not in text
    assert http.calls == []


def test_okx_public_request_retries_rate_limited_history_trades(monkeypatch):
    monkeypatch.setenv("OKX_PUBLIC_REST_RETRY_ATTEMPTS", "2")
    monkeypatch.setenv("OKX_PUBLIC_REST_RETRY_BACKOFF_SECONDS", "0")
    http = FakeHttpClient(
        [
            ExchangeApiError("HTTP 429 from exchange API", status_code=429, payload={"code": "50011", "msg": "Too Many Requests"}),
            {
                "code": "0",
                "data": [{"tradeId": "1", "px": "100.1", "sz": "1", "side": "buy", "ts": "2000"}],
            },
            {"code": "0", "data": []},
        ]
    )
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    rows = asyncio.run(client.fetch_trades("ETH-USDT-PERP", start_time_ms=1000, end_time_ms=3000, limit=100, oldest_first=True))

    assert len(http.calls) == 3
    assert [row.trade_id for row in rows] == ["1"]


def test_okx_public_request_does_not_retry_non_retryable_error(monkeypatch):
    monkeypatch.setenv("OKX_PUBLIC_REST_RETRY_ATTEMPTS", "3")
    monkeypatch.setenv("OKX_PUBLIC_REST_RETRY_BACKOFF_SECONDS", "0")
    error = ExchangeApiError("HTTP 400 from exchange API", status_code=400, payload={"code": "51000"})
    http = FakeHttpClient([error])
    client = create_exchange_client(ExchangeName.OKX, ExchangeConfig(), http_client=http)

    try:
        asyncio.run(client.fetch_ticker("ETH-USDT-PERP"))
    except ExchangeApiError as exc:
        assert exc is error
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("expected ExchangeApiError")

    assert len(http.calls) == 1


def test_okx_history_trades_fails_when_page_cap_cannot_prove_start(
    monkeypatch,
):
    monkeypatch.setenv("OKX_HISTORY_TRADES_MAX_PAGES", "1")
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [
                    {
                        "tradeId": "2",
                        "px": "100",
                        "sz": "1",
                        "side": "buy",
                        "ts": "2000",
                    }
                ],
            }
        ]
    )
    client = create_exchange_client(
        ExchangeName.OKX, ExchangeConfig(), http_client=http
    )

    with pytest.raises(ExchangeApiError, match="pagination limit"):
        asyncio.run(
            client.fetch_trades(
                "ETH-USDT-PERP",
                start_time_ms=1000,
                end_time_ms=3000,
                limit=100,
            )
        )


def _okx_trade_row(trade_id: int) -> dict[str, str]:
    return {
        "tradeId": str(trade_id),
        "px": "100",
        "sz": "1",
        "side": "buy",
        "ts": str(1_782_911_250_000 + trade_id),
    }


def test_okx_trade_id_anchored_history_pages_from_newer_anchor() -> None:
    newer_trade_id = 4_048_126_437
    older_trade_id = 4_048_125_172
    pages = []
    cursor = newer_trade_id
    for _ in range(13):
        page = [_okx_trade_row(trade_id) for trade_id in range(cursor - 1, cursor - 101, -1)]
        pages.append({"code": "0", "data": page})
        cursor -= 100
    http = FakeHttpClient(pages)
    client = create_exchange_client(
        ExchangeName.OKX,
        ExchangeConfig(),
        http_client=http,
    )

    rows = asyncio.run(
        client.fetch_trades_between_ids(
            "ETH-USDT-PERP",
            newer_trade_id=str(newer_trade_id),
            older_trade_id=str(older_trade_id),
            limit=100,
            max_pages=20,
            oldest_first=True,
        )
    )

    assert len(http.calls) == 13
    assert http.calls[0]["params"]["after"] == str(newer_trade_id)
    for call, response in zip(http.calls[1:], pages[:-1], strict=True):
        assert call["params"]["after"] == response["data"][-1]["tradeId"]
    returned_ids = [int(row.trade_id) for row in rows]
    assert returned_ids == sorted(returned_ids)
    assert returned_ids[0] == older_trade_id + 1
    assert returned_ids[-1] == newer_trade_id - 1
    assert older_trade_id not in returned_ids
    assert newer_trade_id not in returned_ids
    assert all(
        older_trade_id < trade_id < newer_trade_id
        for trade_id in returned_ids
    )
    assert client.last_historical_trade_pages == 13


def test_okx_trade_id_anchored_history_first_request_requires_after() -> None:
    class RejectUnanchoredHttpClient(FakeHttpClient):
        async def request(self, method, url, **kwargs):
            params = kwargs.get("params") or {}
            assert params.get("after") == "20"
            return {
                "code": "0",
                "data": [
                    _okx_trade_row(19),
                    _okx_trade_row(10),
                ],
            }

    client = create_exchange_client(
        ExchangeName.OKX,
        ExchangeConfig(),
        http_client=RejectUnanchoredHttpClient([]),
    )

    rows = asyncio.run(
        client.fetch_trades_between_ids(
            "ETH-USDT-PERP",
            newer_trade_id="20",
            older_trade_id="10",
        )
    )

    assert [row.trade_id for row in rows] == ["19"]


def test_okx_trade_id_anchored_history_fails_before_older_coverage() -> None:
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [
                    _okx_trade_row(199),
                    _okx_trade_row(198),
                ],
            },
            {
                "code": "0",
                "data": [
                    _okx_trade_row(197),
                    _okx_trade_row(196),
                ],
            },
        ]
    )
    client = create_exchange_client(
        ExchangeName.OKX,
        ExchangeConfig(),
        http_client=http,
    )

    with pytest.raises(
        ExchangeApiError,
        match="before older_trade_id coverage",
    ):
        asyncio.run(
            client.fetch_trades_between_ids(
                "ETH-USDT-PERP",
                newer_trade_id="200",
                older_trade_id="100",
                limit=100,
                max_pages=2,
            )
        )


def test_okx_trade_id_anchored_history_deduplicates_ids() -> None:
    http = FakeHttpClient(
        [
            {
                "code": "0",
                "data": [
                    _okx_trade_row(14),
                    _okx_trade_row(13),
                    _okx_trade_row(13),
                    _okx_trade_row(10),
                ],
            }
        ]
    )
    client = create_exchange_client(
        ExchangeName.OKX,
        ExchangeConfig(),
        http_client=http,
    )

    rows = asyncio.run(
        client.fetch_trades_between_ids(
            "ETH-USDT-PERP",
            newer_trade_id="15",
            older_trade_id="10",
        )
    )

    assert [row.trade_id for row in rows] == ["13", "14"]
