from src.platform.exchanges import ExchangeConfig, ExchangeName, create_exchange_client


class FakeHttpClient:
    async def request(self, *args, **kwargs):  # pragma: no cover - not used here
        return {}


def test_factory_creates_okx_and_binance_clients():
    okx = create_exchange_client("okx", ExchangeConfig(), http_client=FakeHttpClient())
    binance = create_exchange_client(ExchangeName.BINANCE, ExchangeConfig(), http_client=FakeHttpClient())

    assert okx.exchange is ExchangeName.OKX
    assert binance.exchange is ExchangeName.BINANCE


def test_factory_rejects_unknown_exchange():
    try:
        create_exchange_client("bybit", ExchangeConfig(), http_client=FakeHttpClient())
    except Exception as exc:
        assert "Unsupported exchange" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("unknown exchange should fail")
