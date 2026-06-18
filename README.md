# AetherEdge

AetherEdge 是一个面向 ETH 永续合约的多交易所交易执行框架。

当前核心目标：策略代码只依赖稳定的平台接口，不直接碰 OKX / Binance endpoint、签名、raw symbol、payload 字段。

## 推荐入口

所有平台能力都收敛在 `src/platform/`：

```text
src/platform/
  data/        # 行情、K线、tick、orderbook、本地缓存
  execution/   # 下单、撤单、换单
  account/     # 余额、仓位
  exchanges/   # OKX / Binance adapter，唯一允许放交易所 endpoint 的地方
```

实盘业务建议只用三个入口：

```python
from src.platform import (
    create_market_data_feed,
    create_execution_client,
    create_account_client,
)

symbol = "ETH-USDT-PERP"

data = create_market_data_feed("okx", symbol=symbol)
execution = create_execution_client("okx")
account = create_account_client("okx")
```

## 行情接口

```python
klines = await data.fetch_klines(interval="1m", limit=100)
ticker = await data.fetch_ticker()

async for trade in data.stream_trades():
    print(trade.price, trade.quantity, trade.side)

async for book in data.stream_order_book():
    print(book.bids[0], book.asks[0])
```

带 SQLite 缓存：

```python
data = create_market_data_feed(
    "okx",
    symbol="ETH-USDT-PERP",
    sqlite_path="data/cache/market_data.sqlite3",
)
```

K线默认保留交易所返回顺序。需要统一成从旧到新时再显式传：

```python
klines = await data.fetch_klines(interval="1m", limit=100, oldest_first=True)
```

## 执行接口

```python
from decimal import Decimal
from src.platform import OrderRequest, OrderSide, OrderType

order = await execution.place_order(
    OrderRequest(
        symbol="ETH-USDT-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.01"),
    )
)
```

换单目前是基础版：先撤旧单，再下新单。

```python
await execution.replace_order(cancel_request, new_order_request)
```

## 账户接口

```python
balance = await account.fetch_balance("USDT")
positions = await account.fetch_positions("ETH-USDT-PERP")
```

## API KEY

只保留长期维护的 key 名称，旧的错误 fallback 已删除。

```text
# OKX
OKX_API_KEY=
OKX_SECRET_KEY=
OKX_PASSPHRASE=

# Binance USD-M Futures
BINANCE_API_KEY=
BINANCE_SECRET_KEY=
```

`create_execution_client("okx")` / `create_account_client("okx")` 在未显式传入 `ExchangeConfig` 时会自动读取项目根目录 `.env`，并用系统环境变量覆盖同名 key。

## 交易所符号

业务层统一使用：

```text
ETH-USDT-PERP
```

adapter 内部自动转换：

```text
OKX:     ETH-USDT-SWAP
Binance: ETHUSDT
```

## 当前边界

- `src/platform/data` 不能出现 OKX/Binance REST endpoint。
- `src/platform/data` 不能调用下单、撤单、余额、仓位接口。
- `src/platform/execution` / `src/platform/account` 不能出现交易所 REST endpoint。
- OKX / Binance endpoint、签名、payload 映射只能出现在 `src/platform/exchanges/*/client.py`。

## 测试

```bash
PYTHONPATH=. pytest -q
```
