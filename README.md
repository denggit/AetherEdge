# AetherEdge

AetherEdge 是一个面向 ETH 永续合约的多交易所交易执行框架。

当前阶段只做一件事：把 OKX / Binance 的交易所接口统一封装，业务层以后只依赖 `ExchangeClient` 协议和 `create_exchange_client()` 工厂函数，不直接碰交易所 REST endpoint、签名、raw symbol、payload 字段。

## 快速使用

```python
from decimal import Decimal

from src.exchanges import (
    ExchangeName,
    ExchangeConfig,
    OrderRequest,
    OrderSide,
    OrderType,
    create_exchange_client,
)

client = create_exchange_client(
    ExchangeName.OKX,
    ExchangeConfig(api_key="...", api_secret="...", passphrase="..."),
)

# 两个平台方法名一致，只换 ExchangeName / 配置即可。
await client.fetch_klines("ETH-USDT-PERP", interval="1m", limit=100)

order = await client.place_order(
    OrderRequest(
        symbol="ETH-USDT-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.01"),
        reduce_only=False,
    )
)
```

## 当前边界

- 业务代码只能依赖 `src.exchanges.ports.ExchangeClient` 和统一模型。
- OKX / Binance endpoint、签名、payload 映射只能出现在各自 adapter 目录。
- canonical symbol 使用 `ETH-USDT-PERP`，adapter 内部转换成：
  - OKX: `ETH-USDT-SWAP`
  - Binance USD-M: `ETHUSDT`
- 暂未实现 WebSocket、账户模式切换、杠杆设置、TP/SL algo order。后续独立加，不塞进当前 client。

## 测试

```bash
PYTHONPATH=. pytest -q
```
