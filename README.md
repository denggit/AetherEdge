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


## Stop Market Order Interface

止损单属于执行接口层，不包含策略判断。

手动传数量挂 reduce-only 止损：

```python
from decimal import Decimal
from src.platform import OrderSide, StopMarketOrderRequest

await execution.place_stop_market_order(
    StopMarketOrderRequest(
        symbol="ETH-USDT-PERP",
        side=OrderSide.SELL,
        quantity=Decimal("0.01"),
        trigger_price=Decimal("2800"),
        reduce_only=True,
    )
)
```

根据当前持仓直接挂对应方向止损：

```python
positions = await account.fetch_positions("ETH-USDT-PERP")
position = next(p for p in positions if p.quantity != 0)

await execution.place_stop_loss_for_position(
    position,
    trigger_price=Decimal("2800"),
)
```

方向规则只做接口级映射：

```text
long  position -> SELL stop
short position -> BUY stop
net   position -> 根据 quantity 正负判断平仓方向
```

底层 adapter：

```text
OKX:     POST /api/v5/trade/order-algo
Binance: POST /fapi/v1/algoOrder
```

Binance 对“按当前持仓直接挂止损”默认使用 `closePosition=true`，不传 `quantity` 和 `reduceOnly`，避免数量和持仓变化不一致。OKX 使用当前持仓数量，并带 `reduceOnly=true`。


## Order State + Live Safety Gate

执行层现在补齐了订单状态闭环，但仍然只属于接口层，不包含策略、TP/SL、runtime 主循环。

查询单个订单：

```python
from src.platform import OrderQuery

order = await execution.fetch_order_status(
    OrderQuery(symbol="ETH-USDT-PERP", order_id="123")
)
```

查询当前挂单：

```python
open_orders = await execution.fetch_open_orders()
```

底层 adapter：

```text
OKX:     GET /api/v5/trade/order
OKX:     GET /api/v5/trade/orders-pending
Binance: GET /fapi/v1/order
Binance: GET /fapi/v1/openOrders
```

真实盘写操作有硬保护。默认 `.env` 里应该显式写：

```text
AETHER_LIVE_TRADING=false
```

当 `OKX_SANDBOX=false` 或 `BINANCE_SANDBOX=false` 时，除非同时设置：

```text
AETHER_LIVE_TRADING=true
```

否则这些写操作会被挡住：

```text
place_order
amend_order
replace_order
```

`cancel_order` 不挡，因为误连真实盘时撤单是降风险动作，不应该被安全开关拦住。

完整 `.env` 示例：

```text
AETHER_MARKET=ETH-USDT-PERP
AETHER_LIVE_TRADING=false

API_TIMEOUT_SECONDS=10
BINANCE_RECV_WINDOW_MS=5000
MARGIN_MODE=cross

OKX_API_KEY=
OKX_SECRET_KEY=
OKX_PASSPHRASE=
OKX_SANDBOX=true

BINANCE_API_KEY=
BINANCE_SECRET_KEY=
BINANCE_SANDBOX=true
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

## Execution Safety v1

执行层现在会在下单前尽量读取交易规则：

```text
OKX:     GET /api/v5/public/instruments
Binance: GET /fapi/v1/exchangeInfo
```

会自动处理：

```text
price_tick      价格精度
quantity_step   数量步长
min_quantity    最小下单量
min_notional    最小名义金额，Binance 支持
```

默认下单前会做基础校验和精度向下规整：

```python
from decimal import Decimal
from src.platform import OrderRequest, OrderSide, OrderType

await execution.place_order(
    OrderRequest(
        symbol="ETH-USDT-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.019"),
        price=Decimal("3000.19"),
    )
)
```

原生改单：

```python
from decimal import Decimal
from src.platform import AmendOrderRequest

await execution.amend_order(
    AmendOrderRequest(
        symbol="ETH-USDT-PERP",
        order_id="123",
        new_quantity=Decimal("0.02"),
        new_price=Decimal("3000.1"),
    )
)
```

Binance 原生改单要求 `new_quantity` 和 `new_price` 同时传入；OKX 可以只改数量或只改价格。

## WebSocket reconnect

行情 WebSocket 默认开启自动重连：

```python
data = create_market_data_feed(
    "binance",
    symbol="ETH-USDT-PERP",
    reconnect_streams=True,
    reconnect_delay_seconds=1,
)
```

测试或一次性消费时可以关闭：

```python
data = create_market_data_feed("okx", reconnect_streams=False)
```

## Multi-exchange execution

多交易所并行执行雏形：

```python
from src.platform.execution import MultiExchangeExecutionClient

multi = MultiExchangeExecutionClient([okx_execution, binance_execution])
results = await multi.place_order_all(order_request)
```

单个交易所失败不会把其他交易所的结果吞掉，返回里会保留 `order` 或 `error`。

## Market Profiles

平台默认市场是：

```text
ETH-USDT-PERP
```

本地品种参数放在：

```text
src/platform/markets/profiles/ETH-USDT-PERP.json
```

当前 ETH 配置示例：

```json
{
  "symbol": "ETH-USDT-PERP",
  "base_asset": "ETH",
  "quote_asset": "USDT",
  "contract_type": "perp",
  "default": true,
  "exchange_symbols": {
    "okx": "ETH-USDT-SWAP",
    "binance": "ETHUSDT"
  },
  "contract_value_by_exchange": {
    "okx": "0.01",
    "binance": "1"
  },
  "min_quantity_by_exchange": {
    "okx": "0.01",
    "binance": "0.001"
  },
  "quantity_unit_by_exchange": {
    "okx": "contract",
    "binance": "base_asset"
  }
}
```

获取配置：

```python
from src.platform import get_market_profile

profile = get_market_profile()
print(profile.symbol)
print(profile.raw_symbol("okx"))
print(profile.contract_value("okx"))
```

绑定不同品种：

```python
symbol = "ETH-USDT-PERP"

data = create_market_data_feed("okx", symbol=symbol)
execution = create_execution_client("okx", symbol=symbol)
account = create_account_client("okx", symbol=symbol)
```

以后加其他币种，不改业务代码，新增一个 profile JSON 即可，例如：

```text
src/platform/markets/profiles/SOL-USDT-PERP.json
```

然后业务层只改：

```python
symbol = "SOL-USDT-PERP"
```

## Interface Freeze v1

这一版开始，平台接口层先封口。接口层只负责“能不能稳定调用交易平台”，不包含策略判断、自动开平仓、TP/SL 编排、runtime 主循环。

执行接口现在包括：

```python
await execution.place_order(order_request)
await execution.place_stop_market_order(stop_request)
await execution.place_stop_loss_for_position(position, trigger_price=...)

await execution.cancel_order(cancel_request)
await execution.cancel_all_orders()
await execution.cancel_stop_order(cancel_stop_request)
await execution.cancel_all_stop_orders()

await execution.amend_order(amend_request)
await execution.replace_order(cancel_request, new_order_request)

await execution.fetch_order_status(order_query)
await execution.fetch_open_orders()
await execution.fetch_stop_order_status(stop_order_query)
await execution.fetch_open_stop_orders()
```

账户 / 配置接口现在包括：

```python
await account.fetch_balance("USDT")
await account.fetch_positions()

await account.fetch_leverage()
await account.set_leverage(Decimal("3"))
await account.set_margin_mode(MarginMode.CROSS)
await account.fetch_position_mode()
await account.set_position_mode(PositionMode.ONE_WAY)
```

止损 / 条件单管理底层映射：

```text
OKX:
  POST /api/v5/trade/order-algo
  GET  /api/v5/trade/order-algo
  GET  /api/v5/trade/orders-algo-pending
  POST /api/v5/trade/cancel-algos

Binance USD-M:
  POST   /fapi/v1/algoOrder
  GET    /fapi/v1/algoOrder
  GET    /fapi/v1/openAlgoOrders
  DELETE /fapi/v1/algoOrder
  DELETE /fapi/v1/algoOpenOrders
```

普通订单批量撤单：

```text
OKX:     当前实现为 fetch_open_orders 后逐个 cancel_order
Binance: DELETE /fapi/v1/allOpenOrders
```

杠杆 / 仓位模式：

```text
OKX:
  GET  /api/v5/account/leverage-info
  POST /api/v5/account/set-leverage
  GET  /api/v5/account/config
  POST /api/v5/account/set-position-mode

Binance USD-M:
  GET  /fapi/v3/positionRisk        # 读取当前 leverage
  POST /fapi/v1/leverage
  POST /fapi/v1/marginType
  GET  /fapi/v1/positionSide/dual
  POST /fapi/v1/positionSide/dual
```

注意：OKX 的 `tdMode` 是下单参数，所以 `account.set_margin_mode()` 在 OKX adapter 中是无网络请求的接口级 no-op；真正下单时仍然由 `OrderRequest.margin_mode` 或 `.env` 的 `MARGIN_MODE` 控制。

## Startup Snapshot / Smoke

接口层提供只读启动快照，不做恢复、不撤单、不补单，只把当前交易所状态一次性读出来：

```python
from src.platform import fetch_platform_snapshot

snapshot = await fetch_platform_snapshot(account=account, execution=execution)
```

包含：

```text
balance
positions
open_orders
open_stop_orders
leverage
position_mode
```

只读 smoke 脚本：

```bash
PYTHONPATH=. python tools/smoke_public.py okx
PYTHONPATH=. python tools/smoke_public.py binance

PYTHONPATH=. python tools/smoke_private_readonly.py okx
PYTHONPATH=. python tools/smoke_private_readonly.py binance
```

`smoke_private_readonly.py` 不会下单、改单或撤单；它只验证私有读接口和当前状态读取是否可用。

## Private Event Stream v1

私有事件流是单独接口模块，只负责把交易所私有 WebSocket 事件统一成 `AccountEvent`，不做策略判断、不做自动恢复、不维护本地订单状态机。

```python
from src.platform import create_account_event_stream, AccountEventType

stream = create_account_event_stream("okx")

async for event in stream.stream_events():
    if event.event_type is AccountEventType.ORDER:
        print(event.order_id, event.order_status, event.filled_quantity)
```

统一事件类型：

```text
AccountEventType.ORDER
AccountEventType.BALANCE
AccountEventType.POSITION
AccountEventType.SYSTEM
AccountEventType.UNKNOWN
```

OKX：

```text
wss://ws.okx.com:8443/ws/v5/private
wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999  # demo

login 后订阅：
orders
account
positions
```

Binance USD-M：

```text
POST /fapi/v1/listenKey
wss://fstream.binance.com/ws/<listenKey>
wss://stream.binancefuture.com/ws/<listenKey>  # testnet
```

Binance listenKey 辅助接口：

```python
listen_key = await exchange.create_user_stream_listen_key()
await exchange.keepalive_user_stream_listen_key(listen_key)
await exchange.close_user_stream_listen_key(listen_key)
```

当前 v1 只做事件映射和连接。listenKey 定时续期、断线状态追赶、本地订单状态机、启动恢复动作，后续放 runtime/state 模块，不放进接口层。

## State Store v1

本地状态库只负责留存证据和支持重启读取，不做交易决策。

主要用途：

```text
1. 程序重启后知道之前有什么普通挂单 / 止损挂单
2. 保存私有事件流里的订单、余额、仓位事件
3. 保存成交 fills，方便复盘滑点、手续费、成交质量
4. 保存账户启动快照，方便对账和事故排查
5. 把交易所状态和本地 runtime 状态分开，避免策略层直接依赖交易所 raw payload
```

使用方式：

```python
from src.platform import SqliteStateStore

store = SqliteStateStore("data/state/aether_state.sqlite3")

store.save_order(order)
store.save_account_event(event)
store.save_snapshot(snapshot)

open_orders = store.list_open_orders(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP")
recent_events = store.load_recent_events(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP")
recent_fills = store.load_recent_fills(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP")
```

SQLite 表：

```text
orders              当前订单状态快照
fills               成交记录
events              私有事件原始记录
account_snapshots   账户启动快照
```

边界：State Store 不会调用交易所 API，不会下单，不会撤单，不会恢复订单；恢复逻辑后面放 runtime/state reconciler 单独做。

## Runtime Skeleton v1

Runtime Skeleton 是框架编排层，不包含策略逻辑。

它只负责：

```text
1. 组装 data / execution / account / event_stream / state_store
2. 启动时读取 snapshot
3. 把 snapshot 写入 State Store
4. 消费 Private Event Stream
5. 把私有事件写入 State Store
6. 通过 RuntimeEventHandler 给未来策略/插件预留观察接口
```

它不做：

```text
不开仓
不平仓
不撤单
不补止损
不做 TP/SL 业务判断
不做订单恢复动作
```

代码结构：

```text
src/platform/runtime/
  config.py      # RuntimeConfig
  context.py     # RuntimeContext，依赖注入容器
  factory.py     # build_runtime_context()
  handlers.py    # RuntimeEventHandler / NoopRuntimeEventHandler
  service.py     # PlatformRuntime 生命周期服务

src/platform/strategy/
  ports.py       # StrategyPort，未来策略接入协议
```

设计模式：

```text
Port / Adapter：runtime 只依赖 data / execution / account / state 的协议
Factory：build_runtime_context 负责组装默认实现
Dependency Injection：测试和未来实盘都可以注入自己的 client/store/handler
Observer：RuntimeEventHandler 只观察 snapshot/event，不直接触碰交易所 adapter
Strategy Port：给未来策略留接口，但现在不实现策略
```

空跑方式：

```bash
PYTHONPATH=. python tools/run_runtime_skeleton.py okx --no-event-stream
PYTHONPATH=. python tools/run_runtime_skeleton.py binance --max-events 10
```

`--no-event-stream` 只做启动 snapshot；`--max-events` 用于测试私有事件流，避免进程一直运行。

## Module Placement Rules

这次边界重新审视后，模块归属定为：

```text
src/platform/
  data/        平台行情接口
  execution/   平台执行接口
  account/     平台账户/私有事件接口
  exchanges/   交易所 adapter
  markets/     品种配置
  state/       本地状态存储，不是状态机
  runtime/     平台生命周期编排，只负责启动、snapshot、事件落库
  snapshot.py  只读状态快照
  config.py    env/config 读取

src/strategy/
  ports.py     策略接入协议，未来策略放这里，不放 platform
```

`platform/state` 仍然保留在 platform，因为它是交易平台底座的本地状态存储，服务于订单、成交、事件和账户快照的落库。

但有一条硬边界：

```text
State Store 只存储，不做状态机，不做恢复，不做对账，不做策略，不调用交易所 API。
```

以后这些模块不要放进 `platform/state`：

```text
reconciler
startup recovery
order repair
tp/sl manager
strategy scheduler
signal processor
```

这些属于更上层的 live/app/runtime 扩展，后续应该单独建模块，不能污染平台接口层。

本次调整：

```text
已移动：src/platform/strategy/ -> src/strategy/
已删除：src/platform/strategy/
已新增 boundary test，确保 strategy 不 import 交易所 adapter / REST endpoint
已新增 module placement test，限制 platform 顶层模块继续膨胀
```
