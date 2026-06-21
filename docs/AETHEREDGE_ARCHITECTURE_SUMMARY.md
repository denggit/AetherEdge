# AetherEdge 现有项目架构总结与开发边界说明

> 目的：本文件用于放入项目 Sources，作为后续 AetherEdge 开发的架构边界说明。以后新增功能前必须先判断功能归属，避免把数据管线、实盘编排、订单管理、策略逻辑全部糅杂进同一个目录或同一个文件。

---

## 1. 项目定位

AetherEdge 是实盘执行架构，不是回测框架。

```text
CoinBacktest = 策略研究 / 回测 / 压测 / 参数验证
AetherEdge   = 实盘执行 / 数据流 / 状态恢复 / 多交易所下单 / 策略插件运行
```

两者绝对不能混淆：

- CoinBacktest 的策略逻辑可以迁移为 AetherEdge 策略插件。
- AetherEdge 不能依赖 CoinBacktest 的回测框架。
- AetherEdge 内不能放回测研究脚本。
- CoinBacktest 内不能放实盘交易执行逻辑。

---

## 2. 总体架构原则

### 2.1 `src/` 下每个目录必须代表一个大功能域

每个大功能域必须有清晰职责。不能因为方便就把不属于该功能域的代码塞进去。

```text
src/platform        = 外部平台接口层
src/app             = 应用组装层
src/planner         = 信号转执行计划
src/signals         = 标准信号模型
src/strategy        = 策略插件接口和加载器
src/reconcile       = 对账检查
src/utils           = 极少量真正通用工具
```

未来如果新增实盘数据管线、订单生命周期、运行时编排等功能，应该作为新的大功能域，而不是继续塞进 `src/platform`。

推荐新增：

```text
src/market_data       = 内部市场数据管线、warmup、本地数据、range bar
src/order_management  = 订单意图、订单生命周期、多交易所执行协调
src/runtime           = 实盘运行编排、任务队列、恢复流程
```

### 2.2 大功能域之间必须解耦

禁止出现跨层反向依赖。例如：

- `src/platform` 不能依赖 `strategies`。
- `src/platform` 不能依赖 `src/market_data`、`src/order_management`、`src/runtime`。
- `src/market_data` 不能依赖具体策略。
- `src/order_management` 不能依赖具体策略。
- 策略插件不能 import OKX/Binance adapter。

### 2.3 小模块也必须解耦

一个目录内部也不能写成大杂烩。每个小模块只负责一个明确任务。

不要出现：

```text
helper.py
utils.py
live_engine.py
manager.py
```

这种包罗万象的文件。

应该拆成：

```text
models.py
ports.py
service.py
store.py
builder.py
coordinator.py
```

并且每个文件职责要单一。

---

## 3. 当前 `src/` 目录职责说明

当前 AetherEdge 代码中，`src/` 主要包含以下功能域：

```text
src/app
src/planner
src/platform
src/reconcile
src/signals
src/strategy
src/utils
```

---

## 4. `src/app/`：应用组装层

当前职责：

```text
应用配置、上下文组装、runner、告警分发。
```

现有模块：

```text
src/app/
    __init__.py
    alerts.py      # AppAlert / AlertSink / AsyncAlertDispatcher
    config.py      # AppConfig，从 env/defaults 加载配置
    context.py     # AppContext，组合 data/execution/state/strategy/planner/alerts
    factory.py     # build_app_context
    runner.py      # AppRunner，market event -> strategy -> signal -> planner -> execution
```

允许放：

- 应用入口组装。
- AppConfig。
- AppContext。
- AppRunner。
- Alert dispatcher。

禁止放：

- 具体策略规则。
- OKX/Binance endpoint。
- warmup 细节。
- range bar 算法。
- 订单 journal 细节。
- 具体恢复策略。

`src/app` 是应用层，不是业务逻辑大杂烩。

---

## 5. `src/platform/`：平台接口层

这是最重要的边界。

`src/platform` 只应该负责外部平台/交易所接口，以及标准化平台能力。它不是实盘业务流程目录。

当前目录：

```text
src/platform/
    account/
    data/
    exchanges/
    execution/
    markets/
    runtime/
    state/
    config.py
    snapshot.py
```

### 5.1 `src/platform/exchanges/`

职责：

```text
OKX / Binance raw adapter，交易所 endpoint、签名、payload 映射。
```

现有模块：

```text
src/platform/exchanges/
    models.py          # ExchangeName / Order / Position / Kline / Ticker / InstrumentRule 等
    ports.py           # ExchangeMarketDataClient / ExchangeExecutionClient / ExchangeAccountClient
    factory.py         # create_exchange_client
    http.py            # HTTP client
    symbols.py         # canonical symbol <-> exchange symbol
    errors.py          # ExchangeError
    okx/client.py      # OKX REST adapter
    binance/client.py  # Binance REST adapter
```

允许放：

- 交易所 REST endpoint。
- 交易所签名。
- 交易所 payload mapping。
- 交易所响应 mapping。
- OKX/Binance 专属字段转换。

禁止放：

- 策略逻辑。
- warmup。
- range bar。
- 订单意图 journal。
- runtime 编排。

### 5.2 `src/platform/data/`

职责：

```text
平台行情接口 facade：REST K线、ticker、trades websocket、orderbook websocket、基础 cache。
```

现有模块：

```text
src/platform/data/
    models.py                    # MarketKline / MarketTrade / MarketOrderBook
    ports.py                     # MarketDataFeed
    factory.py                   # create_market_data_feed
    rest_feed.py                 # REST + websocket 组合 feed
    websocket/okx.py             # OKX trade/orderbook WS adapter
    websocket/binance.py         # Binance trade/orderbook WS adapter
    websocket/connector.py       # websocket connector
    storage/sqlite_store.py      # 基础 market data cache
```

允许放：

- 标准化行情模型。
- 平台行情 feed。
- OKX/Binance websocket adapter。
- 基础行情 cache。

禁止放：

- warmup 业务流程。
- range bar builder。
- range bar store。
- 4H feature buffer。
- V8 micro context。
- 策略指标。

说明：

```text
range bar 是内部衍生市场数据，不是平台接口。
warmup 是内部数据管线流程，不是平台接口。
```

因此未来应放入 `src/market_data/`，不要继续塞进 `src/platform/data/`。

### 5.3 `src/platform/execution/`

职责：

```text
平台执行接口 facade：单交易所下单、撤单、止损单、订单状态查询。
```

现有模块：

```text
src/platform/execution/
    ports.py       # ExecutionClient
    service.py     # ExchangeExecutionService
    factory.py     # create_execution_client
    multi.py       # MultiExchangeExecutionClient
    risk.py        # 基础订单校验 / live gate
    rules.py       # 数量/价格规整
```

允许放：

- 单交易所执行 facade。
- 下单请求规范化。
- 交易所规则校验。
- live trading gate。

禁止放：

- 多交易所订单生命周期编排。
- OrderIntent journal。
- 策略下单状态机。
- V8 stop 更新逻辑。
- 策略级仓位管理。

多交易所订单生命周期应该放未来的 `src/order_management/`。

### 5.4 `src/platform/account/`

职责：

```text
平台账户接口 facade：余额、仓位、杠杆、私有 account/order event stream。
```

现有模块：

```text
src/platform/account/
    events.py
    ports.py
    service.py
    factory.py
    stream.py
    event_factory.py
    websocket/okx.py
    websocket/binance.py
```

允许放：

- fetch_balance。
- fetch_positions。
- fetch_leverage。
- fetch_position_mode。
- private websocket account/order/position events。

禁止放：

- 策略恢复决策。
- V8 position state 修复。
- 自动平仓规则。
- 策略状态机。

### 5.5 `src/platform/state/`

职责：

```text
通用平台状态存储：order、fill、snapshot、account event。
```

现有模块：

```text
src/platform/state/
    models.py
    ports.py
    sqlite_store.py
```

允许放：

- StoredOrder。
- StoredFill。
- StoredAccountSnapshot。
- StoredEvent。
- 通用 SQLite state store。

禁止放：

- V8State。
- RangeBarState。
- RecoveryEngine。
- 策略仓位状态机。
- 自动修复逻辑。

### 5.6 `src/platform/runtime/`

当前职责：

```text
平台 runtime skeleton：启动 snapshot、保存 private account event。
```

现有模块：

```text
src/platform/runtime/
    config.py
    context.py
    factory.py
    handlers.py
    service.py
```

注意：

该目录当前已经存在，但未来不建议继续扩张成完整实盘 runtime。真正的实盘运行编排应该放新的 `src/runtime/`。

未来原则：

```text
保留现有 platform/runtime 的轻量平台生命周期能力。
不要把 warmup、strategy dispatch、order recovery、async workers 继续塞到这里。
```

---

## 6. `src/planner/`：信号转执行计划

职责：

```text
TradeSignal -> ExecutionPlan
```

现有模块：

```text
src/planner/
    models.py
    ports.py
    service.py
```

允许放：

- ExecutionPlan。
- PlannedExecution。
- SignalAction 到 OrderRequest 的映射。

禁止放：

- 实际下单。
- 交易所 endpoint。
- 多交易所订单状态管理。
- 策略逻辑。

---

## 7. `src/signals/`：标准信号模型

职责：

```text
策略输出的标准意图模型。
```

现有模块：

```text
src/signals/
    models.py
    ports.py
```

允许放：

- TradeSignal。
- SignalAction。
- SignalOrderType。
- SignalBatch。

禁止放：

- 下单逻辑。
- 交易所字段。
- 策略规则。

---

## 8. `src/strategy/`：策略插件接口和加载器

职责：

```text
定义策略插件接口，加载策略插件。
```

现有模块：

```text
src/strategy/
    ports.py
    loader.py
```

允许放：

- StrategyPort。
- load_strategy。
- 插件接口校验。

禁止放：

- 具体策略。
- V8 规则。
- 回测逻辑。
- 交易所 API。

具体策略必须放在：

```text
strategies/<strategy_name>/
```

---

## 9. `src/reconcile/`：对账检查

职责：

```text
本地状态与交易所状态对账，发现不一致并报告。
```

现有模块：

```text
src/reconcile/
    checker.py
    models.py
    notifier.py
    ports.py
```

允许放：

- 对账检查。
- ReconcileIssue。
- ReconcileReport。
- 对账通知。

禁止放：

- 自动下单修复。
- 策略状态修改。
- 自动平仓。
- V8 专属恢复逻辑。

---

## 10. `src/utils/`：极少量真正通用工具

职责：

```text
无业务归属的真正通用工具。
```

现有模块：

```text
src/utils/
    email_sender.py
    log.py
    log_noise.py
```

禁止把业务逻辑丢进 utils。

不允许放：

- range bar helper。
- order helper。
- strategy helper。
- warmup helper。
- recovery helper。

这些都应该去自己的功能域。

---

## 11. 未来新增大功能域

为了避免污染 `src/platform`，未来实盘 V8 需要的新能力应该新增独立大功能域。

---

## 12. `src/market_data/`：内部市场数据管线

未来新增。

职责：

```text
内部市场数据管线，不是交易所接口。
```

建议结构：

```text
src/market_data/
    __init__.py
    models.py
    ports.py

    warmup/
        __init__.py
        models.py
        service.py
        gap_detector.py
        catchup.py

    storage/
        __init__.py
        kline_store.py
        trade_store.py
        range_bar_store.py

    derived/
        __init__.py
        range_bar.py
        range_bar_builder.py
        range_bar_aggregator.py

    buffers/
        __init__.py
        kline_buffer.py
        rolling_window.py
```

允许放：

- K线 warmup。
- trades warmup。
- 数据缺口检测。
- catchup 到最新 closed bar。
- 本地 K线/trades/range bar 存储。
- range bar builder。
- rolling buffer。

禁止放：

- 交易所 endpoint。
- OKX/Binance payload。
- 策略规则。
- 下单逻辑。
- V8 规则。

依赖方向：

```text
market_data 可以依赖 src/platform/data
market_data 不能依赖 strategies
market_data 不能依赖 order_management
```

---

## 13. `src/order_management/`：订单生命周期管理

未来新增。

职责：

```text
策略信号产生后，订单如何被多个交易所安全执行、记录、追踪。
```

建议结构：

```text
src/order_management/
    __init__.py
    models.py
    ports.py

    journal/
        __init__.py
        models.py
        store.py
        service.py

    coordinator/
        __init__.py
        service.py
        result.py

    idempotency/
        __init__.py
        client_order_id.py
        duplicate_guard.py

    stops/
        __init__.py
        stop_sync.py
        stop_replace.py
```

允许放：

- OrderIntent。
- OrderJournal。
- OKX + Binance 多交易所执行协调。
- client_order_id 生成。
- duplicate order guard。
- stop order sync。
- 部分失败处理。

禁止放：

- OKX/Binance raw endpoint。
- 策略规则。
- market data warmup。
- range bar。
- V8 仓位状态。

依赖方向：

```text
order_management 可以依赖 planner/signals/platform.execution/platform.account
order_management 不能依赖 strategies
order_management 不能依赖 market_data
```

---

## 14. `src/runtime/`：实盘运行编排

未来新增。

职责：

```text
实盘系统生命周期、异步任务、warmup 编排、恢复编排、事件分发。
```

建议结构：

```text
src/runtime/
    __init__.py
    config.py
    context.py
    runner.py

    tasks/
        __init__.py
        worker.py
        queues.py
        scheduler.py

    recovery/
        __init__.py
        models.py
        service.py

    lifecycle/
        __init__.py
        startup.py
        shutdown.py
```

允许放：

- 启动流程。
- warmup 调度。
- market event 分发。
- strategy dispatch。
- async worker。
- recovery orchestration。
- graceful shutdown。

禁止放：

- 具体策略规则。
- 交易所 endpoint。
- range bar 算法细节。
- order payload mapping。
- V8 专属恢复细节。

依赖方向：

```text
runtime 可以依赖 platform / market_data / order_management / strategy / planner
runtime 不能直接 import OKX/Binance adapter
runtime 不能写具体策略逻辑
```

---

## 15. 具体策略插件目录

具体策略不放 `src/`，放：

```text
strategies/<strategy_name>/
```

例如 V8：

```text
strategies/eth_lf_portfolio_v8/
    __init__.py
    strategy.py
    config.json
    README.md

    domain/
        models.py
        position_state.py
        decision.py

    features/
        indicators.py
        htf_features.py
        micro_context.py
        feature_frame.py

    engines/
        momentum_v3.py
        bear_v3.py
        bull_reclaim_v2.py
        router.py

    execution/
        sizing.py
        stops.py
        signal_mapper.py

    persistence/
        state_store.py
        audit_store.py
```

### V8 插件边界

允许放：

- Momentum V3 规则。
- Bear V3 规则。
- Bull Reclaim V2 规则。
- Portfolio Router。
- V8 micro confirmation。
- V8 sizing。
- V8 stop model。
- V8 position state。
- V8 signal audit。

禁止放：

- OKX raw API。
- Binance raw API。
- 通用 range bar builder。
- 通用 warmup service。
- 通用 order journal。
- 通用 multi-exchange coordinator。

策略插件只输出标准 `TradeSignal`，不能直接调用交易所 adapter。

---

## 16. 当前依赖方向总结

当前项目大体依赖方向是健康的：

```text
platform  -> 无业务依赖
signals   -> 无业务依赖
strategy  -> platform, signals
planner   -> platform, signals
app       -> planner, platform, signals, strategy, utils
reconcile -> platform
utils     -> 无业务依赖
```

未来新增后，目标依赖方向应该是：

```text
platform
    不依赖任何业务模块

market_data
    依赖 platform.data

order_management
    依赖 platform.execution / platform.account / planner / signals

runtime
    依赖 platform / market_data / order_management / strategy / planner

strategies
    依赖 strategy interface / signals / market_data
    不依赖 platform.exchanges.okx/binance client
```

---

## 17. 禁止事项

以后开发 AetherEdge 时，以下行为禁止：

```text
1. 把 warmup 放进 src/platform/data
2. 把 range bar builder 放进 src/platform/data
3. 把 order journal 放进 src/platform/execution
4. 把 multi-exchange coordinator 放进 src/platform/execution
5. 把 V8 规则放进 src/platform 或 src/app
6. 把策略插件直接 import OKX/Binance client
7. 把业务逻辑放进 src/utils
8. 把 runtime 编排继续塞进 src/platform/runtime
9. 写一个几千行 strategy.py 什么都做
10. 把 CoinBacktest 回测框架和 AetherEdge 实盘框架混用
```

---

## 18. 开发前检查清单

每新增一个文件前，必须先问：

```text
1. 这个文件属于哪个大功能域？
2. 这个目录是否拥有这个职责？
3. 这个文件是否只做一件事？
4. 有没有 import 不该依赖的模块？
5. 是否把策略逻辑放进了平台层？
6. 是否把平台 adapter 直接暴露给策略？
7. 是否可以未来复用？如果可复用，是否放在 src 的正确功能域？
8. 如果不可复用，是否应该放在 strategies/<strategy_name>/？
```

如果无法回答清楚，就不能写。

---

## 19. 建议新增架构边界测试

后续应添加测试，防止架构被破坏：

```text
1. platform 不能 import market_data / order_management / runtime / strategies
2. market_data 不能 import strategies / order_management
3. order_management 不能 import strategies / market_data
4. runtime 不能 import platform.exchanges.okx/client.py 或 binance/client.py
5. strategies 不能 import platform.exchanges.okx/client.py 或 binance/client.py
6. utils 不能出现 strategy/order/range/warmup 等业务关键词
7. platform/data 不能出现 range_bar / warmup / strategy 等关键词
8. platform/execution 不能出现 OrderIntent / strategy 等关键词
```

---

## 20. V8 实盘开发的正确落点

V8 实盘开发应拆成：

### AetherEdge 通用能力

```text
src/market_data       # warmup、local DB、range bar
src/order_management  # order intent、多交易所执行、journal
src/runtime           # 启动、异步队列、恢复编排
```

### V8 策略插件

```text
strategies/eth_lf_portfolio_v8
```

### 继续保留不动的边界

```text
src/platform = 平台接口层
CoinBacktest = 回测研究
AetherEdge = 实盘执行
```

---

## 21. 最终一句话原则

```text
platform 不是垃圾桶。
app 不是业务大杂烩。
utils 不是临时代码收容所。
strategies 只能放具体策略。
src 下每个目录都必须是一个清晰的大功能域。
```

AetherEdge 的目标是可复用实盘框架，V8 只是其中一个策略插件。
