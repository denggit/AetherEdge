# AetherEdge V8 Live 开发看板

> 规则：每次只做一个或一组合并度高的任务；完成后打勾。能复用的功能必须放到 AetherEdge 常驻模块，策略专属逻辑放 `strategies/<strategy_name>/`。

## Board 0：架构防护

- [x] AE-0001 架构边界测试
  - `tests/architecture/test_domain_boundaries.py`
  - `tests/architecture/test_domain_skeletons.py`
  - `tests/architecture/test_platform_not_polluted.py`
- [x] AE-0002 新常驻功能域骨架
  - `src/market_data/`
  - `src/order_management/`
  - `src/runtime/`

## Board 1：Market Data Foundation

- [x] AE-0101 Market Data Models 扩展
- [x] AE-0102 Kline Local Store
- [x] AE-0103 Trade Local Store
- [x] AE-0104 Warmup Gap Detector
- [x] AE-0105 Warmup Service
- [x] AE-0106 Historical Trades Warmup
- [x] AE-0107 RangeBar Builder
- [x] AE-0108 RangeBar Store
- [x] AE-0109 4H Range Aggregate

## Board 2：Runtime Foundation

- [x] AE-0201 Runtime Config / Context
- [x] AE-0202 Async Task Queues
- [x] AE-0203 4H Closed-Bar Scheduler
- [x] AE-0204 Startup Lifecycle
- [x] AE-0205 `scripts/run_live.py` 接入 `AETHER_RUNTIME_MODE`

## Board 3：Order Management Foundation

- [x] AE-0301 OrderIntent Model 扩展
- [x] AE-0302 Order Journal
- [x] AE-0303 Client Order ID Generator
- [x] AE-0304 MultiExchangeOrderCoordinator
- [x] AE-0305 Stop Order Sync

## Board 4：Recovery / Reconcile

- [x] AE-0401 Runtime Recovery Service
- [x] AE-0402 Strategy Recover Interface


## Board 4.6：Direct Live Runtime Requirements Glue

- [x] AE-0461 StrategyRuntimeRequirements 模型
- [x] AE-0462 策略 config / strategy method -> requirements parser
- [x] AE-0463 LiveRuntimeRunner 根据 requirements 自动执行 closed-kline warmup，并选择 closed-kline / rangebar 参数
- [x] AE-0464 LiveRuntimeRunner 根据 requirements 启动 trades / order_book producer
- [x] AE-0465 private account/order stream requirement 接入 runtime
- [x] AE-0466 V8 预期 requirements：trades yes、order_book no、4H yes、rangebar yes、private account yes

设计结论：

```text
warmup / stream / feature pipeline 不由 LiveRuntimeRunner 写死。closed-kline warmup 可根据 requirements 自动创建；历史 trades warmup 必须由对应 adapter/service 显式提供，缺失时 fail fast。
策略插件通过 runtime_requirements 声明自己需要的数据，runtime 按需启动。
V8 不订阅 order_book，只订阅 trades + closed 4H + rangebar aggregate + private account/order events。
```

## Board 5：V8 Strategy Plugin

- [x] AE-0501 V8 Plugin Skeleton
- [x] AE-0502 V8 Feature Engine
- [x] AE-0503 V8 Micro Context
- [x] AE-0504 V8 Position State
- [x] AE-0505 V8 Signal Mapper
- [x] AE-0506 Live Signal Engine
- [x] AE-0510 Live Runtime Integration
- [x] AE-0508 V8 Live Signal Output

## 当前完成范围

本包完成：

```text
AE-0001 架构边界测试
AE-0002 新常驻功能域骨架
AE-0101 Market Data Models 扩展
AE-0102 Kline Local Store
AE-0103 Trade Local Store
AE-0104 Warmup Gap Detector
AE-0105 Warmup Service
AE-0106 Historical Trades Warmup
AE-0107 RangeBar Builder
AE-0108 RangeBar Store
AE-0109 4H Range Aggregate
AE-0201 Runtime Config / Context
AE-0202 Async Task Queues
AE-0203 4H Closed-Bar Scheduler
AE-0204 Startup Lifecycle
AE-0205 scripts/run_live.py 接入 AETHER_RUNTIME_MODE
AE-0301 OrderIntent Model 扩展
AE-0302 Order Journal
AE-0303 Client Order ID Generator
AE-0304 MultiExchangeOrderCoordinator
AE-0305 Stop Order Sync
AE-0401 Runtime Recovery Service
AE-0402 Strategy Recover Interface
AE-0461 StrategyRuntimeRequirements 模型
AE-0462 策略 config / strategy method -> requirements parser
AE-0463 LiveRuntimeRunner 根据 requirements 自动执行 closed-kline warmup，并选择 closed-kline / rangebar 参数
AE-0464 LiveRuntimeRunner 根据 requirements 启动 trades / order_book producer
AE-0465 private account/order stream requirement 接入 runtime
AE-0466 V8 requirements 预案

AE-0471 OKX master / Binance follower execution policy
AE-0472 下单后查询订单真实状态并同步 journal
AE-0473 journal 记录真实成交数量、平均成交价、手续费、手续费币种
AE-0474 follower 开仓失败：重试后跳过本次持仓，不平 OKX master
AE-0475 master 开仓失败但 follower 有孤儿仓：报警 + 人工宽限窗口 + 宽限后平 follower 的动作建议
AE-0476 入场价偏差阈值告警：默认 0.5%，只提醒，不自动修复
```

本包完成 Market Data Foundation、Runtime Foundation、Order Management Foundation、Recovery / Reconcile 主线；`bash scripts/start_live_watchdog.sh` 入口不变，默认仍走 legacy_app，设置 `AETHER_RUNTIME_MODE=live_runtime` 才启用新 runtime。

## Board 4.7：Master / Follower Execution Sync

- [x] AE-0471 OKX master / Binance follower execution policy
- [x] AE-0472 下单后查询订单真实状态并同步 journal
- [x] AE-0473 journal 记录真实成交数量、平均成交价、手续费、手续费币种
- [x] AE-0474 follower 开仓失败：重试后跳过本次持仓，不平 OKX master
- [x] AE-0475 master 开仓失败但 follower 有孤儿仓：报警 + 人工宽限窗口 + 宽限后平 follower 的动作建议
- [x] AE-0476 入场价偏差阈值告警：默认 0.5%，只提醒，不自动修复

设计结论：

```text
OKX 是 master，Binance 是 follower。
V8 canonical state 只由 OKX/master 决定。
Binance 只跟随 OKX，不反向驱动 OKX。
两边挂同一个 master-derived canonical stop price。
OKX 开仓成功但 Binance 失败：OKX 继续，Binance 重试，仍失败则跳过这笔持仓并告警。
Binance 成功但 OKX 失败：这是 orphan follower，告警，等待人工宽限窗口，之后按策略配置平 follower。
价差超过阈值只发 alert，不自动解决。
下单后必须查询订单真实状态，记录真实 avg_fill_price / filled_quantity / fee / fee_asset。
```

## Master / Follower configuration note

The master/follower relationship is runtime configuration, not a hard-coded
architecture rule. The current recommended deployment can be expressed as:

```text
AETHER_DATA_EXCHANGE=okx
AETHER_EXCHANGES=okx,binance
AETHER_MASTER_EXCHANGE=okx
AETHER_FOLLOWER_EXCHANGES=binance
```

If `AETHER_MASTER_EXCHANGE` is omitted, live runtime uses
`AETHER_DATA_EXCHANGE` as master. If `AETHER_FOLLOWER_EXCHANGES` is omitted,
followers default to `AETHER_EXCHANGES` excluding the master. Reversing the
relationship, for example `AETHER_MASTER_EXCHANGE=binance` and
`AETHER_FOLLOWER_EXCHANGES=okx`, should require only env/config changes.


## Board 5 package 1：V8 Plugin Skeleton / Feature / Micro Context

- [x] AE-0501 V8 Plugin Skeleton
- [x] AE-0502 V8 Feature Engine
- [x] AE-0503 V8 Micro Context

设计结论：

```text
V8 插件位于 strategies/eth_lf_portfolio_v8。
策略声明 runtime_requirements，不订阅 order_book。
插件只消费 closed 4H kline 和 4H range aggregate feature。
当前包不产生交易信号；LF engines / position state execution 在后续包实现。
```


## Board 5 package 2：V8 Position State / Signal Mapper / Engine Hooks

- [x] AE-0504 V8 Position State
- [x] AE-0505 V8 Signal Mapper
- [x] AE-0506 Live Signal Engine

设计结论：

```text
V8 position state 分成 master canonical state 和 exchange leg state。
account/order event 只能让 master exchange 驱动 canonical state，follower 只更新自己的 leg。
SignalMapper 将 V8TradeDecision 映射成标准 TradeSignal，quantity 仍然是 base asset。
Momentum / Bear / Bull engine 类和 PortfolioRouter 钩子已就绪；下一包直接迁移实盘信号规则。
```


## Board 5 package 3：Live LF Engine Signal Rules

- [x] AE-0507 Momentum V3 / Bear V3 / Bull Reclaim V2 LF 规则迁移
- [x] AE-0508 V8 Live Signal Output

设计结论：

```text
已把 Momentum V3、Bear V3 Only、Bull Reclaim V2 的 feature/signal 规则迁移进 AetherEdge 插件。
所有 1D / 1W regime 仍然 shift(1)，4H rolling channel 仍然 rolling(...).shift(1)。
live feature builder 每次只用目标 4H bar 及之前的数据，避免 backlog 场景下未来 bar 泄露。
PortfolioRouter 已按 Momentum > Bear > Bull 优先级从 live features 选出最终 routed signal。
当前包仍不下单；下一包把 routed signal + sizing + stop 组合成真正 TradeSignal。
```


## Board 5 package 4：V8 Live Signal Output

- [x] AE-0508 V8 Live Signal Output
- [x] AE-0509 V8 Add / Stop Update / Full Position Lifecycle

设计结论：

```text
V8 插件现在可以把 routed LF signal 转成 OPEN_LONG / OPEN_SHORT TradeSignal。
初始 open 信号只开仓，不立即放 stop；stop 等 master exchange 实际成交回报后，用真实成交价计算。
master 成交后只给 master 挂 stop；follower 成交后使用同一个 master canonical stop price 给 follower 挂 stop。
active position 下支持基于 entry_engine exit channel / opposite routed signal 输出 reduce-only close。
quantity 仍为 base asset，OKX/Binance native quantity 转换继续由 order_management 负责。
下一包补完整加仓、stop update、max hold、cooldown 与持久化恢复。
```


## Board 5 package 5：V8 Full Position Lifecycle

- [x] AE-0509 V8 Add / Stop Update / Full Position Lifecycle

设计结论：

```text
V8 插件现在支持完整持仓周期第一版：
1. 初始入场后等待 master 成交，再用 master 真实成交价计算 canonical stop。
2. follower 成交后使用同一 canonical stop，不参与策略状态计算。
3. 持仓中支持 add_every_r 加仓触发；add sizing 不乘 micro_entry_risk_scale，只乘 global_risk_scale，与 V8 回测逻辑保持一致。
4. 支持 protected trailing stop 更新；更新 stop 时先发 CANCEL_ALL_STOP_ORDERS，再发新的 PLACE_STOP_LOSS。
5. 支持 entry_engine exit channel、opposite routed signal、max_hold_bars 平仓。
6. 支持 master exit 后 cooldown_bars 阻止立即重新入场。
策略插件仍不直接调用 OKX/Binance API，只输出标准 TradeSignal。
```


## Board 5 package 6：V8 Live Runtime Integration

- [x] AE-0510 V8 Live Runtime Integration

设计结论：

```text
已确认 V8 插件可以接入 live_runtime 主链路：
1. closed 4H kline + 4H range aggregate 进入 V8 on_market_feature。
2. V8 输出 OPEN_LONG / OPEN_SHORT 后进入 LiveOrderIntentFactory。
3. signal.metadata.target_exchanges 现在会被 LiveOrderIntentFactory 尊重，可用于 master/follower leg-specific stop replacement。
4. entry intent 进入 MultiExchangeOrderCoordinator，并写入 OrderJournal。
5. master account fill 回流 V8 后，只给 master leg cancel/replace stop。
6. follower account fill 回流 V8 后，只给 follower leg cancel/replace stop，且使用同一个 master canonical stop price。
7. 策略插件仍不直接调用 OKX/Binance API。
```


## Board 5 package 7：V8 Live Startup Config

- [x] AE-0511 V8 Live Startup Config

设计结论：

```text
已把 AetherEdge 默认配置和 .env.example 切到 V8 live_runtime 主线，但仍保持安全开关：
1. config/aether_defaults.json 默认 AETHER_RUNTIME_MODE=live_runtime 等价配置。
2. 默认策略为 strategies.eth_lf_portfolio_v8:Strategy。
3. 默认 data_streams 为 trades，V8 不订阅 order_book。
4. .env.example 明确 OKX master / Binance follower 是配置，不是代码写死。
5. dry_run 默认 true，live_trading 默认 false；真实启动前必须显式改成 AETHER_DRY_RUN=false 且 AETHER_LIVE_TRADING=true。
6. 新增 startup config 测试，确认 live_runtime 能加载 V8 插件、解析 master/follower、读取 V8 runtime_requirements。
```


## Board 5 package 8：V8 Live Preflight Check

- [x] AE-0512 V8 Live Preflight Check

设计结论：

```text
新增 tools/v8_live_preflight_check.py，只读、不下单、不撤单、不平仓。
检查内容包括：
1. live_runtime + V8 strategy 配置。
2. V8 requirements：closed 4H、trades、range bars、private account stream，不订阅 order_book。
3. master/follower 角色解析。
4. dry_run / live_trading / sandbox 安全开关。
5. 本地 state DB 和 order journal DB 可写。
6. OKX/Binance read API：ticker、balance、positions、leverage、position mode、open orders、open stop orders。
7. 默认不允许残留仓位或残留订单，避免带旧状态启动。
8. 最近 closed 4H K线可拉取。
9. 本地 range bar builder 可用。
```


## Board 5 package 9：V8 Preflight Kline Fetch Fix

- [x] AE-0513 V8 Preflight Latest Closed 4H Kline Fix

设计结论：

```text
修复 preflight 和 live runtime 获取 latest closed 4H K线的方式。
之前用 start_time_ms=end_time_ms=open_time_ms 做精确查询，OKX market/candles 可能返回空。
现在改为拉最近多根 4H K线，再按 expected open_time_ms 过滤目标已闭合 K线。
这同时修复 tools/v8_live_preflight_check.py 和 LiveRuntimeRunner.poll_closed_bar_once。
```


## Board 5 package 10：OKX 4H Kline Interval Mapping Fix

- [x] AE-0514 OKX 4H Kline Interval Mapping Fix

设计结论：

```text
修复 OKX public candles 对 normalized interval 的兼容问题。
AetherEdge 内部统一使用 4h，但 OKX REST candles 需要 4H。
OKX adapter 现在会把 1h/2h/4h 等转换为 1H/2H/4H，同时不影响 Binance 的 4h。
这解决 preflight latest_closed_4h_kline 返回 rows=0 的常见原因。
```

## Board 5 package 11：Current 4H RangeBar Warmup

- [x] AE-0515 Current 4H Trade Backfill for RangeBar Warmup

设计结论：

```text
修复中途重启时当前 4H range aggregate 不完整的问题。
1. V8 trades.warmup_enabled=true。
2. live_runtime 启动时计算当前 open 4H bucket：bucket_start -> now。
3. 使用 data feed 的 fetch_trades 补当前 bucket trades，并写入 SqliteTradeStore。
4. TradeStore 使用 trade_coverage 表记录已覆盖区间，重启后跳过已覆盖部分，避免每次重复下载。
5. 使用本地 trades 重建 current bucket range bars，并写入 SqliteRangeBarStore。
6. RangeBarBuilder 启动时会用已持久化 range bars seed bar_id 序号，避免重启后 bar_id 冲突覆盖。
7. 如果没有 historical trade feed 且本地 coverage 不完整，runtime fail fast，不允许带着不完整 micro context 静默启动。
8. 只补当前 4H bucket，不下载多年 trades；多年历史仍只 warmup 4H K线。
9. 为避免 startup warmup 到 websocket producer 启动之间的秒级 race，runtime 在每次发出 closed 4H range aggregate 前，会对该 4H bucket 再做一次 trades coverage catch-up。
10. 因此即使启动/补数据花了几十秒，只要历史 trades API 能覆盖，最终用于信号的 closed 4H range aggregate 仍会先补齐再发给策略。
```

## Board 5 package 12：V9C Reclaim Priority Live Routing

- [x] AE-0516 V9C Reclaim Priority Routing

设计结论：

```text
根据 CoinBacktest 的 eth_lf_portfolio_v9c_reclaim_priority_backtest.py，V9C 相对当前实盘 V8 的核心变化是 portfolio conflict routing。
1. V8 原优先级：Momentum V3 > Bear V3 Only > Bull Reclaim V2。
2. V9C reclaim_first：Bull Reclaim V2 > Momentum V3 > Bear V3 Only。
3. AetherEdge 实盘插件保持同一套 Momentum/Bear/Bull 特征、micro context、sizing、stop/lifecycle，不引入新的未来函数或时序变化。
4. global_risk_scale 继续使用 1.30；这与 V9C 默认 global-risk-scale=1.30 一致。
5. 策略导入路径暂保留 strategies.eth_lf_portfolio_v8:Strategy，避免启动配置和 preflight 连锁改动；内部 strategy_id 已标记为 eth_lf_portfolio_v9c_reclaim_priority。
```

## Board 5 package 13：Runtime Config Hermetic Test Fix

- [x] AE-0517 Runtime Mode Defaults Test Fix

设计结论：

```text
修复 runtime_mode_from_env(defaults_path=missing, environ={}) 被项目 .env / config/aether_defaults.json 污染的问题。
1. 生产调用 environ=None 时，仍读取项目 .env / defaults。
2. 单测或显式注入 environ={} 且未传 env_file 时，只使用注入的 environ，不继承开发机项目 .env。
3. 因此 missing defaults + empty environ 会回到 RuntimeMode.LEGACY_APP，保持旧测试语义。
4. V8/V9C 正式启动不受影响，因为真实启动不会传 synthetic environ={}.
```

## Board 5 package 14：OKX Kline Close Time Fix

- [x] AE-0518 OKX Kline Close Time Mapping Fix

设计结论：

```text
preflight 已全 OK，但 report 暴露出 OKX latest_closed_4h_kline 的 close_time_ms 等于 open_time_ms。
这会影响 runtime closed_kline feature event_time_ms 和 V8 kline/range aggregate 对齐。
修复 OKX kline adapter：close_time_ms = open_time_ms + interval_ms - 1。
覆盖 1m 和 4h 单测，确认 4h close_time_ms 为 open + 14,400,000 - 1。
```

