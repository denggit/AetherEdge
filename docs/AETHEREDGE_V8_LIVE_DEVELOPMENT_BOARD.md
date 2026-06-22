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

- [ ] AE-0501 V8 Plugin Skeleton
- [ ] AE-0502 V8 Feature Engine
- [ ] AE-0503 V8 Micro Context
- [ ] AE-0504 V8 Position State
- [ ] AE-0505 V8 Signal Mapper
- [ ] AE-0506 Readonly Parity Mode
- [ ] AE-0507 Live Trading Mode

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
