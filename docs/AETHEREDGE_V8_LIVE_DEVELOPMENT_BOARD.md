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

- [ ] AE-0201 Runtime Config / Context
- [ ] AE-0202 Async Task Queues
- [ ] AE-0203 4H Closed-Bar Scheduler
- [ ] AE-0204 Startup Lifecycle
- [ ] AE-0205 `scripts/run_live.py` 接入 `AETHER_RUNTIME_MODE`

## Board 3：Order Management Foundation

- [ ] AE-0301 OrderIntent Model 扩展
- [ ] AE-0302 Order Journal
- [ ] AE-0303 Client Order ID Generator
- [ ] AE-0304 MultiExchangeOrderCoordinator
- [ ] AE-0305 Stop Order Sync

## Board 4：Recovery / Reconcile

- [ ] AE-0401 Runtime Recovery Service
- [ ] AE-0402 Strategy Recover Interface

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
```

本包完成 Market Data Foundation 主线，不改变现有 live 启动链路，不改变策略逻辑。
