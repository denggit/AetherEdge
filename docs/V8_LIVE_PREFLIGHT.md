# V8 Live Preflight Check

`tools/v8_live_preflight_check.py` 是实盘启动前只读检查工具。它不会下单、不会撤单、不会平仓。

## 推荐命令

```bash
python tools/v8_live_preflight_check.py \
  --expect-real-live \
  --report data/state/v8_live_preflight_report.json
```

如果你还在 sandbox / dry-run 配置阶段，可以先不加 `--expect-real-live`：

```bash
python tools/v8_live_preflight_check.py \
  --report data/state/v8_live_preflight_report.json
```

## 检查内容

- `AETHER_RUNTIME_MODE=live_runtime`
- `AETHER_STRATEGY=strategies.eth_lf_portfolio_v8:Strategy`
- `AETHER_DATA_STREAMS=trades`
- V8 runtime requirements：closed 4H、trades、range bars、private account stream，不订阅 order_book
- master/follower 配置是否能解析
- dry-run / live-trading / sandbox 开关
- 本地 state DB / order journal DB 是否可写
- data exchange ticker 是否可读
- 每个交易所：balance、positions、leverage、position mode、open orders、open stop orders 是否可读
- 默认不允许存在残留仓位或残留订单
- 最近 closed 4H K线是否可拉取
- 本地 range bar builder 是否可用

## 残留仓位 / 订单

默认情况下，发现已有仓位、普通挂单或止损挂单会失败，避免带着旧状态启动策略。

临时允许已有仓位：

```bash
python tools/v8_live_preflight_check.py --allow-existing-position
```

临时允许已有订单：

```bash
python tools/v8_live_preflight_check.py --allow-open-orders
```

这两个参数只建议用于人工排查，不建议实盘启动前长期使用。

## 返回码

- `0`：没有 fail 项
- `1`：至少一个 fail 项

报告 JSON 会包含 `ok` 和每一步 check 的 `ok/warn/fail` 状态。
