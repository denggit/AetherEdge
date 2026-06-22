# ETH LF Portfolio V9C AetherEdge Plugin

Live strategy plugin for the CoinBacktest V9C reclaim-priority portfolio.

The Python package path is intentionally kept as:

```text
strategies.eth_lf_portfolio_v8:Strategy
```

This avoids changing the existing live startup and preflight path. The internal
`strategy_id` is now:

```text
eth_lf_portfolio_v9c_reclaim_priority
```

## Portfolio routing

V9C uses the same live LF engines as the previous V8 migration, but changes the
conflict-routing priority:

```text
BULL_RECLAIM_V2 > MOMENTUM_V3 > BEAR_V3_ONLY
```

Current priorities:

```text
BULL_RECLAIM_V2: 150
MOMENTUM_V3: 100
BEAR_V3_ONLY: 50
```

## Runtime boundary

The strategy plugin:

- consumes closed 4H kline and 4H range aggregate `MarketFeatureEvent` objects;
- emits standard `TradeSignal` objects only;
- does not import OKX/Binance raw adapters;
- does not manage generic range-bar storage or order journal internals.

## Default requirements

```json
{
  "closed_kline": {"enabled": true, "interval": "4h", "warmup_days": 365},
  "trades": {"enabled": true, "stream_enabled": true},
  "range_bars": {"enabled": true, "range_pct": "0.002", "aggregate_interval": "4h"},
  "order_book": {"enabled": false},
  "account_state": {"poll_enabled": true, "poll_interval_seconds": 300},
  "order_state": {"poll_when_position_enabled": true, "poll_interval_seconds": 20}
}
```


## Live trades warmup policy

V9C does not use REST historical trade warmup in live runtime. Range bars are built only from live websocket trades. If the process starts in the middle of a 4H bucket, that first bucket is treated as micro context unavailable; subsequent fully captured buckets use rangebar/micro risk scaling normally.
