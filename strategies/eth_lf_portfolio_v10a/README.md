# ETH LF Portfolio V10A AetherEdge Plugin

Live strategy plugin for the V9E range-exit overlay portfolio with V10/V10A
Momentum entry gates.

The Python package path is intentionally kept as:

```text
strategies.eth_lf_portfolio_v10a:Strategy
```

The internal `strategy_id` is:

```text
eth_lf_portfolio_v10a_momentum_micro_short_speed_filter
```

## Portfolio routing

V9E keeps the V9C reclaim-first conflict-routing priority:

```text
BULL_RECLAIM_V2 > MOMENTUM_V3 > BEAR_V3_ONLY
```

Current priorities:

```text
BULL_RECLAIM_V2: 150
MOMENTUM_V3: 100
BEAR_V3_ONLY: 50
```

Before applying that unchanged priority order, the router removes:

- `MOMENTUM_V3 LONG` candidates whose candidate-side micro action is
  `NOT_ALIGNED_RISK_REDUCED`;
- `MOMENTUM_V3 SHORT` candidates whose current completed 4H `rf_bar_count` is
  strictly greater than the Q75 threshold from prior completed buckets.

The range-speed defaults are a 1080-bucket rolling window, 100 minimum prior
periods, and quantile 0.75. The tracker compares before observing the current
bucket, so the threshold is past-only. Missing count or insufficient history
sets `v10a_fast_speed_available=false` and does not block.

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


## Range Exit Overlay

V9E adds only the range/footprint protective exit overlay on top of the frozen
V9C baseline. It does not change entries, sizing, stops, priority, or add logic.

The live implementation supports only immediate closed-bar range exit with
`RANGE_EXIT_NEXT_OPEN` semantics. Delayed range exit is not implemented and
non-zero `range_exit.delay_bars` is rejected at config load.

## Live trades warmup policy

V9E does not use REST historical trade warmup in live runtime. Range bars are built only from live websocket trades. If the process starts in the middle of a 4H bucket, that first bucket is treated as micro context unavailable; subsequent fully captured buckets use rangebar/micro risk scaling normally.

The V10A rolling range-speed history is process-local. With the default
`range_speed_min_periods=100`, short-speed blocking remains unavailable until
100 valid completed 4H range aggregates have been observed after startup.
