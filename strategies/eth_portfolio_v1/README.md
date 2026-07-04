# ETH Portfolio V1 AetherEdge Plugin

ETH Portfolio V1 is an independent, LF-only scaffold forked from
`eth_lf_portfolio_v10b`. Its current behavior is intended to remain equivalent
to the V10B LF portfolio: the alpha rules, router, sizing, range exit,
structural stop, and risk scaling are unchanged.

Plugin path and identity:

```text
strategies.eth_portfolio_v1:Strategy
strategy_id: eth_portfolio_v1
strategy_version: V1
```

## Current scope

- V1 currently contains only the V10B-equivalent LF sleeve.
- Low Sweep MF is not connected or implemented in this plugin.
- The plugin owns its copied domain, engine, execution, feature, and
  persistence modules and does not import another concrete strategy plugin.
- V1 regular stop replacement never uses global `cancel_all_stop_orders`.

## Scoped stop replacement

V1 stop replacement is staged, not an atomic batch:

1. Place the new LF-quantity, `reduce_only` stop.
2. Verify that the new stop exists at the target exchange.
3. Dispatch a scoped cancel for the old LF stop using its existing exchange
   order ID and/or client order ID.

The signal list preserves this order for runtimes that execute signals
sequentially. It must not be interpreted as an atomic venue operation. Both
stages retain `target_exchanges`, and the old-stop cancel includes the V1
strategy, LF sleeve, position, side, symbol, and old stop identifiers.

If an old stop identifier is unavailable, V1 places the new protective stop
but does not fall back to a global cancel. The new-stop metadata marks manual
cleanup as required. Before live rollout, order events and recovery data must
reliably populate old stop exchange/client IDs; otherwise obsolete stops cannot
be cleaned up automatically.

## Future direction

V1 will later add independent LF and MF sleeves. That future live strategy must
run in hedge mode. LF and MF stops must use their own sleeve quantities and
`reduce_only`. This scoped boundary is required so one sleeve cannot remove the
other sleeve's protective stop.

Low Sweep, dual-sleeve behavior, and automatic hedge-mode switching are not
part of the current scaffold.

## Runtime boundary

The plugin consumes normalized closed-kline/range feature events and emits
standard `TradeSignal` intents. It does not call exchange adapters. Runtime
requirements and all behavior parameters other than plugin identity remain
aligned with V10B.

## Start

From PowerShell at the repository root:

```powershell
$env:PYTHONPATH="."
$env:AETHER_STRATEGY="strategies.eth_portfolio_v1:Strategy"
python scripts/run_live.py
```
