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

V1 stop replacement is confirmation-gated:

1. `_replace_stop_signals()` emits only the new LF-quantity, `reduce_only`
   stop. Old-stop identifiers are carried in that signal's metadata.
2. Order-result feedback verifies that the new stop succeeded on every target
   exchange.
3. Only successful feedback emits scoped cancels for the old LF stops, using
   their existing exchange order IDs and/or client order IDs.

Old-stop cancels are never placed in the initial signal list. If any target
exchange fails to confirm the new stop, feedback emits no cancel and the old
stop remains in place. A successful scoped cancel includes the V1 strategy,
LF sleeve, position, side, symbol, target exchange, and old stop identifiers.

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
