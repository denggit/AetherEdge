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

## Future direction

V1 will later add independent LF and MF sleeves. That future live strategy must
run in hedge mode. LF and MF stops must use sleeve-scoped quantities and
`reduce_only`; stop handling must never use a global
`cancel_all_stop_orders`.

These future requirements are documented only. This scaffold does not add
Low Sweep, dual-sleeve behavior, scoped-stop changes, or automatic hedge-mode
switching.

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
