# Exchange Connectivity Smoke Test

Purpose: run a minimal end-to-end API test before strategy live trading.

The tool is intentionally placed under `tools/` and is not a strategy plugin.
It checks market data, account, leverage/margin settings, order placement,
order-status sync, temporary stop order placement/fetch/cancel, and cleanup close.

## Read-only preview

```bash
python tools/exchange_connectivity_smoke.py --report data/state/connectivity_smoke_preview.json
```

This fetches public/private read APIs and prints the order size it would use. It does not place orders.

## Live/sandbox order test

Make sure `.env` is configured explicitly:

```text
AETHER_EXCHANGES=okx,binance
AETHER_DATA_EXCHANGE=okx
AETHER_MASTER_EXCHANGE=okx
AETHER_FOLLOWER_EXCHANGES=binance
AETHER_DRY_RUN=false
AETHER_LIVE_TRADING=true
MARGIN_MODE=isolated
```

Then run:

```bash
python tools/exchange_connectivity_smoke.py \
  --live \
  --margin-usdt 2 \
  --leverage 10 \
  --side long \
  --hold-seconds 3 \
  --report data/state/connectivity_smoke_live.json
```

Default behavior:

- uses `2 USDT` margin budget and `10x` leverage, so roughly `20 USDT` requested notional;
- automatically tops up to the configured minimum order notional when the gap is small; default max top-up is `2 USDT`;
- sets one-way position mode where possible;
- sets isolated margin where possible;
- sets leverage;
- opens a tiny market position through `MultiExchangeOrderCoordinator`;
- queries actual order status and records filled quantity / average fill price / fee when the exchange exposes it;
- optionally places a temporary stop order, queries it, then cancels only that stop order;
- closes the tiny test position;
- writes a JSON report if `--report` is provided.

Safety gates:

- without `--live`, no orders are placed;
- with `--live`, the tool still refuses to place orders when `AETHER_DRY_RUN=true`;
- platform `ExecutionService` still blocks live writes unless the exchange config is sandbox or `AETHER_LIVE_TRADING=true`;
- cleanup close is attempted by default if an error happens after entry.

Useful options:

```bash
--skip-order-test                  # read/config APIs only
--skip-stop-test                   # skip temporary stop-order placement/cancel
--no-min-notional-round-up         # disable automatic small top-up
--max-min-notional-topup-usdt 2    # cap absolute top-up; default 2 USDT
--max-notional-overrun-pct 0.10    # cap percentage overrun as a second guard
--no-cleanup                       # do not attempt emergency cleanup close; not recommended
```

If a strict `2 USDT * 10x` order would round slightly below Binance's minimum notional, the tool now tops it up to the minimum by default as long as the top-up is within `--max-min-notional-topup-usdt` and `--max-notional-overrun-pct`.

The tool uses smoke-test retry defaults, not strategy runtime retry defaults:

```bash
--order-retry-attempts 1
--order-retry-delay-seconds 0
```

This avoids a misleading 30-second silent wait when a follower order is expected to fail.

For safety, when multiple exchanges are configured and strict sizing is expected to fail the follower min-notional check, the tool aborts before opening the master unless `--allow-partial-entry` is explicitly passed.
