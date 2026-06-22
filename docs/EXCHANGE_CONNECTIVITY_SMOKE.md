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

- uses `2 USDT` margin budget and `10x` leverage, so roughly `20 USDT` notional;
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
--skip-order-test      # read/config APIs only
--skip-stop-test       # skip temporary stop-order placement/cancel
--no-cleanup           # do not attempt emergency cleanup close; not recommended
```
