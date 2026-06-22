# ETH LF Portfolio V8 AetherEdge Plugin

This is the live strategy plugin location for the CoinBacktest champion:
`ETH_LF_Portfolio_V8_MicroConfirmScaled`.

Current package scope:

- plugin skeleton and `Strategy` class;
- strategy-local `config.json`;
- runtime requirements declaration;
- 4H closed-kline and 4H range aggregate feature buffering;
- V8 micro confirmation / risk-scale logic;
- no LF engine signals yet;
- no live order state machine yet.

Boundary rules:

- no direct OKX/Binance adapter imports;
- no range-bar builder in the strategy;
- no order journal or coordinator in the strategy;
- the strategy consumes runtime `MarketFeatureEvent` and returns `TradeSignal` only.

Default requirements:

```json
{
  "closed_kline": {"enabled": true, "interval": "4h", "warmup_days": 365},
  "trades": {"enabled": true, "stream_enabled": true, "warmup_enabled": false},
  "range_bars": {"enabled": true, "range_pct": "0.002", "aggregate_interval": "4h"},
  "order_book": {"enabled": false},
  "private_account_stream": {"enabled": true}
}
```
