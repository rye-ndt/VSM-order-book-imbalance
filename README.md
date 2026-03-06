# Order Book Imbalance — Vietnam Stock Market

A Go service that detects high-probability ATO (At-The-Open) trading signals on the Vietnam stock market (HOSE/HNX) using order book imbalance analysis layered with candlestick context, volume confirmation, and market regime filtering.

Data source: [SSI FastConnectData](https://fc-data.ssi.com.vn) API v2.

---

## How it works

### Nightly pipeline (03:30 ICT)

A cron job fetches and stores the previous day's market data into PostgreSQL:

- **Stock OHLCV** — all listed equities via `DailyOhlc`
- **Foreign flow** — net foreign buy/sell volume and value via `DailyStockPrice`
- **VN-Index OHLCV** — for market regime calculation

After storing raw data, it runs a metrics pipeline per symbol:

| Metric | Description |
|---|---|
| CPR | Close position ratio — where did the stock close in its day range? |
| Upper Wick Ratio | Proportion of range that is upper wick (seller pressure indicator) |
| Volume Ratio (1D) | Today's volume vs 20-day MA |
| Volume Trend (3D) | Rising / Flat / Falling over last 3 sessions |
| VPR | Volume-Price Rating: Institutional / Neutral / Weak / Distribution / Capitulation |
| Momentum Score | Composite score from last 5 candles (CPR + higher-lows + gap-downs) |
| Candle Pattern | Three Soldiers, Dip Recover, Compression Breakout, Hammer, Doji, Shooting Star, Three Crows |
| Resistance Distance | Gap to nearest overhead resistance within last 20 days |
| Above 20MA | Whether close is above 20-day moving average |

Each symbol also gets a **FinalScore** and **PositionSizeFlag** (Full / Half / Skip) based on regime-adjusted thresholds:

| Regime | Full | Half |
|---|---|---|
| Bull | >= 10 | >= 7 |
| Choppy | >= 11 | >= 8 |
| Bear | >= 12 | >= 9 |

The **market regime** (Bull / Bear / Choppy) is derived from VN-Index weekly closes using a Weinstein-style 20-week MA + 8-week trend direction.

Stocks passing the daily screen (`ShouldMonitorToday`) are added to the morning watchlist.

---

### Morning monitor (pre-open, until 09:15 ICT)

Polls order books every 2 seconds for watchlisted stocks via the SSI IDS WebSocket stream.

**Signal fires when all conditions hold across 3 consecutive snapshots:**

1. Imbalance ratio >= 3.0x (total bid volume / total ask volume)
2. Imbalance is organic — not concentrated in 1-2 large orders (largest single bid < 40% of total)
3. Bid depth spans >= 5 price levels
4. No ask wall within 3% above indicated price (>= 30% of ask volume at a single level)
5. Indicated price stable within 1% across the 3-snapshot window
6. Indicated price < ceiling price (not already at tran)
7. Gap from prior-day close < 5%
8. Pre-computed FinalScore >= regime threshold

When a signal fires, a Telegram notification is sent with the symbol, ratio, score, indicated price, and a 3% take-profit target. The signal is also persisted to `signal_log`.

Signal window closes at 09:14 ICT (stability windows reset). Stocks with no indicated price by 09:07 ICT are dropped.

---

## Architecture

```
cmd/app
  └── wires everything via DI

internal/
  calculator/
    ato.go        — snapshot analysis, imbalance ratio, bid quality, ask wall detection, stability window
    metrics.go    — per-symbol stock metrics, final score, candle patterns, regime detection

  job/
    market_data.go  — nightly OHLCV + foreign flow fetch + metrics pipeline
    ato_monitor.go  — morning pre-open polling loop
    cleaner.go      — data validation (strips bad rows, filters thinly-traded symbols)

  interface/
    input/          — port interfaces: SSIFastConnect, StockDataClient, OrderBookClient, RelationalDB
    output/         — port interfaces: MarketStore, Notifier; shared types (StockMetrics, MarketRegime, etc.)

  modules/          — adapters: SSIStockClient (REST), SSIOrderBookClient (WebSocket), PostgresMarketStore, TelegramNotifier

  server/           — HTTP server: GET /healthz, GET /imbalance
```

---

## Requirements

- Go 1.22+
- PostgreSQL
- SSI FastConnectData credentials (consumerID + consumerSecret)
- Telegram bot token + chat ID (for notifications)

## Getting started

```bash
go mod tidy
make run
```
