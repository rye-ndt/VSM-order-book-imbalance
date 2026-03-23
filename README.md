# Order Book Imbalance — Vietnam Stock Market

A Go service that detects ATO (At-The-Open) trading signals on the Vietnam stock market (HOSE/HNX) by combining pre-open order book imbalance analysis with nightly stock screening and market regime filtering. Every signal fire is persisted as a full event study record — a self-contained snapshot of all observables at signal time — designed for systematic T+2 outcome analysis.

Data source: [SSI FastConnectData](https://fc-data.ssi.com.vn) API v2.

---

## How it works

### Nightly pipeline (03:30 ICT)

Fetches the previous day's market data in parallel and stores it to PostgreSQL:

- **`stock_ohlcv`** — daily OHLCV for all listed equities (`DailyOhlc`, paginated at 1 000 records/page)
- **`stock_foreign_flow`** — net foreign buy/sell volume and value per symbol (`DailyStockPrice`)
- **`index_ohlcv`** — VN-Index daily OHLCV for regime calculation

Raw data is cleaned before storage: rows with zero prices, zero volume, unparseable dates, or fewer than 5 trading days in the fetch window are dropped.

After storing raw data, the pipeline computes per-symbol metrics:

| Metric | Description |
|---|---|
| CPR | Close position ratio — where the stock closed within its daily range (0 = low, 1 = high) |
| Upper Wick Ratio | Upper wick as a fraction of full range — seller pressure indicator |
| MA20 Volume | 20-day average daily volume |
| Volume Ratio (1D) | Today's volume ÷ MA20 volume |
| Volume Trend (3D) | Rising / Flat / Falling over the last 3 sessions |
| VPR | Volume-Price Rating: Institutional / Neutral / Weak / Distribution / Capitulation |
| Momentum Score | Composite integer score from last 5 candles: CPR signals (+1/−1 each), higher-lows streak (+2), gap-down penalty (−2 per occurrence) |
| Candle Pattern | Three Soldiers, Dip Recover, Compression Breakout, Three Crows, Shooting Star, Hammer, Doji, Neutral |
| Resistance Distance | Distance to nearest swing-high resistance within the last 20 days, as a fraction of close |
| Above 20MA | Whether today's close is above the 20-day moving average |

Each symbol also gets a **FinalScore** and **PositionSizeFlag** (Full / Half / Skip) using regime-adjusted thresholds:

| Regime | Full | Half |
|---|---|---|
| Bull | ≥ 10 | ≥ 7 |
| Choppy | ≥ 11 | ≥ 8 |
| Bear | ≥ 12 | ≥ 9 |

The **market regime** (Bull / Bear / Choppy) is derived from VN-Index weekly closes using a Weinstein-style 20-week MA + 8-week trend direction. Regime is recomputed each night and stored in `market_regime`.

Symbols with `position_size_flag != 'Skip'` form the morning watchlist. The nightly `ShouldMonitorToday` screen applies as a pre-filter: CPR ≥ 0.7 or hammer/doji pattern, no Three Crows or Shooting Star, volume ratio ≥ 1.2×, VPR not Distribution, momentum score ≥ 1.

---

### Morning monitor (09:00–09:15 ICT)

Loads the watchlist from the previous night's `stock_metrics`, subscribes to the SSI IDS WebSocket stream (`X` channel), and polls every 2 seconds.

**Signal fires when all 8 conditions hold across 3 consecutive snapshots:**

1. Bid/ask volume ratio ≥ 3.0×
2. No single bid order dominates — largest bid < 40% of total bid volume (anti-spoof filter)
3. Bid depth spans ≥ 5 price levels
4. No ask wall within 3% above indicated price (no single ask level ≥ 30% of total ask volume)
5. Indicated price stable within ±1% across the 3-snapshot window
6. Indicated price < ceiling (tran) price
7. Gap from prior-day close < 5%
8. Pre-computed FinalScore ≥ regime threshold

On fire: Telegram alert sent, one row written to `signal_log` (see below), and an AI interpretation follow-up is queued (see AI Interpretation below).

**Session rules:** stocks with no indicated price by 09:07 ICT are dropped from the session. The signal window closes at 09:14 ICT — stability windows are reset so no new signals can fire in the final minute of the ATO period.

---

### Event study dataset (`signal_log`)

Each signal row is a complete Layer 1 snapshot — everything observable at the moment the signal fired — structured for later T+1/T+2 outcome back-fill:

| Column | Description |
|---|---|
| `indicated_price` | ATO estimated clearing price at fire time; immutable |
| `entry_price` | Initially = `indicated_price`; update with actual ATO fill to measure auction slippage |
| `tp_price` | `indicated_price × 1.03` |
| `open_gap` | `(indicated_price − ref_price) / ref_price` |
| `imbalance_ratio` | Bid/ask ratio from the triggering snapshot |
| `snapshot_count` | Consecutive stable snapshots at fire (should always be 3; sanity field) |
| `snapshot_fired_at` | `CapturedAt` of the triggering snapshot — more precise than wall-clock `fired_at` |
| `regime` | Bull / Bear / Choppy at session start |
| `final_score` | Composite score from nightly screening |
| `position_size_flag` | Full / Half — sizing decision from the night before |
| `candle_pattern` | Pattern that contributed to the score |
| `vpr` | Volume-price relationship label |
| `volume_trend` | Rising / Flat / Falling |
| `volume_ratio` | Today's volume ÷ MA20 at screening time |
| `momentum_score` | Composite momentum integer |
| `resistance_distance` | Headroom to nearest overhead resistance |
| `above_20ma` | Price position relative to 20-day MA |

Outcome columns (`close_d0`, `close_d1`, `close_d2`, VN-Index returns, etc.) are not yet in the schema — they will be back-filled from `stock_ohlcv` and `index_ohlcv` once T+2 prices land.

---

### AI interpretation

After each signal fires, the `AI` port produces structured interpretations of the event study record. Three methods are available:

| Method | Purpose |
|---|---|
| `Interpret` | Structured JSON analysis: recommendation (Buy/Skip), confidence (1–5), signal strength, entry/TP/SL prices, four-field reasoning, T+2 note, action clarity, avoid-if condition |
| `XInterpret` | Vietnamese plain-text post for X/Twitter (≤280 characters) |
| `TelegramInterpret` | Vietnamese conversational paragraph for the Telegram channel |

`Interpret` uses OpenAI structured output (`strict: true` JSON schema) to prevent field drift. A `data_used` array is pre-built by Go code and echoed verbatim by the model — the model cannot invent values. The system prompt forbids buy/sell/hold advice beyond what the schema supports and prohibits macro commentary not present in the input.

All three methods receive only `SignalRecord` fields — no external queries, no market context, no hallucination surface beyond the event study record itself.

---

## Architecture

```
cmd/app/
  main.go           — DI wiring, cron scheduler, HTTP server startup

internal/
  calculator/
    ato.go          — snapshot analysis: imbalance ratio, bid quality, ask wall detection, stability window
    metrics.go      — per-symbol metrics, VPR, momentum score, candle patterns, regime detection, final score

  job/
    market_data.go  — nightly fetch + clean + store + metrics pipeline
    ato_monitor.go  — morning polling loop, signal gate, event study record assembly
    cleaner.go      — OHLCV and foreign flow validation (row-level + symbol-level minimum history)

  interface/
    input/          — ports: StockDataClient, OrderBookClient, RelationalDB; shared types (OHLCV, ForeignFlow, OrderBookSnapshot)
    output/         — ports: MarketStore, Notifier, SocialPoster; shared types (StockMetrics, MarketRegime, SignalRecord, WatchlistEntry)

  modules/          — adapters:
                      SSIStockClient       — REST: DailyOhlc + DailyStockPrice, paginated, token-cached
                      SSIOrderBookClient   — WebSocket IDS: Quote + Trade message handling, per-symbol snapshot cache
                      PostgresMarketStore  — all DB reads/writes, idempotent migration
                      TelegramBot          — multi-tenant bot: subscriber management, /start /hello /signal /subscribe /unsubscribe commands, broadcasts to all subscribers
                      XPoster              — Twitter API v2 (gotwi, OAuth 1.0a), implements SocialPoster port
                      OpenAIClient         — implements AI port: Interpret (structured JSON), XInterpret (X post), TelegramInterpret (Telegram paragraph)

  server/
    server.go       — GET /healthz, GET /imbalance (placeholder)

  config/
    config.go       — YAML config loader (DB, SSI credentials, Telegram, Twitter, OpenAI, HTTP listen addr, signal thresholds)
```

---

## Database tables

| Table | Contents |
|---|---|
| `stock_ohlcv` | Daily OHLCV for all listed equities |
| `index_ohlcv` | Daily OHLCV for VN-Index |
| `stock_foreign_flow` | Net foreign buy/sell volume and value per symbol per day |
| `stock_metrics` | Nightly per-symbol metrics + final score + position size flag |
| `market_regime` | Daily Bull / Bear / Choppy classification |
| `signal_log` | Full event study snapshot per ATO signal fire |
| `bot_subscribers` | Telegram chat IDs subscribed to signal broadcasts |
| `daily_crawl_status` | Per-day flags: `market_data_crawled` (set after nightly pipeline) and `ato_monitored` (set at ATO session start) — prevents duplicate runs on cold-start |
| `ato_session_log` | Append-only per-session event log: regime loaded, watchlist size, first quote per symbol, stability achieved, gate blocks, drop reasons, session summary. Indexed by `session_date`. Query after any session to trace exactly why each symbol did or did not produce a signal. |

Schema migrations run automatically at startup via `Migrate()` using `CREATE TABLE IF NOT EXISTS` and `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.

---

## Current status

### Nightly pipeline — complete and verified
The full nightly pipeline runs at 03:30 ICT and has been live-verified end-to-end against the SSI FastConnectData API. Last manually verified snapshot (2026-03-16):

| Table | Rows stored | Latest date |
|---|---|---|
| `stock_ohlcv` | 18,673+ | 2026-03-16 (pipeline continues nightly) |
| `stock_foreign_flow` | 63,602+ | 2026-03-16 (pipeline continues nightly) |
| `index_ohlcv` | 15+ | 2026-03-16 (pipeline continues nightly) |
| `stock_metrics` | 1,437+ | 2026-03-16 (pipeline continues nightly) |
| `market_regime` | 1 row per day | Bear as of last check |

The cleaner correctly filters zero-volume rows (today's market was still open at fetch time) and drops symbols with fewer than 5 trading days in the window (newly listed / suspended).

Regime lag bug fixed: market regime is now computed from fresh VNIndex data and written to `market_regime` **before** stock metrics are scored — previously the regime computation happened after the metrics loop, meaning each session used the prior day's regime.

### Morning ATO monitor — instrumented, EstMatchedPrice investigation ongoing
`ATOMonitorJob` is fully implemented: watchlist loading, SSI IDS WebSocket subscription (SignalR negotiate → connect → `/start` handshake), 2-second poll loop, 8-condition signal gate, Telegram notification, and `signal_log` write. Cold-start protection is in place via `daily_crawl_status.ato_monitored`.

**Observability (accumulated through 2026-03-23):**
- Per-symbol `sessionState` tracks quote count, first price received, first stability reached, and gate block reason across the full session
- `dropNoPrice()` logs the specific reason each symbol was dropped at 09:07: 0 quotes (WebSocket issue), N quotes with EstMatchedPrice always 0 (ATO not formed or Trade messages absent), or had price then reverted
- `logSessionSummary()` prints a per-symbol no-signal reason at session end
- `readLoop` tracks total and broadcast frame counts; logs the first raw frame received; logs any non-broadcast hub frames by H/M fields — makes it possible to confirm whether data is flowing at all before the quote-parsing layer
- `/start` HTTP response body is logged to confirm the SignalR handshake succeeded
- Terminal order book visualization renders in ANSI (Binance-style) to stderr every 2 seconds: top 8 symbols by imbalance ratio, 5 ask/bid levels with volume bars
- All key session events are persisted to `ato_session_log` (append-only DB table) so post-session analysis survives process crashes

`signal_log` is empty — no confirmed end-to-end signal fires yet. On the first live session all 45 watchlist symbols were dropped at 09:07 because `EstMatchedPrice` was 0 in all messages. The frame-level WebSocket diagnostics now in place are intended to isolate whether the issue is at the connection layer (no frames arriving), the SignalR framing layer (frames arrive but no broadcast messages), or the data layer (broadcast messages arrive but EstMatchedPrice field is zero).

### Social posting (X / Twitter) — implemented, not yet configured
`SocialPoster` output port added. `XPoster` adapter posts via Twitter API v2 using `github.com/michimani/gotwi` (OAuth 1.0a). Gracefully disabled at startup when credentials are absent.

### AI signal interpretation — implemented and wired
`OpenAIClient` implements `Interpret`, `XInterpret`, and `TelegramInterpret`. Wired into `ATOMonitorJob`: after each signal fires, a goroutine calls `Interpret` within the configured `ai_timeout`, then posts to X via `SocialPoster` and to Telegram via `Notifier`.

### Multi-tenant Telegram bot — implemented
`TelegramBot` handles commands (`/start`, `/hello`, `/signal`, `/subscribe`, `/unsubscribe`) and broadcasts signal alerts to all subscribers in `bot_subscribers`. The `/signal` command returns today's fired signals with entry price, TP, and position size flag.

### Pending
- Confirm `EstMatchedPrice` is non-zero in live Trade messages (root cause of zero signals)
- Observe a confirmed signal fire and `signal_log` write during a live ATO session
- Back-fill outcome columns (`close_d0`, `close_d1`, `close_d2`, VN-Index returns) once T+2 prices are available
- Add corporate events filter (ex-dividend / rights issue days)
- Add foreign ownership room data
- Add subscription/payment gating (Stripe or VNPay)
- Wire `SocialPoster` into a scheduled daily post for regime + watchlist count

---

## Requirements

- Go 1.24+
- PostgreSQL
- SSI FastConnectData credentials (`consumer_id` + `consumer_secret`)
- Telegram bot token + chat ID (optional)
- X (Twitter) OAuth 1.0a credentials — API key/secret + access token/secret (optional)
- OpenAI API key (optional — enables AI signal interpretation)

## Getting started

```bash
go mod tidy
make run
```
