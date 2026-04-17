# Swing Signal Pivot — Implementation Plan

## Goal

Pivot the system from an ATO intraday monitor to an EOD swing signal system that generates a nightly watchlist for T+2 holds. All existing ATO code stays intact and dormant behind a config flag. New code runs alongside as an independent signal mode.

## Hard Constraints (Guardrails)

**Never modify or delete these files:**
- `internal/calculator/ato.go`
- `internal/job/ato_monitor.go`
- `internal/modules/ssi_order_book_client.go`
- `internal/interface/input/order_book_client.go`
- `internal/interface/input/order_book_snapshot.go`
- `internal/interface/input/ssi_fast_connect.go`

**Schema rules:**
- Only `CREATE TABLE IF NOT EXISTS` for new tables
- Only `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for new columns
- Never DROP, TRUNCATE, or ALTER existing column types or constraints

**Config rules:**
- All new fields must have defaults that preserve current behavior when absent
- If `signal_mode` is absent or empty, the system must behave identically to today
- Never remove existing config fields or defaults

**Code rules:**
- All changes to existing files are additive (new methods, new fields, new branches)
- The `"ato"` code path in `main.go` must remain byte-for-byte equivalent to today's behavior
- `go build ./...` and `go vet ./...` must pass after every step

---

## System Architecture After Pivot

```
Nightly (03:30 ICT):  MarketDataJob.Run()
                          → syncStockOHLCV
                          → syncForeignFlow
                          → syncIndexOHLCV
                          → runMetricsPipeline       (stock_metrics, watchlist)
                          → BackfillSignalOutcomes   (ATO outcomes — unchanged)
                          → BackfillSwingOutcomes    (NEW — swing outcomes)

04:00 ICT:            SwingSignalJob.Run()
                          → LoadWatchlist
                          → compute SL/TP per stock
                          → LogSwingSignals
                          → AI.SwingBroadcast
                          → Notifier.Notify
                          → MarkSwingSignalSent
```

The ATO monitor job is gated by `signal_mode` and does not run in `"swing"` mode. The WebSocket client is not constructed or pinged in `"swing"` mode.

---

## Step 1: Config Layer

**File:** `internal/config/config.go`

### 1a. Add `SignalMode` to `Config` struct

Add after `ATO ATOConfig`:

```go
// SignalMode controls which signal jobs are active.
// "ato"   — only ATOMonitorJob runs (existing behavior, default)
// "swing" — only SwingSignalJob runs (EOD watchlist, no WebSocket)
// "both"  — both jobs run simultaneously
SignalMode string `mapstructure:"signal_mode"`

Swing SwingConfig `mapstructure:"swing"`
```

### 1b. Add `SwingConfig` struct

Add alongside existing config structs:

```go
type SwingConfig struct {
	// Cron schedule for the swing signal broadcast (ICT).
	// Default: "0 4 * * *" (04:00 ICT, after nightly pipeline completes)
	Cron string `mapstructure:"cron"`

	// MaxWatchlistSize caps the number of stocks included in the broadcast.
	// Default: 10
	MaxWatchlistSize int `mapstructure:"max_watchlist_size"`

	// AITimeout for generating the broadcast message.
	// Default: 45s
	AITimeout time.Duration `mapstructure:"ai_timeout"`
}
```

### 1c. Add defaults in `Load()`

Add after the existing ATO defaults block:

```go
if cfg.SignalMode == "" {
    cfg.SignalMode = "ato"
}
if cfg.Swing.Cron == "" {
    cfg.Swing.Cron = "0 4 * * *"
}
if cfg.Swing.MaxWatchlistSize == 0 {
    cfg.Swing.MaxWatchlistSize = 10
}
if cfg.Swing.AITimeout == 0 {
    cfg.Swing.AITimeout = 45 * time.Second
}
```

### Validation
- `signal_mode` absent → `Load()` returns `SignalMode = "ato"`, all behavior identical to today
- No existing fields removed or renamed

---

## Step 2: Database Schema

**File:** wherever `Migrate()` is implemented (currently `internal/modules/postgres_market_store.go`)

### 2a. Add `swing_signal_log` table

Add inside `Migrate()` alongside existing `CREATE TABLE IF NOT EXISTS` blocks:

```sql
CREATE TABLE IF NOT EXISTS swing_signal_log (
    id                  SERIAL PRIMARY KEY,
    symbol              TEXT NOT NULL,
    signal_date         DATE NOT NULL,
    regime              TEXT NOT NULL,
    final_score         INT NOT NULL,
    position_size_flag  TEXT NOT NULL,
    candle_pattern      TEXT NOT NULL,
    vpr                 TEXT NOT NULL,
    volume_trend        TEXT NOT NULL,
    volume_ratio        FLOAT NOT NULL,
    momentum_score      INT NOT NULL,
    resistance_distance FLOAT NOT NULL,
    above_20ma          BOOLEAN NOT NULL,
    foreign_net_buy     BOOLEAN NOT NULL,
    entry_price         FLOAT,
    sl_price            FLOAT,
    tp_price            FLOAT,
    close_d1            FLOAT,
    close_d2            FLOAT,
    close_d3            FLOAT,
    close_d5            FLOAT,
    close_d10           FLOAT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, signal_date)
);
```

`entry_price`, `sl_price`, `tp_price` are nullable — they are computed at job time but may be absent if data is unavailable. `close_d1` through `close_d10` are nullable and back-filled nightly.

### 2b. Add `swing_signal_sent` flag to `daily_crawl_status`

```sql
ALTER TABLE daily_crawl_status
    ADD COLUMN IF NOT EXISTS swing_signal_sent BOOLEAN NOT NULL DEFAULT FALSE;
```

### Validation
- Running `Migrate()` twice against a live DB must not error (all statements are idempotent)
- Existing tables and columns are untouched

---

## Step 3: Output Types

**File:** `internal/interface/output/` — add a new file `swing.go`

Define the new types needed by the swing job and AI layer:

```go
package output

import "time"

type SwingSignalRecord struct {
	Symbol             string
	SignalDate         time.Time
	Regime             RegimeLabel
	FinalScore         int
	PositionSizeFlag   string
	CandlePattern      CandlePattern
	VPR                VPRLabel
	VolumeTrend        VolumeTrend
	VolumeRatio        float64
	MomentumScore      int
	ResistanceDistance float64
	Above20MA          bool
	ForeignNetBuy      bool
	EntryPrice         float64
	SLPrice            float64
	TPPrice            float64
}

type SwingBroadcastRecord struct {
	SignalDate     time.Time
	Regime         string
	ScoreThreshold int
	TotalWatchlist int
	Stocks         []SwingSignalRecord
}
```

### Also: add `Close float64` to `WatchlistEntry`

`WatchlistEntry` is defined in `internal/interface/output/`. Add:

```go
Close float64
```

This is needed by `SwingSignalJob` to compute `tp_price = close * (1 + resistance_distance)` and `sl_price` (requires loading prior-day low, but close is needed for TP). This is an additive field — existing code that constructs `WatchlistEntry` without it gets zero value, which is safe since ATO code does not use `Close`.

Populate `Close` in `LoadWatchlist` in the Postgres adapter via a JOIN to `stock_ohlcv` on `(symbol, trading_date)`.

---

## Step 4: MarketStore Port — New Methods

**File:** `internal/interface/output/market_store.go`

Add to the `MarketStore` interface (additive only):

```go
LogSwingSignals(ctx context.Context, records []SwingSignalRecord) error
BackfillSwingOutcomes(ctx context.Context) (int, error)
IsSwingSignalSent(ctx context.Context, date time.Time) (bool, error)
MarkSwingSignalSent(ctx context.Context, date time.Time) error
LoadSwingWatchlist(ctx context.Context, date time.Time) ([]SwingSignalRecord, error)
```

**File:** `internal/modules/postgres_market_store.go`

Implement all five methods:

**`LogSwingSignals`:**
```sql
INSERT INTO swing_signal_log
    (symbol, signal_date, regime, final_score, position_size_flag,
     candle_pattern, vpr, volume_trend, volume_ratio, momentum_score,
     resistance_distance, above_20ma, foreign_net_buy,
     entry_price, sl_price, tp_price)
VALUES (...)
ON CONFLICT (symbol, signal_date) DO NOTHING
```
Batch insert all records in one transaction.

**`BackfillSwingOutcomes`:**
Mirror the existing `BackfillSignalOutcomes` pattern. For each outcome column (`close_d1` through `close_d10`), use a correlated subquery against `stock_ohlcv` that counts trading days after `signal_date`:

```sql
UPDATE swing_signal_log s
SET close_d2 = (
    SELECT o.close
    FROM stock_ohlcv o
    WHERE o.symbol = s.symbol
      AND o.trading_date > s.signal_date
    ORDER BY o.trading_date
    LIMIT 1 OFFSET 1
)
WHERE s.close_d2 IS NULL
  AND (SELECT COUNT(*) FROM stock_ohlcv o
       WHERE o.symbol = s.symbol AND o.trading_date > s.signal_date) >= 2
```

Repeat for d1 (OFFSET 0), d3 (OFFSET 2), d5 (OFFSET 4), d10 (OFFSET 9). Return total rows updated.

**`IsSwingSignalSent` / `MarkSwingSignalSent`:**
Mirror `IsSessionSummarySent` / `MarkSessionSummarySent` but on the `swing_signal_sent` column in `daily_crawl_status`.

**`LoadSwingWatchlist`:**
```sql
SELECT * FROM swing_signal_log
WHERE signal_date = $1
ORDER BY final_score DESC
```

Also update `LoadWatchlist` to populate `WatchlistEntry.Close` via JOIN to `stock_ohlcv`.

### Validation
- `LogSwingSignals` with a duplicate `(symbol, signal_date)` pair does not error
- `BackfillSwingOutcomes` called twice does not double-update (only updates NULL columns)

---

## Step 5: AI Port — New Swing Method

**File:** `internal/interface/output/ai.go`

Add to the `AI` interface:

```go
SwingBroadcast(ctx context.Context, rec SwingBroadcastRecord) (string, error)
```

**File:** `internal/modules/openai_ai.go`

Implement `SwingBroadcast`. Requirements:

- Output language: Vietnamese
- Output format: plain text suitable for Telegram (no markdown headers, use line breaks)
- Message structure:
  1. Date + regime + score threshold context (1–2 lines)
  2. Per-stock block: ticker, FinalScore, PositionSizeFlag, CandlePattern, VPR, MomentumScore, TP level, SL level
  3. Closing line: sizing guidance based on regime
- Build a `data_used` array in Go (mirroring the existing `Interpret` pattern) that lists every field passed to the model — the model must echo it verbatim and cannot invent values not in the input
- System prompt must explicitly prohibit: adding stocks not in the input list, inventing prices, providing investment advice beyond what the schema supports, macro commentary not derived from the input regime field
- Use `strict: true` structured output only if a JSON schema is defined; for plain-text output, use a detailed system prompt with explicit format instructions

The method signature should match the existing pattern in `OpenAIClient`:
```go
func (c *OpenAIClient) SwingBroadcast(ctx context.Context, rec output.SwingBroadcastRecord) (string, error)
```

### Validation
- Calling `SwingBroadcast` with an empty `Stocks` slice returns an empty string without error (guard at top of function)
- Output does not contain stock tickers not present in `rec.Stocks`

---

## Step 6: SwingSignalJob

**New file:** `internal/job/swing_signal.go`

```go
package job

type SwingSignalJob struct {
	store    output.MarketStore
	notifier output.Notifier
	ai       output.AI
	signal   config.SignalConfig
	swing    config.SwingConfig
	location *time.Location
	mu       sync.Mutex
}

func NewSwingSignalJob(
	store output.MarketStore,
	notifier output.Notifier,
	ai output.AI,
	signal config.SignalConfig,
	swing config.SwingConfig,
	location *time.Location,
) *SwingSignalJob
```

**`Run()` logic (in order):**

1. `mu.TryLock()` — if already running, log and return
2. `defer mu.Unlock()`
3. Compute `today` in ICT
4. `store.IsSwingSignalSent(ctx, today)` — if true, log "already sent today, skipping" and return (cold-start idempotency)
5. `store.LoadWatchlist(ctx)` — uses the watchlist computed by last night's `MarketDataJob`
6. Filter: keep only entries where `PositionSizeFlag != "Skip"`
7. Sort by `FinalScore` descending
8. Cap at `swing.MaxWatchlistSize`
9. For each entry, build a `SwingSignalRecord`:
   - `EntryPrice = entry.Close` (closing price of signal_date, used as reference; actual fill is ATC next day)
   - `SLPrice`: query `stock_ohlcv` for the prior trading day's low for this symbol (1 row before today in the DB). If unavailable, set to `entry.Close * 0.97` as a fallback.
   - `TPPrice = entry.Close * (1 + entry.ResistanceDistance)`. If `ResistanceDistance` is 0, fall back to `entry.Close * 1.05`.
   - Populate all other fields from `WatchlistEntry`
10. `store.LogSwingSignals(ctx, records)` — persist to DB
11. Load regime: `store.LoadLatestMarketRegime(ctx)`
12. Load score threshold: `calculator.RegimeScoreThreshold(regime.Regime, signal)`
13. Build `SwingBroadcastRecord` with all records + context
14. If `ai != nil`: call `ai.SwingBroadcast(ctx, broadcastRec)` with timeout `swing.AITimeout`
15. If `notifier != nil` and text is non-empty: `notifier.Notify(ctx, text)`
16. `store.MarkSwingSignalSent(ctx, today)` — mark sent regardless of AI/notifier result (the DB log is the canonical record)
17. Log summary: symbols broadcast, FinalScore range, regime

**Error handling:**
- Step 5 failure (watchlist load) → log error, return (nothing to send)
- Step 10 failure (DB log) → log error, continue (still attempt to send notification)
- Step 14 failure (AI) → log error, send a plain-text fallback message listing tickers and scores without AI interpretation
- Step 15 failure (notifier) → log error, return

**Fallback message format (no AI):**
```
[Swing Watchlist - DATE]
Regime: REGIME | Threshold: N
1. TICKER — Score: N (FULL/HALF) | TP: PRICE | SL: PRICE
2. ...
```

### Validation
- Calling `Run()` twice concurrently: second call exits immediately via `mu.TryLock()`
- Calling `Run()` after `swing_signal_sent = true` for today: returns without sending
- `Run()` with an empty watchlist: logs warning, marks sent, returns (does not send an empty message)
- `Run()` with `ai = nil`: sends fallback plain-text message

---

## Step 7: BackfillSwingOutcomes Hook

**File:** `internal/job/market_data.go`

Add after the existing `BackfillSignalOutcomes` call in `Run()`:

```go
n, err = j.store.BackfillSwingOutcomes(ctx)
if err != nil {
    log.Printf("[job] backfill swing outcomes: %v", err)
} else if n > 0 {
    log.Printf("[job] backfilled %d swing outcome fields", n)
}
```

This is the only change to `market_data.go`. The variable `n` is already declared earlier in the function from the `BackfillSignalOutcomes` call — reuse it.

---

## Step 8: Wire into main.go

**File:** `cmd/app/main.go`

Make three targeted changes:

### 8a. Gate obClient construction and ping

Replace the current unconditional:
```go
obClient := modules.NewSSIOrderBookClient(cfg.SSI)
```

With:
```go
var obClient input.OrderBookClient
if cfg.SignalMode != "swing" {
    obClient = modules.NewSSIOrderBookClient(cfg.SSI)
    if err := obClient.Ping(pingCtx); err != nil {
        log.Printf("[startup] SSI IDS ping failed: %v", err)
    } else {
        log.Printf("[startup] SSI IDS: OK")
    }
}
```

Remove the existing `obClient.Ping` call from the current ping block (it moves inside this gate).

### 8b. Gate ATOMonitorJob construction, cold-start check, and cron registration

Wrap the existing ATOMonitorJob block:
```go
if cfg.SignalMode != "swing" {
    atoMonitorJob := job.NewATOMonitorJob(store, obClient, notifier, socialPoster, aiClient, cfg.Signal, cfg.ATO)

    // existing cold-start checks for ATO and session summary
    // existing cron.AddJob for ATOMonitor
}
```

### 8c. Add SwingSignalJob construction, cold-start check, and cron registration

Add after the ATO block:
```go
if cfg.SignalMode != "ato" {
    swingSignalJob := job.NewSwingSignalJob(store, notifier, aiClient, cfg.Signal, cfg.Swing, ict)

    sent, err := store.IsSwingSignalSent(context.Background(), today)
    if err != nil {
        log.Printf("[startup] check swing signal sent: %v", err)
    } else if !sent {
        log.Printf("[startup] swing signal not yet sent today, running now")
        go swingSignalJob.Run()
    }

    if _, err := c.AddJob(cfg.Swing.Cron, swingSignalJob); err != nil {
        log.Fatalf("register swing signal cron job: %v", err)
    }
}
```

### Validation
- With `signal_mode: "ato"`: `obClient` is constructed, ATOMonitorJob is registered, SwingSignalJob is not constructed
- With `signal_mode: "swing"`: `obClient` is nil, ATOMonitorJob is not constructed, SwingSignalJob is registered
- With `signal_mode: "both"`: both jobs are constructed and registered
- Removing `signal_mode` from config.yaml entirely: defaults to `"ato"`, identical to today

---

## Step 9: Final Validation Checklist

Run these checks in order before marking the implementation complete:

- [ ] `go build ./...` passes with zero errors
- [ ] `go vet ./...` passes with zero warnings
- [ ] `signal_mode: "ato"` — startup logs show `SSI IDS: OK`, ATO cron registered, no swing log lines
- [ ] `signal_mode: "swing"` — startup logs show no IDS ping, swing cron registered, no ATO log lines
- [ ] `signal_mode: "both"` — both jobs registered, IDS pinged
- [ ] `Migrate()` called twice in sequence on a live DB → no errors on second call
- [ ] `LogSwingSignals` with duplicate `(symbol, signal_date)` → no error, row count unchanged
- [ ] `BackfillSwingOutcomes` called twice → second call updates 0 rows (no double-write)
- [ ] `SwingSignalJob.Run()` called with `swing_signal_sent = true` → returns immediately, no Telegram message
- [ ] `SwingSignalJob.Run()` called with empty watchlist → logs warning, marks sent, no message sent
- [ ] `SwingSignalJob.Run()` called with `ai = nil` → fallback plain-text message sent via notifier
- [ ] `WatchlistEntry.Close` populated in `LoadWatchlist` — verify by checking a known symbol's close price against `stock_ohlcv`
