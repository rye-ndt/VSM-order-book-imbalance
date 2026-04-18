# Swing Signal Pivot — Implementation Plan (Index)

This plan is split into 4 parts for sequential agent implementation. Each part begins with a prerequisites checklist that must pass before any code is written.

| Part | File | Steps | Scope |
|------|------|-------|-------|
| 1 | [SWING_PIVOT_PART1.md](SWING_PIVOT_PART1.md) | 1–3 | Config, schema, output types |
| 2 | [SWING_PIVOT_PART2.md](SWING_PIVOT_PART2.md) | 4–5 | MarketStore methods, AI port |
| 3 | [SWING_PIVOT_PART3.md](SWING_PIVOT_PART3.md) | 6–8 | SwingSignalJob, backfill hook, main.go wiring |
| 4 | [SWING_PIVOT_PART4.md](SWING_PIVOT_PART4.md) | 10–12 | ATO daily result log, warnFired fix, prop flow |

---

# Full Plan (reference)

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

---

## Step 10: Daily ATO Result Log (Per-Symbol Session Snapshot)

### Goal

Every watchlist symbol gets one structured row per ATO session, regardless of whether a signal fired. This makes per-symbol post-session analysis queryable without parsing text from `ato_session_log`. The `signal_log` table continues unchanged — it remains the authoritative event study for fired signals only. This table is the companion: what happened to every symbol that was monitored.

---

### 10a. New table `ato_daily_result`

Add inside `Migrate()`:

```sql
CREATE TABLE IF NOT EXISTS ato_daily_result (
    id                      SERIAL PRIMARY KEY,
    session_date            DATE NOT NULL,
    symbol                  TEXT NOT NULL,
    regime                  TEXT NOT NULL,
    final_score             INT NOT NULL,
    position_size_flag      TEXT NOT NULL,
    had_indicated_price     BOOLEAN NOT NULL DEFAULT FALSE,
    quote_count             INT NOT NULL DEFAULT 0,
    early_ratio             FLOAT,
    peak_imbalance_ratio    FLOAT,
    final_indicated_price   FLOAT,
    ref_price               FLOAT,
    ceil_price              FLOAT,
    sell_warn_fired         BOOLEAN NOT NULL DEFAULT FALSE,
    stable_snapshots_at_fire INT,
    signal_fired            BOOLEAN NOT NULL DEFAULT FALSE,
    gate_block_reason       TEXT,
    drop_reason             TEXT,
    close_d0                FLOAT,
    close_d1                FLOAT,
    close_d2                FLOAT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, session_date)
);
```

Column notes:
- `early_ratio` — first imbalance ratio recorded for this symbol after the ATO indicated price forms (first snapshot where `IndicatedPrice > 0`); the "baseline" read before late-session activity. NULL if ATO never formed.
- `peak_imbalance_ratio` — highest bid/ask ratio seen across all snapshots during the session; NULL if no indicated price
- `final_indicated_price` — last known indicated price at session end or drop time; NULL if ATO never formed
- `ref_price` / `ceil_price` — from the first Quote received; NULL if no quotes arrived
- `sell_warn_fired` — true if a sell-side warning fired for this symbol in this session
- `stable_snapshots_at_fire` — value of `StableCount` at the moment the signal fired; NULL if no signal fired. A value of 1 means only the minimum 3-snapshot window was confirmed. Higher values indicate more sustained stability before fire. Together with `sell_warn_fired`, this is the manipulation-pattern detector: a session where `sell_warn_fired = true` AND `stable_snapshots_at_fire = 1` is the lowest-confidence signal profile.
- `signal_fired` — true if a row exists in `signal_log` for this symbol on this session_date
- `gate_block_reason` — last recorded reason the 8-condition gate was not met; NULL if signal fired or symbol dropped before gate evaluation
- `drop_reason` — reason from `dropNoPrice()` if symbol was dropped at the cutoff; NULL otherwise
- `close_d0/d1/d2` — back-filled nightly from `stock_ohlcv`, same pattern as `signal_log`

**The early_ratio → final_ratio gap is the manipulation detector.** The gap between `early_ratio` and `peak_imbalance_ratio` (or the ratio at signal fire in `signal_log`) measures how much the order book shifted during the session. Apr 17 VIC: `early_ratio ≈ 0.44` (sell-warn), `final ratio at fire = 3.74` — a ~8.5× swing in 43 seconds. Once this is a table, the query is: `WHERE sell_warn_fired = true AND signal_fired = true` — these are the sessions worth auditing for coordinated order book manipulation.

---

### 10b. New output type `ATODailyResult`

**File:** `internal/interface/output/` — add to an appropriate existing file or a new `ato_result.go`

```go
type ATODailyResult struct {
    SessionDate          time.Time
    Symbol               string
    Regime               string
    FinalScore           int
    PositionSizeFlag     string
    HadIndicatedPrice    bool
    QuoteCount           int
    PeakImbalanceRatio   float64
    FinalIndicatedPrice  float64
    RefPrice             float64
    CeilPrice            float64
    SignalFired          bool
    GateBlockReason      string
    DropReason           string
}
```

Zero values are safe defaults: `PeakImbalanceRatio = 0`, `FinalIndicatedPrice = 0`, etc. signal fired = false by default.

---

### 10c. New MarketStore methods

**File:** `internal/interface/output/market_store.go` — add to the interface:

```go
LogATODailyResults(ctx context.Context, records []ATODailyResult) error
BackfillATODailyOutcomes(ctx context.Context) (int, error)
```

**File:** `internal/modules/postgres_market_store.go`

**`LogATODailyResults`:**
```sql
INSERT INTO ato_daily_result
    (session_date, symbol, regime, final_score, position_size_flag,
     had_indicated_price, quote_count, peak_imbalance_ratio,
     final_indicated_price, ref_price, ceil_price,
     signal_fired, gate_block_reason, drop_reason)
VALUES (...)
ON CONFLICT (symbol, session_date) DO UPDATE SET
    peak_imbalance_ratio  = EXCLUDED.peak_imbalance_ratio,
    final_indicated_price = EXCLUDED.final_indicated_price,
    signal_fired          = EXCLUDED.signal_fired,
    gate_block_reason     = EXCLUDED.gate_block_reason,
    drop_reason           = EXCLUDED.drop_reason,
    quote_count           = EXCLUDED.quote_count,
    had_indicated_price   = EXCLUDED.had_indicated_price
```

Use `DO UPDATE` (not `DO NOTHING`) so a restart mid-session that re-writes the row does not silently drop updated state.

**`BackfillATODailyOutcomes`:**
Mirror `BackfillSignalOutcomes` exactly — correlated subqueries on `stock_ohlcv` for `close_d0` (same trading_date), `close_d1` (OFFSET 0 after), `close_d2` (OFFSET 1 after). Only updates NULL columns.

---

### 10d. Flush in ATOMonitorJob

**File:** `internal/job/ato_monitor.go`

All changes are additive. `sessionState` already tracks everything needed:

At the end of `Run()` — after `logSessionSummary()` and before returning — add a flush step:

```go
results := make([]output.ATODailyResult, 0, len(j.states))
for sym, state := range j.states {
    entry := watchlistBySymbol[sym]
    results = append(results, output.ATODailyResult{
        SessionDate:         sessionDate,
        Symbol:              sym,
        Regime:              string(regime.Regime),
        FinalScore:          entry.FinalScore,
        PositionSizeFlag:    entry.PositionSizeFlag,
        HadIndicatedPrice:   state.hadPrice,
        QuoteCount:          state.quoteCount,
        PeakImbalanceRatio:  state.peakImbalanceRatio,
        FinalIndicatedPrice: state.lastIndicatedPrice,
        RefPrice:            state.refPrice,
        CeilPrice:           state.ceilPrice,
        SignalFired:         state.signalFired,
        GateBlockReason:     state.lastGateBlockReason,
        DropReason:          state.dropReason,
    })
}
if err := j.store.LogATODailyResults(ctx, results); err != nil {
    log.Printf("[ato] log daily results: %v", err)
}
```

`watchlistBySymbol` is a `map[string]output.WatchlistEntry` built at the start of `Run()` from the loaded watchlist — add this if it does not already exist (currently the watchlist is likely a slice; build the map once at load time).

Fields that may not yet exist on `sessionState` — add them if missing (all are simple value updates, no logic changes):
- `peakImbalanceRatio float64` — update with `max(current, snapshot.ImbalanceRatio)` on each snapshot evaluation
- `lastGateBlockReason string` — set whenever the gate rejects a snapshot, with the first failing condition
- `dropReason string` — already set in `dropNoPrice()`; ensure it is written to the struct
- `signalFired bool` — set to `true` when the signal write to `signal_log` succeeds

---

### 10e. Wire BackfillATODailyOutcomes into nightly pipeline

**File:** `internal/job/market_data.go`

Add after the existing `BackfillSignalOutcomes` call (and the `BackfillSwingOutcomes` call from Step 7):

```go
n, err = j.store.BackfillATODailyOutcomes(ctx)
if err != nil {
    log.Printf("[job] backfill ato daily outcomes: %v", err)
} else if n > 0 {
    log.Printf("[job] backfilled %d ato daily outcome fields", n)
}
```

---

### Validation

- [ ] `LogATODailyResults` called twice for the same `(symbol, session_date)` → second call updates mutable fields, does not error, row count unchanged
- [ ] `BackfillATODailyOutcomes` called twice → second call updates 0 rows
- [ ] After a live ATO session: `SELECT COUNT(*) FROM ato_daily_result WHERE session_date = TODAY` equals the number of symbols that were on the watchlist that morning
- [ ] Symbols dropped at 09:07 with no indicated price → `had_indicated_price = false`, `drop_reason` populated, `peak_imbalance_ratio = NULL`
- [ ] Symbol where signal fired → `signal_fired = true`, `gate_block_reason = NULL`
- [ ] Symbol monitored through full session with no signal → `signal_fired = false`, `gate_block_reason` populated with last failing condition

---

---

## Step 11: warnFired Gate Fix + signal_quality Field

### Background

Apr 17 confirmed an empirical bug: a sell-side warning fired at 09:00:26 (`warnFired["VIC"] = true`), and a buy signal fired at 09:01:09 in the same session. The gate at `ato_monitor.go:349` (`if warnFired[sym] { continue }`) should have blocked this. The root cause must be traced before the next live session.

Note on `StableCount`: the analyst's concern about "fired on 1 snapshot" is a misread of the log. `StableCount = 1` means 1 completed window of `stability_window` (3) consecutive passing snapshots — not 1 snapshot. The stability counter itself is working correctly. The bug is specifically the `warnFired` gate failing to block the signal.

---

### 11a. Investigate and fix the warnFired gate

**File:** `internal/job/ato_monitor.go`

Trace the exact snapshot sequence for Apr 17 from the terminal logs (not just `ato_session_log` — the live `log.Printf` lines at line 305 print every snapshot). Confirm whether `warnFired["VIC"]` was `true` at 09:01:09. Likely candidates for the failure:

- Race between the goroutine spawned by `go j.postSellWarn(...)` and the main poll loop — but `warnFired[sym] = true` is set synchronously before the goroutine is spawned, so this should not be the cause. Confirm by inspection.
- A mid-session restart that re-initialized `warnFired` but did not re-run `session_start` (possible if `MarkATOMonitored` was already true and the restart path skips the session entirely — but then no signal would fire either). Rule out by checking `daily_crawl_status`.
- A code path where `warnFired` is not passed by reference and a stale copy is used in `poll()`. Go maps are reference types, so this should not apply. Confirm.

Once the cause is identified, apply the minimal fix. If the cause cannot be reproduced, add a defensive guard:

```go
// In poll(), after the existing warnFired check — belt-and-suspenders:
if ss.sellWarnFiredThisSession[sym] {
    // ... gate blocked
    continue
}
```

Where `ss.sellWarnFiredThisSession` is written by the same code path that sets `warnFired[sym] = true`, but persisted on `sessionState` rather than the local map — ensuring it survives any map-copy edge case.

---

### 11b. Add `signal_quality` column to `signal_log`

**File:** `internal/modules/postgres_market_store.go` — inside `Migrate()`:

```sql
ALTER TABLE signal_log
    ADD COLUMN IF NOT EXISTS signal_quality TEXT NOT NULL DEFAULT 'Confirmed';
```

Default `'Confirmed'` so all historical rows get the optimistic label. Rows will be retroactively correctable via a one-time UPDATE.

Two values:
- `Confirmed` — signal fired with no sell-warn in the same session AND `stable_snapshots >= stability_window` at fire time
- `Degraded` — sell-warn fired in the same session before this signal OR `stable_snapshots < stability_window` at fire time

**File:** `internal/interface/output/` — add `SignalQuality` to `SignalRecord`:

```go
SignalQuality string // "Confirmed" or "Degraded"
```

**File:** `internal/job/ato_monitor.go` — in `fireSignal` / the signal record assembly:

```go
quality := "Confirmed"
if warnFired[c.sym] || c.stableCount < j.signal.StabilityWindow {
    quality = "Degraded"
}
```

Persist `quality` to `signal_log.signal_quality` in the INSERT.

**One-time backfill:** Apr 17 VIC signal should be manually updated to `Degraded` since it fired after a sell-warn in the same session.

---

### Validation

- [ ] After fix: simulate sell-warn followed by buy-dominant snapshots in the same session — confirm `gate_blocked` is logged and no signal fires
- [ ] `signal_quality = 'Confirmed'` on a clean signal (no sell-warn, stable_snapshots ≥ 3)
- [ ] `signal_quality = 'Degraded'` on a signal where sell-warn preceded it in the same session
- [ ] All existing `signal_log` rows default to `'Confirmed'` after migration (retroactively fix Apr 17 VIC manually)

---

## Step 12: Proprietary Flow (Domestic Institutional)

### Goal

Add domestic securities firm (proprietary / tự doanh) buy/sell flow alongside the existing foreign flow. This is the missing column that can answer: who absorbed the Apr 8 foreign dump of −1,167B while price closed up 2.3%? If proprietary flow was strongly net positive on Apr 8 (and similarly on Mar 18/20), it confirms coordinated domestic accumulation — the full manipulation cycle becomes visible in the data.

The SSI `DailyStockPrice` feed already contains proprietary flow fields. This is a schema extension + pipeline change, not a new data source.

---

### 12a. Schema

**File:** `internal/modules/postgres_market_store.go` — inside `Migrate()`:

```sql
ALTER TABLE stock_foreign_flow
    ADD COLUMN IF NOT EXISTS prop_buy_volume  BIGINT,
    ADD COLUMN IF NOT EXISTS prop_sell_volume BIGINT,
    ADD COLUMN IF NOT EXISTS prop_net_volume  BIGINT,
    ADD COLUMN IF NOT EXISTS prop_buy_value   NUMERIC(20,2),
    ADD COLUMN IF NOT EXISTS prop_sell_value  NUMERIC(20,2),
    ADD COLUMN IF NOT EXISTS prop_net_value   NUMERIC(20,2);
```

Nullable columns — existing rows stay NULL until a re-fetch or the nightly pipeline fills them going forward.

Column naming mirrors the foreign flow columns exactly (`buy_volume` → `prop_buy_volume`, etc.) so queries can compare the two flows side-by-side without aliasing.

---

### 12b. Pipeline change

**File:** `internal/interface/input/ssi_fast_connect.go` (or wherever `DailyStockPrice` response is parsed)

Locate the struct that maps to the SSI `DailyStockPrice` response. Add fields for proprietary flow — check the SSI FastConnectData spec (`SSI_FastConnectData_Specs.pdf`) for the exact JSON field names. They are typically named something like `propBuyVol`, `propSellVol`, `propBuyVal`, `propSellVal`.

**File:** `internal/interface/output/market_store.go` — `ForeignFlowRecord` (or equivalent type):

Add:
```go
PropBuyVolume  int64
PropSellVolume int64
PropNetVolume  int64
PropBuyValue   float64
PropSellValue  float64
PropNetValue   float64
```

**File:** `internal/modules/postgres_market_store.go` — `StoreForeignFlow` INSERT:

Extend the INSERT to include the six new columns. Use `ON CONFLICT ... DO UPDATE` to fill in `prop_*` columns on rows that already exist with NULL prop values (handles re-fetch scenarios).

---

### 12c. Historical backfill query (one-time, operator-run)

Once the pipeline is updated and a few days of prop flow data have accumulated, run a manual re-fetch of historical dates to fill in `prop_*` columns for the manipulation-period dates (Mar 18, Mar 20, Apr 8 at minimum). This is done by calling the nightly pipeline with a date-range override, or via a one-off SQL update from a separate fetch script.

---

### The manipulation diagnostic query

Once both `prop_net_value` and the existing `net_value` (foreign) are populated, the key query is:

```sql
SELECT
    f.trading_date,
    o.close,
    o.volume,
    f.net_value        AS foreign_net,
    f.prop_net_value   AS prop_net,
    f.net_value + f.prop_net_value AS combined_net
FROM stock_foreign_flow f
JOIN stock_ohlcv o ON o.symbol = f.symbol AND o.trading_date = f.trading_date
WHERE f.symbol = 'VIC'
ORDER BY f.trading_date;
```

Sessions where `foreign_net < -500B` AND `prop_net > 0` AND `close >= prior_close` are the coordinated accumulation fingerprint. If VIC shows this pattern on Mar 18, Mar 20, and Apr 8 — three major foreign exits absorbed by proprietary flow with price holding or recovering — the manipulation thesis moves from hypothesis to evidence.

---

### Validation

- [ ] `Migrate()` called twice on live DB → no errors (all `ADD COLUMN IF NOT EXISTS`)
- [ ] Nightly pipeline run after change → new rows have non-NULL `prop_*` values
- [ ] `prop_net_value = prop_buy_value - prop_sell_value` for all new rows (sanity check)
- [ ] Historical rows for VIC on Mar 18, Mar 20, Apr 8 backfilled with prop flow data

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
