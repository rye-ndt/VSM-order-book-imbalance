# Swing Signal Pivot — Part 3: Job + Integration

## Goal

Implement `SwingSignalJob`, wire the backfill hook into the nightly pipeline, and gate all signal-mode logic in `main.go`. After this part, the full swing signal path is operational end-to-end.

## Hard Constraints (Guardrails)

**Never modify or delete these files:**
- `internal/calculator/ato.go`
- `internal/job/ato_monitor.go`
- `internal/modules/ssi_order_book_client.go`
- `internal/interface/input/order_book_client.go`
- `internal/interface/input/order_book_snapshot.go`
- `internal/interface/input/ssi_fast_connect.go`

**Code rules:**
- All changes to existing files are additive (new methods, new fields, new branches)
- The `"ato"` code path in `main.go` must remain byte-for-byte equivalent to today's behavior
- `go build ./...` and `go vet ./...` must pass after every step

---

## Prerequisites — Verify Part 2 Is Complete

Do not write any code until all of the following are confirmed true by reading the actual files:

- [ ] `go build ./...` passes
- [ ] `MarketStore` interface contains `LogSwingSignals`, `BackfillSwingOutcomes`, `IsSwingSignalSent`, `MarkSwingSignalSent`, `LoadSwingWatchlist`
- [ ] All 5 methods are implemented in `internal/modules/postgres_market_store.go`
- [ ] `AI` interface contains `SwingBroadcast`
- [ ] `SwingBroadcast` is implemented in `internal/modules/openai_ai.go`

If any check fails, stop and report which check failed. Do not proceed.

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

The variable `n` is already declared earlier in the function from the `BackfillSignalOutcomes` call — reuse it.

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

## Part 3 Completion Checklist

Before handing off to Part 4, verify all of the following:

- [ ] `go build ./...` passes with zero errors
- [ ] `go vet ./...` passes with zero warnings
- [ ] `internal/job/swing_signal.go` exists and contains `SwingSignalJob` and `NewSwingSignalJob`
- [ ] `Run()` checks `IsSwingSignalSent` before doing any work
- [ ] `Run()` calls `MarkSwingSignalSent` regardless of AI/notifier outcome
- [ ] `Run()` sends a fallback message when `ai == nil`
- [ ] `internal/job/market_data.go` calls `BackfillSwingOutcomes` after `BackfillSignalOutcomes`
- [ ] `cmd/app/main.go` gates `obClient` construction on `cfg.SignalMode != "swing"`
- [ ] `cmd/app/main.go` gates `ATOMonitorJob` on `cfg.SignalMode != "swing"`
- [ ] `cmd/app/main.go` gates `SwingSignalJob` on `cfg.SignalMode != "ato"`
- [ ] With `signal_mode` absent from config: startup logs show `SSI IDS: OK`, ATO cron registered, no swing log lines
- [ ] With `signal_mode: "swing"`: startup logs show no IDS ping, swing cron registered, no ATO log lines
- [ ] With `signal_mode: "both"`: both jobs registered, IDS pinged
- [ ] `LogSwingSignals` with duplicate `(symbol, signal_date)` → no error, row count unchanged
- [ ] `BackfillSwingOutcomes` called twice → second call updates 0 rows
- [ ] `SwingSignalJob.Run()` called with `swing_signal_sent = true` → returns immediately, no Telegram message
- [ ] `SwingSignalJob.Run()` called with empty watchlist → logs warning, marks sent, no message sent
- [ ] `WatchlistEntry.Close` populated — verify a known symbol's close matches `stock_ohlcv`
