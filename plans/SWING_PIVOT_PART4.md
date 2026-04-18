# Swing Signal Pivot — Part 4: ATO Enhancements

## Goal

Three independent ATO-side improvements: a per-symbol session result log (`ato_daily_result`), a fix for the `warnFired` gate that failed to block a signal on Apr 17, and proprietary (domestic institutional) flow tracking. None of these touch the swing signal path.

## Hard Constraints (Guardrails)

**Never modify or delete these files:**
- `internal/calculator/ato.go`
- `internal/modules/ssi_order_book_client.go`
- `internal/interface/input/order_book_client.go`
- `internal/interface/input/order_book_snapshot.go`
- `internal/interface/input/ssi_fast_connect.go`

**Schema rules:**
- Only `CREATE TABLE IF NOT EXISTS` for new tables
- Only `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for new columns
- Never DROP, TRUNCATE, or ALTER existing column types or constraints

**Code rules:**
- All changes to `internal/job/ato_monitor.go` are additive (new fields on structs, new flush call at end of `Run()`)
- `go build ./...` and `go vet ./...` must pass after every step

---

## Prerequisites — Verify Part 3 Is Complete

Do not write any code until all of the following are confirmed true by reading the actual files:

- [ ] `go build ./...` passes
- [ ] `internal/job/swing_signal.go` exists with `SwingSignalJob`
- [ ] `internal/job/market_data.go` calls `BackfillSwingOutcomes`
- [ ] `cmd/app/main.go` gates both `ATOMonitorJob` and `SwingSignalJob` on `cfg.SignalMode`
- [ ] `MarketStore` interface contains `LogSwingSignals`, `BackfillSwingOutcomes`, `IsSwingSignalSent`, `MarkSwingSignalSent`, `LoadSwingWatchlist`

If any check fails, stop and report which check failed. Do not proceed.

---

## Step 10: Daily ATO Result Log (Per-Symbol Session Snapshot)

### Goal

Every watchlist symbol gets one structured row per ATO session, regardless of whether a signal fired. This makes per-symbol post-session analysis queryable without parsing text from `ato_session_log`. The `signal_log` table continues unchanged.

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
- `early_ratio` — first imbalance ratio recorded after `IndicatedPrice > 0`; NULL if ATO never formed
- `peak_imbalance_ratio` — highest bid/ask ratio seen across all snapshots; NULL if no indicated price
- `final_indicated_price` — last known indicated price at session end or drop time; NULL if no price ever formed
- `ref_price` / `ceil_price` — from the first Quote received; NULL if no quotes arrived
- `sell_warn_fired` — true if a sell-side warning fired for this symbol in this session
- `stable_snapshots_at_fire` — value of `StableCount` at signal fire time; NULL if no signal fired
- `signal_fired` — true if a signal was written to `signal_log` for this symbol this session
- `gate_block_reason` — last recorded reason the gate was not met; NULL if signal fired or symbol dropped before gate evaluation
- `drop_reason` — reason from `dropNoPrice()` if symbol was dropped at the cutoff; NULL otherwise
- `close_d0/d1/d2` — back-filled nightly from `stock_ohlcv`

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
    EarlyRatio           float64
    PeakImbalanceRatio   float64
    FinalIndicatedPrice  float64
    RefPrice             float64
    CeilPrice            float64
    SellWarnFired        bool
    StableSnapshotsAtFire int
    SignalFired          bool
    GateBlockReason      string
    DropReason           string
}
```

Zero values are safe defaults.

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
     had_indicated_price, quote_count, early_ratio, peak_imbalance_ratio,
     final_indicated_price, ref_price, ceil_price, sell_warn_fired,
     stable_snapshots_at_fire, signal_fired, gate_block_reason, drop_reason)
VALUES (...)
ON CONFLICT (symbol, session_date) DO UPDATE SET
    peak_imbalance_ratio   = EXCLUDED.peak_imbalance_ratio,
    final_indicated_price  = EXCLUDED.final_indicated_price,
    sell_warn_fired        = EXCLUDED.sell_warn_fired,
    stable_snapshots_at_fire = EXCLUDED.stable_snapshots_at_fire,
    signal_fired           = EXCLUDED.signal_fired,
    gate_block_reason      = EXCLUDED.gate_block_reason,
    drop_reason            = EXCLUDED.drop_reason,
    quote_count            = EXCLUDED.quote_count,
    had_indicated_price    = EXCLUDED.had_indicated_price
```

Use `DO UPDATE` (not `DO NOTHING`) so a restart mid-session that re-writes the row does not silently drop updated state.

**`BackfillATODailyOutcomes`:**
Mirror `BackfillSignalOutcomes` — correlated subqueries on `stock_ohlcv` for `close_d0` (same trading_date), `close_d1` (OFFSET 0 after), `close_d2` (OFFSET 1 after). Only updates NULL columns.

---

### 10d. Flush in ATOMonitorJob

**File:** `internal/job/ato_monitor.go`

All changes are additive. At the end of `Run()` — after `logSessionSummary()` and before returning — add a flush step:

```go
results := make([]output.ATODailyResult, 0, len(j.states))
for sym, state := range j.states {
    entry := watchlistBySymbol[sym]
    results = append(results, output.ATODailyResult{
        SessionDate:           sessionDate,
        Symbol:                sym,
        Regime:                string(regime.Regime),
        FinalScore:            entry.FinalScore,
        PositionSizeFlag:      entry.PositionSizeFlag,
        HadIndicatedPrice:     state.hadPrice,
        QuoteCount:            state.quoteCount,
        EarlyRatio:            state.earlyRatio,
        PeakImbalanceRatio:    state.peakImbalanceRatio,
        FinalIndicatedPrice:   state.lastIndicatedPrice,
        RefPrice:              state.refPrice,
        CeilPrice:             state.ceilPrice,
        SellWarnFired:         state.sellWarnFiredThisSession,
        StableSnapshotsAtFire: state.stableSnapshotsAtFire,
        SignalFired:           state.signalFired,
        GateBlockReason:       state.lastGateBlockReason,
        DropReason:            state.dropReason,
    })
}
if err := j.store.LogATODailyResults(ctx, results); err != nil {
    log.Printf("[ato] log daily results: %v", err)
}
```

`watchlistBySymbol` is a `map[string]output.WatchlistEntry` built at the start of `Run()` from the loaded watchlist — add this if it does not already exist.

Fields to add to `sessionState` if missing (all are simple value updates, no logic changes):
- `earlyRatio float64` — set once, on the first snapshot where `IndicatedPrice > 0`
- `peakImbalanceRatio float64` — updated with `max(current, snapshot.ImbalanceRatio)` on each snapshot evaluation
- `sellWarnFiredThisSession bool` — set to `true` in the same code path that sets `warnFired[sym] = true`
- `stableSnapshotsAtFire int` — set to `StableCount` value when the signal fires
- `lastGateBlockReason string` — set whenever the gate rejects a snapshot, with the first failing condition
- `dropReason string` — ensure it is written to the struct in `dropNoPrice()`
- `signalFired bool` — set to `true` when the signal write to `signal_log` succeeds

---

### 10e. Wire BackfillATODailyOutcomes into nightly pipeline

**File:** `internal/job/market_data.go`

Add after the existing `BackfillSwingOutcomes` call:

```go
n, err = j.store.BackfillATODailyOutcomes(ctx)
if err != nil {
    log.Printf("[job] backfill ato daily outcomes: %v", err)
} else if n > 0 {
    log.Printf("[job] backfilled %d ato daily outcome fields", n)
}
```

---

## Step 11: warnFired Gate Fix + signal_quality Field

### Background

Apr 17 confirmed an empirical bug: a sell-side warning fired at 09:00:26 (`warnFired["VIC"] = true`), and a buy signal fired at 09:01:09 in the same session. The gate at `ato_monitor.go` (`if warnFired[sym] { continue }`) should have blocked this.

Note: `StableCount = 1` means 1 completed window of `stability_window` (3) consecutive passing snapshots — not 1 snapshot. The stability counter is working correctly. The bug is specifically the `warnFired` gate.

---

### 11a. Investigate and fix the warnFired gate

**File:** `internal/job/ato_monitor.go`

Trace the snapshot sequence for Apr 17. Confirm whether `warnFired["VIC"]` was `true` at 09:01:09. Likely candidates:

- Race between the goroutine spawned by `go j.postSellWarn(...)` and the main poll loop — but `warnFired[sym] = true` is set synchronously before the goroutine is spawned. Confirm by inspection.
- A mid-session restart that re-initialized `warnFired` — rule out by checking `daily_crawl_status`.
- A code path where `warnFired` is not passed by reference — Go maps are reference types so this should not apply. Confirm.

Once the cause is identified, apply the minimal fix. If the cause cannot be reproduced, add a defensive guard using `sessionState.sellWarnFiredThisSession` (added in Step 10d) — since it is persisted on the struct it survives any map-copy edge case:

```go
if ss.sellWarnFiredThisSession {
    // gate blocked
    continue
}
```

---

### 11b. Add `signal_quality` column to `signal_log`

**File:** `internal/modules/postgres_market_store.go` — inside `Migrate()`:

```sql
ALTER TABLE signal_log
    ADD COLUMN IF NOT EXISTS signal_quality TEXT NOT NULL DEFAULT 'Confirmed';
```

Default `'Confirmed'` so all historical rows get the optimistic label.

Two values:
- `Confirmed` — signal fired with no sell-warn in the same session AND `stable_snapshots >= stability_window` at fire time
- `Degraded` — sell-warn fired in the same session before this signal OR `stable_snapshots < stability_window` at fire time

**File:** `internal/interface/output/` — add `SignalQuality string` to `SignalRecord`.

**File:** `internal/job/ato_monitor.go` — in signal record assembly:

```go
quality := "Confirmed"
if warnFired[c.sym] || c.stableCount < j.signal.StabilityWindow {
    quality = "Degraded"
}
```

Persist `quality` to `signal_log.signal_quality` in the INSERT.

**One-time backfill:** Apr 17 VIC signal should be manually updated to `Degraded`.

---

### Validation for Steps 10–11

- [ ] `LogATODailyResults` called twice for the same `(symbol, session_date)` → second call updates mutable fields, does not error, row count unchanged
- [ ] `BackfillATODailyOutcomes` called twice → second call updates 0 rows
- [ ] After a live ATO session: `SELECT COUNT(*) FROM ato_daily_result WHERE session_date = TODAY` equals the number of watchlist symbols
- [ ] Symbols dropped at 09:07 with no indicated price → `had_indicated_price = false`, `drop_reason` populated
- [ ] Symbol where signal fired → `signal_fired = true`, `gate_block_reason = NULL`
- [ ] Symbol monitored with no signal → `signal_fired = false`, `gate_block_reason` populated
- [ ] After fix: sell-warn followed by buy-dominant snapshots in same session → gate blocked, no signal fires
- [ ] `signal_quality = 'Confirmed'` on a clean signal (no sell-warn, stable_snapshots ≥ 3)
- [ ] `signal_quality = 'Degraded'` on a signal where sell-warn preceded it

---

## Step 12: Proprietary Flow (Domestic Institutional)

### Goal

Add domestic securities firm (proprietary / tự doanh) buy/sell flow alongside the existing foreign flow. The SSI `DailyStockPrice` feed already contains these fields — this is a schema extension + pipeline change, not a new data source.

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

Nullable — existing rows stay NULL until a re-fetch or the nightly pipeline fills them going forward.

---

### 12b. Pipeline change

**File:** `internal/interface/input/ssi_fast_connect.go` (or wherever `DailyStockPrice` response is parsed)

Locate the struct that maps to the SSI `DailyStockPrice` response. Add fields for proprietary flow — check the SSI FastConnectData spec for the exact JSON field names (typically `propBuyVol`, `propSellVol`, `propBuyVal`, `propSellVal`).

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

Extend the INSERT to include the six new columns. Use `ON CONFLICT ... DO UPDATE` to fill in `prop_*` columns on rows that already exist with NULL values (handles re-fetch scenarios).

---

### 12c. Historical backfill (operator-run, after pipeline is live)

Once the pipeline is updated and a few days of prop flow data have accumulated, run a manual re-fetch of historical dates to fill in `prop_*` columns for the manipulation-period dates (Mar 18, Mar 20, Apr 8 at minimum).

---

### The manipulation diagnostic query

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

Sessions where `foreign_net < -500B` AND `prop_net > 0` AND `close >= prior_close` are the coordinated accumulation fingerprint.

---

### Validation for Step 12

- [ ] `Migrate()` called twice on live DB → no errors
- [ ] Nightly pipeline run after change → new rows have non-NULL `prop_*` values
- [ ] `prop_net_volume = prop_buy_volume - prop_sell_volume` for all new rows
- [ ] `prop_net_value = prop_buy_value - prop_sell_value` for all new rows
- [ ] Historical rows for VIC on Mar 18, Mar 20, Apr 8 backfilled with prop flow data

---

## Part 4 Completion Checklist

- [ ] `go build ./...` passes with zero errors
- [ ] `go vet ./...` passes with zero warnings
- [ ] `ato_daily_result` DDL exists in `Migrate()`
- [ ] `ATODailyResult` type exists in `internal/interface/output/`
- [ ] `MarketStore` interface contains `LogATODailyResults` and `BackfillATODailyOutcomes`
- [ ] Both methods are implemented in `postgres_market_store.go`
- [ ] `LogATODailyResults` uses `ON CONFLICT ... DO UPDATE`
- [ ] `ato_monitor.go` flushes `ATODailyResult` records at end of `Run()`
- [ ] `market_data.go` calls `BackfillATODailyOutcomes` in the nightly pipeline
- [ ] `warnFired` gate root cause identified and fix applied (or defensive guard added)
- [ ] `signal_quality` column DDL in `Migrate()`
- [ ] `SignalRecord` has `SignalQuality string` field
- [ ] Signal assembly sets `quality = "Degraded"` when sell-warn preceded the signal
- [ ] `stock_foreign_flow` prop columns DDL in `Migrate()`
- [ ] `ForeignFlowRecord` has the six `Prop*` fields
- [ ] `StoreForeignFlow` INSERT includes prop columns
