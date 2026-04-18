# Swing Signal Pivot — Part 2: Data Layer

## Goal

Implement the MarketStore methods and AI port for the swing signal system. No job logic yet. After this part, all persistence and AI interfaces are complete and the codebase compiles cleanly.

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
- `go build ./...` and `go vet ./...` must pass after every step

---

## Prerequisites — Verify Part 1 Is Complete

Do not write any code until all of the following are confirmed true by reading the actual files:

- [ ] `go build ./...` passes
- [ ] `internal/config/config.go` has `SignalMode string` and `Swing SwingConfig` on `Config`
- [ ] `SwingConfig` struct exists with `Cron`, `MaxWatchlistSize`, `AITimeout`
- [ ] `swing_signal_log` DDL exists in `Migrate()`
- [ ] `swing_signal_sent` column DDL exists in `Migrate()`
- [ ] `SwingSignalRecord` and `SwingBroadcastRecord` exist in `internal/interface/output/swing.go`
- [ ] `WatchlistEntry` has a `Close float64` field

If any check fails, stop and report which check failed. Do not proceed.

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

The method signature must match the existing pattern in `OpenAIClient`:
```go
func (c *OpenAIClient) SwingBroadcast(ctx context.Context, rec output.SwingBroadcastRecord) (string, error)
```

### Validation
- Calling `SwingBroadcast` with an empty `Stocks` slice returns an empty string without error (guard at top of function)
- Output does not contain stock tickers not present in `rec.Stocks`

---

## Part 2 Completion Checklist

Before handing off to Part 3, verify all of the following:

- [ ] `go build ./...` passes with zero errors
- [ ] `go vet ./...` passes with zero warnings
- [ ] `MarketStore` interface in `internal/interface/output/market_store.go` contains all 5 new method signatures: `LogSwingSignals`, `BackfillSwingOutcomes`, `IsSwingSignalSent`, `MarkSwingSignalSent`, `LoadSwingWatchlist`
- [ ] All 5 methods are implemented on the Postgres adapter in `internal/modules/postgres_market_store.go`
- [ ] `LogSwingSignals` uses `ON CONFLICT (symbol, signal_date) DO NOTHING`
- [ ] `BackfillSwingOutcomes` only updates rows where the outcome column `IS NULL`
- [ ] `IsSwingSignalSent` / `MarkSwingSignalSent` operate on the `swing_signal_sent` column
- [ ] `AI` interface in `internal/interface/output/ai.go` contains `SwingBroadcast`
- [ ] `SwingBroadcast` is implemented on `OpenAIClient` in `internal/modules/openai_ai.go`
- [ ] `SwingBroadcast` returns `("", nil)` when `rec.Stocks` is empty
