# Plan: Signal Outcome Back-fill (close_d0 / close_d1 / close_d2)

## Goal

After a signal fires and lands in `signal_log`, the next 1–2 trading days will deposit
the actual close prices into `stock_ohlcv`. A back-fill step must read those prices back
and write them into `signal_log` so each row becomes a complete event study record.

---

## What "D0 / D1 / D2" means

| Label | Trading date | Available in stock_ohlcv after |
|---|---|---|
| D0 | Same day as signal | Next morning's 08:45 pipeline |
| D1 | Next trading day | Pipeline on D1+1 |
| D2 | Trading day after D1 | Pipeline on D2+1 |

Vietnam uses T+2 settlement. D2 close is the last date where a buyer from D0 is still
locked in (cannot sell until D2+1). So D2 is the natural outcome horizon.

---

## Step 1 — Add columns to the migration

**File:** `internal/modules/postgres_market_store.go` → `Migrate()`

After the last existing `ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS` line (currently
`position_size_flag` at line 127), add:

```sql
ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS close_d0  NUMERIC(18, 2);
ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS close_d1  NUMERIC(18, 2);
ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS close_d2  NUMERIC(18, 2);
ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS entry_price_actual NUMERIC(18, 2);
```

All four are nullable — a NULL means "not yet available", not an error.

`entry_price_actual` is the real ATO fill price (may differ from `indicated_price` due
to auction slippage). Add it now while the schema is open — back-filled manually by the
operator after each session.

---

## Step 2 — Add a port method to MarketStore

**File:** `internal/interface/output/market_store.go`

Add one method to the `MarketStore` interface:

```go
// BackfillSignalOutcomes scans signal_log rows where close_d0/d1/d2 are NULL
// and the corresponding trading date is present in stock_ohlcv, then fills them in.
// Safe to call repeatedly — only updates rows where the column is still NULL.
BackfillSignalOutcomes(ctx context.Context) error
```

---

## Step 3 — Implement the method in PostgresMarketStore

**File:** `internal/modules/postgres_market_store.go`

The implementation does three UPDATE passes — one per outcome day — in a single DB call
using a lateral join or correlated subquery:

```sql
UPDATE signal_log s
SET close_d0 = o.close
FROM stock_ohlcv o
WHERE o.symbol      = s.symbol
  AND o.trading_date = s.fired_at::date   -- D0 = same calendar date as signal
  AND s.close_d0    IS NULL;

UPDATE signal_log s
SET close_d1 = o.close
FROM stock_ohlcv o
JOIN (
    SELECT symbol, trading_date,
           LEAD(trading_date) OVER (PARTITION BY symbol ORDER BY trading_date) AS next_td
    FROM stock_ohlcv
) nav ON nav.symbol = s.symbol AND nav.trading_date = s.fired_at::date
WHERE o.symbol       = s.symbol
  AND o.trading_date = nav.next_td
  AND s.close_d1    IS NULL;
```

D2 follows the same pattern as D1 but uses two `LEAD` steps.

**Simpler alternative** (avoid window functions): since `stock_ohlcv` has one row per
symbol per trading date, you can just compute the next two trading dates by querying the
actual dates present in the table:

```sql
-- D1: smallest trading_date > fired_at::date for this symbol
UPDATE signal_log s
SET close_d1 = (
    SELECT close FROM stock_ohlcv
    WHERE symbol       = s.symbol
      AND trading_date = (
          SELECT MIN(trading_date) FROM stock_ohlcv
          WHERE symbol > s.symbol OR (symbol = s.symbol AND trading_date > s.fired_at::date)
          -- simpler:
      )
)
WHERE close_d1 IS NULL;
```

Cleanest approach — use a correlated subquery that finds `MIN(trading_date) > signal_date`
for the same symbol. This handles weekends, holidays, and trading suspensions correctly
because it uses actual dates from the table rather than adding fixed day offsets.

Final implementation shape:

```go
func (s *PostgresMarketStore) BackfillSignalOutcomes(ctx context.Context) error {
    for _, q := range []string{queryD0, queryD1, queryD2} {
        if _, err := s.db.ExecContext(ctx, q); err != nil {
            return err
        }
    }
    return nil
}
```

Where `queryD0`, `queryD1`, `queryD2` are package-level `const` strings — one UPDATE each.

---

## Step 4 — Call BackfillSignalOutcomes from the nightly pipeline

**File:** `internal/job/market_data.go`

At the end of `MarketDataJob.Run()`, after all OHLCV data is stored and metrics are
computed, add:

```go
if err := j.store.BackfillSignalOutcomes(ctx); err != nil {
    log.Printf("[job] backfill signal outcomes: %v", err)
}
```

This means every morning at 08:45, after yesterday's closes land in `stock_ohlcv`,
the back-fill runs automatically and fills whatever outcome columns are now available.
No manual intervention needed.

---

## Step 5 — Log what was filled

After `BackfillSignalOutcomes` executes, log how many rows were updated so you can
confirm it ran. The simplest way: have the method return `(int64, error)` where the
int64 is `rows affected` summed across the three UPDATE statements.

Change the port method signature to:

```go
BackfillSignalOutcomes(ctx context.Context) (int64, error)
```

In `market_data.go`:

```go
n, err := j.store.BackfillSignalOutcomes(ctx)
if err != nil {
    log.Printf("[job] backfill signal outcomes: %v", err)
} else if n > 0 {
    log.Printf("[job] backfilled %d signal outcome fields", n)
}
```

---

## Files touched

| File | Change |
|---|---|
| `internal/modules/postgres_market_store.go` | Add 4 ALTER TABLE lines to Migrate(); add BackfillSignalOutcomes() |
| `internal/interface/output/market_store.go` | Add BackfillSignalOutcomes to MarketStore interface |
| `internal/job/market_data.go` | Call BackfillSignalOutcomes at end of Run() |

No new files. No new config. No new dependencies.

---

## Verification

After the 2026-03-26 pipeline runs (08:45), query:

```sql
SELECT id, symbol, fired_at::date AS d0,
       close_d0, close_d1, close_d2
FROM signal_log;
```

- `close_d0` should be populated (HAG's 2026-03-25 close)
- `close_d1` should be NULL until 2026-03-27 pipeline
- `close_d2` should be NULL until 2026-03-28 pipeline
