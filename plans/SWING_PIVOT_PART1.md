# Swing Signal Pivot — Part 1: Foundation

## Goal

Lay the foundation for the swing signal pivot: config layer, database schema, and output types. No job logic, no wiring. After this part, the codebase must compile cleanly with all new types in place.

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
- `go build ./...` and `go vet ./...` must pass after every step

---

## Step 1: Config Layer

**File:** `internal/config/config.go`

### 1a. Add `SignalMode` to `Config` struct

Add after `ATO ATOConfig`:

```go
SignalMode string `mapstructure:"signal_mode"`

Swing SwingConfig `mapstructure:"swing"`
```

### 1b. Add `SwingConfig` struct

Add alongside existing config structs:

```go
type SwingConfig struct {
	Cron             string        `mapstructure:"cron"`
	MaxWatchlistSize int           `mapstructure:"max_watchlist_size"`
	AITimeout        time.Duration `mapstructure:"ai_timeout"`
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

`entry_price`, `sl_price`, `tp_price` are nullable — computed at job time but may be absent if data is unavailable. `close_d1` through `close_d10` are nullable and back-filled nightly.

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

This is needed by `SwingSignalJob` to compute `tp_price = close * (1 + resistance_distance)`. This is an additive field — existing code that constructs `WatchlistEntry` without it gets zero value, which is safe since ATO code does not use `Close`.

Populate `Close` in `LoadWatchlist` in the Postgres adapter via a JOIN to `stock_ohlcv` on `(symbol, trading_date)`.

---

## Part 1 Completion Checklist

Before handing off to Part 2, verify all of the following:

- [ ] `go build ./...` passes with zero errors
- [ ] `go vet ./...` passes with zero warnings
- [ ] `internal/config/config.go` contains `SignalMode string` and `Swing SwingConfig` fields on the `Config` struct
- [ ] `SwingConfig` struct exists with `Cron`, `MaxWatchlistSize`, `AITimeout` fields
- [ ] `Load()` sets `SignalMode = "ato"` when absent
- [ ] `swing_signal_log` table DDL is present in `Migrate()`
- [ ] `swing_signal_sent` column DDL is present in `Migrate()`
- [ ] `Migrate()` called twice on a live DB produces no errors
- [ ] `SwingSignalRecord` and `SwingBroadcastRecord` types exist in `internal/interface/output/swing.go`
- [ ] `WatchlistEntry` has a `Close float64` field
- [ ] `LoadWatchlist` populates `Close` via JOIN to `stock_ohlcv`
