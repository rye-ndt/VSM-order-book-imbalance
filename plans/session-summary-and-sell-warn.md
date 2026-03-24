# Plan: Session Summary + Sell-Side Warning

## Overview

Two new features triggered from `ATOMonitorJob`:

1. **Session Summary** — after the ATO session ends, AI produces a Vietnamese recap of the day and pushes it to all Telegram subscribers. Fires whether or not any buy signals were sent.
2. **Sell-Side Warning** — during the live poll loop, if a watchlist stock shows persistent ask-side dominance (ratio below a threshold for N consecutive snapshots), AI produces a short warning message and pushes it to subscribers.

Both features follow the existing pattern: Go code assembles structured data from in-memory session state → passes it to the `AI` port → delivers via `Notifier`.

---

## Feature 1: End-of-Session AI Summary

### When it fires

At the very end of `ATOMonitorJob.Run()`, immediately before the function returns. The existing `logSessionSummary()` call already happens here — the AI summary runs after it.

Only fires if `ai` and `notifier` are both non-nil.

---

### Data collected

All of this is already in-memory at session end. No new DB reads needed.

**From the `sessionState` map** (per-symbol state accumulated during polling):
- Symbol
- Quote count received
- First indicated price seen
- Peak imbalance ratio seen during session
- Whether stability was ever achieved
- Last gate block reason (if signal didn't fire)
- Whether the symbol was dropped at 09:07 and why

**From the `fired` map**: which symbols actually produced a buy signal.

**From pre-loaded data at session start**: watchlist entries (FinalScore, PositionSizeFlag, CandlePattern, VPR, MomentumScore), market regime, score threshold.

**From the session clock**: session date, actual start time, end time.

---

### New struct: `SessionSummaryRecord`

Location: `internal/interface/output/ai.go` (alongside existing types)

```go
type SymbolSessionOutcome struct {
    Symbol          string
    FinalScore      int
    PositionSize    string  // Full / Half
    CandlePattern   string
    VPR             string
    MomentumScore   int
    PeakRatio       float64 // highest imbalance ratio seen during session
    IndicatedPrice  float64 // last non-zero indicated price seen
    RefPrice        float64
    SignalFired     bool
    Dropped         bool    // dropped at 09:07 (no price)
    DropReason      string  // "no quotes", "EstMatchedPrice always 0", etc.
    GateBlockReason string  // if not fired and not dropped, why the gate didn't pass
}

type SessionSummaryRecord struct {
    SessionDate    time.Time
    Regime         string
    ScoreThreshold int
    Outcomes       []SymbolSessionOutcome
    SignalCount     int // total buy signals fired
}
```

---

### New AI port method

Add to `internal/interface/output/ai.go`:

```go
SummarizeSession(ctx context.Context, rec SessionSummaryRecord) (string, error)
```

---

### AI prompt design (OpenAI implementation)

System prompt (`summarizeSessionSystemPrompt`) — Vietnamese, conversational Telegram tone:

```
You are a Vietnamese stock market assistant. You will receive a structured summary of
today's ATO (pre-open) monitoring session on the Vietnam stock exchange.

Write a single Telegram message (3–6 sentences, Vietnamese) that covers:
- How many stocks were monitored and the market regime
- Which stocks showed genuine buy-side demand (signals fired), if any
- Which stocks were on the watchlist but did not fire, and the most relevant reason why
  (e.g., ask-side dominated, no price formed, price unstable)
- A brief closing verdict: was today a tradeable session or a watch-and-wait day?

Rules:
- Report only facts from the data provided. Do not add external market commentary.
- Do not give buy/sell advice beyond what the signal data supports.
- Tone: calm, factual, 1–2 emojis maximum.
- If no signals fired, say so clearly — do not imply missed opportunities that the data
  doesn't support.
```

User message: JSON-marshaled `SessionSummaryRecord`.

Method is a simple `Response()`-style call (no structured schema needed — plain text output, same as `TelegramInterpret`).

---

### Changes to `ato_monitor.go`

1. Extend `sessionState` struct to track `peakRatio float64`, `lastGateBlock string`, `lastIndicatedPrice float64`, `lastRefPrice float64` — updated on each poll iteration.

2. At the end of `Run()`, after `logSessionSummary()`:

```go
if j.ai != nil && j.notifier != nil {
    go j.postSessionSummary(context.Background(), watchlistEntries, regime, scoreThreshold, sessionDate)
}
```

3. New method `postSessionSummary(...)`:
   - Builds `SessionSummaryRecord` from `sessionState`, `fired`, and watchlist entry map
   - Calls `j.ai.SummarizeSession(ctx, rec)` with a timeout (same `ato.AITimeout`)
   - Calls `j.notifier.Notify(ctx, text)` if non-empty result

---

### Changes to `openai_ai.go`

Add `SummarizeSession(ctx, rec) (string, error)`:
- Marshal `rec` to JSON as the user message
- Use `summarizeSessionSystemPrompt` as system message
- Call the same `chat.Completions.New()` pattern used by `TelegramInterpret`
- Return raw string content

---

## Feature 2: Sell-Side Warning

### When it fires

During the main poll loop, after `ProcessSnapshot` is called for each symbol, before the buy-signal gate check. A sell warning fires when:

1. `analysis.ImbalanceRatio < cfg.SellWarnRatio` (asks dominant)
2. `snap.IndicatedPrice > 0` (ATO price is forming — not a pre-open artifact)
3. N consecutive snapshots all meet condition 1 (configurable stability requirement)
4. Symbol has not already triggered a sell warning this session (`warnFired[sym] == false`)

N=2 is a reasonable default (4 seconds of data, avoids one-tick noise).

---

### New config fields

Add to `config.SignalConfig` (both struct and YAML):

```go
SellWarnRatio          float64 // ratio below which asks are considered dominant, e.g. 0.50
SellWarnStabilityCount int     // consecutive snapshots required before warning, e.g. 2
```

Default values (in `config.example.yaml`):
```yaml
signal:
  sell_warn_ratio: 0.50
  sell_warn_stability_count: 2
```

---

### New struct: `SellWarnRecord`

Location: `internal/interface/output/ai.go`

```go
type SellWarnRecord struct {
    Symbol         string
    SessionDate    time.Time
    WarnedAt       time.Time
    IndicatedPrice float64
    RefPrice       float64
    CeilingPrice   float64
    ImbalanceRatio float64
    TopAskLevels   []input.PriceLevel // top 5 ask levels (ascending by price)
    TopBidLevels   []input.PriceLevel // top 5 bid levels (descending by price)
    FinalScore     int
    Regime         string
    CandlePattern  string
    VPR            string
    MomentumScore  int
}
```

---

### New AI port method

Add to `internal/interface/output/ai.go`:

```go
WarnSellPressure(ctx context.Context, rec SellWarnRecord) (string, error)
```

---

### AI prompt design

System prompt (`sellWarnSystemPrompt`) — Vietnamese, short, factual:

```
You are a Vietnamese stock market assistant monitoring the ATO (pre-open) session.

You will receive order book data for a stock showing strong sell-side pressure.
Write a short Telegram warning (2–4 sentences, Vietnamese) that:
- Names the stock and its current indicated price
- States the imbalance ratio and what it means (asks dominate bids by X×)
- Names the price levels with the heaviest ask volume (the supply walls)
- Closes with one line: what this likely means for the ATO open

Rules:
- Only use data provided. No external commentary.
- Do not recommend selling short or buying. Factual observation only.
- Tone: calm, informational. 1 emoji maximum.
```

User message: JSON-marshaled `SellWarnRecord`.

---

### Changes to `ato_monitor.go`

1. Add two new tracking maps alongside `fired map[string]bool`:

```go
warnFired  map[string]bool
warnCount  map[string]int
```

2. In the poll loop, after `ProcessSnapshot` returns `analysis`:

```go
if snap.IndicatedPrice > 0 && analysis.ImbalanceRatio < j.signal.SellWarnRatio {
    warnCount[sym]++
    if warnCount[sym] >= j.signal.SellWarnStabilityCount && !warnFired[sym] {
        warnFired[sym] = true
        go j.postSellWarn(context.Background(), sym, snap, analysis, entry, regime)
    }
} else {
    warnCount[sym] = 0
}
```

3. New method `postSellWarn(...)`:
   - Builds `SellWarnRecord` from snap, analysis, watchlist entry
   - Calls `j.ai.WarnSellPressure(ctx, rec)` with timeout
   - Logs the warning to `ato_session_log` as a WARN event (event=`sell_warn`)
   - Calls `j.notifier.Notify(ctx, text)` if non-empty result

---

### Changes to `openai_ai.go`

Add `WarnSellPressure(ctx, rec) (string, error)`:
- Marshal `rec` to JSON as the user message
- Use `sellWarnSystemPrompt` as system message
- Same `chat.Completions.New()` call pattern
- Return raw string content

---

## Files to modify

| File | Change |
|---|---|
| `internal/interface/output/ai.go` | Add `SummarizeSession` and `WarnSellPressure` to the `AI` interface; add `SessionSummaryRecord`, `SymbolSessionOutcome`, `SellWarnRecord` structs |
| `internal/job/ato_monitor.go` | Extend `sessionState`, add `warnFired`/`warnCount` maps, add sell warn check in poll loop, add `postSessionSummary` and `postSellWarn` methods, call summary at session end |
| `internal/modules/openai_ai.go` | Implement `SummarizeSession` and `WarnSellPressure` with their system prompts |
| `internal/config/config.go` | Add `SellWarnRatio` and `SellWarnStabilityCount` to `SignalConfig` |
| `config.yaml` + `config.example.yaml` | Add `sell_warn_ratio: 0.50` and `sell_warn_stability_count: 2` |

---

## What does NOT change

- No new DB tables. Sell warnings log to the existing `ato_session_log` as WARN events. Session summaries are ephemeral (Telegram only).
- No changes to `TelegramBot`, `TelegramNotifier`, or `Notifier` interface — both new features use the existing `notifier.Notify()` path, which already broadcasts to all subscribers.
- No changes to `calculator/ato.go` — `ProcessSnapshot` already returns `ImbalanceRatio` in `SnapshotAnalysis`.
- No changes to any buy-signal logic.

---

## Implementation order

1. Add structs and interface methods to `ai.go` (no logic, just contracts)
2. Add config fields to `config.go` + YAML files
3. Implement `SummarizeSession` + `WarnSellPressure` in `openai_ai.go`
4. Wire sell warn into poll loop in `ato_monitor.go`
5. Wire session summary into end of `Run()` in `ato_monitor.go`
6. Extend `sessionState` to track `peakRatio` and `lastGateBlock`
