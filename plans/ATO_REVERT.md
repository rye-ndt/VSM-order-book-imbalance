# ATO Revert Plan

## When to use this plan

When Vietnam's stock exchange (HOSE/HNX) migrates to T+0 settlement under the KRX system, intraday exits become possible. The ATO signal system's edge — buying at the open, exiting same-session — becomes viable again. This plan describes how to reactivate it.

---

## What "revert" means

The ATO infrastructure was never modified during the swing pivot. Every file is intact. The revert is a config change, not a code change.

---

## Step 1: Config change

In `config.yaml`, change `signal_mode`:

```yaml
# From:
signal_mode: swing

# To:
signal_mode: ato
```

Restart the process. The ATO monitor is live again.

To run both signal modes simultaneously during a transition period (recommended for 2–4 weeks):

```yaml
signal_mode: both
```

This generates both EOD swing signals (04:00 ICT) and live ATO signals (09:00–09:15 ICT) simultaneously, letting you compare signal quality across both modes with live data before fully committing.

---

## Step 2: Verify startup behavior

After restart, confirm in logs:

- `[startup] SSI IDS: OK` — WebSocket client is constructed and pinged
- `[ato] market regime: ...` — ATOMonitorJob loaded regime successfully
- `[ato] monitoring N stocks until 09:15 ICT` — watchlist loaded, session running
- No `[startup] SSI IDS ping failed` errors

If the IDS ping fails, check:
1. SSI FastConnectData credentials in config (`consumer_id`, `consumer_secret`)
2. `stream_url` in config is still correct (SSI may have updated their endpoint)
3. Network connectivity to `fcmarket.ssi.com.vn`

---

## Step 3: Re-tune ATO thresholds for T+0

T+0 changes the risk profile. With same-session exits possible:

**TPRatio** — keep at `1.03` initially. A 3% intraday target is still appropriate. Revisit after 30 signals.

**MaxGapFromRef** — consider raising from `0.05` to `0.07`. A 7% gap-up is less risky when you can exit same-day if the momentum fades.

**Regime score thresholds** — consider lowering all thresholds by 1–2 points in Bull regime. A tighter exit reduces the cost of a bad entry, so slightly lower-quality setups become tradeable.

**SellWarnRatio / SellWarnStabilityCount** — these may become more important at T+0 since you can act on a sell warning intraday. Keep defaults initially.

All of these are config.yaml changes only. No code changes required.

---

## Step 4: Future code work at T+0 (not required for revert)

These are improvements that become valuable at T+0 but are out of scope for the revert itself:

**Intraday SL monitoring:**
The current `ATOMonitorJob` has no continuous-session monitoring. It only watches the ATO window (09:00–09:15). At T+0, adding an `ATCMonitorJob` that watches the afternoon session and fires sell alerts when price drops below a threshold would be the natural extension. This is new code, not a revert item.

**`signal_log.exit_price` column:**
Currently there is no field to record the actual intraday exit price. At T+0, add:
```sql
ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS exit_price FLOAT;
ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS exit_time TIMESTAMPTZ;
ALTER TABLE signal_log ADD COLUMN IF NOT EXISTS exit_reason TEXT;
```
Populate manually or via a future monitoring job.

**AI prompt update:**
The `TelegramInterpret` and `XInterpret` prompts in `openai_ai.go` reference T+2 outcome framing. Update them to reflect T+0 same-session exit context once live.

---

## Data continuity

The database is unaffected by the revert:

- `swing_signal_log` remains intact and continues to be backfilled nightly by `BackfillSwingOutcomes`. No data is lost.
- `signal_log` (ATO history) is intact — every signal fired before the swing pivot is still there.
- `stock_ohlcv`, `stock_metrics`, `market_regime`, `stock_foreign_flow` — unchanged throughout.
- `daily_crawl_status.swing_signal_sent` stays in the schema (harmless if `SwingSignalJob` is not running).

---

## Quick reference: mode behavior

| `signal_mode` | ATOMonitorJob | SwingSignalJob | obClient | WebSocket |
|---|---|---|---|---|
| `"ato"` (default) | active | dormant | pinged | active |
| `"swing"` | dormant | active | skipped | unused |
| `"both"` | active | active | pinged | active |

---

## Rollback from "both" to "ato"

If running in `"both"` mode and ready to fully disable swing:

```yaml
signal_mode: ato
```

Restart. `SwingSignalJob` stops scheduling. Existing `swing_signal_log` data is preserved. No cleanup required.
