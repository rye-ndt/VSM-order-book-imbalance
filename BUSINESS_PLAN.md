# Business Plan — Vietnam ATO Signal Service

## The Problem

Vietnam retail traders enter every ATO session (09:00–09:15 ICT) blind. They see the same SSI board everyone else sees: an indicated price that moves, no context for whether demand is genuine, no systematic way to separate institutional accumulation from noise. A 15-minute window with T+2 settlement and ceiling/floor limits means a bad entry has real, locked-in downside.

The anxiety is real. The information asymmetry is worse.

---

## The Product

A signal service that tells Vietnamese retail traders which stocks have genuine pre-open institutional demand — before the open, every morning.

The core product is a Telegram bot that fires alerts during the ATO window when all 8 signal conditions hold for 3 consecutive order book snapshots:

1. Bid/ask volume ratio ≥ 3.0×
2. No single bid dominates (anti-spoof: largest bid < 40% of total)
3. Bid depth spans ≥ 5 price levels
4. No ask wall within 3% above indicated price
5. Indicated price stable ±1% across snapshots
6. Indicated price below ceiling (trần)
7. Gap from prior close < 5%
8. FinalScore ≥ regime threshold (regime-adjusted nightly)

Every signal is already pre-screened the night before: only stocks passing CPR, momentum, volume, and candle pattern filters make the morning watchlist.

---

## Differentiation

### What we show that SSI doesn't

| Signal Component | What it means |
|---|---|
| Imbalance ratio (number) | SSI shows the order book visually; we give you the ratio |
| Anti-spoof filter | Whether the bid depth is genuine or a single large fake order |
| Ask wall detection | Hidden supply sitting just above the indicated price |
| FinalScore | Composite of 10 nightly factors — no equivalent on SSI board |
| Market regime | Bull/Bear/Choppy classification driving position-size guidance |
| CPR, VPR, Momentum | Derived metrics not displayed anywhere on SSI |

### What SSI shows that we'll add

- Foreign ownership room (%) — required for sizing decisions on restricted stocks
- Corporate events (ex-div, rights issue) — mandatory before public launch to avoid sending signals on dangerous entry days
- Sector/industry label — for relative strength context
- Put-through volume — institutional block trade signal

---

## Current Build Status

| Component | Status |
|---|---|
| Nightly data pipeline | Complete and live-verified |
| Metrics computation (10 factors) | Complete |
| ATO monitor (signal gate) | Coded, not yet live-tested |
| Multi-tenant Telegram bot (subscribe/unsubscribe, /signal command) | Complete |
| signal_log (event study) | Schema live, 0 rows (not yet fired) |
| AI signal interpretation (Interpret, XInterpret, TelegramInterpret) | Complete — wired into ATO job, posts to X and Telegram after each signal |
| Corporate events filter | 0% |
| Foreign ownership room | 0% |
| Subscription/payment | 0% |

**Live data as of 2026-03-16:** 18,673 OHLCV rows, 63,602 foreign flow rows, 1,437 stock_metrics rows, regime = Bear.

---

## Pre-Launch Requirements (Non-Negotiable)

1. **Live-test ATO monitor** — must observe at least one real 09:00–09:15 session firing signals into signal_log before any public release.
2. **Corporate events filter** — never send a signal on an ex-dividend or rights issue day. This is a trust-killer that cannot wait.
3. **Build signal track record** — publish every signal publicly (wins and losses) for at least 2–4 weeks before charging anyone. No track record = no credibility.

---

## Product Tiers

### Tier 1 — Free (Top of funnel)
**Channel:** Public Telegram channel or X/Twitter bot

**Delivers daily:**
- Market regime (Bull / Bear / Choppy) + what it means for sizing
- VN-Index weekly trend summary
- Number of stocks on today's watchlist

**Goal:** Demonstrate the regime signal is useful. Build audience. No subscription required.

### Tier 2 — Watchlist (Entry paid tier)
**~200,000–300,000 VND/month**

**Delivers nightly (after market close):**
- Full watchlist: ticker, FinalScore, PositionSizeFlag (Full/Half), candle pattern, VPR, momentum score
- Regime-adjusted position sizing guidance
- "Why this stock passed screening" in plain language

**For:** Traders who do their own ATO entry but want pre-screened candidates.

### Tier 3 — Live Signal (Core product)
**~500,000–700,000 VND/month**

**Delivers during ATO (09:00–09:14 ICT):**
- Real-time Telegram alert when all 8 conditions fire
- Indicated price, imbalance ratio, position size flag
- Context: FinalScore, regime, candle pattern, resistance distance

**For:** Traders who want to act immediately on ATO signals without doing their own analysis.

---

## Sales Strategy

### Lead with the pain, not the tech

Wrong: "Order book imbalance ratio with anti-spoof detection"
Right: "Know which stocks have real demand before the open — while everyone else is guessing"

The pitch is about the 15-minute window where getting in early on a real move is worth 3–7% against ceiling. Traders already feel this anxiety every morning.

### Trust before revenue

Publish the full signal log publicly — every signal, the indicated price, the outcome at close D0/D1/D2. Include losses. Traders who see transparent track records trust the product more than any marketing copy.

Target 30–60 signals with outcomes before monetizing Tier 3.

### Distribution

1. **X/Twitter** — daily regime + watchlist count. Automated. Zero marginal cost. Attracts traders who search for Vietnam stock content.
2. **Telegram free channel** — regime + market context. Converts to paid via watchlist tease ("5 stocks passed tonight — subscribe for the list").
3. **Facebook groups / forums** — Vietnam retail traders are concentrated in Vndirect/SSI communities. Post signal outcomes weekly.

### Framing by tier question answered

- Tier 1: "Is today a good day to trade?"
- Tier 2: "Which stocks should I watch tomorrow morning?"
- Tier 3: "When exactly should I hit buy?"

---

## Revenue Model

| Tier | Price (VND/mo) | Target subscribers at launch | Monthly revenue |
|---|---|---|---|
| Free | 0 | — | — |
| Watchlist | 250,000 | 50 | 12,500,000 |
| Live Signal | 600,000 | 20 | 12,000,000 |
| **Total** | | | **~24,500,000 (~$950 USD)** |

Break-even is achievable with ~30–40 paid subscribers. Server + SSI API costs are low (single VPS + PostgreSQL). The constraint is trust, not cost.

---

## Risks

### Regulatory
SSI FastConnectData terms likely prohibit redistribution of real-time data. Vietnam SSC may require advisory licensing for investment signal services. This is the highest-impact risk. Options:
- Frame as "market observation tool," not investment advice
- Add standard disclaimer on every signal
- Consult a Vietnamese securities lawyer before public launch

### Technical
- ATO monitor has never fired a real signal — unknown failure modes exist
- WebSocket reconnection during the 09:00–09:15 window is critical; a dropped connection means missed signals
- Signal quality degrades in Bear regime — more false positives expected; communicate this to subscribers

### Trust
- First bad batch of signals will damage reputation disproportionately
- Track record must be established before charging

---

## 90-Day Execution Plan

| Week | Milestone |
|---|---|
| ~~1–2~~ | ~~Build multi-tenant Telegram bot.~~ ✓ Done |
| ~~2–3~~ | ~~Wire AI interpretation into ATO job.~~ ✓ Done |
| 1–2 | Live-test ATO monitor across multiple sessions. Fix any issues. |
| 3 | Add corporate events filter. |
| 4 | Launch free Tier 1 (X bot + Telegram channel). Start publishing signal outcomes publicly. |
| 5–8 | Accumulate track record (30+ signals with D0/D1/D2 outcomes). Build audience. |
| 9 | Add foreign ownership room data. |
| 10 | Add subscription/payment gating (Stripe or VNPay). |
| 11–12 | Launch paid Tier 2 and Tier 3. |
