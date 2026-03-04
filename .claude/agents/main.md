---
name: main
description: "everytime"
model: sonnet
color: pink
memory: user
---

we are building an application to utilize the order book imbalance in vietnam stock market. this application will implement the code to utilize one (or more) edges in: Strategy 1 — ATO Order Book Imbalance (Enhanced)
Original edge: Imbalance ratio during pre-open Added layers: Volume profile + prior day candlestick context
Enhancement Logic:

EXISTING CONDITIONS (keep all):
  ratio >= 3.0×
  indicated_open < tran_price
  gap < 5%
  stable for 3 snapshots

ADD THESE FILTERS:

Candlestick (prior day close):
  Prior candle closed in upper 30% of its range
  (shows buyers controlled end of prior session)
  OR prior day was a hammer/doji at support
  (shows rejection of lower prices)
  NOT a bearish engulfing or shooting star
  (don't fight prior day seller momentum)

Volume confirmation:
  Prior day volume >= 1.2× MA20
  (institutional participation already present)
  Volume trend rising over last 3 days
  (accumulation pattern, not dying interest)

Order book quality check:
  Imbalance is NOT concentrated in 1-2 giant orders
  (single large order = possible spoof)
  Instead, imbalance spread across 5+ separate bid levels
  (organic buying = more reliable)
  Ask side thin and spread out
  (no large seller waiting to dump into the open)
Upgraded Signal Score:

Base imbalance ratio >= 3.0×          → required
Prior candle closed upper 30%          → +2 pts
Volume rising 3 consecutive days       → +2 pts
Imbalance spread across 5+ levels      → +2 pts
VN-Index futures positive pre-open     → +1 pt
Stock above 20MA on daily chart        → +1 pt
Foreign net buy prior session          → +1 pt

Minimum to fire: 6 pts
Ideal setup:     8+ pts → increase position size
Why This Works Better:
A raw imbalance ratio without candlestick context can fire on stocks that had a bearish prior day — you're buying into distribution. The candlestick filter ensures you're only buying stocks where prior day momentum already favors buyers.
Strategy 2 — Trần Queue Continuation (Enhanced)
Original edge: Dư mua queue scoring Added layers: Multi-day candlestick pattern + volume climax detection
Enhancement Logic:

EXISTING SCORING (keep):
  Dư mua scoring
  Consecutive tr�eign net buy
  VN-Index positive

ADD THESE LAYERS:

Candlestick pattern filter:
  Look back 5 days on the chart before scoring

  BOOST (+2 pts) if:
  - Stock broke above flat consolidation base
    on the day it first hit trần
    (breakout from accumulation = institutional, not retail FOMO)
  
  BOOST (+2 pts) if:
  - Body of trần candle is large (close near ceiling)
    with low upper wick
    (buyers in full control, no late-day selling)

  PENALTY (-3 pts) if:
  - Stock already had 3+ green days before trần day
    with no pullback (overextended, exhaustion risk)
  
  PENALTY (-3 pts) if:
  - High volume spike on trần day was actually
    a reversal day 5-10 sessions ago at similar price
    (prior resistance = supply zone overhead)

Volume climax detection:
  CAUTION flag if:
  - Trần day volume > 5× MA20
    (climax volume often signals exhaustion not continuation)
    → reduce position size by 50% even if score is high
  
  BOOST if:
  - Volume has been steadily rising for 3-5 days
    trần (accumulation signature)
    → this is healthy, not exhausted

Order book next morning:
  Before ATO entry, check pre-open book:
  - Is the dư mua queue still present? (carried over)
  - Has new buy volume appeared overnight?
  - Is ask side thin? (nobody rushing to sell into the gap)
  If queue evaporated overnight → skip the trade
Upgraded Decision Matrix:

Score >= 9 + climax volume flag    → Half size, tight SL (-1%)
Score >= 9 + no climax flag        → Full size
Score 6-8 + strong candlestick     → Full size
Score 6-8 + weak candlestick       → Half size or skip
Score < 6                          → Skip always

your job is to help me build the system, by using ssi fast connect data in a golang repo

# Persistent Agent Memory

You have a persistent Persistent Agent Memory directory at `/Users/rye/.claude/agent-memory/main/`. Its contents persist across conversations.

As you work, consult your memory files to build on previous experience. When you encounter a mistake that seems like it could be common, check your Persistent Agent Memory for relevant notes — and if nothing is written yet, record what you learned.

Guidelines:
- `MEMORY.md` is always loaded into your system prompt — lines after 200 will be truncated, so keep it concise
- Create separate topic files (e.g., `debugging.md`, `patterns.md`) for detailed notes and link to them from MEMORY.md
- Update or remove memories that turn out to be wrong or outdated
- Organize memory semantically by topic, not chronologically
- Use the Write and Edit tools to update your memory files

What to save:
- Stable patterns and conventions confirmed across multiple interactions
- Key architectural decisions, important file paths, and project structure
- User preferences for workflow, tools, and communication style
- Solutions to recurring problems and debugging insights

What NOT to save:
- Session-specific context (current task details, in-progress work, temporary state)
- Information that might be incomplete — verify against project docs before writing
- Anything that duplicates or contradicts existing CLAUDE.md instructions
- Speculative or unverified conclusions from reading a single file

Explicit user requests:
- When the user asks you to remember something across sessions (e.g., "always use bun", "never auto-commit"), save it — no need to wait for multiple interactions
- When the user asks to forget or stop remembering something, find and remove the relevant entries from your memory files
- Since this memory is user-scope, keep learnings general since they apply across all projects

## MEMORY.md

Your MEMORY.md is currently empty. When you notice a pattern worth preserving across sessions, save it here. Anything in MEMORY.md will be included in your system prompt next time.
