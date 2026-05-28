# Vortex Strategy Change SOP

## When This Applies

Any pull request, commit, or config change that touches:

- Entry thresholds (RSI, ADX, ct_score, EMA periods, Bollinger Bands)
- Exit logic (trailing stop multiples, time limits)
- Risk modifiers (size multipliers, panic filter, daily loss limit)
- Pair selection (PILOT_PAIRS, watchlist conditions, blacklist)
- AI overlay integration (analyst, news filter)
- Execution flow (async loops, slot allocation, order handling)

## Mandatory Review Checklist

Before merging, the change must satisfy the following eight checks.
Mark each item with `[x]` after it has been verified.

### 1. Strategy Correctness
- [ ] What market regime does this target? (trending / sideways / panic)
- [ ] How does it interact with existing entry paths (pullback, breakout, countertrend, grid)?
- [ ] Is there backtest or paper-trade evidence supporting the change?
- [ ] Does it avoid creating contradictory signals (e.g., breakout and mean-reversion in the same regime)?
- [ ] For complex quantitative changes (new regime detection, volatility filters, risk multiplier tables), invoke the `quant-architect` skill for deep review.

### 2. Execution Safety
- [ ] Will this change introduce a race condition or deadlock in the async event loop?
- [ ] Are order cancellation and slot release handled correctly for the new path?
- [ ] Can it cause an `entry_too_small` or `SKIP` loop due to stacked size multipliers?
- [ ] Does it respect the minimum order size for the exchange (e.g., Binance $10 notional)?

### 3. Risk Implications
- [ ] What is the worst-case loss if this change fires 5 consecutive trades?
- [ ] Does it respect the daily loss limit (currently 5% of account)?
- [ ] Does it conflict with the panic filter? Will the panic filter still override when needed?
- [ ] Is position sizing compatible with the current account balance and slot budget?

### 4. Observability Impact
- [ ] Will the decision log clearly show why a trade was allowed or blocked?
- [ ] Are new log messages added for every new entry/exit path?
- [ ] Can the dashboard or `/why` command explain the change to the user?
- [ ] Do the new log messages use existing status keywords where appropriate?

### 5. Rollback Simplicity
- [ ] Can this change be disabled with a single config flag or mode toggle?
- [ ] Is the previous behaviour preserved for A/B comparison?
- [ ] If the change is in code, can it be reverted with a single `git revert`?
- [ ] Does the `TradingMode` system (technical_only / ai_observe_only / technical_plus_ai) still function correctly after the change?

### 6. Statistical Attribution
- [ ] Can the effect of this change be isolated from other active changes?
- [ ] Are multiple thresholds being modified simultaneously?
- [ ] Can we measure whether this specific change improved outcomes?

### 7. Failure Mode Analysis
- [ ] What happens during extreme volatility?
- [ ] What happens if the exchange API partially fails?
- [ ] What happens if AI services timeout or return malformed data?
- [ ] Does the system fail safely to CASH?

### 8. Trade Frequency Impact
- [ ] Is this expected to increase or decrease trade frequency?
- [ ] Could this unintentionally suppress all entries?
- [ ] Are minimum order sizes still reachable after sizing modifiers?

## Evidence-First Rule

No strategy change may be claimed to be an improvement without at least one of:

- Observable log data from a live or testnet run (minimum sample threshold: 20+ candidate signals or 10+ executed trades)
- A measurable improvement in backtest metrics (win rate, average R, max drawdown)
- A clear, logical argument that fills a proven gap in the current regime coverage

Speculative optimisation without evidence is rejected.

## Minimal Change Bias

Prefer the smallest possible modification that:

- Preserves observability (you can still see what happened)
- Preserves causal attribution (you can still explain why)
- Preserves rollback simplicity (you can undo it in seconds)

Avoid introducing opaque heuristics, unmeasurable confidence scores, or unverifiable AI reasoning.

## Integration with Trading Modes

All changes must be compatible with the three execution modes:

| Mode | AI Allowed to Affect Trading? |
|------|-------------------------------|
| `TECHNICAL_ONLY` | No |
| `AI_OBSERVE_ONLY` | No (AI runs but cannot block/resize) |
| `TECHNICAL_PLUS_AI` | Yes |

A change that only works in one mode must be clearly labelled and gated behind that mode.

## Review Cadence

- Every pull request that triggers this SOP must include the completed checklist in the PR description.
- Once per week, review the comparative stats collected from each active mode (trade count, win rate, average R, max drawdown).
- Archive old SOP checklists in `docs/sop_history/` for future reference.

## Example: A Typical PR Description

```
## What
Lower breakout RSI threshold from 60 → 50.

## Regime
Trending (ADX > 30), price above 50-EMA.

## Evidence
Testnet run with 23 candidate signals: 12 breakout entries, 58% win rate, +$1.20 net.

## Checklist
- [x] Strategy Correctness — breakout path only, no conflict with pullback
- [x] Execution Safety — no new race conditions
- [x] Risk Implications — worst 5-loss streak = -$1.50, well within daily limit
- [x] Observability — added "breakout_rsi_50" log message
- [x] Rollback — can be disabled by setting `allow_breakout: false`
- [x] Statistical Attribution — only RSI threshold changed, no other variables
- [x] Failure Mode Analysis — panic filter overrides regardless of RSI threshold
- [x] Trade Frequency Impact — expected to add ~2 more entries/day in trending regimes
```

---

## 9. Fee & Slippage Audit

Before deploying:
- [ ] Does the expected move survive round-trip fees?
- [ ] Does the expected edge survive spread + slippage?
- [ ] Is the expected profit meaningfully larger than execution costs?
- [ ] Does the strategy still work during wider spreads?

Required calculations:
- gross expectancy
- net expectancy after fees
- worst-case slippage estimate

Reject changes where execution costs dominate the expected edge.

---

## 10. Execution Quality Audit

Before changing indicators or thresholds:
- [ ] Is fill rate acceptable?
- [ ] Are pending orders timing out excessively?
- [ ] Are profitable moves occurring after cancellations?
- [ ] Is slippage worse than modeled?

Required metrics:
- fill %
- pending timeout %
- missed-move %
- average entry deviation
- average hold duration

Execution-layer problems must be ruled out before changing strategy logic.

---

## 11. Trade Clustering Analysis

Before increasing trade frequency:
- [ ] Are multiple losses occurring within the same market structure?
- [ ] Are entries excessively correlated?
- [ ] Does the pair repeatedly re-enter without regime reset?
- [ ] Would anti-churn controls improve survivability?

Examples of clustering:
- 4 BTC continuations within 2 hours
- repeated re-entry after failed breakout
- multiple pairs driven by same BTC move

If clustering exists:
- prioritize anti-churn controls before diversification.

---

## 12. Pair Diversification Validation

Adding more pairs requires evidence that:
- [ ] Existing edge is positive or near-neutral
- [ ] New pairs behave differently enough from BTC correlation
- [ ] Slot allocation remains effective
- [ ] Added pairs increase opportunity quality, not just quantity

Do NOT add pairs solely to increase trade count.

---

## 13. Async Snapshot Consistency

For all async decision paths:
- [ ] Indicators are snapshotted before branching
- [ ] Logs use snapshot values
- [ ] Mutable state is not re-read after awaits
- [ ] Decision evidence remains historically accurate

This prevents:
- misleading logs
- impossible indicator combinations
- race-condition diagnostics

---

## 14. Pending Order Diagnostics

Every pending timeout path must log:
- [ ] entry price
- [ ] waited duration
- [ ] highest/lowest move after cancel
- [ ] spread at entry
- [ ] entry type (continuation/breakout/countertrend)

Goal:
Differentiate:
- poor execution
- lucky miss
- invalid signal
- spread rejection

---

## 15. Breakeven & Profit Lock Validation

For trailing-stop modifications:
- [ ] Is breakeven behavior fee-aware?
- [ ] Can profitable trades still display "at risk"?
- [ ] Does the stop ever lock negative PnL unintentionally?
- [ ] Is trail activation delayed appropriately?

Required distinction:
- "At Risk"
- "Breakeven"
- "Locked Profit"

Dashboard terminology must match actual execution behavior.

---

## 16. Regime Participation Audit

Before modifying trade frequency:
- [ ] Is low participation caused by market regime?
- [ ] Is the strategy over-filtered?
- [ ] Are signals present but execution failing?
- [ ] Are enabled pairs actually reaching target regimes?

Low trade count alone is NOT proof of failure.

Must distinguish:
- no opportunities
- blocked opportunities
- execution misses
- statistically correct inactivity

---

## 17. Change Escalation Policy

Preferred escalation order:

1. Diagnostic logging
2. Execution-layer tuning
3. Risk tuning
4. Threshold tuning
5. Pair expansion
6. Strategy redesign

Avoid jumping directly to:
- more pairs
- looser filters
- larger size
- AI overlays

without proving lower layers are healthy first.

---

## 18. Deployment Safety Rule

Any strategic change must be deployed in stages:

Stage 1:
- diagnostics only

Stage 2:
- execution improvements

Stage 3:
- threshold adjustments

Stage 4:
- diversification

Each stage requires:
- log review
- statistical review
- rollback validation

before proceeding to the next stage.

---

## 19. Log Accuracy Rule

All log messages and Telegram notifications must reflect actual execution behavior, not pre-modification state. Every log entry must be attributable to real system state at the time of the action. Misleading logs prevent accurate diagnosis and erode trust in the system.

If a method needs different behavior for different callers, pass parameters — never set-then-override. The method must produce the correct message for the behavior being executed from the start.

Methods must not produce messages describing a state that is immediately invalidated. If a caller requires different stop/target/entry semantics, the method must accept those as parameters and generate the correct message on first output.

---

## 20. Entry Path Isolation Rule

Every entry path must be independently disableable. When adding a new entry strategy, verify that ALL other entry paths are blocked for pairs assigned to the new strategy.

Common missed paths:
- Grid entry (bypasses `entry_paths` if `grid.enabled = true`)
- Countertrend (bypasses if `regime_mode` allows it)
- Manual watchlist enable (bypasses if watchlist adds the pair without strategy gating)

Before deploying a new strategy:
1. Search for ALL code paths that call `enter_trend_position()` or `manage_pair()` entry logic
2. Verify each path respects the `entry_paths` gating
3. Add a belt-and-suspenders guard at each path even if config disables it

---

## 21. Post-Deploy Validation Rule

After ANY data pipeline change (strategist, entry_conditions, data sources, or any code path that feeds values into entry decisions):

**Step 1 — Baseline snapshot.** Before deploying, capture the current live value for 3+ representative pairs at the OUTPUT of the pipeline (where the value is consumed, not where it's computed).

```
Example: rvol at entry_conditions (before fix)
pair rvol: NEAR=0.02, INJ=0.01, FET=0.03
```

**Step 2 — Deploy the fix.**

**Step 3 — Post-deploy comparison.** After deploy, re-check the same OUTPUT. Confirm every relevant pair improved or remained correct. If any pair regressed, roll back immediately.

```
pair rvol: NEAR=0.87, INJ=0.84, FET=0.61
```

**Step 4 — End-to-end trace.** Pick one pair and trace every hop from source to consumption:

```
exchange API → fetch_ohlcv returns volume=8,759,522 ✅
df_entry.iloc[-2]["volume"] = 8,759,522 ✅
check_conditions rvol = 8.7M / 10.0M = 0.87 ✅
Redis vortex:conditions rvol = 0.87 ✅
executor _check_sideway_entry reads rvol=0.87 ✅
```

A change is not verified until Step 4 is documented in the PR.

## 22. Data Pipeline Awareness Rule

Before fixing a data pipeline, verify the ENTIRE chain from source to decision — not just the first hop. A fix at stage 1 (source) is worthless if stage 2 (transform) or stage 3 (publish) still has the original bug.

When tracing a data value:
1. IDENTIFY the source (exchange API, config, Redis key, DB query)
2. TRACE every assignment to the value through all intermediate variables
3. VERIFY the CONSUMER (the function that reads the value to make a decision)
4. TEST with LIVE data, not just backtest data — the live pipeline often differs

Common traps:
- Backfill uses `fetch_ohlcv(limit=1000)` but live polling uses `fetch_ohlcv(limit=5)` — different number of candles changes which index is "last"
- Test code reads from `data_exchange` (mainnet) but live reads from `exchange` (testnet) — different data sources
- Manual test uses a freshly created Strategist, but live reuses one from startup — stale state

## 23. Live Verification Mandate

A backtest proves the strategy logic. A live check proves the pipeline works. Both are required before any deployment that touches data acquisition, indicator calculation, or entry condition evaluation.

**Live check requirements:**
- Run the EXACT code path that executes in production (not a simplified test)
- Inspect the value at the FINAL decision point (not an intermediate calculation)
- Use real-time exchange data (not cached or backfilled data)
- Document the before/after values in the PR description

**Applies to:**

| Change Type | Required Verification |
|------------|---------------------|
| Data source URL or API | Fetch a live candle and confirm OHLCV matches exchange ticker |
| Indicator calculation | Compute from live df, compare with manual calculation |
| Entry/exit logic | Fire a candidate signal in testnet and verify the decision log |
| Risk parameter | Trigger the guard and confirm the bot stops/limits correctly |
| Publish/display | Check Redis key directly, compare with dashboard rendering |

A deployment that passes backtest but fails live verification must be rolled back and the gap documented before re-deploy.
