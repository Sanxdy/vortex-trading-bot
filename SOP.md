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
