# Phase 2 Follow-up: Trailing Stop Activation Threshold

**Created:** 2026-05-19
**Follow-up date:** 2026-05-26 (or later, after Phase 1 has been observed for 3–7 days)

## Context

Phase 1 fixed the "Locked -$0.01" display bug (frontend-only, no trade impact).
Phase 2 is a behavioral change that requires observation and paper-trading first.

## What Phase 2 Does

Add a configurable trailing activation threshold so the stop doesn't start
tightening immediately from tick 1. This prevents premature exits due to
normal crypto noise/breathing.

## Config to Add

```yaml
risk:
  trailing:
    activation_profit_pct: 0.2     # price must move +0.2% before trailing arms
    move_to_breakeven_pct: 0.15     # once armed, first stop move = entry + fees
    include_fees_in_be: true
```

## Backend Changes Needed

### 1. `executor.py` — `trail_trend_position()` (~line 1697)

Add a `_trail_armed` flag to `GridState`. Gate the trailing ratchet:

```python
if not state._trail_armed:
    if (price - state.trend_entry_price) / state.trend_entry_price >= self.trail_activation_pct:
        state._trail_armed = True
        # Move stop to fee-aware break-even
        state.trend_stop = max(state.trend_stop, state.true_break_even_price)
```

### 2. `GridState.__init__` (~line 106)

Add new fields:
```python
self._trail_armed = False
self.true_break_even_price = 0.0
```

### 3. `enter_trend_position()` — after order placed

Compute and store true break-even:
```python
entry_fee = entry_price * size * config["fees"]["maker"]
exit_fee = entry_price * size * config["fees"]["taker"]
state.true_break_even_price = entry_price + (entry_fee + exit_fee) / size
```

## What to Observe Before Deploying Phase 2

Run Phase 1 for at least 3–7 days and track:

- [ ] Average winner size vs before
- [ ] Number of premature exits (was price briefly above entry but stopped out?)
- [ ] Stop distance evolution (does the stop get tighter or wider?)
- [ ] BE activation frequency (how often does the stop reach break-even?)
- [ ] Continuation trade survival rate (are we holding winners longer?)
- [ ] Any new "death by noise" patterns?

## Rollback

Phase 2 is config-driven. Set `activation_profit_pct: 0` to restore
immediate-trailing behavior (same as Phase 1 / current).

## Files That Changed in Phase 1

For reference — these were the Phase 1 changes:

| File | Change |
|------|--------|
| `dashboard/static/index.html` | Lines 1176-1178 and 1219-1226: replaced "Locked" with "At Risk" when stop below break-even |
