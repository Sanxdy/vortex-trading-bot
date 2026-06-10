# Vortex Agent Protocol

This file binds the AI's behaviour when working on this project.
Read it on every session start and before any planning or code change.

## Mandatory Pre-Work

Before making ANY change that touches:
- Entry/exit logic
- Risk parameters
- Pair selection
- AI overlay
- Execution flow
- Data pipeline (strategist, entry_conditions, indicators)

You MUST:

1. **Read SOP.md** — the full Strategy Change SOP lives there. Follow the
   checklist for every change. Do not skip sections.

2. **Load the `quant-architect` skill** — invoke it before any strategy,
   threshold, or risk change. It provides fee audit, regime gating, and
   edge-case QA.

3. **Check AGENTS.md** (this file) — remember that these instructions
   override your default behaviour.

## Investigation Rules

1. **Never assume "market conditions" without proof.** When trades are
   silent for 2+ hours, follow the Trade Silence Investigation Protocol
   (SOP section 24) step by step. Start with Docker logs, then DB queries.
   Do NOT skip to strategy tuning.

2. **Trace the data pipeline before touching strategy.** Check exchange
   connectivity → data ingestion → indicator computation → entry conditions
   → preflight → execution. Each layer must be proven healthy before moving
   up.

3. **Check for silent failures first.** numpy type leaks, DB connection
   poisoning, WebSocket timeouts — these produce no ERROR-level logs but
   silently kill entries. Always search for `DB log_decision error`,
   `DB reconnecting`, and `invalid_entry: price=0` in the bot logs.

4. **Search before writing any code.** For every change, before editing:
   - Search for similar implementations in the codebase
   - Search for related tests
   - Find existing abstractions and reuse them
   - Confirm there is no utility that already does what you need
   - Only write code after you have confirmed the pattern does not exist

## Feature Size Gating

Before starting, estimate the change size:

- **Small (<50 lines, 1-2 files)** — Standard SOP workflow. Implement directly.
- **Medium (50-200 lines, 2-4 files)** — Write a brief design doc in the PR
  body before implementing. Show the user before coding.
- **Large (>200 lines, >4 files)** — Four-phase process:
  1. **Investigate** — Search codebase, trace data flow, read all affected files
  2. **Design** — Write design doc with before/after, affected files, risks.
     **Wait for user approval before implementing.**
  3. **Implement** — Execute the approved design, minimal changes
  4. **Validate** — Run tests, lint, typecheck, re-run validation

## Subagent Delegation

When a task involves multiple concerns, delegate to specialized agents:

| Role | Responsibility |
|------|---------------|
| **Architect** | Design and planning only. Never edits files. |
| **Reviewer** | Reviews code after implementation. Catches bugs, edge cases, security flaws. |
| **Tester** | Generates and runs tests independently. |

Use the `task` tool to dispatch subagents. Do not implement and review
in the same pass — always delegate review to a separate agent.

## SOP Change Protocol

1. Propose the change to the user with a clear "before/after" plan.
2. Wait for approval.
3. Implement.
4. Validate (lint, typecheck, tests). Fix failures. Re-run.
5. Verify no regressions — check that existing functionality still works.
6. Deploy with transparent before/after evidence.
7. Update `SOP.md` with any new failure modes discovered.
8. Archive the completed checklist in `docs/sop_history/`.

### Completion Checklist

Before declaring any change done:

- [ ] Code compiles / passes syntax check
- [ ] Lint passes (no errors)
- [ ] Type checks pass
- [ ] Tests pass
- [ ] Existing patterns followed (no duplicate utilities)
- [ ] Edge cases considered (empty state, error state, race conditions)
- [ ] SOP checklist in commit message (for strategy changes)

Never declare completion without running verification.
"It should work" is not an acceptable answer.

## Skills

Always invoke the `quant-architect` skill for:
- New regime detection logic
- Volatility filters
- Risk multiplier tables
- Fee/slippage audits
- Any change where mathematical correctness matters

## Budget Architecture

**Critical: The bot uses ALLOCATED risk, not exchange wallet balance.**
Never confuse these. The exchange wallet may hold $80k in testnet or real funds,
but the bot only risks what SIMULATED_BALANCE defines.

### How It Works

| Concept | Value | Meaning |
|---------|-------|---------|
| `SIMULATED_BALANCE` | $250 | Total risk the user commits |
| Reserve (20%) | $50 | Safety buffer — never deployed |
| Deployable budget | $200 | Split across slots |
| Per slot | $40 | 5 slots × $40 = $200 |
| `budget_remaining` | Varies | Tracks remaining allocation after realized PnL |
| `/refill` command | Resets to $250 | Refills budget when depleted |

### The SINGLE Source of Truth for Balance

`budget_remaining` in Redis is the ONLY number that matters for allocation.
- Dashboard reads `budget_remaining` — that's the displayed balance
- It is NOT the exchange wallet — it's the remaining allocated budget
- User runs `/refill` via Telegram to reset it to $250
- `/refill` is the EXPECTED way to restore budget — not a workaround

### Common Mistakes to Avoid

- ❌ Do NOT check `balance:current` or any other Redis key for allocation status
- ❌ Do NOT say "the balance is $X" based on exchange wallet API calls
- ❌ Do NOT treat `budget_remaining` discrepancy as a bug — the user may have refilled
- ✅ Always check `budget_remaining` via the `_rk("budget_remaining", exchange)` pattern
- ✅ When discussing balance, always clarify: "remaining allocated budget" vs "exchange wallet"

### How Losses Hit budget_remaining

Realized PnL from trades is deducted from `budget_remaining` in the executor's
exit paths. The flow:
1. Trade closes with realized PnL
2. If PnL < 0, executor reads current `budget_remaining`
3. Deducts: `new_remaining = max(0, budget_remaining + pnl)`
4. Writes back to Redis

The deduction path covers grid sells, trend exits, and stop-losses.
If `budget_remaining` seems off, check if the user ran `/refill` recently.

## Performance Analysis Protocol

When asked to analyze bot performance, follow this exact sequence.

### Pre-Flight Validation

```sql
SELECT COUNT(*) FROM trades;
SELECT exchange, COUNT(*) FROM trades GROUP BY exchange;
SELECT status, COUNT(*) FROM trades GROUP BY status;
SELECT COUNT(*) FROM trades WHERE realized_pnl IS NOT NULL;
SELECT COUNT(*) FROM trades WHERE fee_cost IS NULL;
```

If `realized_pnl` is null for most rows, check open orders first.

### Summary Metrics (both bots)

```sql
SELECT exchange, COUNT(*), ROUND(SUM(realized_pnl)::numeric,2) as net_pnl,
  ROUND(AVG(realized_pnl)::numeric,4) as avg_pnl,
  ROUND(COUNT(*) FILTER (WHERE realized_pnl > 0)::numeric / COUNT(*) * 100, 1) as wr_pct,
  ROUND(SUM(fee_cost)::numeric,2) as fees
FROM trades WHERE realized_pnl IS NOT NULL GROUP BY exchange;
```

### Q1 — PnL by Strategy

Match trades to decisions via timestamp proximity (within 1 hour):

```sql
WITH trade_strategy AS (
  SELECT DISTINCT ON (t.id)
    t.id, t.realized_pnl, t.fee_cost, t.pair,
    COALESCE(NULLIF(REGEXP_REPLACE(d.reason, '_placed$', ''), ''), d.decision) as strategy
  FROM trades t
  LEFT JOIN trade_decisions d
    ON d.symbol = t.pair AND t.timestamp > d.timestamp
    AND t.timestamp < d.timestamp + INTERVAL '1 hour'
    AND d.exchange = t.exchange
  WHERE t.realized_pnl IS NOT NULL AND t.exchange = '<target>'
  ORDER BY t.id, d.timestamp DESC
)
SELECT strategy, COUNT(*) as trades,
  ROUND(COUNT(*) FILTER (WHERE realized_pnl > 0)::numeric / COUNT(*) * 100, 1) as wr_pct,
  ROUND(AVG(realized_pnl)::numeric,4) as avg_pnl,
  ROUND(SUM(realized_pnl)::numeric,2) as net_pnl,
  ROUND(SUM(realized_pnl)::numeric / NULLIF(SUM(ABS(realized_pnl)) FILTER (WHERE realized_pnl < 0), 0) * -1, 2) as profit_factor,
  ROUND(SUM(fee_cost)::numeric,2) as fees
FROM trade_strategy GROUP BY strategy ORDER BY net_pnl;
```

### Q2 — PnL by Pair

```sql
SELECT pair, COUNT(*) as trades,
  ROUND(SUM(realized_pnl)::numeric, 2) as net_pnl,
  ROUND(AVG(realized_pnl)::numeric, 4) as avg_pnl,
  ROUND(COUNT(*) FILTER (WHERE realized_pnl > 0)::numeric / COUNT(*) * 100, 1) as wr_pct,
  ROUND(SUM(fee_cost)::numeric, 2) as fees
FROM trades WHERE realized_pnl IS NOT NULL AND exchange = '<target>'
GROUP BY pair ORDER BY net_pnl;
```

### Q3 — Fee Impact

Determine if `realized_pnl` is gross or net (check correlation with fee_cost).
Report: gross PnL vs net PnL (after fees) as fee drag percentage.

### Q4 — PnL by Regime

```sql
SELECT d.regime, COUNT(t.*) as trades,
  ROUND(SUM(t.realized_pnl)::numeric, 2) as net_pnl,
  ROUND(AVG(t.realized_pnl)::numeric, 4) as avg_pnl,
  ROUND(COUNT(*) FILTER (WHERE t.realized_pnl > 0)::numeric / COUNT(*) * 100, 1) as wr_pct
FROM trade_decisions d
JOIN trades t ON t.pair = d.symbol
  AND t.timestamp > d.timestamp AND t.timestamp < d.timestamp + INTERVAL '1 hour'
  AND d.exchange = t.exchange
WHERE t.realized_pnl IS NOT NULL AND d.regime IS NOT NULL
  AND t.exchange = '<target>'
GROUP BY d.regime ORDER BY net_pnl;
```

### Q5 — Position Sizing

```sql
SELECT CASE
  WHEN price * quantity < 10 THEN 'tiny (<$10)'
  WHEN price * quantity < 30 THEN 'small ($10-30)'
  WHEN price * quantity < 50 THEN 'medium ($30-50)'
  ELSE 'large (>$50)'
  END as size_bucket,
  COUNT(*) as trades,
  ROUND(SUM(realized_pnl)::numeric, 2) as net_pnl,
  ROUND(AVG(realized_pnl)::numeric, 4) as avg_pnl,
  ROUND(COUNT(*) FILTER (WHERE realized_pnl > 0)::numeric / COUNT(*) * 100, 1) as wr_pct
FROM trades WHERE realized_pnl IS NOT NULL AND exchange = '<target>'
GROUP BY size_bucket ORDER BY MIN(price * quantity);
```

### Q6 — Entry Paths (Futures-specific)

```sql
SELECT
  CASE
    WHEN reason LIKE '%exhaustion%' THEN 'short_exhaustion'
    WHEN reason LIKE '%trend_short%' THEN 'short_signal'
    WHEN reason LIKE '%mean_reversion%' THEN 'short_mr'
    WHEN reason LIKE '%breakout%' THEN 'short_breakout'
    WHEN reason LIKE '%grid%' THEN 'short_grid'
    WHEN reason LIKE '%scaling%' THEN 'short_scaling'
    ELSE reason
  END as path,
  COUNT(*) as trades,
  ROUND(COUNT(*) FILTER (WHERE decision LIKE 'ENTER_TREND_PLACED')::numeric / COUNT(*) * 100, 1) as accept_rate,
  ROUND(AVG(adx)::numeric, 1) as avg_adx,
  ROUND(AVG(rsi)::numeric, 1) as avg_rsi
FROM trade_decisions
WHERE exchange = 'futures' AND decision IN ('ENTER_TREND_ATTEMPT','ENTER_TREND_PLACED')
GROUP BY path ORDER BY COUNT(*) DESC;
```

### Deliverable Format

```
=== PERFORMANCE ANALYSIS ===

Top 3 reasons the bot is losing/making money:
1.
2.
3.

Most profitable pair:     [pair]  [PnL]
Least profitable pair:    [pair]  [PnL]
Most profitable regime:   [regime]
Least profitable regime:  [regime]
Fee impact:               [% drag]

Strategy    Trades  WR%    PF    NetPnL   Fees
...
Pair        Trades  WR%    NetPnL
...

Evidence strength: High / Medium / Low
```

### Success Criteria

| Metric | Threshold |
|--------|-----------|
| Profit Factor | >1.2 |
| Max Drawdown | <20% |
| Trades | >200 |
| Outperform Random | >20% |
| Outperform BTC | >10% |

## Pre-Deploy Gate — MANDATORY

**I MUST run this before ANY deployment to the server.**
No exception. Skipping this is a SOP violation.

### Required Steps

```bash
# Step 1: Run the pre-deploy check script
./scripts/pre_deploy_check.sh
```

The script checks:
1. **Working tree clean** — no uncommitted changes
2. **Syntax passes** — all changed Python files compile
3. **SOP checklist** — commit message contains `[x]` items
4. **Docker builds** — changed images build without error
5. **Baseline snapshot** — capture current API state before deploy

**If the script fails at any step, I must stop and fix before deploying.**

### Baseline Snapshot (for data pipeline changes)

Before deploying changes that touch data acquisition, indicators, or entry conditions:

```bash
# Capture current conditions for 3 representative pairs
curl -s http://100.84.188.57:8000/futures/api/conditions | python3 -c "
import sys,json
d=json.load(sys.stdin)
for k in list(d.keys())[:3]:
    if k not in ['_meta','_stats']:
        print(k,': ADX=',d[k].get('adx'),'RSI=',d[k].get('rsi'))
"

# Capture current PnL
curl -s http://100.84.188.57:8000/futures/api/pnl/summary

# Capture current positions
curl -s http://100.84.188.57:8000/futures/api/orders/active
```

After deploy, re-run the same three commands and compare. If any value regressed, roll back.

### Deploy Command

After pre-deploy check passes and baseline is captured:

```bash
./scripts/pre_deploy_check.sh deploy    # prompts user for confirmation
ssh -i ~/.ssh/vortex root@100.84.188.57 "cd /root/vortex && git pull && docker compose build <service> && docker compose up -d <service>"
# Re-run baseline queries to verify
```
