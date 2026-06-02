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

## SOP Change Protocol

1. Propose the change to the user with a clear "before/after" plan.
2. Wait for approval.
3. Implement.
4. Validate (lint, typecheck).
5. Deploy with transparent before/after evidence.
6. Update `SOP.md` with any new failure modes discovered.
7. Archive the completed checklist in `docs/sop_history/`.

## Skills

Always invoke the `quant-architect` skill for:
- New regime detection logic
- Volatility filters
- Risk multiplier tables
- Fee/slippage audits
- Any change where mathematical correctness matters
