# SOP Archive - Watchlist Expectancy Shortlist

Date: 2026-06-04

## What Changed

- Added `WatchlistMonitor.get_expectancy_candidates()` to rank watched pairs by recent live expectancy.
- Added Telegram commands:
  - `/wl_candidates`
  - `/wl_promote`
- Documented the workflow in `README.md`.

## Live Evidence

Server query over the last 14 days showed the best non-active watchlist candidates were still negative, but materially better than the rest of the book:

- `FET/USDT`
- `TAO/USDT`
- `INJ/USDT`
- `OP/USDT`
- `FIL/USDT`
- `SUI/USDT`
- `TON/USDT`
- `NEAR/USDT`

The live watchlist status was still `watching` for all pairs at the time of the scan, so no pair was auto-promoted.

## Checklist

- [x] Strategy Correctness - This targets pair selection and staging, not entry thresholds or exit logic.
- [x] Execution Safety - Read-only ranking path; promotion requires explicit command.
- [x] Risk Implications - No sizing or risk parameters changed.
- [x] Observability Impact - Candidate ranking is visible in Telegram and can be explained from logs/data.
- [x] Rollback Simplicity - Remove the two Telegram commands and helper method to revert.
- [x] Statistical Attribution - Change is isolated to candidate staging and promotion.
- [x] Failure Mode Analysis - If DB ranking fails, commands return no candidates and do not alter trading state.
- [x] Trade Frequency Impact - No automatic increase in trades; pairs only move on explicit promotion.

