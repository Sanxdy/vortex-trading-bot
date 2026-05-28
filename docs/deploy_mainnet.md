# Mainnet Deployment Guide

## Step-by-Step

| Step | Action | Config Change | Verification | Risk |
|------|--------|---------------|-------------|------|
| **1** | Disable `force_market_entries` | `config.yaml`: `force_market_entries: true → false` | Bot uses limit orders again | Orders may not fill instantly on mainnet (but should fill quickly with real liquidity) |
| **2** | Set mainnet exchange credentials | `.env`: Remove `EXCHANGE_TESTNET=true` or set to `false`. Update `EXCHANGE_API_KEY` and `EXCHANGE_API_SECRET` with mainnet keys | `docker logs` shows connection to `api.binance.com` (not testnet) | 🛑 Keys must be **read-only + trading only** — withdraw disabled |
| **3** | Reduce position size for safety | `config.yaml`: `risk_percent: 1.0 → 0.5` (under `profiles.sideway.strategy.trend`) | Position notional stays under $10-20 on $50 capital | Smaller losses while validating |
| **4** | Reset daily loss limit | Redis: `SET vortex:max_daily_loss 10` (or `DEL` to use config `max_daily_loss_percent: 30`) | Bot kills at $10 loss instead of $50 | Tight limit prevents runaway losses |
| **5** | Set initial balance | Redis: `SET vortex:balance:initial 50`. `.env`: `SIMULATED_BALANCE=50` | Bot shows `Balance: $50` at startup | Matches your actual deposit |
| **6** | Fund the wallet | Send $50 USDT to mainnet Binance spot wallet | `docker logs` shows available USDT | 🛑 Only send what you can lose |
| **7** | Deploy code + restart | `docker compose up -d` | Monitor logs for first 30 min | Rollback: `git revert <last_commit>` |
| **8** | Watch first 10 trades | Manual log review | Verify TP/SL hit correctly, daily loss not exceeded | If losses mount, stop and analyze |
| **9** | Re-enable limit orders | Already done in Step 1 | Monitor fill rates | If fills are slow, consider market orders for active pairs |
| **10** | Scale up if profitable | `risk_percent: 0.5 → 1.0`, increase deposit | Track WR vs backtest (63.6%) | Only after 50+ trades with positive expectancy |

---

## Critical Pre-Flight Checklist

Before connecting to mainnet:

```
□ Exchange API key has WITHDRAW DISABLED (Binance API management page)
□ Exchange API key has SPOT TRADING enabled only (no futures, no margin)
□ IP whitelist set (optional but recommended)
□ Test with minimum order ($10 notional) before full size
□ Telegram bot connected and kill switch works (/kill command)
□ Daily loss limit tested (set low, trigger manually to verify)
```

---

## Rollback Plan

| Issue | Action |
|-------|--------|
| Bot loses $10+ in first hour | `DEL vortex:max_daily_loss` — raises limit; analyze strategy |
| Orders not filling | Switch to `force_market_entries: true` temporarily |
| API key compromised | Revoke immediately on Binance, restart with new key |
| Strategy PnL diverges from backtest | Stop bot, compare live fills vs backtest assumptions, adjust |
| Any unexpected behavior | `git revert HEAD` and go back to testnet |
