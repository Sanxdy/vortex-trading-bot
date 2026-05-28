# Vortex — Systematic BB Squeeze + Trend Bounce Trading Bot

Multi-strategy, event-driven trading bot for **Binance spot** running 2 strategies across 22 pairs with regime detection, safety shields, and a real-time dashboard.

## Strategies

| Strategy | Pairs | Timeframe | Entry Logic | TP/SL |
|----------|-------|-----------|-------------|-------|
| **bb_squeeze** (Moderate) | 19 altcoins | 4h | BB expansion + volume > 0.8 + near band | 0.8%/0.4% |
| **trend_bounce** | BTC, ETH, SOL | 4h | Lower BB pullback in uptrend (above 200-EMA) | 0.5%/0.4% |

## Features

- **Market regime detection** — ADX/ATR classifies each pair as sideways / trending / high_vol
- **Breakeven lock** — moves SL to entry +0.1% when price reaches +0.2%
- **SL cooldown** — 5-min wait before SL exit to avoid intra-candle noise
- **Anti-churn** — 2 consecutive losses triggers 45min cooldown per pair
- **Staggered profit-taking** — 50% at +0.6%, 50% at +0.8% (bb_squeeze)
- **Daily loss limit** — configurable absolute or percentage-based kill switch
- **Kill switch** — Telegram `/kill` or dashboard button — cancels all, sells coins, stops
- **Force market entries** — testnet flag for instant order fills
- **Sweep on start** — clears leftover coins from previous sessions
- **Real-time dashboard** — candlestick chart, positions, TP/SL lines, trade history, log download
- **Backtest API** — cached per-pair results updated daily

## Backtest Performance

| Strategy | Period | WR | $/day | Trades |
|----------|--------|:--:|:-----:|:------:|
| bb_squeeze (19 pairs) | 167 days (bull window) | 63.6% | +$6.07 | 4,360 |
| bb_squeeze (19 pairs) | 333 days (bear window) | 17.5% | -$2.00 | 4,600 |
| trend_bounce (BTC/ETH/SOL) | 167 days | ~46% | ~+$0.004 | ~1,500 |

## Active Pairs (22)

- **bb_squeeze**: SUI, DOGE, ADA, NEAR, TON, STX, FIL, ENA, TAO, INJ, IMX, W, JUP, ARB, FET, WIF, ALGO, TIA, OP
- **trend_bounce**: BTC, ETH, SOL

## Quick Start

```bash
git clone <repo-url> && cd vortex
cp .env.example .env
# Edit .env with API keys (see .env.example)
docker compose up -d
```

Dashboard at `http://localhost:8000`

## Architecture

```
Binance (REST + WebSocket)
  → Ingestor (Redis ticker cache)
  → Strategist (indicator calculation + regime classification)
  → Executor (entry logic, position monitor, exit management)
  → TimescaleDB (trades, decisions, balance snapshots)
  → Dashboard (FastAPI + Web UI)
  → Notifier (Telegram alerts + commands)
```

## Key Files

| File | Purpose |
|------|---------|
| `src/executor.py` | Entry logic, _position_monitor, exit_trend_position, kill switch |
| `src/strategist.py` | BB, ADX, RSI, RVOL calculation + check_conditions |
| `src/exchange_wrapper.py` | Binance CCXT wrapper |
| `dashboard/app.py` | FastAPI backend + backtest API |
| `dashboard/static/index.html` | Dashboard UI (lightweight-charts) |
| `config/config.yaml` | Pairs, profiles, risk parameters |
| `SOP.md` | Strategy change protocol (§21-23) |

## SOP

See `SOP.md` for strategy change protocol including Post-Deploy Validation (§21), Data Pipeline Awareness (§22), and Live Verification Mandate (§23). Any strategy change must pass the 22-point checklist before deployment.
