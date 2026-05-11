# Vortex — Systematic Grid + Trend Trading Bot

Multi-strategy, event-driven trading bot supporting **grid trading** (sideways markets) and **trend-pullback** entries (trending markets). Runs on Binance spot (testnet or live) with regime detection, safety shields, and a real-time dashboard.

---

## Features

| Layer | What it does |
|-------|-------------|
| **Market Regime** | ADX + ATR classifies each pair as sideways / trending / high_vol |
| **Safety Shields** | Daily loss limit, loss streak cooldown, ATR-based stops, balance snapshots |
| **Grid Mode** | Geometric or arithmetic grids in sideways regime |
| **Trend Mode** | EMA20/50 + RSI pullback entries in trending regime, trailing stop |
| **Analyst Filter** | DeepSeek AI analyzes price + news + on-chain; blocks unsafe entries |
| **Filter Override** | `/filter override HIGH_VOLATILITY 2h` — temporary bypass with auto-expiry |
| **Decision Log** | Every entry/block logged with ADX, ATR, RSI, price, balance |
| **4 Profiles** | Standard, Scalper, Trend-only, Conservative — switchable via `/profile` |
| **Backtesting** | Replay historical candles, compare profiles, DeepSeek recommends best |
| **Adaptive Balance** | Pairs and budget auto-scale to your wallet balance — no manual config |
| **Kill Switch** | Telegram `/kill` or Dashboard button — cancels all, sells coins, stops |
| **Dashboard** | Live chart, regime viewer, active orders, trend positions, decision log |
| **Telegram** | Full command set: status, grid, trades, performance, filter, backtest |

---

## Profiles

| Profile | Grid | Trend | Timeframe | Best for |
|---------|------|-------|-----------|----------|
| **Standard** | ✅ 1.5% geometric | ✅ | 15m | Balanced swing |
| **Scalper** | ✅ 0.4% arithmetic | ✅ | 5m | High frequency |
| **Trend-only** | ❌ | ✅ only | 15m | Strong trending markets |
| **Conservative** | ✅ 2% geometric | ✅ (cautious) | 15m | Lower risk |

Switch via Telegram: `/profile scalper`

---

## Adaptive Balance

The bot automatically adapts to your wallet balance at startup. Budget slots are allocated competitively — whichever pair signals first gets the slot:

| Balance | Slots | Budget/slot | Levels (budget-capped) |
|---------|-------|-------------|------------------------|
| **$500+** | `balance ÷ 25` | $25/slot | up to 5 |
| **$100** | 4 | $25 | up to 5 |
| **$50** | **2** | **$25** | up to 5 |
| **$30** | 1 | $30 | up to 6 |
| **$15** | 1 | $15 | up to 3 |

- Each level is guaranteed ≥ exchange minimum notional ($5-10)
- Buy levels also capped by **±2× ATR** from current price — low-vol pairs deploy fewer, tighter levels; high-vol pairs deploy more, wider levels

- Each level is guaranteed ≥ exchange minimum notional ($5-10)
- All pairs are monitored for entry signals; first to signal gets a slot
- When a position exits, its slot is freed for another pair to use
- Use `/status` to see current slot usage
- Override with `SIMULATED_BALANCE=50` in `.env` for testing
- Changing `SIMULATED_BALANCE` (or removing it) **resets trade history** — DB cleared, Redis state wiped, fresh start
- Notification sent to Telegram and shown as a dashboard banner on change

---

## Architecture

```
                    ┌──────────────┐
                    │   Binance    │
                    │ (WebSocket)  │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────────┐
              │            │                │
        watch_ticker  watch_ohlcv      watch_orders
        (all pairs)  (all pairs)      (per pair)
              │            │                │
        ┌─────▼────┐ ┌────▼────┐      ┌─────▼─────┐
        │ Ingestor │ │Strategist│      │ Executor  │
        │(Redis)   │ │(Indic.)  │      │(Grid+Trend)│
        └─────┬────┘ └────┬────┘      └─────┬─────┘
              │           │                 │
        ┌─────▼────┐     │           ┌──────▼──────┐
        │  Redis   │     │           │  TimescaleDB │
        │(live data)     │           │  (trades,    │
        │(overrides)     │           │   decisions) │
        └──────────┘     │           └─────────────┘
                         │
              ┌──────────┼──────────┐
              │          │          │
         ┌────▼────┐ ┌──▼───┐ ┌───▼────┐
         │ Analyst │ │HB    │ │Notifier│
         │(DS API) │ │(30s) │ │(TG+WS) │
         └─────────┘ └──────┘ └──┬─────┘
                                 │
                          ┌──────▼──────┐
                          │  Dashboard  │
                          │  (FastAPI + │
                          │   Web UI)   │
                          └─────────────┘
```

---

## Telegram Commands

| Command | Description |
|---------|-------------|
| `/status` | Grid status for all pairs |
| `/grid` | Grid levels (all pairs or `/grid BTC`) |
| `/balance` | Account balances |
| `/positions` | Open positions |
| `/config` | Bot configuration |
| `/pnl` | Realized profit & loss |
| `/why` | Diagnose why no position is opening |
| `/trades` | Recent trades with P&L |
| `/performance` | Portfolio growth from start |
| `/suggest` | Scan best coins + auto-backtest each |
| `/apply` | Apply last suggestions or backtest result |
| `/switch BTC,ETH,SOL` | Change active pairs (restarts) |
| `/profile` | Show/switch trading profile |
| `/backtest SOL/USDT` | Backtest all profiles + DeepSeek analysis |
| `/filter override HIGH_VOLATILITY 2h` | Temporarily bypass a filter |
| `/filter list` | Show active overrides |
| `/filter remove HIGH_VOLATILITY` | Remove an override |
| `/kill` | Cancel all orders, sell coins, stop bot |
| `/sim 50` | Set simulated balance to $50 (resets trade history) |
| `/sim off` | Disable simulation, return to real balance |

---

## Dashboard

Runs at `http://localhost:8000` alongside the bot.

**Sidebar sections:**
- Strategy — profile, grid type, width, levels
- Performance — start balance, current, profit %
- Market Regime — ADX, RSI, regime per pair
- Active Orders — grid orders for selected pair
- Trend Positions — active trend trades with entry/SL/TP
- Exposure — total orders count
- Decision Log — every entry/block with context
- Trade History — recent realized PnL trades

**Chart:**
- Candlestick with Bollinger Bands + EMA
- Green/red dashed lines for active buy/sell orders
- GMT offset selector, USDT/IDR currency toggle

---

## Setup

### Option A: Docker (everything in containers)

```bash
cp .env.example .env
# Edit .env — API keys, Telegram, TRADE_PAIRS, etc.

docker compose up -d --build
```

### Option B: Local (bot outside Docker, infra in Docker)

```bash
cp .env.example .env
# Edit .env — API keys, Telegram, TRADE_PAIRS, etc.

# Start infrastructure only
docker compose up -d redis timescaledb

# Run bot locally
./run.sh                # macOS (uses caffeinate to prevent sleep)
# or: python src/main.py
# or: caffeinate -i python src/main.py

# Run dashboard separately
python -m dashboard.app
```

**macOS sleep note**: The `run.sh` script wraps the bot with `caffeinate -i`, which prevents idle sleep so WebSocket connections survive screen lock / display off. Display sleep and lock screen still work normally. If running without the script, prefix with `caffeinate -i`.

Opens:
- `http://localhost:8000` — Dashboard
- Telegram bot — commands listed above

---

## Live Deployment Checklist

Before moving from testnet to real capital:

| # | Step | Details |
|---|------|---------|
| 1 | **Start small** | Deposit $50-100 USDT max |
| 2 | **Disable testnet** | Set `EXCHANGE_TESTNET=false` in `.env` |
| 3 | **API key security** | Use trade-only API key (disable withdraw) |
| 4 | **Conservative profile** | Start with `/profile conservative` |
| 5 | **Single pair** | Set `TRADE_PAIRS=BTC` — one pair only |
| 6 | **Monitor first 10 trades** | Watch Telegram alerts + dashboard closely |
| 7 | **Check slippage** | Compare limit order prices vs fills |
| 8 | **Confirm alerts** | Verify every Telegram notification works |
| 9 | **Review after 24h** | Use `/performance` and Decision Log |
| 10 | **Expand slowly** | Add pairs one at a time, not all at once |

---

## Environment Variables (`.env`)

| Variable | Required | Description |
|----------|----------|-------------|
| `EXCHANGE_API_KEY` | Yes | Binance API key |
| `EXCHANGE_API_SECRET` | Yes | Binance API secret |
| `EXCHANGE_TESTNET` | No | `true` for testnet (default) |
| `TRADE_PAIRS` | No | `BTC,ETH,SOL` — empty = all config pairs |
| `TELEGRAM_TOKEN` | Yes | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Yes | Your Telegram chat ID (comma-sep for multiple) |
| `DEEPSEEK_API_KEY` | No | Required for analyst + backtest analysis |
| `ACTIVE_PROFILE` | No | `standard`, `scalper`, `trend_only`, `conservative` |
| `SIMULATED_BALANCE` | No | Override balance for testing (e.g. `50` for $50) |

---

## Backtesting

```bash
# Manual CLI
python -m backtest.run SOL/USDT --days 14 --profile both

# Telegram
/backtest SOL/USDT --days=14
```

Both profiles are tested. DeepSeek recommends the best one. Reply `/apply` to switch.

---

## Decision Hierarchy

```
1. Daily loss check         → kill if exceeded
2. Cooldown check           → skip if in cooldown
3. Trend inversion check    → skip if 1h below 200 EMA
4. Regime classification    → ADX/ATR → sideways/trending/high_vol
5. Analyst check            → DeepSeek → blocks unsafe environments
6. Signal engine            → BB touch + EMA alignment
7. Risk sizing              → equity % per level
8. Execution                → place orders
```

Every decision point logs to `trade_decisions` table for later analysis.

---

## Files

| File | Purpose |
|------|---------|
| `src/exchange_wrapper.py` | CCXT Pro WebSocket, rate limiter |
| `src/ingestor.py` | Multi-pair ticker → Redis |
| `src/strategist.py` | Indicators (BB, EMA, ADX, ATR, RSI) + regime classification |
| `src/executor.py` | Grid/trend entry, safety shields, decision logging |
| `src/analyst.py` | DeepSeek + CoinGecko + news + on-chain |
| `src/notifier.py` | Telegram commands + push alerts |
| `src/db.py` | TimescaleDB — trades, decisions, balance snapshots |
| `src/heartbeat.py` | Health check, Redis kill signal watcher |
| `src/main.py` | Entry point, wires all components |
| `dashboard/app.py` | FastAPI backend + WebSocket |
| `dashboard/static/index.html` | Single-page dashboard UI |
| `backtest/run.py` | Backtesting harness |
| `config/config.yaml` | Profiles, pairs, strategy, risk params |

---

## Development Phases

| Phase | Feature |
|-------|---------|
| **1** | Safety shields: daily loss limit, loss streak cooldown, ATR stops, balance snapshots |
| **2** | Regime detection: ADX, ATR volatility spike |
| **3** | Trend-pullback entry: EMA20/50, RSI, trailing stop |
| **4** | Dashboard, backtesting, 4 profiles, `/suggest` auto-backtest |
| **5** | Feed-forward filter, decision logging, `/filter` override, debug snapshots |
