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
| **Confidence Gate** | Blocks entries when DeepSeek confidence < 70 — only high-conviction trades pass |
| **Fee Tracking** | Exchange fees (maker 0.1%, taker 0.1%) deducted from every PnL calculation; stored per-trade in DB; displayed on dashboard |
| **Fee Guard** | Blocks grid deploy if net profit per flip falls below `min_net_profit_percent` (0.1%) after fees |
| **Filter Override** | `/filter override HIGH_VOLATILITY 2h` — temporary bypass with auto-expiry |
| **Decision Log** | Every entry/block logged with ADX, ATR, RSI, price, balance |
| **4 Profiles** | Standard, Scalper, Trend-only, Conservative — switchable via `/profile` |
| **`/suggest`** | Profile-aware 4-stage scan: Binance tickers → OHLCV indicators → deterministic scoring → DeepSeek reasoning (12s, no CoinGecko) |
| **Slot System** | Competitive slot allocation — 1 slot per $50, ≤10% of deployable per slot, 20% reserve always held back |
| **Dynamic Grid Depth** | Grid levels adapt to market quality: full grid in sideways (ADX≤18, good candle eff), half in weak trend/sideways+ADX>18, 1-2 in strong trend, skip in volatile/unknown/low RVOL/low eff |
| **Execution Hardening** | Binance precision/notional validation, bot-owned client order IDs, post-only grid orders, idempotent fill handling |
| **Kill Switch** | Telegram `/kill` or Dashboard button — cancels all, sells coins, stops |
| **Dashboard** | Live chart (trend lines solid, grid lines dashed), slot holders display, PnL by regime, mobile responsive, dark/light theme, USDT/IDR toggle, persistent pair selection, total fees paid |
| **Telegram** | Full command set: status (with decision log), grid, trades, performance, filter, backtest, debug, report, sim |

---

## Profiles

| Profile | Grid | Trend | Timeframe | Levels | Entry Trigger | Best for |
|---------|------|-------|-----------|--------|---------------|----------|
| **Standard** | ✅ 1.5% geometric | ✅ | 15m | 20 (10B/10S) | BB touch + EMA | Balanced swing |
| **Scalper** | ✅ 0.8% arithmetic | ✅ | 5m | 8 (4B/4S) | RSI<35 + EMA in trend, BB touch in sideways | Adaptive scalping |
| **Trend-only** | ❌ | ✅ only | 15m | — | Pullback to EMA20 | Strong trending markets |
| **Conservative** | ✅ 2% geometric | ✅ (cautious) | 15m | 15 (7B/8S) | BB touch + EMA | Lower risk |

Switch via Telegram: `/profile scalper`

### Scalper Entry Logic
- **Trending regime:** RSI < 35 (oversold) AND price above 50-EMA (pullback to trend support)
- **Sideways regime:** price within 0.5% of lower Bollinger Band (mean reversion)
- **Confidence gate:** DeepSeek verdict must be `confidence >= 70`
- **Fee guard:** Grid blocked if net per flip (0.8% - 0.2% fees = 0.6%) < minimum 0.1%

---

## Slot & Budget System

The bot splits your balance into competitive slots. **First pair to signal gets the slot** and deploys the full grid. Other pairs wait with `BLOCKED: no_budget_slot` until TP/SL releases it.

| Balance | Slots with 20% reserve | Budget/slot | Deployable buys (@ $10 min/level) |
|---------|------------------------|-------------|-----------------------------------|
| **$50** | 1 | $40 | 4 |
| **$100** | 1 | $50 | 5 |
| **$200** | 3 | $50 | 5 per slot |
| **$500** | 8 | $50 | 5 per slot |

- **Restart ≠ Reset** — restarting the bot preserves PnL history and balance state. `SIMULATED_BALANCE`
  only caps sizing unless `SIM_RESET_ON_START` or `SIM_RESET_ON_CHANGE` is explicitly enabled.
- Cooldowns: SL/inversion → 1h, TP → 5min, false TP → 5min
- Slot release order: exchange orders cancelled first → slot released (prevents race conditions)
- False TP (no actual position) still releases the slot and sets cooldown
- Use `/status` to see `used/slots (holder names)` on Telegram and dashboard
- Override with `SIMULATED_BALANCE=50` in `.env` for testing

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
| `/status` | Slot usage + per-pair status with latest decision reason (ENTER/BLOCKED) |
| `/grid` | Grid levels (all pairs or `/grid BTC`) |
| `/balance` | Account balances (simulated or real) |
| `/positions` | Open positions |
| `/config` | Bot configuration |
| `/pnl` | Raw realized P&L from DB (wins, losses, trade count) |
| `/why` | Diagnose per-pair entry blocks (candle count, BB, EMA200, regime, analyst) |
| `/trades` | Recent 10 realized P&L trades |
| `/performance` | Portfolio growth from start (Redis-based balance snapshots) |
| `/suggest` | Scan best coins for active profile (4-stage: Binance tickers → OHLCV → deterministic scoring → DeepSeek reasoning) |
| `/apply` | Apply last suggestions or backtest result |
| `/switch BTC,ETH,SOL` | Change active pairs (restarts) |
| `/profile` | Show/switch trading profile |
| `/backtest SOL/USDT` | Backtest all profiles + DeepSeek analysis |
| `/filter override HIGH_VOLATILITY 2h` | Temporarily bypass a filter |
| `/filter list` | Show active overrides |
| `/filter remove HIGH_VOLATILITY` | Remove an override |
| `/debug BTC` | Show last entry snapshot for a pair |
| `/report` | AI analysis of recent decisions |
| `/kill` | Cancel all orders, sell coins, stop bot |
| `/sim 50` | Cap sizing as if balance is $50 |
| `/sim off` | Disable simulation, return to real balance |

---

## Dashboard

Runs at `http://localhost:8000` alongside the bot. Accessible on the same WiFi via Mac's LAN IP.

**Sidebar sections:**
- Strategy — profile, grid type, width, levels, slot health bar
- Portfolio — total value, start balance, portfolio Δ, closed PnL, USDT free/in orders, coin holdings
- PnL by Regime — realized PnL broken down by market regime (sideways/trending/high_vol)
- Market Regime — ADX, RSI, regime per pair
- Active Orders — grid orders for selected pair
- Trend Positions — active trend trades with entry/SL/TP
- Exposure — order count (buys/sells), free vs used USDT %
- Decision Log — every entry/block with context
- Trade History — recent realized PnL trades

**Chart features:**
- Candlestick chart (TradingView Lightweight Charts)
- Grid buy orders — green dashed lines
- Grid sell orders — red dashed lines
- Trend entry — solid blue line
- Trend stop-loss — solid red line
- Trend take-profit — solid green line
- Timeframe selector (1m → 1w), GMT offset, USDT/IDR toggle
- Dark/light theme toggle
- ☰ sidebar toggle on mobile

**Mobile responsive:** Single-column layout at ≤768px, compact topbar, sidebar as overlay, ☰ toggle button, auto-adjusted chart size.

**Persistent selection:** Selected pair saved to `localStorage` — survives page refreshes.

---

## One-Command Setup

```bash
git clone <repo-url> && cd vortex

# Everything in one command:
chmod +x setup.sh && ./setup.sh

# Edit API keys:
nano .env
docker compose restart vortex-bot
```

The `setup.sh` script handles everything automatically:

| # | Step | What it does |
|---|------|-------------|
| 1 | Python check | Verifies Python 3.12+ |
| 2 | Dependencies | Creates venv, `pip install -r requirements.txt` |
| 3 | Environment | Copies `.env.example` → `.env` if not exists |
| 4 | Docker | Starts all 4 containers (Redis, TimescaleDB, bot, dashboard) |
| 5 | Tailscale Funnel | Installs LaunchAgent / systemd service for public dashboard |
| 6 | Summary | Prints URLs + useful commands |

**After first run**, open `.env` (created automatically) and configure:

```
EXCHANGE_API_KEY=          # Binance API key
EXCHANGE_API_SECRET=       # Binance API secret
TELEGRAM_TOKEN=            # Telegram bot token (from BotFather)
TELEGRAM_CHAT_ID=          # Your chat ID
DEEPSEEK_API_KEY=          # (optional) for AI analyst
SHARPE_API_KEY=            # (optional) for news filter — get at https://www.sharpe.ai/login
```

Then restart to apply:
```bash
docker compose restart vortex-bot
```

### Manual start (without setup.sh)

```bash
cp .env.example .env
# Edit .env with your API keys
docker compose up -d --build
```

### What you get

| Service | URL | Purpose |
|---------|-----|---------|
| **Dashboard** | `http://localhost:8000` | Real-time chart, positions, logs |
| **Funnel** | `https://YOUR-MACHINE.ts.net` | Public dashboard URL (Tailscale) |
| **Telegram** | `/start` on your bot | Trading commands + alerts |
| **Logs** | `docker compose logs -f` | Live bot activity |

### Platform support

| OS | Python | Docker | Tailscale | Setup |
|----|--------|-------|-----------|-------|
| **macOS** | ✅ Native | ✅ Docker Desktop | ✅ brew install | ✅ `./setup.sh` |
| **Linux** | ✅ Native | ✅ Native | ✅ `apt`/`yum` | ✅ `./setup.sh` |
| **Windows** | ✅ Install | ✅ Docker Desktop | ✅ GUI installer | ⚠️ Git Bash + `./setup.sh` |

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
| `SIMULATED_BALANCE` | No | Cap sizing/budget for small-account testing (e.g. `50` for $50); does not reset by default |
| `SIM_RESET_ON_START` | No | Set `true` only when you explicitly want DB/Redis/order reset on startup |
| `SIM_RESET_ON_CHANGE` | No | Set `true` to reset when `SIMULATED_BALANCE` changes |
| `SIM_SWEEP_ON_START` | No | Set `true` only when you explicitly want startup coin sweeping |
| `AUTO_PROFILE` | No | Enable automatic profile switching (safe default: switches only when flat) |
| `ANALYST_CONCURRENCY` | No | Limits concurrent Analyst jobs (default `2`) to prevent restarts/OOM on small machines |

### Auto Profile Switching

Enable:

```bash
AUTO_PROFILE=true
```

Behavior:
- Periodically chooses a best-fit profile from live regimes (`sideways`/`trending`/`high_vol`).
- Switches only when flat (no grid/trend/pending exposure) and enforces a minimum hold time to avoid flip-flopping.

Tuning lives in `config/config.yaml` under `auto_profile`.

### Pending Trend Entries

Trend entries are often limit orders. A “Pending” entry means the bot placed a limit buy and is waiting for price to trade into it. For low-priced coins, the dashboard shows more decimals so you can see the true limit price (not just `0.25` rounding).

### Ticker Reliability (Testnet vs Live)

On Binance spot testnet, websocket ticker streams can be unreliable. The bot’s trend-entry preflight now prefers REST `fetch_ticker()` with a candle-close fallback, so it won’t get stuck in `SKIP preflight_no_ticker` loops when WS data is missing.

### Small-Budget Simulation

To test how the allocator behaves with a $50 account without wiping state or selling coins:

```bash
SIMULATED_BALANCE=50
SIM_RESET_ON_START=false
SIM_RESET_ON_CHANGE=false
SIM_SWEEP_ON_START=false
```

The bot will size slots from the simulated balance while continuing to use the configured exchange connection for data/orders.

### Sweep (“Coins held”)

Sweeps place market sells to clear leftover non-USDT balances. After sweeping, the bot now immediately refreshes the dashboard balance snapshot so the “Coins held” panel doesn’t lag for up to an hour.

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
6. Confidence gate          → skip if DeepSeek confidence < 70
7. Signal engine            → RSI/BB/touch (regime-aware, per profile)
8. Fee guard                → block grid if net per flip < 0.1%
9. Slot acquire             → claim allocator slot
10. Risk sizing             → split budget across buy levels
11. Execution               → place orders
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
| `src/analyst.py` | DeepSeek + Binance tickers + deterministic scoring + news + on-chain |
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
| **6** | Slot system (competitive allocation, cancel-before-release, false TP release), PnL display fixes (removed initial_pnl subtraction), sim mode PnL-inclusive balance, restart≠reset, trend positions on chart, decision log in `/status`, mobile responsive dashboard, persistent pair selection, `recenter_grid` snowball fix |
| **7** | Fee tracking (0.1% maker/taker deducted from all PnL, `fee_cost` column in DB, dashboard Fees Paid display), fee guard (blocks unprofitable grids), scalper profile rework (adaptive 0.8% width, 8 levels 4B/4S, ADX threshold 25), `/suggest` rewritten with profile-aware deterministic scoring + DeepSeek reasoning, CoinGecko → Binance |
