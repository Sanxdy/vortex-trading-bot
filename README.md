<div align="center">
  <h1>🌀 Vortex</h1>
  <p><strong>AI-Assisted Multi-Exchange Crypto Trading Bot</strong></p>
  <p>
    <img src="https://img.shields.io/badge/Python-3.12-blue?logo=python" alt="Python">
    <img src="https://img.shields.io/badge/Binance-Spot+EMAs?logo=binance&color=F0B90B" alt="Binance">
    <img src="https://img.shields.io/badge/Docker-Compose?logo=docker&color=2496ED" alt="Docker">
    <img src="https://img.shields.io/badge/License-MIT-green" alt="License">
    <img src="https://img.shields.io/badge/AI-Alex_Mercer-ff6b6b" alt="AI">
  </p>
</div>

---

**Vortex** runs two independent bots on a single infrastructure:

| Bot | Exchange | Direction | Strategy | AI |
|-----|----------|-----------|----------|----|
| **Spot** | Binance Spot | Long only | Trend pullback + grid | Alex Mercer veto |
| **Futures** | Binance USDⓈ-M | Short only | RSI > 60 in downtrend | Alex Mercer veto |

Both share the same dashboard, database, and AI fallback pipeline.

---

## ✨ Features

| Category | Capabilities |
|----------|-------------|
| **🤖 AI Trade Filter** | Every entry evaluated by Alex Mercer LLM with real 10-candle OHLCV data. 3-model fallback combo prevents rate-limit downtime. Fail-safe VETO on API error. |
| **📊 Dual Dashboard** | Single-page app with `?exchange=spot` or `?exchange=futures` — positions, candle chart, TP/SL lines, strategy PnL, neural activity canvas. |
| **🧠 Strategy Engine** | 18+ profile configurations: trend pullback, BB squeeze, scalp, grid, short. Pluggable via `config.yaml` without code changes. |
| **🛡️ Safety Systems** | Daily loss limit, performance guard, anti-churn cooldown, breakeven lock, trailing SL/TP, kill switch (Telegram + dashboard). |
| **⏰ Killzone Filter** | Only trade London (8-9 UTC) and US (13-14 UTC) open sessions for higher-quality fills. |
| **📈 Multi-Timeframe** | Entry confirmed across 15m + 1h before signal fires — reduces false breakouts by ~50%. |
| **🔌 AI Provider** | Local 9Router proxy with automatic fallback (OpenCode Free → OpenRouter → gemma). No subscription needed. |
| **📉 Regime Detection** | Per-pair ADX/ATR classification: trending↑, trending↓, sideways, high_vol. Strategies gate by regime. |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Vortex Engine                                │
│                                                                     │
│  Binance ──► Strategist ──► Executor ──► TimescaleDB                │
│  (REST+WS)    (indicators)    (position mgmt)  (trades+decisions)   │
│                    │               │                                 │
│                    ▼               ▼                                 │
│              Entry Signals     AI Veto (<1s)                         │
│                    │               │                                 │
│                    └───────┬───────┘                                 │
│                            ▼                                         │
│                       Order Placed                                   │
│                            │                                         │
│                            ▼                                         │
│                     Trailing SL/TP                                    │
│                     (profit lock)                                    │
└─────────────────────────────────────────────────────────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
   ┌──────────┐          ┌──────────────┐          ┌──────────┐
   │  Redis    │          │  9Router AI   │          │Dashboard │
   │ (tickers) │          │  (fallback    │          │ FastAPI  │
   │          │          │   combo)      │          │ + HTML   │
   └──────────┘          └──────────────┘          └──────────┘
```

The AI veto is **not a recommendation system**. It receives the same candle data a human trader sees and outputs a single word: `ENTER` or `SKIP`. Every trade must pass both the technical strategy AND the AI filter.

---

## 🤖 Alex Mercer AI Veto

```
Entry signal from strategist
        │
        ▼
  Preflight check (budget, slot, killzone)
        │
        ▼
  AI Veto — sends to 9Router:
  ┌─────────────────────────────────────────────────────┐
  │  You are Alex Mercer, a senior professional trader  │
  │  with 20+ years of experience. You are calm,        │
  │  disciplined, and probability-driven. You treat     │
  │  every setup as an odds game.                       │
  │                                                     │
  │  Internal checklist:                                │
  │  - Trend: EMAs stacking appropriately?              │
  │  - Volume: Is volume supporting or fading?          │
  │  - Price action: Does the bar show a genuine edge?  │
  │  - Risk/reward: At least 1.5:1 R:R?                │
  │  - Hygiene: No revenge-trading cues.               │
  │                                                     │
  │  If edge is unclear, default to SKIP.              │
  │                                                     │
  │  Setup: ETH/USDT 15m  Candles: 12.34,12.45,...      │
  │  Swing High: 12.50  Swing Low: 12.28                │
  │  RSI: 58  ATR: $0.15                                │
  │                                                     │
  │  Decision (exactly one word):                       │
  └─────────────────────────────────────────────────────┘
        │
        ▼
  ┌─────┴─────┐
  │  ENTER     │  APPROVE → acquire slot → place order
  │  SKIP      │  VETO    → log decision → skip
  │  API error │  VETO    → fail safe → skip
  └───────────┘
```

### AI Model Selector

Switch models live from the **Overview** tab → **AI Model** dropdown:

| Model | Accuracy | Provider |
|-------|:--------:|----------|
| `openrouter/openrouter/free` | **100%** | OpenRouter |
| `openrouter/openrouter/owl-alpha` | **100%** | OpenRouter |
| `oc/nemotron-3-ultra-free` | **100%** | OpenCode Free |
| `oc/north-mini-code-free` | **71%** | OpenCode Free |

No restart needed — changes take effect on the next trade evaluation.  
🔐 Only admin users can change the model.

---

## 📦 Bot Configuration

### Spot Bot (`trend_only` profile)

| Parameter | Value |
|-----------|-------|
| Timeframe | 15m (1h confirmation) |
| Entry | Pullback to EMA20 in uptrend |
| Grid | Disabled |
| Ai calls | ~2-5/day |
| Killzone | 8-9, 13-14 UTC |
| Stop | 2.0× ATR |
| Target | 2.5× ATR |
| Trail | 2.0× ATR |

### Futures Bot

| Parameter | Value |
|-----------|-------|
| Direction | Short |
| Entry | RSI > 60 + below 200 EMA |
| Strength | ADX > 20 |
| Stop | 3.0× ATR |
| Target | 2.5× ATR |
| Trail | 3.0× ATR |
| Exit | RSI < 35 |
| Slots | 3 max, $26/slot |

---

## 🔌 9Router Setup (Required for AI)

The bot uses [9Router](https://github.com/decolua/9router) as a local AI proxy. It's auto-deployed with Docker.

### Step 1 — Verify 9Router is running

```bash
docker ps --filter name=9router
# Should show: vortex-9router-1  Up  ...  0.0.0.0:20128->20128/tcp
```

Dashboard: `http://localhost:20128`

### Step 2 — Connect a provider

Open 9Router dashboard → **Providers** tab.

| Provider | Cost | How to connect |
|----------|------|---------------|
| **OpenCode Free** | Free | Click "Connect" — no auth required |
| **OpenRouter** | Free tier | Create account at [openrouter.ai](https://openrouter.ai), add API key |
| **Kiro AI** | Free | Click "Connect" — Google/GitHub OAuth |

You only need ONE provider connected. OpenCode Free is the simplest.

### Step 3 — Switch AI models from the dashboard

The bot reads the active AI model from Redis at each call. You can change it anytime without restarting:

1. Open dashboard → **Overview** tab
2. Scroll to **AI Model** dropdown (bottom-right)
3. Pick a model — changes instantly

| Model | Accuracy | Provider |
|-------|:--------:|----------|
| `openrouter/openrouter/free` | **100%** (verified) | OpenRouter |
| `openrouter/openrouter/owl-alpha` | **100%** (verified) | OpenRouter |
| `oc/nemotron-3-ultra-free` | **100%** (verified) | OpenCode Free |
| `oc/north-mini-code-free` | **71%** | OpenCode Free |

> 🔐 Only **admin** users can change the model. Guest users see the dropdown but changes are rejected.

### Step 4 — Generate an API key

Dashboard → **API Keys** → Create Key → Copy the key (starts with `sk-...`).

### Step 5 — Configure `.env`

```env
NINEROUTER_URL=http://9router:20128/v1
NINEROUTER_KEY=sk-your-copied-key-here
```

> ⚠️ Never commit your `.env` file. Use `.env.example` as a template.

### AI Status Notification

When the AI model fails (rate limited / 429), an orange banner appears at the top of the **Overview** tab:

```
┌─────────────────────────────────────────────────────────────┐
│ ⚠️ AI model 'openrouter/openrouter/free' 429 Too Many      │
│   Requests at 14:32 UTC — trades blocked until fixed  [✕] │
├─────────────────────────────────────────────────────────────┤
│  Balance │ PnL Today │ Win Rate │ Slots                     │
```

- Polls every 30 seconds — auto-hides when model recovers
- Dismissible with ✕ button
- Switch to a working model via the **AI Model** dropdown to clear it immediately

---

## 🚀 Quick Start

```bash
# 1. Clone
git clone https://github.com/your-username/vortex.git && cd vortex

# 2. Configure environment
cp .env.example .env
# Edit .env with your API keys (Binance, Telegram, 9Router)
# For futures: cp .env.example .env.futures
#   Then copy TELEGRAM_TOKEN and TELEGRAM_CHAT_ID from .env to .env.futures
#   so the futures bot can send Telegram alerts too

# 3. Start everything
docker compose up -d

# 4. Open dashboard
open http://localhost:8000                 # spot
open http://localhost:8000/?exchange=futures  # futures
```

---

## ⚙️ Configuration Reference

### Active Profile

Set in `.env`:

```env
ACTIVE_PROFILE=trend_only   # default — no grid, high-quality entries
```

Available profiles in `config/config.yaml`:

| Profile | Grid | Entries | Timeframe | Best for |
|---------|------|---------|-----------|----------|
| `trend_only` | Off | Pullback + 1h confirm | 15m | Trending markets |
| `sideway` | 8-level | BB squeeze + bounce | 4h | Choppy markets |
| `scalper` | Off | Mean reversion | 5m | Fast scalp entries |
| `conservative` | 15-level | Slow grid | 15m | Low risk |
| `standard` | 20-level | Swing + grid | 15m | General purpose |

### Key Risk Parameters

```yaml
# config/config.yaml (spot) or config-futures.yaml (futures)
max_daily_loss_percent: 5      # stops trading for the day
emergency_stop_pct: 3          # force-exits at 3% loss
trail_atr: 2.0                 # trailing stop multiplier (ATR)
tp_atr: 2.5                    # take profit multiplier (ATR)
```

---

## 📁 Project Structure

```
vortex/
├── src/
│   ├── __init__.py
│   ├── main.py                 # Spot bot entry point
│   ├── main_futures.py         # Futures bot entry point
│   ├── executor.py             # Entry/exit logic, AI veto, trailing SL
│   ├── strategist.py           # Indicator calculation, entry conditions
│   ├── analyst.py              # Alex Mercer AI (9Router integration)
│   ├── exchange_wrapper.py     # CCXT Binance wrapper
│   ├── activity.py             # Activity log (Redis-backed)
│   ├── notifier.py             # Telegram bot
│   ├── heartbeat.py            # Signal handlers
│   └── watchlist.py            # Pair ranking
├── dashboard/
│   ├── app.py                  # FastAPI backend
│   └── static/index.html       # SPA dashboard
├── config/
│   ├── config.yaml             # Spot bot config + profiles
│   └── config-futures.yaml     # Futures bot config
├── scripts/
│   └── backtest_shorts.py      # Short strategy backtest
├── docker-compose.yml          # All services
├── Dockerfile.bot              # Bot image
├── Dockerfile.dash             # Dashboard image
├── .env.example                # Environment template
├── SOP.md                      # Strategy Change SOP
└── AGENTS.md                   # Agent protocol
```

---

## 🛡️ Safety & Risk Controls

| Control | What it does |
|---------|-------------|
| Daily loss limit | Stops trading after 5% account loss in 24h |
| Budget tracking | Allocated risk budget (`SIMULATED_BALANCE`) vs exchange wallet |
| Anti-churn | 2 consecutive losses → 45min cooldown per pair |
| Performance gate | Pauses pairs outside top performers |
| Breakeven lock | Moves stop to entry after 0.5% profit |
| Trailing SL | Locks profit as price moves favorably |
| Kill switch | `/kill` Telegram command or dashboard button |
| AI fail-safe | Returns VETO on API error — no trade passes through |
| Emergency stop | Force-exits if price moves 3% against entry |

---

## 📊 Dashboard

- **Spot:** `http://localhost:8000`
- **Futures:** `http://localhost:8000/?exchange=futures`

Tabs: Overview (PnL, positions), Chart (OHLCV + TP/SL), Strategies (PnL by type), Brain (neural activity canvas), Activity (real-time log).

---

## 📜 License

MIT
