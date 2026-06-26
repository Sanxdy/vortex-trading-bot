<div align="center">
  <h1>🌀 Vortex</h1>
  <p><strong>Multi-Exchange Crypto Trading Bot — Spot Long + Futures Short</strong></p>
  <p>
    <img src="https://img.shields.io/badge/Python-3.12-blue?logo=python" alt="Python">
    <img src="https://img.shields.io/badge/Binance-Testnet?logo=binance&color=F0B90B" alt="Binance">
    <img src="https://img.shields.io/badge/Docker-Compose?logo=docker&color=2496ED" alt="Docker">
    <img src="https://img.shields.io/badge/License-MIT-green" alt="License">
  </p>
</div>

---

**Vortex** runs two independent algorithmic trading bots on a shared infrastructure:

| Bot | Exchange | Direction | Entry Logic |
|-----|----------|-----------|-------------|
| **Futures** | Binance USDⓈ-M Testnet | Short only | Quickie Short — ADX>30 + TEMA/BB/SMA200 crossover |
| **Spot** | Binance Spot Testnet | Long only | Trend pullback to EMA20 |

Both share the same dashboard, TimescaleDB trade log, and Redis state layer.

---

## Quick Start

```bash
# 1. Infrastructure (Redis + TimescaleDB)
docker compose up -d redis timescaledb

# 2. Configure
cp .env.example .env
# Edit .env with Binance testnet API keys
# For futures: cp .env.futures.example .env.futures

# 3. Mac native (no Docker for bots)
./start-mac.sh

# 4. Dashboard
open http://localhost:8000           # spot
open http://localhost:8000/?exchange=futures  # futures
```

---

## Architecture

```
Exchange (REST+WS)
    │
    ▼
Strategist ──► Entry Conditions (ADX, RSI, TEMA, BB, SMA200)
    │
    ▼
Executor ──► Preflight ──► Slot Allocator ──► Order Placed
    │                                                 │
    ▼                                                 ▼
Position Monitor (TP/SL, ROI time exits, ADX>70)    Trade logged
    │
    ▼
TimescaleDB (trades, decisions, fees)
    │
    ▼
Dashboard (FastAPI + SPA HTML)
```

### Bot Architecture Detail

```
┌─────────────────────────────────────────────┐
│             manage_pair (loop)               │
│  for each pair every cycle:                  │
│    ticker → conditions → entry check         │
│    │                                         │
│    ├─ Entry signal? → preflight → slot       │
│    │   → market order → position monitor     │
│    │                                         │
│    ├─ Position active? → monitor (TP/SL)     │
│    │   → exit if target/stop/timeout hit     │
│    │                                         │
│    └─ No signal? → log CASH → next pair      │
│                                              │
│  Redis publish (grid_state) every cycle       │
└─────────────────────────────────────────────┘
```

---

## Futures Strategy — Quickie Short

### Entry Conditions

| Signal | Conditions | Interpretation |
|--------|-----------|----------------|
| **Quickie Short** | ADX>30, TEMA>BB_mid, TEMA falling, close>SMA200 | Bearish reversal — price extended up, momentum exhausting |
| **Quickie Bear Short** | ADX>30, TEMA<BB_mid, TEMA falling, close<SMA200 | Bear continuation — already in downtrend, momentum accelerating |

All on 5m candles, ADX(14), TEMA(9), BB(20,2), SMA(200).

### Execution

| Step | Detail |
|------|--------|
| **Order type** | Market sell (taker, 4bps) |
| **Entry price** | `round(bid * 0.999, 4)` |
| **Size** | `min(USDT * 2%, pair_budget * 0.5) / (ATR * 1.0)` |
| **Max slots** | 3 |
| **Budget** | $250 simulated, $200 deployable (20% reserve), ~$40/slot |
| **Leverage** | 5× isolated |

### TP/SL & Exits

| Exit | Trigger |
|------|---------|
| **Fixed TP** | Price falls 15% from entry |
| **Fixed SL** | Price rises 25% from entry |
| **ROI table** | 15% @ 10m, 6% @ 15m, 3% @ 30m, 1% @ 100m |
| **ADX>70** | Exhaustion in extreme trend (TEMA reversal) |
| **Time cascade** | 2h @ -1.5% SL, 4h @ any loss SL, 8h @ <0.5% TP, 16h @ <1% TP |

### Fee Impact

| Leg | Rate |
|-----|------|
| Entry (market sell) | 4bps taker |
| Exit (market buy) | 4bps taker |
| Round trip | **8bps** minimum |

---

## Spot Strategy — Trend Pullback

### Entry Conditions

- 15m candles with 1h confirmation
- Price pulls back to EMA20 in uptrend
- RSI > 50 (bullish momentum)
- Above 200 EMA (macro uptrend)

### Exit

- Trailing stop at 2.0× ATR
- Take profit at 2.5× ATR
- Breakeven lock after 0.5% profit

---

## Dashboard

Single-page application — real-time positions, PnL, candle charts, decision log.

| Endpoint | Purpose |
|----------|---------|
| `/` | Spot dashboard |
| `/?exchange=futures` | Futures dashboard |

### API

| Route | Returns |
|-------|---------|
| `/api/orders/active` | Open positions with entry, stop, target, PnL, age |
| `/api/conditions` | Per-pair indicators (ADX, RSI, ATR, entry flags) |
| `/api/decisions` | Trade decision history |
| `/api/pnl/summary` | Aggregate PnL per pair |
| `/api/trades` | Trade log |

---

## Risk Controls

| Control | Futures | Spot |
|---------|---------|------|
| Daily loss limit | 15% of simulated balance | 5% |
| Slot budget | $40/slot × 3 max | Dynamic |
| Anti-churn | 60s cooldown on fail, 15min after SL | 45min after 2 losses |
| Stop cooldown | 900s after any SL exit | 900s |
| Cooldown cascade | 2h ban after 3 sideway losses | — |
| Funding roll skip | 15min before/after 00/08/16 UTC | — |

---

## Configuration

### Files

| File | Purpose |
|------|---------|
| `config/config.yaml` | Spot bot: profiles, pairs, risk, indicators |
| `config-futures.yaml` | Futures bot: leverage, slots, entry thresholds |
| `.env` | Spot API keys, database, Telegram |
| `.env.futures` | Futures API keys (overrides Telegram token) |

### Active Profile

```env
ACTIVE_PROFILE=standard   # spot (config/config.yaml)
```

Profiles control grid type, width, risk, and entry timeframes per market regime.

---

## Data Storage

| System | Purpose |
|--------|---------|
| **TimescaleDB** | Trade log (`trades`), decisions (`trade_decisions`), PnL history |
| **Redis** | Real-time state (`grid_state`), ticker cache, allocator state, activity log |

### Schema

```sql
CREATE TABLE trades (
  id SERIAL,
  timestamp TIMESTAMPTZ NOT NULL,
  pair TEXT NOT NULL,
  side TEXT NOT NULL,
  price NUMERIC(16,8) NOT NULL,
  quantity NUMERIC(16,8) NOT NULL,
  order_id TEXT,
  status TEXT NOT NULL,
  grid_level INTEGER,
  realized_pnl NUMERIC(16,8),
  fee_cost NUMERIC(16,8),
  exchange TEXT DEFAULT 'spot'
);

CREATE TABLE trade_decisions (
  id SERIAL,
  timestamp TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  decision TEXT NOT NULL,
  reason TEXT,
  regime TEXT,
  adx NUMERIC(8,2),
  atr NUMERIC(16,8),
  rsi NUMERIC(8,2),
  price NUMERIC(16,8),
  balance_usdt NUMERIC(16,8),
  exchange TEXT DEFAULT 'spot',
  trend_uptrend BOOLEAN
);
```

---

## Project Structure

```
vortex/
├── src/
│   ├── main.py                 # Spot bot entry point
│   ├── main_futures.py         # Futures bot entry point
│   ├── executor.py             # Entry/exit, position monitor, TP/SL
│   ├── strategist.py           # Indicator calc, entry conditions
│   ├── exchange_wrapper.py     # CCXT Binance wrapper (REST + WebSocket)
│   ├── activity.py             # Activity log (Redis-backed)
│   ├── notifier.py             # Telegram alerts
│   └── heartbeat.py            # Graceful shutdown
├── dashboard/
│   ├── app.py                  # FastAPI backend
│   └── static/index.html       # SPA dashboard
├── config/
│   ├── config.yaml             # Spot bot config + profiles
│   └── config-futures.yaml     # Futures bot config
├── tests/
├── scripts/
├── docker-compose.yml
├── start-mac.sh                # Mac native launcher
├── stop-mac.sh                 # Mac native stopper
├── SOP.md                      # Strategy change protocol
└── AGENTS.md                   # Dev agent protocol
```

---

## Scripts

| Script | What it does |
|--------|-------------|
| `start-mac.sh` | Start Redis + TimescaleDB + spot + futures + dashboard natively |
| `stop-mac.sh` | Gracefully stop all processes |
| `scripts/pre_deploy_check.sh` | Deploy safety gate (syntax, Docker build, baseline) |
| `scripts/validate-dashboard.sh` | Dashboard API regression test |

---

## Operating Modes

| Mode | Effect |
|------|--------|
| `technical_only` | No AI — pure indicator-based entries |
| `ai_observe_only` | AI runs but cannot block entries |
| `technical_plus_ai` | AI veto active (requires 9Router proxy) |

Current: `technical_only` configured in both `config.yaml` and `config-futures.yaml`.

---

## Mac Native Setup

The bots run as native Python processes (not Docker) for lower latency and easier debugging. Only Redis and TimescaleDB run in Docker.

```bash
# One-time
./setup.sh

# Daily
./start-mac.sh    # starts everything
./stop-mac.sh     # stops everything
```

Dependencies: Python 3.12+, Docker Desktop, Homebrew.

---

## License

MIT
