# Vortex — Mean Reversion Grid Bot

A professional-grade, event-driven grid trading bot that harvests volatility in sideways markets using a **Geometric Grid Strategy** on SOL/USDT.

---

## Architecture

```
                    ┌──────────────┐
                    │   Binance    │
                    │ (WebSocket)  │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
         watch_ticker  watch_ohlcv  watch_orders
              │            │            │
        ┌─────▼────┐ ┌────▼────┐ ┌─────▼─────┐
        │ Ingestor │ │Strategist│ │ Executor  │
        │(Redis)   │ │(Indic.) │ │(Orders)   │
        └─────┬────┘ └────┬────┘ └─────┬─────┘
              │           │            │
        ┌─────▼────┐      │      ┌─────▼─────┐
        │  Redis   │      │      │TimescaleDB│
        │(hot cache│      │      │(trades)   │
        └──────────┘      │      └───────────┘
                          │
                    ┌─────▼─────┐
                    │ Heartbeat │──Telegram──► You
                    │(30s ping) │
                    └───────────┘
```

---

## How It Works

### 1. Entry Trigger (Idle → Active)

The bot stays idle until these **15m timeframe** conditions align:
- Price touches the **Lower Bollinger Band (20,2)**
- Price is **above 200 EMA** (no falling knife)

When both are true, it deploys the grid midpoint at current price.

### 2. Grid Geometry

- **20 levels**: 10 buy orders below center, 10 sell orders above center
- **1.5% geometric spacing** between levels
- Each level uses **1% of total equity** (e.g., $500 equity → $5 per level)
- Orders are placed as limit orders simultaneously

### 3. The Flip (The Core Loop)

When any order fills:
- **Buy fill →** instantly place a Sell at +1.5% (lock profit)
- **Sell fill →** instantly place a Buy at -1.5% (re-enter)

This continuously harvests the 1.5% spread as price oscillates.

### 4. Exit Conditions

| Condition | Action |
|-----------|--------|
| Price hits **Upper Bollinger Band** (15m) | Cancel grid, take profit |
| Price drops **3% below lowest grid level** | Liquidate all positions, pause 4h |
| 1h candle closes **below 200 EMA** | Exit immediately |
| Connection lost (>30s) | Kill switch: cancel all orders, alert via Telegram |

### 5. Rebalancing

Every 24 hours, the grid recalculates its center to the current price, preventing drift.

---

## Setup

### Prerequisites

- Python 3.12+
- Docker & Docker Compose (for Redis + TimescaleDB)
- Binance API key (testnet recommended first)

### Quick Start

```bash
# 1. Clone and enter directory
cd vortex

# 2. Configure environment
cp .env.example .env
# Edit .env — fill in API keys, Telegram token, chat ID

# 3. Start infrastructure (Redis + TimescaleDB)
docker-compose up -d redis timescaledb

# 4. (Optional) Run WebSocket latency test
pip install -r requirements.txt
python tests/test_ws_latency.py

# 5. Start the bot
docker-compose up vortex-bot
```

### Environment Variables (`.env`)

| Variable | Required | Description |
|----------|----------|-------------|
| `EXCHANGE_API_KEY` | Yes | Binance API key (trading permissions only) |
| `EXCHANGE_API_SECRET` | Yes | Binance API secret |
| `EXCHANGE_TESTNET` | No | `true` for testnet (default), `false` for live |
| `TELEGRAM_TOKEN` | Yes | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Yes | Your Telegram chat ID |
| `REDIS_HOST` | No | Default: `localhost` |
| `TIMESCALE_DB_*` | No | Default credentials in docker-compose |

---

## Configuration (`config/config.yaml`)

Key parameters you may want to tune:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `grid.width_percent` | `1.5` | % spacing between grid levels |
| `grid.count` | `20` | Total levels (10 buy + 10 sell) |
| `grid.equity_percent_per_level` | `1.0` | % of equity per order |
| `strategy.entry.timeframe` | `15m` | Entry condition timeframe |
| `strategy.exit.stop_loss.percent_below_lowest_grid` | `3.0` | Hard stop distance |
| `risk.slippage_max_percent` | `0.05` | Max allowed spread |
| `risk.safety_cap` | `500` | Max capital (USD) for initial live run |

---

## Deployment Phases (from TSD)

| Phase | Goal |
|-------|------|
| **1** | CCXT Pro wrapper + WebSocket latency < 100ms |
| **2** | Telegram alerts + Redis live cache |
| **3** | 72h dry-run on Binance Testnet |
| **4** | Live deployment with $500 cap until 100 trades |

---

## Telegram Notifications

The bot sends alerts for:
- ✅ Grid deployment
- ✅ Order fills and flips
- 🛑 Stop-loss triggers
- 🎉 Take-profit triggers
- ⚠️ Connection issues / kill switch
- 🔄 24h rebalance

---

## Risk Management

- **Slippage guard**: Skips orders if spread > 0.05%
- **Position sizing**: Fixed 1% per grid level
- **Stop-loss**: 3% below lowest grid level
- **Trend inversion**: 1h close below 200 EMA → exit
- **Kill switch**: Connection loss > 30s → cancel all + alert
- **Dust cleaner**: Weekly conversion of small balances to USDT

---

## Files

| File | Purpose |
|------|---------|
| `src/exchange_wrapper.py` | CCXT Pro WebSocket connection, rate limiter |
| `src/ingestor.py` | Watches ticker via WebSocket, pushes to Redis |
| `src/strategist.py` | Calculates EMA, Bollinger Bands, checks entry/exit |
| `src/executor.py` | Grid order placement, flip logic, stop-loss, rebalance |
| `src/heartbeat.py` | 30s exchange ping, kill switch on failure |
| `src/notifier.py` | Telegram alert sender |
| `src/db.py` | TimescaleDB trade logger |
| `src/main.py` | Wires all components, event loop |
| `tests/test_ws_latency.py` | Phase 1 WebSocket latency benchmark |
