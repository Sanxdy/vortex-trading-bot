# Vortex — Mean Reversion Grid Bot

Multi-pair, event-driven grid trading bot that harvests volatility in sideways markets using a **Geometric Grid Strategy**. Supports BTC, ETH, SOL, XRP, BNB, ADA, DOGE, AVAX, DOT, LINK and any other Binance spot pair.

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
         watch_ticker  watch_ohlcv     watch_orders
         (all pairs)  (all pairs)     (per pair)
              │            │                │
        ┌─────▼────┐ ┌────▼────┐      ┌─────▼─────┐
        │ Ingestor │ │Strategist│      │ Executor  │
        │(Redis)   │ │(Indic.)  │      │(Grid/Pair)│
        └─────┬────┘ └────┬────┘      └─────┬─────┘
              │           │                 │
        ┌─────▼────┐     │           ┌──────▼──────┐
        │  Redis   │     │           │  TimescaleDB │
        │(hot cache│     │           │  (trades)    │
        └──────────┘     │           └─────────────┘
                         │
              ┌──────────┼──────────┐
              │          │          │
         ┌────▼────┐ ┌──▼───┐ ┌───▼────┐
         │ Analyst │ │HB    │ │Notifier│
         │(DS API) │ │(30s) │ │(TG cmds)│
         └─────────┘ └──────┘ └────────┘
```

---

## How It Works

### 1. Entry Trigger (Idle → Active per pair)

Each pair runs independently. The bot checks **15m timeframe** conditions:
- Price touches the **Lower Bollinger Band (20,2)**
- Price is **above 200 EMA** (no falling knife)
- **(Optional) Analyst check** — DeepSeek analyzes price + news + on-chain data. Blocks entry if a strong trend is detected.

When all conditions pass for a pair, it deploys that pair's grid.

### 2. Grid Geometry

Per-pair configuration (configurable in `config.yaml`):
| Pair | Width | Levels |
|------|-------|--------|
| BTC | 1.0% | 20 |
| ETH | 1.2% | 20 |
| SOL | 1.5% | 20 |
| DOGE | 2.0% | 20 |
| Others | 1.5% | 20 |

Each level uses **1% of total equity**. 20 levels × 1% = 20% of capital deployed per pair.

### 3. The Flip (The Core Loop)

When any order fills:
- **Buy fill →** instantly place a Sell at +width% (lock profit)
- **Sell fill →** instantly place a Buy at -width% (re-enter)

Each flip sends a Telegram alert with the profit amount.

### 4. Exit Conditions (per pair)

| Condition | Action |
|-----------|--------|
| Price hits **Upper Bollinger Band** (15m) | Cancel grid, take profit |
| Price drops **3% below lowest grid level** | Liquidate, pause 4h |
| 1h candle closes **below 200 EMA** | Exit immediately |
| Connection lost (>30s) | Kill switch: cancel ALL pairs, alert |

### 5. Rebalancing

Every 24 hours, each active grid recalculates its center to the current price.

---

## Analyst Module (DeepSeek + News + On-Chain)

Before deploying a grid, the bot can run a macro check via **DeepSeek V4 Flash**:
1. Fetches **7d price data** (CoinGecko — free, no key needed)
2. Fetches **news sentiment** (CryptoPanic — optional)
3. Fetches **on-chain data** (per chain — optional API keys)
4. DeepSeek returns: `SAFE`, `STRONG_UPTREND`, `STRONG_DOWNTREND`, or `HIGH_VOLATILITY`
5. If unsafe, entry is blocked and Telegram alert sent

Without DeepSeek key, analyst is skipped and bot runs on technicals only.

---

## Telegram Commands

Send these to your bot once running:

| Command | Response |
|---------|----------|
| `/start` or `/help` | List all commands |
| `/status` | Active/idle status for all pairs |
| `/grid` | Grid levels for all pairs |
| `/grid BTC` | Grid levels for a specific pair |
| `/balance` | USDT and SOL balance |
| `/positions` | Open positions |
| `/pnl` | Total realized profit/loss |
| `/config` | Current bot configuration |

---

## Setup

### Prerequisites

- Python 3.12+
- Docker & Docker Compose
- Binance API key (testnet recommended first)

### Quick Start

```bash
cd vortex
cp .env.example .env
# Edit .env — API keys, Telegram, etc.

docker compose up -d redis timescaledb
pip install -r requirements.txt
docker compose up --build vortex-bot
```

### Environment Variables (`.env`)

| Variable | Required | Description |
|----------|----------|-------------|
| `EXCHANGE_API_KEY` | Yes | Binance API key (trading permissions only) |
| `EXCHANGE_API_SECRET` | Yes | Binance API secret |
| `EXCHANGE_TESTNET` | No | `true` for testnet (default) |
| `TRADE_PAIRS` | No | Filter: `BTC,ETH,SOL` — empty = all enabled pairs |
| `TELEGRAM_TOKEN` | Yes | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Yes | Your Telegram chat ID |
| `DEEPSEEK_API_KEY` | No | Required for analyst module |
| `CRYPTOPANIC_API_KEY` | No | News sentiment for all coins |
| `SOLSCAN_API_KEY` | No | SOL on-chain data |
| `ETHERSCAN_API_KEY` | No | ETH on-chain data |

---

## Configuration (`config/config.yaml`)

### Pairs

Each pair has its own grid settings:

```yaml
pairs:
  - name: "BTC/USDT"
    enabled: true
    grid:
      width_percent: 1.0
      count: 20
      equity_percent_per_level: 1.0
```

### Strategy

| Parameter | Default | Description |
|-----------|---------|-------------|
| `strategy.entry.timeframe` | `15m` | Entry condition timeframe |
| `strategy.exit.stop_loss.percent_below_lowest_grid` | `3.0` | Hard stop distance |
| `risk.slippage_max_percent` | `0.05` | Max allowed spread |
| `risk.safety_cap` | `500` | Max capital (USD) |

---

## Deployment Phases

| Phase | Goal |
|-------|------|
| **1** | CCXT Pro wrapper + WebSocket latency < 100ms |
| **2** | Telegram alerts + Redis cache |
| **3** | 72h dry-run on Binance Testnet |
| **4** | Live deployment with $500 cap until 100 trades |

---

## Risk Management

- **Slippage guard**: Skips orders if spread > 0.05%
- **Position sizing**: Fixed 1% per grid level
- **Stop-loss**: 3% below lowest grid level
- **Trend inversion**: 1h close below 200 EMA → exit
- **Analyst filter**: DeepSeek blocks entry during strong trends
- **Kill switch**: Connection loss > 30s → cancel all + alert

---

## Files

| File | Purpose |
|------|---------|
| `src/exchange_wrapper.py` | CCXT Pro WebSocket, rate limiter |
| `src/ingestor.py` | Multi-pair ticker → Redis |
| `src/strategist.py` | EMA, Bollinger Bands per pair |
| `src/executor.py` | Per-pair grid, flip, SL, rebalance |
| `src/analyst.py` | DeepSeek + CoinGecko + news + on-chain |
| `src/heartbeat.py` | 30s ping, kill switch |
| `src/notifier.py` | Telegram commands + push alerts |
| `src/db.py` | TimescaleDB trade logger |
| `src/main.py` | Entry point, wires all components |
| `tests/test_ws_latency.py` | WebSocket latency benchmark |
