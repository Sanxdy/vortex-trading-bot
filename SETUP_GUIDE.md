# Vortex Bot — Step-by-Step Setup Guide

---

## Prerequisites

- macOS / Linux / Windows (WSL2)
- Python 3.12+
- Docker Desktop (for Redis + TimescaleDB)
- Binance account (free)
- Telegram account (free)

---

## Step 1: Install Docker (macOS via Homebrew)

```bash
# Install Docker Desktop via brew
brew install --cask docker

# Open Docker app once to complete setup
open /Applications/Docker.app

# Wait for Docker to start, then verify
docker --version
docker compose version
```

---

## Step 2: Create Binance API Keys

1. Go to https://testnet.binance.vision/ (testnet) or https://www.binance.com/en/my/settings/api-management (live)
2. Click **Create API** → label it "vortex"
3. **Security**: Enable only **Enable Trading** — disable withdrawals
4. Copy **API Key** and **Secret Key** (show once only)
5. For testnet, deposit free test USDT/SOL from the faucet tab

---

## Step 3: Create Telegram Bot

1. Open Telegram, search for **@BotFather**
2. Send `/newbot` → name it (e.g. `Vortex Alert Bot`) → username (e.g. `vortex_alert_bot`)
3. BotFather replies with your **HTTP API token** — save it
4. Search for your bot username on Telegram, click **Start**
5. Send a message to the bot (anything, e.g. "hi")
6. Get your **Chat ID**: visit `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates`
   - Find `"chat":{"id":123456789}` in the JSON response — that number is your Chat ID

---

## Step 4: Configure the Bot

```bash
# Navigate to the vortex directory
cd /Users/sandykusuma/trading_analytics/vortex

# Copy env template
cp .env.example .env
```

Edit `.env` with your actual keys:

```env
EXCHANGE_API_KEY=your_binance_api_key_here
EXCHANGE_API_SECRET=your_binance_secret_here
EXCHANGE_TESTNET=true

TELEGRAM_TOKEN=123456789:ABCdefGhIJKlmNoPQRsTUVwxyz
TELEGRAM_CHAT_ID=123456789

REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

TIMESCALE_DB_HOST=localhost
TIMESCALE_DB_PORT=5432
TIMESCALE_DB_NAME=vortex_trades
TIMESCALE_DB_USER=vortex
TIMESCALE_DB_PASSWORD=vortex_password
```

---

## Step 5: Start Infrastructure (Redis + TimescaleDB)

```bash
docker compose up -d redis timescaledb
```

Wait 30s for databases to initialize.

Verify:
```bash
docker compose ps
# Both should show "Up" and "healthy"
```

---

## Step 6: Install Python (if not installed) & Dependencies

```bash
# Install Python 3.12 via brew
brew install python@3.12

# Verify
python3.12 --version
```

### Install Python Dependencies

```bash
# Navigate to project
cd /Users/sandykusuma/trading_analytics/vortex

# Use a virtual environment (recommended)
python3.12 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

---

## Step 7: Run WebSocket Latency Test (Phase 1)

```bash
python tests/test_ws_latency.py
```

Expected output:
```
Sample 1: 45.23ms | Last price: 142.50
Sample 2: 38.91ms | Last price: 142.48
...
Average latency: 42.15ms
✅ Latency under 100ms target
```

If it fails, check your internet connection or Binance API status.

---

## Step 8: Start the Bot (Dry-Run / Testnet)

```bash
docker compose up vortex-bot
```

Expected Telegram alerts (in order):
1. "Telegram bot connected"
2. "Connected to binance (testnet)"
3. "🚀 Entry conditions met, deploying grid" (may take minutes/hours — waits for price to touch lower BB)
4. "✅ Buy filled at X, placed sell at Y" (when grid starts flipping)

The bot will idle until the 15m candle triggers entry. To force-test, temporarily change `config.yaml` entry conditions to trigger faster.

---

## Step 9: Verify Data in TimescaleDB

```bash
docker compose exec timescaledb psql -U vortex -d vortex_trades -c "SELECT * FROM trades LIMIT 10;"
```

---

## Step 10: Monitor & Stop

- Watch logs: `docker compose logs -f vortex-bot`
- Stop: `docker compose down` (stops everything)

---

## Going Live (Phase 4)

1. In `.env`: set `EXCHANGE_TESTNET=false`
2. In `config.yaml`: ensure `risk.safety_cap: 500` (max $500)
3. Start the bot and let it run until 100 successful trades are logged
4. After 100 trades, increase safety cap or remove it

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError: aioredis` | `pip install -r requirements.txt` again |
| Redis connection refused | Check `docker compose ps` — is redis running? |
| TimescaleDB connection refused | Check `docker compose ps` — is timescaledb running? |
| "API key format invalid" | Binance testnet uses different API keys — make sure you're on testnet |
| Telegram not sending | Verify token and chat ID, ensure you messaged the bot first |
| Orders not placing | Check `.env` API keys, ensure trading permission is enabled |
| No entry triggered | Bot waits for price to touch lower BB + above 200 EMA — may take hours |

---

## File Reference

| File | What to edit |
|------|-------------|
| `.env` | API keys, Telegram, DB credentials |
| `config/config.yaml` | Trading pair, grid width, risk params |
| `src/main.py` | Entry point (no change needed) |
| `src/strategist.py` | Indicator logic (advanced) |
| `src/executor.py` | Order/flip logic (advanced) |
