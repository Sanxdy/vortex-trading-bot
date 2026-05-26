# STB Deployment Guide — HG680P (Amlogic S905, 2GB RAM, 8GB eMMC)

## 📋 Prerequisites

| Item | Status |
|------|--------|
| HG680P STB (Amlogic S905) | ✅ |
| Armbian installed | ☐ |
| Casa OS installed | ☐ |
| SSH access configured | ☐ |
| Git installed | ☐ |
| Docker installed | ☐ |

---

## ⚙️ STB Configuration

SSH into the STB and run these commands:

```bash
# 1. Set up swap (prevents OOM on 2GB RAM)
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# 2. Install git
sudo apt update && sudo apt install git -y

# 3. Clone the project
cd ~
git clone https://github.com/Sanxdy/vortex-trading-bot.git vortex
cd vortex

# 4. Create .env file
cat > .env << 'ENVEOF'
ACTIVE_PROFILE=sideway
TRADE_PAIRS=SOL,SUI,AVAX,LINK,BNB,DOT,DOGE,ADA
SIMULATED_BALANCE=150
SIM_RESET_ON_START=false
SIM_RESET_ON_CHANGE=true
SIM_RESET_ON_DISABLE=false
TELEGRAM_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_chat_id
EXCHANGE_API_KEY=your_binance_testnet_api_key
EXCHANGE_API_SECRET=your_binance_testnet_secret
EXCHANGE_TESTNET=true
DEEPSEEK_API_KEY=
ENVEOF

# 5. Start the bot (first build takes 10-20 min on S905)
docker compose up -d
```

---

## 🔐 Credentials Required

| Credential | Where to get | Done |
|-----------|-------------|:----:|
| Telegram Bot Token | [@BotFather](https://t.me/botfather) on Telegram | ☐ |
| Telegram Chat ID | Message [@userinfobot](https://t.me/userinfobot) | ☐ |
| Binance Testnet API Key | [testnet.binance.vision](https://testnet.binance.vision/) → API Management | ☐ |
| Binance Testnet Secret | Same page | ☐ |

---

## ✅ Verification Checklist

```bash
# 1. All containers running
docker ps
# Expected: 4 containers (vortex-bot, dashboard, timescaledb, redis)

# 2. Storage usage
df -h
# Expected: at least 1.5GB free

# 3. Memory usage
free -h
# Expected: at least 300MB free

# 4. Bot logs (no errors)
docker logs vortex-vortex-bot-1 2>&1 | grep -i error | head -5

# 5. Dashboard accessible
curl -s http://localhost:8000/api/status | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print('Online:', d.get('online'))"

# 6. Trade pairs loaded
curl -s http://localhost:8000/api/status | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print('Pairs:', list(d.get('pairs',{}).keys()))"

# 7. Check entry_paths config
docker exec vortex-vortex-bot-1 python3 -c "
import yaml
with open('/app/config/config.yaml') as f:
    cfg = yaml.safe_load(f)
ep = cfg.get('entry_paths', {})
for p,v in ep.items():
    active = [k for k,v2 in v.items() if v2]
    print(f'{p}: {active}')
"
```

---

## ⏰ First-Run Timeline

| Timeframe | What happens |
|:---------:|-------------|
| 0-5 min | Bot starts, backfills 800 4h candles from mainnet, connects to testnet |
| 5-60 min | First sideway signals evaluated (CASH / REJECTED messages in logs) |
| 1-24 hours | First trade enters (SOL, AVAX, or whichever pair meets EMA50 conditions) |
| 24-48 hours | First trade closes (TP, SL, or cooldown exit) |
| 1 week | ~5-15 trades accumulated across all pairs |

---

## 🚨 Emergency Stop

```bash
# Stop everything
docker compose down

# Restart fresh
docker compose up -d

# Update code to latest version
git pull origin main
docker compose up -d --build
```

---

## 📈 Monthly Maintenance

```bash
# Prune old Docker images
docker system prune -f

# Check storage
df -h

# Check 7-day PnL
docker exec vortex-timescaledb-1 psql -U vortex -d vortex_trades -c \
  "SELECT ROUND(SUM(realized_pnl),2) FROM trades WHERE realized_pnl IS NOT NULL AND timestamp > NOW() - INTERVAL '7 days'"

# Check settings in dashboard
curl -s http://localhost:8000/api/risk/limit | python3 -m json.tool
```

---

## 🧠 Memory Optimization (Optional)

If you experience OOM crashes, add memory limits to `docker-compose.yml`:

```yaml
services:
  vortex-bot:
    deploy:
      resources:
        limits:
          memory: 512M
  dashboard:
    deploy:
      resources:
        limits:
          memory: 256M
  timescaledb:
    deploy:
      resources:
        limits:
          memory: 512M
  redis:
    deploy:
      resources:
        limits:
          memory: 128M
```

Then `docker compose up -d` to apply.

---

## 📝 Daily Loss Limit

Default: **$50 absolute** (set via dashboard Risk Limit card).

To change:
- **Dashboard**: Enter amount in Risk Limit card, click Set
- **Telegram**: `/risk 30` (sets $30 limit), `/risk off` (uses config percentage)

When hit: bot stops permanently. Restart manually via `docker compose restart`.

---

## 🔄 Updating the Bot

When I push new code to GitHub:

```bash
cd ~/vortex
git pull origin main
docker compose up -d --build
```
