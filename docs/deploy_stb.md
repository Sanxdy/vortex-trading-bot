# STB Deployment Guide — HG680P (Amlogic S905, 2GB RAM, 8GB eMMC)

## Prerequisites

| Item | Status |
|------|--------|
| HG680P STB | ✅ |
| Armbian installed | ✅ |
| USB drive formatted ext4 | ✅ |
| SSH access | ✅ |
| Git installed | ✅ |
| Docker installed | ✅ |
| Tailscale installed + logged in | ✅ |

---

## One-Command Setup

```bash
# 1. Clone and enter project
git clone https://github.com/Sanxdy/vortex-trading-bot.git ~/vortex
cd ~/vortex

# 2. Create .env with your API keys (REQUIRED — do this before setup.sh)
cp .env.example .env
nano .env
```

Edit `.env` to set:
- `EXCHANGE_API_KEY` / `EXCHANGE_API_SECRET` — Binance testnet or mainnet
- `TELEGRAM_TOKEN` / `TELEGRAM_CHAT_ID` — Telegram bot
- `SIMULATED_BALANCE` — paper balance for testnet (e.g. `250`)

```bash
# 3. Run setup (swap, USB mount, Docker, systemd service, Funnel — all idempotent)
sudo bash setup.sh
```

After completion, the script prints the **Funnel URL** — open it on your phone.  
Reboot the STB: everything comes back automatically via `vortex.service`.

> If your ISP blocks Binance (Telkomsel/others), Step 5 auto-detects the SSL failure,
> resolves the real Binance IPs via AdGuard DNS, and applies the fix to both the host
> (`/etc/hosts`) and Docker containers (`docker-compose.override.yml`).

---

## ⚠️ Security

The dashboard has **no login**. Anyone with the Funnel URL can see live positions, PnL, and config. The URL contains a random tailnet hash and is not guessable — treat it as a shared secret.

---

## What setup.sh Does

| Step | What | Idempotent |
|:----:|------|:----------:|
| 4a | 2G swap file (prevents OOM) | ✅ skips if active |
| 4b | USB mount in fstab (auto-mount on boot) | ✅ skips if present |
| 4c | containerd symlink → USB (image storage) | ✅ skips if correct |
| 4d | Docker daemon.json (data-root on USB) | ✅ skips if exists |
| **5** | **Binance SSL auto-fix** — detects ISP interception, resolves real IPs via AdGuard DNS, writes `/etc/hosts` + `docker-compose.override.yml` | ✅ skips if SSL works |
| 6 | docker compose up -d --build | ✅ skips if running |
| 7 | Install + enable vortex.service (auto-start on boot) | ✅ skips if installed |
| 7 | tailscale funnel --bg (public URL) | ✅ skips if active |

---

## Updating the Bot

```bash
cd ~/vortex
git pull origin main
sudo bash setup.sh
```

The script re-runs all steps. Existing config is preserved; only changed files redeploy.

## Rollback

```bash
sudo systemctl disable --now vortex.service
tailscale funnel reset
```

---

## Manual Verification

```bash
# Containers running
docker ps

# Funnel active
tailscale funnel status

# Dashboard locally
curl -s http://localhost:8000/api/status | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print('Online:', d.get('online'))"
```

---

## First-Run Timeline

| Timeframe | What happens |
|:---------:|-------------|
| 0-5 min | Bot backfills candles, connects to exchange |
| 5-60 min | First signals evaluated |
| 1-24 hours | First trade enters |
| 24-48 hours | First trade closes |

---

## Monthly Maintenance

```bash
docker system prune -f
df -h
docker exec vortex-timescaledb-1 psql -U vortex -d vortex_trades -c \
  "SELECT ROUND(SUM(realized_pnl),2) FROM trades WHERE realized_pnl IS NOT NULL AND timestamp > NOW() - INTERVAL '7 days'"
```
