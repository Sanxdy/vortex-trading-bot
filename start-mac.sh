#!/bin/bash
set -e

echo "========================================="
echo "  Vortex — Mac Startup"
echo "========================================="
echo ""

# 1. Start Redis if not already running
if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^vortex-redis$'; then
  echo "  📦 Starting Redis..."
  docker run -d --name vortex-redis -p 6379:6379 redis:7-alpine 2>/dev/null || docker start vortex-redis 2>/dev/null
fi

# 2. Start TimescaleDB if not already running
if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^vortex-timescaledb$'; then
  # Remove any stale container that failed
  docker rm vortex-timescaledb 2>/dev/null || true
  echo "  📦 Starting TimescaleDB..."
  docker run -d --name vortex-timescaledb \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=vortex_password \
    -e POSTGRES_USER=vortex \
    -e POSTGRES_DB=vortex_trades \
    timescale/timescaledb:latest-pg15
fi

# Wait for TimescaleDB to accept connections
echo "  ⏳ Waiting for TimescaleDB..."
for i in $(seq 1 12); do
  if docker exec vortex-timescaledb pg_isready 2>/dev/null; then
    echo "  ✅ TimescaleDB ready"
    sleep 2
    break
  fi
  sleep 5
done

# 3. Activate venv
source .venv/bin/activate

# Skip Telegram on Mac — avoids flood control errors
export TELEGRAM_TOKEN=""

# 4. Start spot bot
echo "  🤖 Starting Spot bot..."
REDIS_HOST=localhost \
TIMESCALE_DB_HOST=localhost \
python -m src.main &
echo $! > /tmp/vortex-spot.pid
sleep 2

# 5. Start futures bot
echo "  🤖 Starting Futures bot..."
set -a
source .env.futures 2>/dev/null
set +a
REDIS_HOST=localhost \
TIMESCALE_DB_HOST=localhost \
python -m src.main_futures &
echo $! > /tmp/vortex-futures.pid
sleep 2

# 6. Start dashboard
echo "  📊 Starting Dashboard..."
REDIS_HOST=localhost \
TIMESCALE_DB_HOST=localhost \
python -m dashboard.app &
echo $! > /tmp/vortex-dashboard.pid

echo ""
echo "========================================="
echo "  ✅ All Vortex services running!"
echo ""
echo "  Dashboard  →  http://localhost:8000"
echo "  Remote     →  https://sandys-macbook-pro.taild68cf9.ts.net/"  
echo "  Login      →  admin / B01l1ng@1"
echo ""
echo "  Stop all   →  kill \$(cat /tmp/vortex-*.pid)"
echo "========================================="
