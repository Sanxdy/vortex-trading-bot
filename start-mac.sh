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
  echo "  📦 Starting TimescaleDB..."
  docker run -d --name vortex-timescaledb -p 5432:5432 timescale/timescaledb:latest-pg15 2>/dev/null || docker start vortex-timescaledb 2>/dev/null
fi

# Wait for DB to accept connections
echo "  ⏳ Waiting for TimescaleDB..."
sleep 5

# 3. Activate venv
source .venv/bin/activate

# 4. Start spot bot
echo "  🤖 Starting Spot bot..."
REDIS_HOST=localhost \
TIMESCALE_DB_HOST=localhost \
python -m src.main &
echo $! > /tmp/vortex-spot.pid
sleep 2

# 5. Start futures bot
echo "  🤖 Starting Futures bot..."
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
