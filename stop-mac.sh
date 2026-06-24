#!/bin/bash
echo "🛑 Stopping Vortex services..."

# Kill Python processes
kill $(cat /tmp/vortex-*.pid) 2>/dev/null
rm -f /tmp/vortex-*.pid

# Stop Docker containers (keep data)
docker stop vortex-redis vortex-timescaledb 2>/dev/null

echo "All services stopped."
