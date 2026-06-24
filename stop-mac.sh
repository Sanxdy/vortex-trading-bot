#!/bin/bash
echo "🛑 Stopping Vortex services..."
kill $(cat /tmp/vortex-*.pid) 2>/dev/null
rm -f /tmp/vortex-*.pid
echo "All services stopped."
