#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "🚀 Starting Vortex dashboard + Tailscale Funnel..."
echo ""

# Step 1: Ensure dashboard is running
echo "1/3 Starting dashboard container..."
docker compose up -d dashboard
echo "   ✅ Dashboard running at http://localhost:8000"
echo ""

# Step 2: Ensure tailscale funnel is active
echo "2/3 Enabling Tailscale Funnel..."
tailscale funnel --yes --bg 8000 > /dev/null 2>&1
echo "   ✅ Funnel enabled"
echo ""

# Step 3: Print access URL
echo "3/3 Your dashboard URL:"
MACHINE=$(tailscale status --json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print([v['DNSName'] for k,v in d['Peer'].items() if v.get('Self')][0])" 2>/dev/null || echo "unknown")
if [ "$MACHINE" != "unknown" ]; then
    echo ""
    echo "   🔗 https://$MACHINE"
    echo ""
    echo "   Open in browser or scan QR to share."
else
    echo ""
    echo "   🔗 https://$(hostname -s).tail*.ts.net/"
    echo "   (Run 'tailscale funnel status' to find exact URL)"
fi
echo ""
echo "✨ Dashboard is live. Press Ctrl+C to stop the script (tunnel stays active)."
echo "   To stop the tunnel: tailscale funnel reset"
echo "   To stop the dashboard: docker compose stop dashboard"

# Keep script alive so user sees the URL
wait 2>/dev/null || sleep infinity
