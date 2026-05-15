#!/usr/bin/env bash
set -e

# ───────────────────────────────────────────────────────────────
# Vortex Trading Bot — One-Command Setup
# ───────────────────────────────────────────────────────────────
# Supports: macOS, Linux, Windows (Git Bash / WSL)
# Usage:   chmod +x setup.sh && ./setup.sh
# ───────────────────────────────────────────────────────────────

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn()  { echo -e "${YELLOW}[!]${NC} $1"; }
err()   { echo -e "${RED}[✗]${NC} $1"; }

detect_os() {
    case "$(uname -s)" in
        Darwin*)  echo "macOS" ;;
        Linux*)   echo "Linux" ;;
        MINGW*|MSYS*|CYGWIN*) echo "Windows" ;;
        *)        echo "unknown" ;;
    esac
}

OS=$(detect_os)
echo -e "${BOLD}╔══════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║       Vortex Trading Bot — Setup        ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════╝${NC}"
echo ""
echo "Detected OS: $OS"
echo ""

# ── Step 1: Check Python ────────────────────────────────────
echo -e "${BOLD}Step 1/6 — Checking Python${NC}"
if command -v python3 &>/dev/null; then
    PY=$(python3 --version 2>&1)
    info "Python $PY"
else
    err "Python 3.12+ not found. Install from https://python.org"
    exit 1
fi

# ── Step 2: Virtual Environment + Dependencies ──────────────
echo ""
echo -e "${BOLD}Step 2/6 — Installing Python dependencies${NC}"
if [ ! -d "venv" ]; then
    python3 -m venv venv
    info "Virtual environment created"
fi
source venv/bin/activate
pip install --quiet -r requirements.txt
info "Python dependencies installed"

# ── Step 3: Environment file ────────────────────────────────
echo ""
echo -e "${BOLD}Step 3/6 — Environment file${NC}"
if [ ! -f ".env" ]; then
    cp .env.example .env
    warn ".env created from .env.example — you MUST edit it before running"
    echo "  Open .env and set:"
    echo "    - EXCHANGE_API_KEY / EXCHANGE_API_SECRET"
    echo "    - TELEGRAM_TOKEN / TELEGRAM_CHAT_ID"
    echo "    - DEEPSEEK_API_KEY (optional)"
    echo "    - SHARPE_API_KEY (optional)"
else
    info ".env already exists"
fi

# ── Step 4: Docker + Containers ─────────────────────────────
echo ""
echo -e "${BOLD}Step 4/6 — Docker containers${NC}"
if command -v docker &>/dev/null; then
    info "Docker found"
    docker compose up -d --build
    info "Containers started:"
    echo "    - vortex-bot      (trading engine)"
    echo "    - vortex-dashboard (web UI)"
    echo "    - redis           (live data)"
    echo "    - timescaledb     (trades + decisions)"
else
    warn "Docker not found — install from https://docker.com"
    echo "  Once installed, run: docker compose up -d"
fi

# ── Step 5: Tailscale Funnel ────────────────────────────────
echo ""
echo -e "${BOLD}Step 5/6 — Public dashboard (Tailscale Funnel)${NC}"
if command -v tailscale &>/dev/null; then
    info "Tailscale found"
    echo "  To expose dashboard publicly:"
    echo "    tailscale funnel 8000"
    echo ""
    echo "  Auto-start on boot:"
    case "$OS" in
        macOS)
            PLIST="$HOME/Library/LaunchAgents/com.vortex.funnel.plist"
            if [ ! -f "$PLIST" ]; then
                cat > "$PLIST" <<- 'EOPLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.vortex.funnel</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/tailscale</string>
        <string>funnel</string>
        <string>--bg</string>
        <string>8000</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardErrorPath</key>
    <string>/tmp/vortex-funnel.log</string>
    <key>StandardOutPath</key>
    <string>/tmp/vortex-funnel.log</string>
</dict>
</plist>
EOPLIST
                info "LaunchAgent created at $PLIST"
            else
                info "LaunchAgent already exists"
            fi
            ;;
        Linux)
            echo "  Create a systemd service or add to crontab:"
            echo "    @reboot /usr/bin/tailscale funnel --bg 8000"
            ;;
        Windows)
            echo "  Enable in Tailscale GUI: Settings → Funnel"
            ;;
    esac
else
    warn "Tailscale not found"
    echo "  Install:"
    case "$OS" in
        macOS) echo "    brew install --cask tailscale" ;;
        Linux) echo "    curl -fsSL https://tailscale.com/install.sh | sh" ;;
        Windows) echo "    Download from https://tailscale.com/download" ;;
    esac
    echo "  Then run: tailscale funnel 8000"
fi

# ── Step 6: Summary ─────────────────────────────────────────
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║               Setup Complete                ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${BOLD}Dashboard:${NC}   http://localhost:8000"
echo -e "  ${BOLD}Funnel:${NC}       https://YOUR-MACHINE.ts.net"
echo -e "  ${BOLD}Logs:${NC}         docker compose logs -f vortex-bot"
echo -e "  ${BOLD}Telegram:${NC}     /start on your bot"
echo ""
echo -e "  ${YELLOW}After first start, edit .env if you haven't, then:${NC}"
echo "    docker compose restart vortex-bot"
echo ""
echo -e "  ${BOLD}Commands:${NC}"
echo "    /status  — per-pair status + last decision"
echo "    /why     — diagnose why a pair isn't entering"
echo "    /suggest — scan for best scalping pairs"
echo "    /grid    — show active grid levels"
echo "    /kill    — emergency stop"
echo ""
