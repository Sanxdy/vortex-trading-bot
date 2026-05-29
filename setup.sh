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
echo -e "${BOLD}Step 1/8 — Checking Python${NC}"
if command -v python3 &>/dev/null; then
    PY=$(python3 --version 2>&1)
    info "Python $PY"
else
    err "Python 3.12+ not found. Install from https://python.org"
    exit 1
fi

# ── Step 2: Virtual Environment + Dependencies ──────────────
echo ""
echo -e "${BOLD}Step 2/8 — Installing Python dependencies${NC}"
SKIP_VENV=false
if ! python3 -m ensurepip --version &>/dev/null; then
    warn "ensurepip not available — skipping venv (Docker-only deploy is fine)"
    SKIP_VENV=true
fi
if [ "$SKIP_VENV" = false ]; then
    if [ ! -d "venv" ]; then
        python3 -m venv venv && info "Virtual environment created" || warn "venv creation failed — continuing with Docker deploy"
    fi
    if [ -f "venv/bin/activate" ]; then
        source venv/bin/activate
        pip install --quiet -r requirements.txt && info "Python dependencies installed"
    fi
fi

# ── Step 3: Environment file ────────────────────────────────
echo ""
echo -e "${BOLD}Step 3/8 — Environment file${NC}"
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

# ── Step 4: STB Infrastructure (Linux only) ─────────────────
echo ""
echo -e "${BOLD}Step 4/8 — STB hardware setup${NC}"
STB_CHANGED=false
if [ "$OS" = "Linux" ]; then
    # 4a. Swap (2G for 2GB RAM STB)
    if swapon --show | grep -q '/swapfile' 2>/dev/null; then
        info "Swap: already active (2G)"
    else
        sudo fallocate -l 2G /swapfile 2>/dev/null || sudo dd if=/dev/zero of=/swapfile bs=1M count=2048 status=none
        sudo chmod 600 /swapfile
        sudo mkswap /swapfile
        sudo swapon /swapfile
        grep -q '/swapfile' /etc/fstab 2>/dev/null && sudo sed -i '/swapfile/d' /etc/fstab
        echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
        info "Swap: 2G created"
    fi

    # 4b. USB auto-mount via fstab
    if grep -q '/mnt/usb' /etc/fstab 2>/dev/null; then
        info "USB fstab: already set"
    else
        USB_UUID=$(blkid /dev/sda1 -s UUID -o value 2>/dev/null || true)
        if [ -n "$USB_UUID" ]; then
            echo "UUID=$USB_UUID /mnt/usb ext4 defaults,noatime 0 2" | sudo tee -a /etc/fstab >/dev/null
            info "USB fstab: added ($USB_UUID)"
        else
            warn "USB drive not found at /dev/sda1 — skipping fstab"
        fi
    fi

    # 4c. Containerd symlink to USB
    if [ -L /var/lib/containerd ] && [ "$(readlink /var/lib/containerd)" = "/mnt/usb/containerd" ]; then
        info "Containerd: symlink OK"
    else
        sudo systemctl stop containerd docker 2>/dev/null || true
        sudo rm -rf /var/lib/containerd
        sudo mkdir -p /mnt/usb/containerd
        sudo ln -s /mnt/usb/containerd /var/lib/containerd
        sudo systemctl start containerd docker
        STB_CHANGED=true
        info "Containerd: symlinked to USB, Docker restarted"
    fi

    # 4d. Docker daemon.json (store images on USB)
    if [ -f /etc/docker/daemon.json ]; then
        info "daemon.json: already exists"
    else
        sudo mkdir -p /etc/docker
        echo '{"data-root": "/mnt/usb/docker"}' | sudo tee /etc/docker/daemon.json >/dev/null
        if systemctl is-active --quiet docker; then
            sudo systemctl restart docker
        else
            sudo systemctl start docker
        fi
        STB_CHANGED=true
        info "daemon.json: created (data-root: /mnt/usb/docker), Docker started"
    fi
else
    info "STB setup: skipped (not Linux)"
fi

# ── Step 5: Binance SSL Check & Auto-Fix ────────────────────
echo ""
echo -e "${BOLD}Step 5/8 — Binance SSL check${NC}"
BINANCE_FIXED=false
if curl -s --max-time 5 https://api.binance.com/api/v3/ping >/dev/null 2>&1; then
    info "Binance SSL: OK"
else
    warn "Binance SSL failed — attempting auto-fix"
    BINANCE_HOSTS="api.binance.com fapi.binance.com dapi.binance.com papi.binance.com"
    HOSTS_FILE="/tmp/binance-hosts.txt"
    OVERRIDE_FILE="docker-compose.override.yml"
    rm -f "$HOSTS_FILE"

    for HOST in $BINANCE_HOSTS; do
        IP=""
        IP=$(curl -s "https://dns.adguard-dns.com/resolve?name=$HOST&type=A" 2>/dev/null | \
            python3 -c "import json,sys;d=json.load(sys.stdin);[print(a['data']) for a in d.get('Answer',[]) if a['type']==1]" 2>/dev/null | head -1)
        if [ -z "$IP" ]; then
            case "$HOST" in
                "api.binance.com") IP="18.64.21.130" ;;
                "fapi.binance.com") IP="108.138.141.5" ;;
                "dapi.binance.com") IP="13.192.247.222" ;;
                "papi.binance.com") IP="16.76.102.8" ;;
            esac
        fi
        if [ -n "$IP" ]; then
            echo "$IP $HOST" >> "$HOSTS_FILE"
        fi
    done

    if [ -s "$HOSTS_FILE" ]; then
        while read -r LINE; do
            HOST=$(echo "$LINE" | awk '{print $2}')
            if ! grep -qF "$HOST" /etc/hosts 2>/dev/null; then
                echo "$LINE" | sudo tee -a /etc/hosts >/dev/null
            fi
        done < "$HOSTS_FILE"

        echo "services:" > "$OVERRIDE_FILE"
        for SERVICE in vortex-bot dashboard; do
            echo "  $SERVICE:" >> "$OVERRIDE_FILE"
            echo "    extra_hosts:" >> "$OVERRIDE_FILE"
            while read -r LINE; do
                IP=$(echo "$LINE" | awk '{print $1}')
                HOST=$(echo "$LINE" | awk '{print $2}')
                echo "      - \"$HOST:$IP\"" >> "$OVERRIDE_FILE"
            done < "$HOSTS_FILE"
        done
        BINANCE_FIXED=true
    fi

    if [ "$BINANCE_FIXED" = true ]; then
        if curl -s --max-time 5 https://api.binance.com/api/v3/ping >/dev/null 2>&1; then
            info "Binance SSL: fixed ✅"
        else
            warn "Binance SSL: still failing — /etc/hosts added, docker-compose.override.yml created, but SSL still fails"
        fi
    else
        warn "Binance SSL: could not resolve real IPs — no fix applied"
    fi
fi

# ── Step 6: Docker + Containers ─────────────────────────────
echo ""
echo -e "${BOLD}Step 6/8 — Docker containers${NC}"
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

# ── Step 7: Tailscale Funnel + Auto-Start ───────────────────
echo ""
echo -e "${BOLD}Step 7/8 — Public dashboard (Tailscale Funnel)${NC}"
FUNNEL_URL=""
if command -v tailscale &>/dev/null; then
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
            launchctl load "$PLIST" 2>/dev/null || true
            ;;
        Linux)
            SVC="/etc/systemd/system/vortex.service"
            if [ -f "$SVC" ]; then
                info "vortex.service: already installed"
            else
                if [ -f "deploy/vortex.service" ]; then
                    sudo cp deploy/vortex.service "$SVC"
                    sudo systemctl daemon-reload
                    info "vortex.service: installed"
                else
                    warn "deploy/vortex.service not found — skipping systemd install"
                fi
            fi
            if systemctl is-enabled vortex.service &>/dev/null; then
                info "vortex.service: already enabled"
            else
                sudo systemctl enable vortex.service
                info "vortex.service: enabled to start on boot"
            fi
            if systemctl is-active --quiet vortex.service; then
                info "vortex.service: already running"
            else
                sudo systemctl start vortex.service
                info "vortex.service: started"
            fi
            ;;
        Windows)
            info "Tailscale found"
            echo "  Enable Funnel in Tailscale GUI: Settings → Funnel"
            ;;
    esac

    # Ensure Funnel is active
    if tailscale funnel status 2>/dev/null | grep -q 'Funnel on'; then
        info "Funnel: already active"
    else
        tailscale funnel --bg --https=443 http://localhost:8000
        sleep 2
        info "Funnel: activated"
    fi

    # Get the public URL
    HOST=$(tailscale hostname 2>/dev/null || hostname -s)
    TAILNET=$(tailscale debug tailnet-name 2>/dev/null || true)
    if [ -n "$TAILNET" ]; then
        FUNNEL_URL="https://${HOST}.${TAILNET}.ts.net"
    else
        FUNNEL_URL="https://${HOST}.ts.net"
    fi
else
    warn "Tailscale not found"
    echo "  Install:"
    case "$OS" in
        macOS) echo "    brew install --cask tailscale" ;;
        Linux) echo "    curl -fsSL https://tailscale.com/install.sh | sh" ;;
        Windows) echo "    Download from https://tailscale.com/download" ;;
    esac
    echo "  Then run: setup.sh again after installing"
fi

# ── Step 7: Summary ──────────────────────────────────────────
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║               Setup Complete                ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${BOLD}Dashboard:${NC}   http://localhost:8000"
if [ -n "$FUNNEL_URL" ]; then
    echo -e "  ${BOLD}Funnel URL:${NC}   $FUNNEL_URL"
    warn "Dashboard has no login — anyone with the URL can see this bot."
fi
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
echo -e "  ${BOLD}Rollback:${NC}     sudo systemctl disable --now vortex.service && tailscale funnel reset"
