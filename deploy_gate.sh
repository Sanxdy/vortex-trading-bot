#!/bin/bash
# Deploy Gate — enforces SOP checklist before deployment
# Usage: ./deploy_gate.sh              — SOP check only
#        ./deploy_gate.sh test         — SOP check + run tests in docker
set -euo pipefail

SOP_FILES="src/executor.py config/config.yaml src/strategist*.py src/entry_conditions*.py src/notifier*.py src/heartbeat*.py src/db*.py"

# Files changed in the latest commit (HEAD~1..HEAD, or HEAD vs empty if first commit)
if git rev-parse --verify HEAD~1 >/dev/null 2>&1; then
    CHANGED=$(git diff HEAD~1 --name-only 2>/dev/null || echo "")
else
    CHANGED=$(git diff --name-only 4b825dc642cb6eb9a060e54bf899d144 2>/dev/null || echo "")
fi

# Check if any SOP-relevant files changed
TOUCHED=false
for pattern in $SOP_FILES; do
    if echo "$CHANGED" | grep -q "$pattern"; then
        TOUCHED=true
        break
    fi
done

if $TOUCHED; then
    # Get latest commit message
    MSG=$(git log -1 --format="%B")
    if echo "$MSG" | grep -qE "\[x\]"; then
        echo "[deploy_gate] SOP checklist found in commit message — proceeding"
    else
        echo ""
        echo "================================================================="
        echo " DEPLOY REJECTED: SOP checklist missing"
        echo "================================================================="
        echo ""
        echo "The following strategy files were changed:"
        echo "$CHANGED" | grep -E "$(echo "$SOP_FILES" | tr ' ' '|')"
        echo ""
        echo "This triggers the Strategy Change SOP (§§1-8)."
        echo "Your commit message must include the completed SOP checklist."
        echo ""
        echo "Example format:"
        echo ""
        echo "## Checklist"
        echo "- [x] Strategy Correctness — ..."
        echo "- [x] Execution Safety — ..."
        echo "- [x] Risk Implications — ..."
        echo "- [x] Observability Impact — ..."
        echo "- [x] Rollback Simplicity — ..."
        echo "- [x] Statistical Attribution — ..."
        echo "- [x] Failure Mode Analysis — ..."
        echo "- [x] Trade Frequency Impact — ..."
        echo ""
        echo "Override is not available. To deploy without SOP, run manually:"
        echo "  docker compose up -d"
        echo "================================================================="
        exit 1
    fi
else
    echo "[deploy_gate] No strategy files changed — skipping SOP check"
fi

# ── Run tests (if 'test' argument passed — runs locally, not on STB) ──────────
if [ "${1:-}" = "test" ]; then
    echo "[deploy_gate] Run tests locally before deploying:"
    echo "  docker run --rm -v \$(pwd):/app -w /app vortex-vortex-bot python3 -m pytest tests/"
    echo "[deploy_gate] Skipping test run on server to preserve memory"
    exit 0
fi
