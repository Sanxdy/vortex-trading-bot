#!/bin/bash
# Pre-Deploy Gate — enforces SOP before every push
# Usage: ./scripts/pre_deploy_check.sh [deploy]
#
# Without 'deploy' argument: read-only check, outputs pass/fail
# With 'deploy' argument: prompts user to confirm, then exits clean
#
# This script MUST be run BEFORE any push to the server.
# If it fails, you are not allowed to deploy. Period.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

fail_count=0
pass_count=0

pass() {
  pass_count=$((pass_count + 1))
  echo -e "  ${GREEN}✅${NC} $1"
}

fail() {
  fail_count=$((fail_count + 1))
  echo -e "  ${RED}❌${NC} $1"
}

echo ""
echo "============================================="
echo "      PRE-DEPLOY GATE — SOP ENFORCEMENT      "
echo "============================================="
echo ""

# ── 1. Uncommitted changes check ──
echo "[1/7] Checking working tree..."
if [ -n "$(git status --porcelain)" ]; then
  echo ""
  echo "  ${YELLOW}⚠️  Uncommitted changes detected:${NC}"
  git status --short
  echo ""
  echo "  ${YELLOW}Please commit or stash before deploying.${NC}"
  echo ""
  fail "Uncommitted changes"
else
  pass "Working tree clean"
fi

# ── 2. Syntax check all changed Python files ──
echo "[2/7] Syntax checking changed files..."
changed_files=$(git diff HEAD~1 --name-only --diff-filter=ACMR 2>/dev/null || echo "")
syntax_errors=0
for f in $changed_files; do
  case "$f" in
    *.py)
      if ! python3 -m py_compile "$f" 2>/dev/null; then
        echo "  Syntax error in: $f"
        syntax_errors=$((syntax_errors + 1))
      fi
      ;;
  esac
done
if [ "$syntax_errors" -gt 0 ]; then
  fail "$syntax_errors file(s) have syntax errors"
else
  pass "All Python files pass syntax check"
fi

# ── 3. Check SOP checklist in commit message ──
echo "[3/7] Checking SOP checklist..."
recent_files=$(git diff HEAD~1 --name-only --diff-filter=ACMR 2>/dev/null || echo "")
sop_pattern="src/executor.py\|config/config.yaml\|config-futures.yaml\|src/strategist\|src/entry_conditions\|src/exchange_wrapper\|src/db\|src/main_futures\|dashboard/app.py"
touches_sop=false
for f in $recent_files; do
  if echo "$f" | grep -q "$sop_pattern"; then
    touches_sop=true
    break
  fi
done

if $touches_sop; then
  msg=$(git log -1 --format="%B" 2>/dev/null || echo "")
  if echo "$msg" | grep -qE "\[x\]"; then
    pass "SOP checklist found in commit message"
  else
    fail "SOP checklist missing from commit message"
  fi
else
  pass "No SOP-relevant files changed (skipping checklist check)"
fi

# ── 4. Docker build check ──
echo "[4/7] Checking Docker build..."
dashboard_changed=false
bot_changed=false
for f in $recent_files; do
  case "$f" in
    dashboard/*|Dockerfile.dash) dashboard_changed=true ;;
    src/*|config*|Dockerfile.bot|requirements*) bot_changed=true ;;
  esac
done

if $dashboard_changed; then
  if docker build -f Dockerfile.dash -t vortex-dashboard-test . --quiet 2>/dev/null; then
    pass "Dashboard Docker image builds"
    docker rmi vortex-dashboard-test >/dev/null 2>&1 || true
  else
    fail "Dashboard Docker build failed"
  fi
fi

if $bot_changed; then
  if docker build -f Dockerfile.bot -t vortex-bot-test . --quiet 2>/dev/null; then
    pass "Bot Docker image builds"
    docker rmi vortex-bot-test >/dev/null 2>&1 || true
  else
    fail "Bot Docker build failed"
  fi
fi

if ! $dashboard_changed && ! $bot_changed; then
  pass "No Docker-relevant files changed (skipping build check)"
fi

# ── 5. Baseline snapshot reminder ──
echo "[5/7] Baseline snapshot..."
if $touches_sop; then
  echo ""
  echo "  ${YELLOW}⚠️  SOP requires a baseline snapshot before deploying.${NC}"
  echo "  Run these BEFORE deploying:"
  echo ""
  echo "    # For data pipeline changes:"
  echo "    curl -s http://localhost:8000/futures/api/conditions | python3 -c"
  echo "    \"import sys,json; d=json.load(sys.stdin); [print(k,':',v.get('adx'),v.get('rsi')) for k,v in d.items() if k not in ['_meta','_stats']][:3]\""
  echo ""
  echo "    # For balance/PnL changes:"
  echo "    curl -s http://localhost:8000/futures/api/pnl/summary"
  echo ""
  echo "  ${YELLOW}After deploy, re-run the same commands and compare.${NC}"
  echo ""
  if [ "${BASELINE_CAPTURED:-0}" = "1" ]; then
    pass "Baseline override acknowledged"
  else
    fail "Baseline not captured (run command above first, or set BASELINE_CAPTURED=1)"
  fi
else
  pass "No SOP-relevant changes (baseline not required)"
fi

# ── 6. Summary ──
echo "[6/7] Summary..."
echo ""
if [ "$fail_count" -gt 0 ]; then
  echo -e "  ${RED}${fail_count} check(s) FAILED — cannot deploy${NC}"
  echo -e "  ${GREEN}${pass_count} check(s) passed${NC}"
  echo ""
  echo "  Fix the failures above, commit, and re-run this script."
  exit 1
else
  echo -e "  ${GREEN}All checks passed${NC}"
fi

# ── 7. User confirmation ──
echo "[7/7] User confirmation..."
if [ "${1:-}" = "deploy" ]; then
  echo ""
  echo "  Changes to deploy:"
  git log --oneline HEAD~3..HEAD 2>/dev/null || echo "  (single commit)"
  echo ""
  echo -n "  Proceed with deploy to server? [y/N] "
  read -r answer
  if [ "$answer" != "y" ] && [ "$answer" != "Y" ]; then
    echo "  Deploy cancelled."
    exit 1
  fi
  pass "User confirmed deployment"
else
  echo ""
  echo "  ${YELLOW}Read-only check complete. Run with 'deploy' argument to confirm.${NC}"
  echo "  Usage: ./scripts/pre_deploy_check.sh deploy"
fi

echo ""
echo "============================================="
if [ "$fail_count" -eq 0 ]; then
  echo -e "      ${GREEN}PRE-DEPLOY GATE: PASSED${NC}"
else
  echo -e "      ${RED}PRE-DEPLOY GATE: FAILED${NC}"
fi
echo "============================================="
echo ""
exit $([ "$fail_count" -gt 0 ] && echo 1 || echo 0)
