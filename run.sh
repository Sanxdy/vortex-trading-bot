#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# macOS: prevent idle sleep while bot runs (screen can still lock/turn off)
if [[ "$(uname)" == "Darwin" ]]; then
  exec caffeinate -i python src/main.py "$@"
else
  exec python src/main.py "$@"
fi
