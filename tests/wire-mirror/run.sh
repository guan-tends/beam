#!/usr/bin/env bash
# Layer 2: Node.js mirror tests for BEAM Gun.js wire compatibility.
#
# Usage:  ./run.sh
#
# Requires Node.js 18+ (for node:test built-in runner).
# Dependencies are installed automatically on first run.

set -euo pipefail
cd "$(dirname "$0")"

# Ensure deps are installed
if [ ! -d "node_modules" ]; then
  echo "Installing dependencies..."
  npm ci 2>/dev/null || npm install
fi

echo "Running BEAM ↔ Gun.js wire mirror tests..."
node mirror_test.js
