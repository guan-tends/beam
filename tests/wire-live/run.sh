#!/usr/bin/env bash
# Layer 3: Live integration tests for BEAM ↔ Gun.js wire compatibility.
#
# Usage:  ./run.sh
#
# Requires Node.js 18+ and Rust toolchain.
# Dependencies are installed automatically on first run.

set -euo pipefail
cd "$(dirname "$0")"

# Ensure Gun.js is installed
if [ ! -d "node_modules" ]; then
  echo "Installing dependencies..."
  npm ci 2>/dev/null || npm install
fi

# Run the live integration tests
echo "Running BEAM ↔ Gun.js live integration tests..."
cd ../..
cargo test --test wire_live -- --ignored
