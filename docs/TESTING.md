# BEAM Test Suite — Standardized Workflow

## Overview

BEAM uses a multi-tier testing strategy covering native Rust, WASM logic,
WASM network integration, and browser interop with Gun.js.

| Suite | Count | What It Tests | How to Run |
|---|---|---|---|
| Native unit | 320 | Core logic (parsing, serialization, graph ops, actor model) | `cargo test --lib` |
| Binary (CLI) | 21 | CLI argument parsing, env vars, subcommand dispatch | `cargo test --bin beam` |
| Integration | 10 | Multi-node mesh, relay forwarding, quorum, shutdown | `cargo test --test integration` |
| Wire live | 19 | Gun.js <-> BEAM interop over real WebSocket | `cargo test --test wire_live -- --test-threads=1` |
| WASM unit | 5 | Pure WASM logic (parsing, serialization, local put/get) | `wasm-pack test --node --no-default-features` |
| Node.js integration | 7 | WASM WebSocket networking in Node's real event loop | See below |
| Browser (Playwright) | 3 | Gun.js <-> BEAM interop in Chromium browser | See below |

**Total: 385 tests. All must pass before release.**

---

## Prerequisites

### Rust toolchain
```bash
source ~/.cargo/env
```

### Node.js (via NVM)
```bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
nvm use 22
```

### Build artifacts needed
```bash
# Native relay binary (needed for integration, wire_live, WASM integration, Playwright)
cargo build --bin beam

# WASM package — Node.js target (for Node integration tests)
wasm-pack build --target nodejs --no-default-features

# WASM package — browser target (for Playwright tests)
wasm-pack build --target web --no-default-features

# Copy browser WASM to browser-test directory
cp pkg/beam_bg.wasm pkg/beam.js pkg/beam.d.ts browser-test/
```

---

## Running Each Suite

### 1. Native Unit Tests (320)
```bash
cargo test --lib
```

### 2. Binary/CLI Tests (21)
```bash
cargo test --bin beam
```

### 3. Integration Tests (10)
```bash
cargo test --test integration
```

### 4. Wire Live Tests (19) — Gun.js Interop
```bash
# Must run single-threaded to avoid port conflicts
cargo test --test wire_live -- --test-threads=1
```

### 5. All Native Tests (one command)
```bash
cargo test
```

### 6. WASM Unit Tests (5)
```bash
wasm-pack test --node --no-default-features
```

### 7. Node.js WASM Integration Tests (7)
```bash
# Prerequisites: relay binary + nodejs-target WASM built
node tests/wasm-integration/node-integration.mjs
```

### 8. Browser/Playwright Tests (3)
```bash
# Prerequisites: relay binary + web-target WASM built + copied to browser-test/

# Start static file server for browser-test/
tmux new-session -d -s http 'cd browser-test && python3 -m http.server 8080'

# Run Playwright
npx playwright test tests/e2e/gun-beam-interop.spec.mjs --reporter=line

# Cleanup
tmux kill-session -t http
```

---

## Full Test Run (All Suites)

```bash
#!/bin/bash
set -e
source ~/.cargo/env
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
nvm use 22

cd /home/guan/src/beam

echo "=== Building artifacts ==="
cargo build --bin beam
wasm-pack build --target nodejs --no-default-features
wasm-pack build --target web --no-default-features
cp pkg/beam_bg.wasm pkg/beam.js pkg/beam.d.ts browser-test/

echo "=== Native tests ==="
cargo test

echo "=== WASM unit tests ==="
wasm-pack test --node --no-default-features

echo "=== Node.js WASM integration tests ==="
node tests/wasm-integration/node-integration.mjs

echo "=== Browser/Playwright tests ==="
tmux new-session -d -s http 'cd browser-test && python3 -m http.server 8080'
sleep 1
npx playwright test tests/e2e/gun-beam-interop.spec.mjs --reporter=line
tmux kill-session -t http

echo "=== ALL TESTS COMPLETE ==="
```

---

## Test Architecture

### Why WASM tests are split

`wasm-bindgen-test-runner` uses a microtask-based event loop executor. It
processes ALL microtasks before ANY I/O events (macrotasks). This means:

- Actor `handle()` calls (via `spawn_local`) run as microtasks
- WebSocket `onopen`/`onmessage` callbacks run as macrotasks
- ALL handle() calls execute before onopen fires → messages buffer to outbox
  but outbox is never flushed during the test

**Industry standard**: `wasm-bindgen-test` is for pure logic tests. Network
I/O tests belong in a real event loop (Node.js or browser).

- **`src/wasm_tests.rs`** (wasm-bindgen-test): parsing, serialization, local
  graph operations — no network I/O
- **`tests/wasm-integration/node-integration.mjs`** (Node.js): WebSocket
  connectivity, cross-talk, throughput — real event loop
- **`tests/e2e/gun-beam-interop.spec.mjs`** (Playwright/Chromium): full
  browser interop with Gun.js

### WASM WebSocket send strategy

`web_sys::WebSocket::ready_state()` is unreliable in WASM — it may report
`CONNECTING` (0) even after `onopen` has fired. Instead of checking
`ready_state()`, we call `send_with_str()` directly:

- If `Ok(())` — message sent successfully
- If `Err(_)` — WebSocket still in CONNECTING state, buffer to outbox
- `onopen` callback flushes the outbox when connection opens

This is more idiomatic than state-checking and avoids the unreliable
`ready_state()` entirely. Per MDN, `send()` throws `InvalidStateError`
during CONNECTING — so `Err` from `send_with_str()` is expected behavior.

---

## Clippy

Zero warnings required on both targets:
```bash
cargo clippy
cargo clippy --target wasm32-unknown-unknown --no-default-features
```

---

## Troubleshooting

### Port conflicts
```bash
# Kill stale relay processes
pkill -f "target/debug/beam"

# Kill processes on specific ports
fuser -k 4920/tcp 4930/tcp 4940/tcp 4950/tcp
```

### Stale WASM binary
If WASM tests pass (compiling from source) but Node.js integration fails,
the built pkg/ may be stale:
```bash
cargo clean --target wasm32-unknown-unknown
wasm-pack build --target nodejs --no-default-features
```

### Playwright browser not found
```bash
npx playwright install chromium
```

### npm/npx not found in SSH
NVM is not loaded in non-interactive shells. Always source it:
```bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
nvm use 22
```
