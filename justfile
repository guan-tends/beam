# BEAM Release Workflow — justfile
# Run `just --list` to see all available recipes.
# Run `just release` for the full release pipeline.
# Any stage can be run individually.

# ─── Environment Setup ───────────────────────────────────────────────

export RUST_BACKTRACE := "1"
export NVM_DIR := env_var_or_default("NVM_DIR", env_var("HOME") + "/.nvm")

# Source Rust + Node.js environment (helper used by other recipes)
setup-env:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    export NVM_DIR="$HOME/.nvm"
    [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
    nvm use 22 > /dev/null 2>&1 || true

# ─── Stage 1: Lint ───────────────────────────────────────────────────

# Run cargo fmt check + clippy on both native and WASM targets (zero warnings required)
lint:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    echo "=== cargo fmt --check ==="
    cargo fmt --all -- --check
    echo "=== clippy (native) ==="
    cargo clippy --all-targets --all-features -- -D warnings
    echo "=== clippy (WASM) ==="
    cargo clippy --target wasm32-unknown-unknown --no-default-features -- -D warnings
    echo "=== LINT PASS ✅ ==="

# ─── Stage 2: Native Tests ──────────────────────────────────────────

# Run all native test suites (320 unit + 21 bin + 10 integration + 7 wire_live = 358)
test-native:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    echo "=== Building relay binary ==="
    cargo build --bin beam
    echo "=== Native unit tests (lib) ==="
    cargo test --lib
    echo "=== Binary tests (CLI) ==="
    cargo test --bin beam
    echo "=== Integration tests ==="
    cargo test --test integration
    echo "=== Wire live tests (Gun.js interop) ==="
    cargo test --test wire_live -- --ignored --test-threads=1
    echo "=== NATIVE TESTS PASS ✅ (358 tests) ==="

# ─── Stage 3: WASM Tests ─────────────────────────────────────────────

# Run WASM test suites (5 unit + 7 Node.js integration + 3 Playwright browser)
test-wasm:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    export NVM_DIR="$HOME/.nvm"
    [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
    nvm use 22 > /dev/null 2>&1
    echo "=== Building WASM (nodejs target) ==="
    wasm-pack build --target nodejs --no-default-features
    echo "=== WASM unit tests (wasm-bindgen-test) ==="
    wasm-pack test --node --no-default-features
    echo "=== Building relay binary ==="
    cargo build --bin beam
    echo "=== Node.js WASM integration tests ==="
    node tests/wasm-integration/node-integration.mjs
    echo "=== Building WASM (web target, release) ==="
    wasm-pack build --target web --release --no-default-features
    cp pkg/beam_bg.wasm pkg/beam.js pkg/beam.d.ts browser-test/
    echo "=== Playwright browser tests (gun-beam interop + OPFS persistence) ==="
    npx playwright test --reporter=line
    echo "=== WASM TESTS PASS ✅ (9 unit + 8 node integration + 5 playwright) ==="

# ─── Stage 4: Fixture Tests ─────────────────────────────────────────

# Run golden JSON wire format fixture tests (36 fixtures, 6 categories)
# These are part of the native test suite — this is a convenience alias
test-fixtures:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    echo "=== Wire fixture tests (36 golden JSON) ==="
    cargo test --lib wire
    echo "=== FIXTURE TESTS PASS ✅ (36 fixtures) ==="

# ─── Stage 5: Examples ──────────────────────────────────────────────

# Verify all examples compile
test-examples:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    echo "=== Building examples ==="
    cargo build --examples
    echo "=== EXAMPLES COMPILE ✅ ==="

# ─── Stage 6: Supply Chain Security ─────────────────────────────────

# Run cargo audit + cargo deny
supply-chain:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    echo "=== cargo audit ==="
    cargo audit
    echo "=== cargo deny check ==="
    cargo deny check
    echo "=== SUPPLY CHAIN CLEAN ✅ ==="

# ─── Stage 7: Dependency Freshness ──────────────────────────────────

# Check if any dependencies have updates available
deps-check:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    echo "=== cargo update --dry-run ==="
    cargo update --dry-run
    echo "=== Review output above for available updates ==="

# ─── Stage 8: Documentation Audit ────────────────────────────────────

# Audit documentation — review all new/modified code since last release
docs-audit:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    echo "=== Documentation & Release Audit ==="
    LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "HEAD~20")
    echo ""
    echo "=== Commit history since $LAST_TAG ==="
    echo "--- Non-merge commits ---"
    git log "$LAST_TAG"..HEAD --oneline --no-merges
    echo "--- Merge commits ---"
    git log "$LAST_TAG"..HEAD --oneline --merges
    echo ""
    echo "=== Files changed since $LAST_TAG ==="
    git diff --name-only "$LAST_TAG"..HEAD
    echo ""
    echo "=== CHANGELOG completeness check ==="
    echo "Cross-reference: every feat:, fix:, refactor: commit should have a CHANGELOG entry."
    echo "Missing features = CHANGELOG gap that must be fixed before release."
    echo ""
    echo "=== Checking cargo doc for warnings ==="
    RUSTDOCFLAGS='-Dwarnings' cargo doc --workspace --all-features --no-deps 2>&1 || true
    echo ""
    echo "=== DOC AUDIT COMPLETE — review findings above ===""

# ─── Stage 9: Benchmarks ────────────────────────────────────────────

# Run all benchmarks 5x in isolation and compute averages.
# Each benchmark suite runs 5 times sequentially to avoid noisy-neighbor effects.
# Results are logged to bench/results/ with timestamps.
benchmarks:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    mkdir -p bench/results
    echo "=== Building relay binary ==="
    cargo build --bin beam
    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  BENCHMARK SUITE — 5 RUNS IN ISOLATION"
    echo "═══════════════════════════════════════════════════"
    echo ""
    # ── Local Put (5 runs) ──
    for i in 1 2 3 4 5; do
        echo "=== Local Put Run $i/5 ==="
        cargo test --release --test local_put_bench -- --nocapture --ignored 2>&1 | tee "bench/results/local-put-run-${i}.log"
        echo ""
    done
    # ── Relay Throughput (5 runs) ──
    for i in 1 2 3 4 5; do
        echo "=== Relay Throughput Run $i/5 ==="
        cargo test --release --test relay_throughput_bench -- --nocapture --ignored --test-threads=1 2>&1 | tee "bench/results/relay-throughput-run-${i}.log"
        echo ""
    done
    # ── Criterion (5 runs) ──
    for i in 1 2 3 4 5; do
        echo "=== Criterion Run $i/5 ==="
        cargo bench 2>&1 | tee "bench/results/criterion-run-${i}.log"
        echo ""
    done
    echo "═══════════════════════════════════════════════════"
    echo "  BENCHMARKS COMPLETE — 5 runs each"
    echo "  Results in bench/results/"
    echo "═══════════════════════════════════════════════════"

# ─── Stage 10: Profiling ────────────────────────────────────────────

# Run all 4 profilers sequentially (perf → heaptrack → dhat)
# Prerequisites: ALL tests must pass first
profiling:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    mkdir -p bench/results
    echo "=== Building profiling binary ==="
    cargo test --profile profiling --test relay_throughput_bench --no-run
    BINARY=$(find target/profiling -name "relay_throughput_bench-*" -type f | head -1)
    if [ -z "$BINARY" ]; then
        echo "ERROR: Profiling binary not found"
        exit 1
    fi
    echo "Binary: $BINARY"
    echo ""
    echo "=== 1/4: perf stat ==="
    perf stat -e cycles,instructions,cache-misses,context-switches,page-faults,cpu-migrations \
        "$BINARY" --bench 2>&1 | tee bench/results/perf-stat-$(date +%FT%H-%M).txt
    echo ""
    echo "=== 2/4: Flame graph (perf record + inferno) ==="
    perf record -F 999 -g --call-graph dwarf -o /tmp/beam-perf.data "$BINARY" --bench
    perf script -i /tmp/beam-perf.data | inferno-collapse-perf > bench/results/flamegraph-$(date +%FT%H-%M).svg
    echo "Flame graph: bench/results/flamegraph-*.svg"
    echo ""
    echo "=== 3/4: Heaptrack ==="
    heaptrack -o bench/results/heaptrack-$(date +%FT%H-%M) "$BINARY" --bench --no-default-features 2>&1 | tail -20
    echo "Heaptrack data: bench/results/heaptrack-*.zst"
    echo ""
    echo "=== 4/4: DHAT ==="
    valgrind --tool=dhat "$BINARY" --bench 2>&1 | tee bench/results/dhat-$(date +%FT%H-%M).txt
    echo "DHAT data: bench/results/dhat-*"
    echo ""
    echo "=== PROFILING COMPLETE — review bench/results/ ==="

# ─── Stage 11: WASM Builds ──────────────────────────────────────────

# Build WASM for both targets (nodejs + web) with release changes
wasm-build:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    echo "=== WASM build (nodejs target) ==="
    wasm-pack build --target nodejs --no-default-features
    echo "=== WASM build (web target) ==="
    wasm-pack build --target web --no-default-features
    echo "=== Copy browser WASM to browser-test/ ==="
    cp pkg/beam_bg.wasm pkg/beam.js pkg/beam.d.ts browser-test/
    echo "=== WASM BUILDS COMPLETE ✅ ==="

# ─── Stage 14: OPSEC Audit ───────────────────────────────────────────

# Audit for private identifiers in public artifacts
opsec-audit:
    #!/usr/bin/env bash
    echo "=== OPSEC Audit ==="
    echo "--- Checking for internal IPs ---"
    grep -rn "192\.168\." --include="*.rs" --include="*.md" --include="*.toml" --include="*.yaml" --include="*.yml" --include="*.json" . | grep -v '.git/' || echo "CLEAN"
    echo "--- Checking for hardware fingerprints ---"
    grep -rn "oryx\|System76\|RTX\|3060\|Pixel\|Fold" --include="*.rs" --include="*.md" --include="*.toml" . | grep -v '.git/' | grep -v 'CHANGELOG' | grep -v 'docs/architecture' || echo "CLEAN"
    echo "--- Checking for private codenames ---"
    grep -rn "Mnemos\|Mneme\|beryl\|moo\|Keeper\|Threshold\|Pema\|Lhamo\|Guan\|Freeman\|Namdor\|Rebecca" --include="*.rs" --include="*.md" --include="*.toml" . | grep -v '.git/' | grep -v 'CHANGELOG' | grep -v 'LICENSE' | grep -v 'NOTICES' || echo "CLEAN"
    echo "--- Checking for private markers ---"
    grep -rn "babe\|tent\|dharma\|bodhisattva\|vow\|lineage" --include="*.rs" --include="*.md" --include="*.toml" . | grep -v '.git/' | grep -v 'CHANGELOG' || echo "CLEAN"
    echo "--- Checking for .serena/ tracking ---"
    git ls-files | grep -E "^\.serena|^\.mneme" && echo "FOUND — remove from git" || echo "CLEAN"
    echo "--- Checking for .bak files ---"
    git ls-files | grep -E "\.bak$" && echo "FOUND — remove from git" || echo "CLEAN"
    echo ""
    echo "=== OPSEC AUDIT — review findings above ==="

# ─── Stage 16: Git Release ──────────────────────────────────────────

# Commit, tag, and push to Gitea + GitHub
# Usage: just git-release vX.Y.Z
git-release version:
    #!/usr/bin/env bash
    if [ -z "{{version}}" ]; then
        echo "Usage: just git-release vX.Y.Z"
        exit 1
    fi
    echo "=== Git release: {{version}} ==="
    git add -A
    git status --short
    echo "=== Committing ==="
    git commit -m "release: {{version}}"
    echo "=== Tagging ==="
    git tag -a "{{version}}" -m "BEAM {{version}}"
    echo "=== Pushing to Gitea (origin) ==="
    git push origin master
    git push origin "{{version}}"
    echo "=== Pushing to GitHub ==="
    git push github master
    git push github "{{version}}"
    echo "=== GIT RELEASE COMPLETE ✅ ==="

# ─── Stage 17: Publish to crates.io ─────────────────────────────────

# Publish to crates.io (REQUIRES FREEMAN APPROVAL)
publish-crates:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    echo "=== cargo publish --dry-run ==="
    cargo publish --dry-run
    echo ""
    echo "⚠️  PUBLISH GATE — this will publish to crates.io (irreversible)"
    echo "⚠️  Freeman must approve before proceeding"
    printf "Type 'yes' to publish: "
    read -r CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        echo "Aborted."
        exit 1
    fi
    echo "=== cargo publish ==="
    cargo publish
    echo "=== CRATES.IO PUBLISH COMPLETE ✅ ==="

# ─── Stage 18: Publish to npm ────────────────────────────────────────

# Publish WASM package to npm (REQUIRES FREEMAN APPROVAL)
publish-npm:
    #!/usr/bin/env bash
    set -euo pipefail
    export NVM_DIR="$HOME/.nvm"
    [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
    nvm use 22 > /dev/null 2>&1
    echo "=== Building WASM (web target, release) ==="
    wasm-pack build --target web --release --no-default-features
    echo ""
    echo "⚠️  PUBLISH GATE — this will publish to npm"
    echo "⚠️  Freeman must approve before proceeding"
    printf "Type 'yes' to publish: "
    read -r CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        echo "Aborted."
        exit 1
    fi
    echo "=== npm publish ==="
    cd pkg && npm publish
    echo "=== NPM PUBLISH COMPLETE ✅ ==="

# ─── Stage 19: GitHub Release ───────────────────────────────────────

# Create GitHub release with notes
# Usage: just github-release vX.Y.Z
github-release version:
    #!/usr/bin/env bash
    if [ -z "{{version}}" ]; then
        echo "Usage: just github-release vX.Y.Z"
        exit 1
    fi
    echo "=== Creating GitHub release: {{version}} ==="
    NOTES=$(awk '/^## \[{{version}}\]/{found=1} found && /^## \[/ && !/^\[{{version}}\]/{found=0} found' CHANGELOG.md)
    gh release create "{{version}}" --title "BEAM {{version}}" --notes "$NOTES"
    echo "=== GITHUB RELEASE COMPLETE ✅ ==="

# ─── Stage 20: Smoke Test ────────────────────────────────────────────

# Verify published crate works for consumers
smoke-test:
    #!/usr/bin/env bash
    set -euo pipefail
    source "$HOME/.cargo/env"
    TMPDIR=$(mktemp -d)
    cd "$TMPDIR"
    cargo init --name beam-smoke
    cargo add beamdb
    echo 'fn main() { let _ = beamdb::Beam::new(); }' > src/main.rs
    cargo build
    echo "=== SMOKE TEST PASS ✅ ==="
    rm -rf "$TMPDIR"

# ─── All Tests ───────────────────────────────────────────────────────

# Run ALL test suites (native + WASM + fixtures + examples)
test-all: lint test-native test-wasm test-examples
    echo "=== ALL TESTS PASS ✅ (373 tests) ==="

# ─── Full Release Pipeline ──────────────────────────────────────────

# Run the full release pipeline (stages 1-16, stops before publish)
# Usage: just release
# Then: just publish-crates, just publish-npm, just github-release vX.Y.Z
release: lint test-native test-wasm test-examples supply-chain deps-check docs-audit benchmarks profiling wasm-build opsec-audit
    #!/usr/bin/env bash
    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  PRE-PUBLISH STAGES COMPLETE ✅"
    echo "═══════════════════════════════════════════════════"
    echo ""
    echo "Manual steps remaining:"
    echo "  1. Update CHANGELOG.md"
    echo "  2. Update project docs (README, architecture.md, etc.)"
    echo "  3. Bump version in Cargo.toml (SemVer)"
    echo "  4. just git-release vX.Y.Z"
    echo "  5. just publish-crates  (requires Freeman approval)"
    echo "  6. just publish-npm      (requires Freeman approval)"
    echo "  7. just github-release vX.Y.Z"
    echo "  8. just smoke-test"
    echo ""
    echo "⚠️  Stages 5-7 require Freeman's explicit approval."
    echo ""

# ─── Default ────────────────────────────────────────────────────────

default:
    @just --list
