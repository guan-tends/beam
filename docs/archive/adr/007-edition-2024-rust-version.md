# ADR-007: Edition 2024 and rust-version 1.85

## Status
Accepted, 2026-05-18

## Context

Rod was originally written in Rust edition 2018. Mnemos (downstream consumer) requires edition 2024 with `rust-version = "1.85"`. The mismatch meant:
1. Mnemos could not consume Rod as a workspace member (edition mismatch)
2. Modern Rust features (let-else, if-let chains, `gen` keyword prep) unavailable
3. `rust-version` metadata missing — no compile-time check for toolchain compatibility

## Decision

Bump Rod to **edition 2024** and set **rust-version = "1.85"**.

## Changes

### Cargo.toml
```toml
[package]
name = "rod"
version = "0.2.3"
edition = "2024"
rust-version = "1.85"
resolver = "3"
```

### Code Adaptations
- `gen` keyword is reserved in 2024. No conflicts in current code.
- `if let` chains now stable (1.85). No migration needed.
- `impl Trait` in `let` bindings: not yet stable, no impact.
- Lifetime elision changes in 2024: no impact on current API surface.

## Consequences

### Positive
- Mnemos workspace integration seamless (both use edition 2024)
- Access to 2024 features as they stabilize
- `rust-version` enforces minimum compiler at build time
- `resolver = "3"` aligns with edition 2024 dependency resolution

### Negative
- Minimum supported Rust version is now 1.85 (released 2025-02)
- Developers on older toolchains must upgrade
- Some CI runners may need Rust version update

## Verification

```bash
rustc --version  # must be >= 1.85.0
cargo check      # must pass with edition = "2024"
```

On oryx: `rustc 1.97.0-nightly` — fully compatible.

## References
- Rust edition guide: https://doc.rust-lang.org/edition-guide/
- Edition 2024 summary: https://doc.rust-lang.org/edition-guide/rust-2024/
- Commit: `3a97ac3` — "A1+A2: Bump edition to 2024, add rust-version 1.85"
