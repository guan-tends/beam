# Contributing to BEAM

Thank you for your interest in contributing!

## Development Setup

```bash
git clone https://github.com/guan-tends/beam.git
cd beam
cargo build
cargo test
```

## Code Style

- **Formatting:** `cargo fmt --all` (enforced in CI)
- **Lints:** `cargo clippy --all-targets --all-features -- -D warnings` (enforced in CI)
- **No warnings:** All compiler and clippy warnings must be resolved

## Testing

```bash
# Unit + doc tests
cargo test --workspace

# Integration tests (starts real servers)
cargo test --workspace --test '*' -- --test-threads=1

# Gun.js wire-format compatibility
cd tests/wire-mirror && npm ci && node mirror_test.js
```

All tests must pass. Integration tests run with `--test-threads=1` to avoid
port conflicts.

## Branching

- `master` — stable, release-ready
- Feature branches: `feature/<name>`
- Bug fixes: `fix/<name>`

## Pull Requests

1. Fork and create a feature branch from `master`
2. Ensure all tests pass and there are no warnings
3. Write a clear PR description explaining what and why
4. For changes affecting Gun.js wire compatibility, include wire-format test
   fixtures if applicable

## Gun.js Wire Compatibility

BEAM maintains wire-format compatibility with Gun.js. Changes to message
serialization, signing, or verification must be validated against the
`tests/wire-mirror` test suite. See `docs/architecture.md` for protocol details.
