//! Wire-protocol compatibility test suite entry point.
//!
//! This test file runs the golden wire-fixture harness defined in
//! [`tests/wire/mod.rs`]. It is the single `#[test]` function that
//! discovers, loads, and asserts every fixture under
//! `tests/wire/fixtures/`.
//!
//! See `tests/wire/mod.rs` for the fixture schema and harness design.

mod wire;

// The test function lives inside `tests/wire/mod.rs` to keep all
// harness logic in one module. This file exists solely to declare
// the module so `cargo test` picks it up as an integration test.
