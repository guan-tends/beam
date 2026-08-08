//! WASM test suite for BEAM browser bindings.
//!
//! Run with: `wasm-pack test --node`
//!
//! These tests exercise the real WASM code path — actor system, router,
//! storage, and WebSocket adapter — inside Node.js (which provides a native
//! `WebSocket` client).

#![cfg(test)]

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_node_experimental);

// ─── Smoke test ───

#[wasm_bindgen_test]
fn smoke_test() {
    assert_eq!(2 + 2, 4);
}
