//! WASM unit tests for BEAM browser bindings.
//!
//! Run with:
//!   wasm-pack test --node --no-default-features
//!
//! These tests cover pure WASM logic: serialization, parsing, local graph
//! operations. Network integration tests (relay connectivity, cross-talk,
//! throughput) live in `tests/wasm-integration/node-integration.mjs` because
//! wasm-bindgen-test-runner's microtask executor doesn't pump I/O events
//! between poll cycles, making WebSocket testing unreliable.

#![cfg(test)]

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_node_experimental);

// ─── T1: Smoke test ─────────────────────────────────────────────────

#[wasm_bindgen_test]
fn smoke_test() {
    assert_eq!(2 + 2, 4);
}

// ─── T2: Local put/get (no relay) ───────────────────────────────────

#[wasm_bindgen_test(async)]
async fn local_put_get_roundtrip() {
    use crate::wasm::Beam;

    let mut beam = Beam::new();
    beam.put("chat.001", "hello world");

    let result = wasm_bindgen_futures::JsFuture::from(beam.get("chat.001"))
        .await
        .expect("get should resolve");

    assert_eq!(result.as_string(), Some("hello world".to_string()));
    beam.stop();
}

// ─── T3: WASM Benchmarks ────────────────────────────────────────────
// Pure computation benchmarks — no network I/O.

#[wasm_bindgen_test]
fn wasm_bench_parse_throughput() {
    let json =
        r##"{"#":"bench/test","put":{"bench/test":{"msg":"hello world","time":1234567890.123}}}"##;
    let iterations = 10_000;
    let start = web_time::Instant::now();
    for _ in 0..iterations {
        let _: serde_json::Value = serde_json::from_str(json).unwrap();
    }
    let elapsed = start.elapsed();
    let per_op_ns = elapsed.as_nanos() as f64 / iterations as f64;
    console_log!(
        "WASM parse small JSON: {:.0} ns/op ({:.0} ops/sec)",
        per_op_ns,
        1_000_000_000.0 / per_op_ns
    );
}

#[wasm_bindgen_test]
fn wasm_bench_serialize_throughput() {
    let obj = serde_json::json!({
        "#": "bench/test",
        "put": {
            "bench/test": {
                "msg": "hello world",
                "time": 1234567890.123
            }
        }
    });
    let iterations = 10_000;
    let start = web_time::Instant::now();
    for _ in 0..iterations {
        let _ = obj.to_string();
    }
    let elapsed = start.elapsed();
    let per_op_ns = elapsed.as_nanos() as f64 / iterations as f64;
    console_log!(
        "WASM serialize small JSON: {:.0} ns/op ({:.0} ops/sec)",
        per_op_ns,
        1_000_000_000.0 / per_op_ns
    );
}

#[wasm_bindgen_test]
fn wasm_bench_get_parse_throughput() {
    let json = r##"{"#":"bench/test","get":{"#":"bench/test"}}"##;
    let iterations = 10_000;
    let start = web_time::Instant::now();
    for _ in 0..iterations {
        let _: serde_json::Value = serde_json::from_str(json).unwrap();
    }
    let elapsed = start.elapsed();
    let per_op_ns = elapsed.as_nanos() as f64 / iterations as f64;
    console_log!(
        "WASM parse Get: {:.0} ns/op ({:.0} ops/sec)",
        per_op_ns,
        1_000_000_000.0 / per_op_ns
    );
}
