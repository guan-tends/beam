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

// ─── T4: Node.js fs storage adapter ─────────────────────────────────
// These tests require the `node-fs` feature and run in Node.js via
// wasm-bindgen-test's `run_in_node_experimental` mode.
//
// Run with:
//   wasm-pack test --node --features node-fs --no-default-features

#[cfg(feature = "node-fs")]
mod node_fs_tests {
    use super::*;
    use crate::adapters::WasmNodeFsStorage;
    use crate::types::*;
    use arena_btreemap::BTreeMap;

    /// T4a: Postcard serialization roundtrip — serialize Children to
    /// postcard bytes and deserialize back, verifying data integrity.
    #[wasm_bindgen_test]
    fn node_fs_postcard_roundtrip() {
        let mut children: Children = BTreeMap::default();
        children.insert(
            "key1".to_string(),
            NodeData {
                value: Value::Text("hello".to_string()),
                updated_at: 12345.0,
            },
        );
        children.insert(
            "key2".to_string(),
            NodeData {
                value: Value::Number(42.0),
                updated_at: 67890.0,
            },
        );

        // Serialize
        let bytes = postcard::to_allocvec(&children).expect("serialize");
        assert!(!bytes.is_empty(), "postcard bytes should not be empty");

        // Deserialize
        let deserialized: Children = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(deserialized.len(), 2);
        assert_eq!(
            deserialized.get("key1").unwrap().value,
            Value::Text("hello".to_string())
        );
        assert_eq!(deserialized.get("key2").unwrap().value, Value::Number(42.0));
    }

    /// T4b: Empty Children serialize and deserialize correctly.
    #[wasm_bindgen_test]
    fn node_fs_postcard_empty() {
        let children: Children = BTreeMap::default();
        let bytes = postcard::to_allocvec(&children).expect("serialize empty");
        let deserialized: Children = postcard::from_bytes(&bytes).expect("deserialize empty");
        assert!(deserialized.is_empty());
    }

    /// T4c: WasmNodeFsStorage can be constructed with with_dir.
    #[wasm_bindgen_test]
    fn node_fs_construction() {
        let storage = WasmNodeFsStorage::with_dir("/tmp/beam_construction_test");
        assert_eq!(storage.base_dir_str(), "/tmp/beam_construction_test");
        assert!(!storage.is_ready());
    }

    /// T4d: WasmNodeFsStorage default uses beam_data directory.
    #[wasm_bindgen_test]
    fn node_fs_default_dir() {
        let storage = WasmNodeFsStorage::new();
        assert_eq!(storage.base_dir_str(), "beam_data");
    }
}
