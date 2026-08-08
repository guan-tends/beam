//! WASM test suite for BEAM browser bindings.
//!
//! Run with:
//!   CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
//!     cargo test --target wasm32-unknown-unknown --lib
//!
//! These tests compile to WASM and run inside Node.js, which provides
//! a native `WebSocket` client — the same API the browser uses.
//! A BEAM relay server is spawned as a subprocess for integration tests.

#![cfg(test)]

use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_node_experimental);

// ─── Helpers ───

/// Await a JS expression that evaluates to a Promise.
async fn eval_promise(js: &str) -> JsValue {
    let promise = js_sys::eval(js)
        .expect("eval failed")
        .dyn_into::<js_sys::Promise>()
        .expect("eval did not return a Promise");
    wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .expect("promise rejected")
}

/// Sleep for `ms` milliseconds (lets the actor system process).
async fn sleep(ms: u64) {
    eval_promise(&format!("new Promise(r => setTimeout(r, {}))", ms)).await;
}

// ─── T2: Relay lifecycle helper ───

/// Spawn a BEAM relay on `port`. Returns a handle that kills the
/// process when dropped.
///
/// Uses dynamic `import('node:child_process')` — the ESM-compatible
/// way to access Node built-ins (wasm-bindgen-test-runner runs in
/// module mode, so `require()` is not available).
struct Relay {
    port: u16,
    proc: JsValue,
}

impl Relay {
    async fn start(port: u16) -> Self {
        let proc = eval_promise(&format!(
            r#"
            import('node:child_process').then(cp => {{
                const child = cp.spawn('target/debug/beam', [
                    'start', '--port', '{port}',
                    '--memory-storage', 'true', '--redb-storage', 'false',
                    '--allow-public-space', 'true'
                ], {{
                    cwd: '/home/guan/src/beam',
                    stdio: ['ignore', 'pipe', 'pipe']
                }});
                child.stdout.on('data', d => process.stderr.write('[relay] ' + d));
                child.stderr.on('data', d => process.stderr.write('[relay] ' + d));
                return child;
            }})
            "#
        ))
        .await;

        // Wait for the TCP port to accept connections.
        eval_promise(&format!(
            r#"
            new Promise((resolve, reject) => {{
                import('node:net').then(net => {{
                    const deadline = Date.now() + 10000;
                    const tryConnect = () => {{
                        const sock = net.connect({port}, '127.0.0.1');
                        sock.on('connect', () => {{ sock.destroy(); resolve(); }});
                        sock.on('error', () => {{
                            if (Date.now() > deadline) reject(new Error('relay did not start'));
                            else setTimeout(tryConnect, 50);
                        }});
                    }};
                    tryConnect();
                }});
            }})
            "#
        ))
        .await;

        Self { port, proc }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        // Best-effort kill — relay process should clean up on SIGTERM.
        let f = js_sys::Function::new_with_args(
            "p",
            "if (p && p.kill) p.kill('SIGTERM');",
        );
        let _ = f.call1(&JsValue::UNDEFINED, &self.proc);
    }
}

// ─── T1: Smoke test ───

#[wasm_bindgen_test]
fn smoke_test() {
    assert_eq!(2 + 2, 4);
}

// ─── T3: Local put/get (no relay) ───

#[wasm_bindgen_test(async)]
async fn local_put_get_roundtrip() {
    use crate::wasm::Beam;

    let mut beam = Beam::new();
    beam.put("chat.001", "hello world");
    sleep(100).await;

    let result = wasm_bindgen_futures::JsFuture::from(beam.get("chat.001"))
        .await
        .expect("get should resolve");

    assert_eq!(result.as_string(), Some("hello world".to_string()));
    beam.stop();
}

// ─── T4: Connect to relay ───

#[wasm_bindgen_test(async)]
async fn relay_connect() {
    use crate::wasm::Beam;

    let _relay = Relay::start(4960).await;

    let mut beam = Beam::new();
    beam.connect("ws://127.0.0.1:4960");
    sleep(500).await; // WebSocket handshake + Hi exchange

    // If we got here without panicking, the connection succeeded.
    beam.stop();
}

// ─── T5: Put reaches relay (local echo + relay forwarding) ───

#[wasm_bindgen_test(async)]
async fn relay_put_echo() {
    use crate::wasm::Beam;

    let _relay = Relay::start(4961).await;

    let mut beam = Beam::new();
    beam.connect("ws://127.0.0.1:4961");
    sleep(500).await;

    // Put should not panic, and the value should be locally readable.
    beam.put("chat.relay_test", "relay payload");
    sleep(300).await;

    let result = wasm_bindgen_futures::JsFuture::from(beam.get("chat.relay_test"))
        .await
        .expect("get should resolve");

    assert_eq!(result.as_string(), Some("relay payload".to_string()));
    beam.stop();
}

// ─── T6: Two clients cross-talk via relay ───
//
// This is the killer test. If it passes, WASM cross-window delivery
// works and we can merge to master + NPM publish.

#[wasm_bindgen_test(async)]
async fn two_clients_cross_talk() {
    use crate::wasm::Beam;

    let _relay = Relay::start(4970).await;

    // Two independent BEAM nodes, both connecting to the same relay.
    let mut client1 = Beam::new();
    client1.connect("ws://127.0.0.1:4970");

    let mut client2 = Beam::new();
    client2.connect("ws://127.0.0.1:4970");

    sleep(1000).await; // Both WebSockets open + Hi exchanged

    // Register a callback on client2 that stores received values globally.
    js_sys::eval(
        r#"
        globalThis.__received = [];
        globalThis.__on_msg = function(val) {
            globalThis.__received.push(val);
        };
        "#,
    )
    .unwrap();

    let callback = js_sys::eval("globalThis.__on_msg")
        .unwrap()
        .dyn_into::<js_sys::Function>()
        .unwrap();

    client2.on("chat", callback);
    sleep(200).await; // Subscription registered

    // Client 1 sends a message.
    client1.put("chat.42", "cross-talk!");
    sleep(1000).await; // Propagate through relay

    client1.stop();
    client2.stop();

    // Check what client2 received.
    let received = js_sys::eval("JSON.stringify(globalThis.__received)")
        .unwrap()
        .as_string()
        .unwrap_or_default();

    // The callback should have fired with "cross-talk!".
    assert!(
        received.contains("cross-talk"),
        "client2 should have received 'cross-talk!' but got: {}",
        received
    );
}

// ─── T6b: Bidirectional cross-talk ───
//
// Both clients send AND receive. This is what the browser chat example needs.

#[wasm_bindgen_test(async)]
async fn bidirectional_cross_talk() {
    use crate::wasm::Beam;

    let _relay = Relay::start(4980).await;

    let mut client1 = Beam::new();
    client1.connect("ws://127.0.0.1:4980");

    let mut client2 = Beam::new();
    client2.connect("ws://127.0.0.1:4980");

    sleep(1000).await;

    // Separate receive buffers per client.
    js_sys::eval(
        r#"
        globalThis.__c1_received = [];
        globalThis.__c2_received = [];
        globalThis.__c1_cb = v => globalThis.__c1_received.push(v);
        globalThis.__c2_cb = v => globalThis.__c2_received.push(v);
        "#,
    )
    .unwrap();

    let c1_cb = js_sys::eval("globalThis.__c1_cb")
        .unwrap()
        .dyn_into::<js_sys::Function>()
        .unwrap();
    let c2_cb = js_sys::eval("globalThis.__c2_cb")
        .unwrap()
        .dyn_into::<js_sys::Function>()
        .unwrap();

    client1.on("chat", c1_cb);
    client2.on("chat", c2_cb);
    sleep(200).await;

    // Direction 1: client1 → client2
    client1.put("chat.001", "from_client_1");
    sleep(500).await;

    // Direction 2: client2 → client1
    client2.put("chat.002", "from_client_2");
    sleep(500).await;

    client1.stop();
    client2.stop();

    let c1 = js_sys::eval("JSON.stringify(globalThis.__c1_received)")
        .unwrap()
        .as_string()
        .unwrap_or_default();
    let c2 = js_sys::eval("JSON.stringify(globalThis.__c2_received)")
        .unwrap()
        .as_string()
        .unwrap_or_default();

    // client2 should have received "from_client_1"
    assert!(
        c2.contains("from_client_1"),
        "client2 should have received 'from_client_1' but got: {}",
        c2
    );

    // client1 should have received "from_client_2"
    assert!(
        c1.contains("from_client_2"),
        "client1 should have received 'from_client_2' but got: {}",
        c1
    );
}
