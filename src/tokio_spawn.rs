//! Spawn shim — redirects `tokio::spawn` to `tokio_with_wasm::spawn` on WASM.
//!
//! On native, `tokio::spawn` puts tasks on the runtime scheduler, driven by
//! `block_on` or worker threads. On WASM with a current-thread runtime,
//! `rt.enter()` only sets context — it doesn't poll the task queue. Tasks
//! spawned via `tokio::spawn` sit in the queue forever.
//!
//! `tokio_with_wasm::spawn` wraps `wasm_bindgen_futures::spawn_local`,
//! which is driven by the browser's event loop. This is the only way to
//! actually run async tasks in the browser.

#[cfg(not(target_arch = "wasm32"))]
pub use tokio::spawn;

#[cfg(not(target_arch = "wasm32"))]
pub use tokio::task::JoinHandle;

#[cfg(target_arch = "wasm32")]
pub use tokio_with_wasm::spawn;

#[cfg(target_arch = "wasm32")]
pub use tokio_with_wasm::task::JoinHandle;
