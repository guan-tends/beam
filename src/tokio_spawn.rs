//! Spawn + yield shim — redirects `tokio::spawn` and `tokio::task::yield_now`
//! to their WASM-compatible equivalents on WASM.
//!
//! On native, `tokio::spawn` puts tasks on the runtime scheduler, driven by
//! `block_on` or worker threads. On WASM with a current-thread runtime,
//! `rt.enter()` only sets context — it doesn't poll the task queue. Tasks
//! spawned via `tokio::spawn` sit in the queue forever.
//!
//! `tokio_with_wasm::spawn` wraps `wasm_bindgen_futures::spawn_local`,
//! which is driven by the browser's event loop. This is the only way to
//! actually run async tasks in the browser.
//!
//! `yield_now` on native delegates to `tokio::task::yield_now`. On WASM,
//! there is no equivalent — we use `std::future::poll_once` with a no-op
//! future to force the JS event loop to interleave other microtasks.

#[cfg(not(target_arch = "wasm32"))]
pub use tokio::spawn;

#[cfg(not(target_arch = "wasm32"))]
pub use tokio::task::JoinHandle;

/// Yields control to the async runtime, allowing other tasks to run.
///
/// On native this calls `tokio::task::yield_now`. On WASM there is no
/// direct equivalent — we yield via a ready future poll, which lets the
/// browser microtask queue interleave other spawned tasks.
#[cfg(not(target_arch = "wasm32"))]
pub async fn yield_now() {
    tokio::task::yield_now().await;
}

#[cfg(target_arch = "wasm32")]
pub use tokio_with_wasm::spawn;

#[cfg(target_arch = "wasm32")]
pub use tokio_with_wasm::task::JoinHandle;

/// WASM-safe yield — a no-op.
///
/// The starvation that `yield_now` prevents is specific to tokio's
/// `current_thread` scheduler, where a tight loop with non-blocking
/// `await` points can starve other tasks indefinitely. On WASM, tasks
/// are driven by the browser's event loop via `spawn_local`, which
/// naturally interleaves microtasks between `await` points.
#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
pub async fn yield_now() {}
