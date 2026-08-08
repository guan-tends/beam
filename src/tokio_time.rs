//! Shim module: re-exports tokio::time on native, tokio_with_wasm::time on WASM.
//! This avoids tokio's `time` feature on WASM which panics (no std::time::Instant).

#[cfg(not(target_arch = "wasm32"))]
pub use tokio::time::{interval, sleep, timeout};

#[cfg(target_arch = "wasm32")]
pub use tokio_with_wasm::time::{interval, sleep, timeout};
