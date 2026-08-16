// Global allocator — mimalloc.
//
// mimalloc is a compact general-purpose allocator by Microsoft with excellent
// performance characteristics. It uses per-thread heap segments with deferred
// freeing, reducing contention and fragmentation compared to glibc's malloc.
//
// Enabled by default on native targets via the `mimalloc` feature. Disabled on
// WASM (no system allocator to replace) and when the feature is turned off.
//
// To use the system allocator instead: `cargo build --no-default-features --features native`
#[cfg(all(feature = "mimalloc", not(target_arch = "wasm32")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod ack;
pub mod actor;
pub mod adapters;
/// Thread-safe bump arena allocator (native only — uses `std::sync::Mutex`).
#[cfg(not(target_arch = "wasm32"))]
pub mod arena;
mod dup;
pub mod mailbox;
#[doc(hidden)]
pub mod message; // pub for benchmarking
pub mod metrics;
mod tokio_spawn;
mod tokio_time;

#[cfg(not(target_arch = "wasm32"))]
pub mod migration;

mod node;
mod router;
#[cfg(feature = "webrtc")]
mod stun;
pub mod types;
mod utils;
pub use dup::Dup;
pub mod sea;
pub use node::{Config, Node};
pub use types::Value;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

#[cfg(all(target_arch = "wasm32", test))]
mod wasm_tests;
// Include README.md as doctests — all ```rust code blocks in README are
// compiled and run when `cargo test --doc` (or `cargo test`) is executed.
// The struct only exists during doctest collection, so it's invisible in
// the public API and has zero runtime cost.
#[cfg(not(target_arch = "wasm32"))]
#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;
