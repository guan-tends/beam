pub mod ack;
mod tokio_time;
mod tokio_spawn;
pub mod actor;
pub mod adapters;
mod dup;
#[doc(hidden)]
pub mod message; // pub for benchmarking
pub mod metrics;

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
