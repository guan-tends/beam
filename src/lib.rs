pub mod ack;
pub mod actor;
pub mod adapters;
pub mod migration;
mod dup;
#[doc(hidden)]
pub mod message; // pub for benchmarking
pub mod metrics;
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

// Include README.md as doctests — all ```rust code blocks in README are
// compiled and run when `cargo test --doc` (or `cargo test`) is executed.
// The struct only exists during doctest collection, so it's invisible in
// the public API and has zero runtime cost.
#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;
