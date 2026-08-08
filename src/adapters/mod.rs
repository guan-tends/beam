//! Storage and network adapters for BEAM.
//!
//! This module contains all adapter implementations that connect the
//! [`crate::Node`] graph engine to external systems:
//!
//! # Storage Adapters
//!
//! - [`MemoryStorage`] — in-memory `HashMap`-backed storage (default)
//! - [`RedbStorage`] — persistent embedded storage via [`redb`] (native only)
//!
//! # Storage Read/Write Split
//!
//! Storage adapters that implement [`Actor::try_clone_storage`] are started
//! as two actors by the Router: a read actor (receives `Get`) and a write
//! actor (receives `Put`, `BatchPut`, `Flush`). Both share the same
//! underlying store via `Arc`, so reads see committed writes immediately.
//!
//! - [`RedbStorage`] splits — the write actor's `spawn_blocking` fsync no
//!   longer blocks the read actor's concurrent `Get` queries.
//! - [`MemoryStorage`] does not split — in-memory writes are synchronous
//!   (no fsync), so splitting provides no benefit and would break
//!   read-after-write ordering.
//!
//! # Network Adapters
//!
//! - [`OutgoingWebsocketManager`] — outgoing WebSocket client manager (native only)
//! - [`WsServer`] — incoming WebSocket server with optional TLS (native only)
//! - [`WsConn`] — per-connection WebSocket actor (native only)
//! - [`Multicast`] — UDP multicast LAN discovery (native only)
//! - [`WebRtcPeer`] — WebRTC data channel P2P connection (feature-gated)
//!
//! # Adapter Protocol
//!
//! All adapters implement the [`crate::actor::Actor`] trait and receive
//! [`crate::message::Message`] via the actor system. Storage adapters
//! handle `Get`, `Put`, `BatchPut`, and `Flush`. Network adapters
//! handle `Put` and `Get` by serializing to the wire format and
//! forwarding to remote peers.

mod memory_storage;

#[cfg(not(target_arch = "wasm32"))]
mod multicast;

#[cfg(feature = "persy")]
pub mod persy_storage;

#[cfg(not(target_arch = "wasm32"))]
mod redb_storage;

#[cfg(feature = "webrtc")]
mod webrtc;

#[cfg(not(target_arch = "wasm32"))]
mod ws_client;

#[cfg(not(target_arch = "wasm32"))]
mod ws_conn;

#[cfg(not(target_arch = "wasm32"))]
mod ws_server;

pub use memory_storage::MemoryStorage;

#[cfg(not(target_arch = "wasm32"))]
pub use multicast::Multicast;

#[cfg(feature = "persy")]
pub use persy_storage::PersyStorage;

#[cfg(not(target_arch = "wasm32"))]
pub use redb_storage::RedbStorage;

#[cfg(not(target_arch = "wasm32"))]
pub use ws_client::OutgoingWebsocketManager;

#[cfg(not(target_arch = "wasm32"))]
pub use ws_conn::WsConn;

#[cfg(not(target_arch = "wasm32"))]
pub use ws_server::{WsServer, WsServerConfig};

#[cfg(feature = "webrtc")]
pub use webrtc::{WebRtcPeer, WebRtcRole};
