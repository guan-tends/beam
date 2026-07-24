//! Storage and network adapters for Rod.
//!
//! This module contains all adapter implementations that connect the
//! [`crate::Node`] graph engine to external systems:
//!
//! # Storage Adapters
//!
//! - [`MemoryStorage`] — in-memory `HashMap`-backed storage (default)
//! - [`RedbStorage`] — persistent embedded storage via [`redb`]
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
//! - [`OutgoingWebsocketManager`] — outgoing WebSocket client manager
//! - [`WsServer`] — incoming WebSocket server with optional TLS
//! - [`WsConn`] — per-connection WebSocket actor (used by both client and server)
//! - [`Multicast`] — UDP multicast LAN discovery
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
mod multicast;
mod redb_storage;
#[cfg(feature = "persy")]
pub mod persy_storage;
#[cfg(feature = "webrtc")]
mod webrtc;
mod ws_client;
mod ws_conn;
mod ws_server;

pub use memory_storage::MemoryStorage;
pub use multicast::Multicast;
pub use redb_storage::RedbStorage;
#[cfg(feature = "persy")]
pub use persy_storage::PersyStorage;
pub use ws_client::OutgoingWebsocketManager;
pub use ws_conn::WsConn;
pub use ws_server::{WsServer, WsServerConfig};

#[cfg(feature = "webrtc")]
pub use webrtc::{WebRtcPeer, WebRtcRole};
