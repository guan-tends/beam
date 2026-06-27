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
#[cfg(feature = "webrtc")]
mod webrtc;
mod ws_client;
mod ws_conn;
mod ws_server;

pub use memory_storage::MemoryStorage;
pub use multicast::Multicast;
pub use redb_storage::RedbStorage;
pub use ws_client::OutgoingWebsocketManager;
pub use ws_conn::WsConn;
pub use ws_server::{WsServer, WsServerConfig};

#[cfg(feature = "webrtc")]
pub use webrtc::{WebRtcPeer, WebRtcRole};
