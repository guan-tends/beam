//! UDP multicast LAN discovery and sync adapter.
//!
//! [`Multicast`] uses UDP multicast to discover and sync with Rod peers on
//! the local network. It broadcasts `Put` and `Get` messages to a multicast
//! group, enabling zero-config peer discovery on LANs.
//!
//! # Configuration
//!
//! - Multicast group: `233.255.255.255:7654`
//! - Buffer size: 64 KB
//! - Interfaces: all IPv4 interfaces
//!
//! # Behavior
//!
//! - `pre_start`: Joins the multicast group and starts a blocking receive
//!   loop in a `blocking_child_task`
//! - `handle`: Broadcasts outgoing `Put` and `Get` messages to the group
//! - Incoming messages are parsed and forwarded to the [`crate::router::Router`]
//! - Marks itself as `subscribe_to_everything` (receives all messages)
//!
//! # Limitations
//!
//! The receive loop uses `blocking_child_task` — the `MulticastSocket::receive`
//! call is synchronous and blocks. This is not optimal for async contexts
//! but is required by the `multicast_socket` crate's API.

use multicast_socket::{MulticastOptions, MulticastSocket, all_ipv4_interfaces};
use std::net::SocketAddrV4;

use crate::Config;
use crate::actor::{Actor, ActorContext};
use crate::message::Message;
use async_trait::async_trait;
use log::{debug, error, info};
use std::sync::Arc;
use tokio::sync::RwLock;

/// UDP multicast adapter for LAN peer discovery and sync.
///
/// Broadcasts Gun protocol messages to the multicast group `233.255.255.255:7654`
/// and receives messages from other peers on the same LAN.
pub struct Multicast {
    socket: Arc<RwLock<MulticastSocket>>,
    config: Config,
}

impl Multicast {
    /// Creates a new multicast adapter bound to the default group.
    ///
    /// # Panics
    ///
    /// Panics if the multicast socket cannot be created (e.g. no network
    /// interfaces available, or port 7654 is in use).
    pub fn new(config: Config) -> Self {
        let bind_address = SocketAddrV4::new([233, 255, 255, 255].into(), 7654);
        let options = MulticastOptions {
            buffer_size: 64 * 1024,
            ..MulticastOptions::default()
        };
        let interfaces = all_ipv4_interfaces().expect("could not list multicast interfaces");
        let socket = MulticastSocket::with_options(bind_address, interfaces, options)
            .expect("could not create and bind multicast socket");
        let socket = Arc::new(RwLock::new(socket));
        Multicast { socket, config }
    }

    /// Parses an incoming multicast message and forwards it to the router.
    ///
    /// Only `Put` and `Get` messages are forwarded — other message types
    /// (Hi, Flush, RtcSignal) are not meaningful over multicast.
    fn handle_incoming_message(data: &str, ctx: &ActorContext, allow_public_space: bool) {
        debug!("in {}", data);
        match Message::try_from(data, ctx.addr.clone(), allow_public_space) {
            Ok(msgs) => {
                for msg in msgs.into_iter() {
                    match msg {
                        Message::Put(put) => {
                            let put = put.clone();
                            if let Err(e) = ctx.router.send(Message::Put(put)) {
                                error!("failed to send message to node: {:?}", e);
                            }
                        }
                        Message::Get(get) => {
                            let get = get.clone();
                            if let Err(e) = ctx.router.send(Message::Get(get)) {
                                error!("failed to send message to node: {:?}", e);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => error!("message parsing failed: {}", e),
        }
    }
}

#[async_trait]
impl Actor for Multicast {
    async fn handle(&mut self, msg: Message, ctx: &ActorContext) {
        debug!("out {}", msg.get_id());
        if msg.is_from(&ctx.addr) {
            return;
        }
        match msg {
            Message::Put(mut put) => {
                if let Err(e) = self
                    .socket
                    .read()
                    .await
                    .broadcast(put.to_string().as_bytes())
                {
                    error!("multicast send error {}", e);
                }
            }
            Message::Get(get) => {
                if let Err(e) = self
                    .socket
                    .read()
                    .await
                    .broadcast(get.to_string().as_bytes())
                {
                    error!("multicast send error {}", e);
                }
            }
            _ => {
                debug!("not sending");
            }
        }
    }

    /// Returns `true` — multicast subscribes to all messages.
    fn subscribe_to_everything(&self) -> bool {
        true
    }

    async fn pre_start(&mut self, ctx: &ActorContext) {
        info!("Syncing over multicast\n");

        let ctx_clone = ctx.clone();

        let bind_address = SocketAddrV4::new([233, 255, 255, 255].into(), 7654);
        let options = MulticastOptions {
            buffer_size: 64 * 1024,
            ..MulticastOptions::default()
        };
        let interfaces = all_ipv4_interfaces().expect("could not list multicast interfaces");
        let socket = MulticastSocket::with_options(bind_address, interfaces, options)
            .expect("could not create and bind multicast socket");

        let allow_public_space = self.config.allow_public_space;
        ctx.blocking_child_task(move || {
            // blocking — not optimal!
            loop {
                if let Ok(message) = socket.receive() {
                    // TODO: if message.from == multicast_[interface], don't resend to [interface]
                    if let Ok(data) = std::str::from_utf8(&message.data) {
                        Self::handle_incoming_message(data, &ctx_clone, allow_public_space);
                    }
                }
                if *ctx_clone.is_stopped.read() {
                    break;
                }
            }
        });
    }
}
