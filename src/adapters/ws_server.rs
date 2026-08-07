//! WebSocket server adapter — accepts inbound WebSocket connections.
//!
//! [`WsServer`] listens on a TCP port and accepts incoming WebSocket
//! connections. Each connection is handled by a [`WsConn`] actor. The
//! server also starts a web server (on `port + 1`) for peer ID discovery.
//!
//! # TLS Support
//!
//! When `cert_path` and `key_path` are configured in [`WsServerConfig`],
//! the server uses TLS for both the WebSocket and web server. Otherwise,
//! plain TCP is used.
//!
//! # Ports
//!
//! - WebSocket port: `ws_config.port` (default 4944)
//! - Web UI port: `ws_config.port + 1` (default 4945)

use crate::Config;
use crate::actor::{Actor, ActorContext, Addr};
use crate::adapters::ws_conn::WsConn;
use crate::message::Message;

use async_trait::async_trait;
use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use tokio::sync::RwLock;

use futures_util::StreamExt;
use log::info;
use tokio::net::TcpListener;
use tokio_native_tls::native_tls::Identity;

use tokio_tungstenite::MaybeTlsStream;

/// Shared set of connected client addresses.
type Clients = Arc<RwLock<HashSet<Addr>>>;

/// Configuration for the [`WsServer`] adapter.
#[derive(Clone)]
pub struct WsServerConfig {
    /// Port to listen for WebSocket connections (default: 4944).
    pub port: u16,
    /// Path to TLS certificate file (PEM/PKCS8). If `None`, plain TCP is used.
    pub cert_path: Option<String>,
    /// Path to TLS private key file. Required when `cert_path` is set.
    pub key_path: Option<String>,
}

impl Default for WsServerConfig {
    fn default() -> Self {
        WsServerConfig {
            port: 4944,
            cert_path: None,
            key_path: None,
        }
    }
}

/// WebSocket server adapter that accepts inbound connections.
///
/// Listens on a TCP port and spawns a [`WsConn`] actor for each incoming
/// WebSocket connection. Optionally serves a web UI on `port + 1` for
/// peer ID discovery.
#[derive(Clone)]
pub struct WsServer {
    config: Config,
    ws_config: WsServerConfig,
    clients: Clients,
}

impl WsServer {
    /// Creates a new WebSocket server with default config.
    pub fn new(config: Config) -> Self {
        Self::new_with_config(config, WsServerConfig::default())
    }

    /// Creates a new WebSocket server with custom config.
    ///
    /// # Arguments
    ///
    /// * `config` - Node configuration
    /// * `ws_config` - WebSocket server config (port, TLS)
    pub fn new_with_config(config: Config, ws_config: WsServerConfig) -> Self {
        Self {
            config,
            ws_config,
            clients: Clients::default(),
        }
    }

    /// Handles a single incoming WebSocket stream by upgrading it and
    /// spawning a [`WsConn`] actor.
    async fn handle_stream(
        stream: MaybeTlsStream<tokio::net::TcpStream>,
        ctx: &ActorContext,
        clients: Clients,
        allow_public_space: bool,
    ) {
        let ws_stream = match tokio_tungstenite::accept_async(stream).await {
            Ok(s) => s,
            Err(_e) => {
                // Suppress errors from receiving normal HTTP requests
                // (e.g. browser preflight checks).
                return;
            }
        };

        let (sender, receiver) = ws_stream.split();

        let conn = WsConn::new(sender, receiver, allow_public_space);
        let addr = ctx.start_actor(Box::new(conn));
        clients.write().await.insert(addr);
    }

    /// Starts the web server for peer ID discovery.
    ///
    /// Serves on `config.port + 1`. Routes:
    /// - `/peer_id` — returns this node's peer ID
    async fn start_web_server(config: WsServerConfig, peer_id: String) {
        use warp::Filter;
        let peer_id_route = warp::path("peer_id").map(move || peer_id.to_string());
        let routes = warp::get().and(peer_id_route);

        let port = config.port + 1;
        if let Some(cert_path) = config.cert_path {
            let key_path = config.key_path.unwrap();
            let addr = format!("https://localhost:{}", port);
            eprintln!("Web UI:             {}", addr);
            warp::serve(routes)
                .tls()
                .cert_path(cert_path)
                .key_path(key_path)
                .run(([0, 0, 0, 0], port))
                .await;
            return;
        }

        let addr = format!("http://localhost:{}", port);
        eprintln!("Web UI:             {}", addr);
        warp::serve(routes).run(([0, 0, 0, 0], port)).await;
    }

    /// Returns the current number of connected WebSocket peers.
    ///
    /// Test-only helper to poll mesh readiness without relying on
    /// blind sleeps. The count reflects the number of [`WsConn`]
    /// actors that have completed the WebSocket handshake and
    /// registered themselves in the server's client set.
    ///
    /// # Use
    ///
    /// ```ignore
    /// // Wait for both peers to connect before broadcasting a Put.
    /// while ws_server.peer_count() < 2 {
    ///     sleep(Duration::from_millis(50)).await;
    /// }
    /// ```
    pub fn peer_count(&self) -> usize {
        // Try a non-blocking read. If the lock is held, treat as "no
        // observed peers yet" so the caller polls again. Blocking
        // would risk stalling the actor's mailbox.
        if let Ok(count) = self.clients.try_read() {
            count.len()
        } else {
            0
        }
    }

}

#[async_trait]
impl Actor for WsServer {
    async fn handle(&mut self, msg: Message, _ctx: &ActorContext) {
        let client_count = self.clients.read().await.len();
        eprintln!("[WSSERVER-DIAG] handle called with {} clients, msg type: {}", client_count, match &msg { Message::Put(_) => "Put", Message::Get(_) => "Get", _ => "Other" });
        for conn in self.clients.read().await.iter() {
            if msg.is_from(conn) {
                eprintln!("[WSSERVER-DIAG] skipping msg.from == conn");
                continue;
            }
            eprintln!("[WSSERVER-DIAG] relaying to WsConn client");
            if conn.send(msg.clone()).is_err() {
                eprintln!("[WSSERVER-DIAG] send failed, removing client");
                self.clients.write().await.remove(conn);
            }
        }
    }

    async fn pre_start(&mut self, ctx: &ActorContext) {
        let addr = format!("0.0.0.0:{}", self.ws_config.port).to_string();
        let ctx = ctx.clone();

        let peer_id = ctx.peer_id.read().clone();
        let config_clone = self.ws_config.clone();
        ctx.child_task(async move {
            Self::start_web_server(config_clone, peer_id).await;
        });


        // Create the TCP listener
        let try_socket = TcpListener::bind(&addr).await;
        let listener = try_socket.expect("Failed to bind");
        eprintln!("Websocket endpoint: ws://{}/ws", addr);

        let allow_public_space = self.config.allow_public_space;
        let clients = self.clients.clone();
        if let Some(cert_path) = &self.ws_config.cert_path {
            let mut cert_file = File::open(cert_path).unwrap();
            let mut cert = vec![];
            cert_file.read_to_end(&mut cert).unwrap();

            let key_path = self.ws_config.key_path.as_ref().unwrap();
            let mut key_file = File::open(key_path).unwrap();
            let mut key = vec![];
            key_file.read_to_end(&mut key).unwrap();

            let identity = Identity::from_pkcs8(&cert, &key).unwrap();
            let acceptor = tokio_native_tls::native_tls::TlsAcceptor::new(identity).unwrap();
            let acceptor = tokio_native_tls::TlsAcceptor::from(acceptor);
            let acceptor = Arc::new(acceptor);

            ctx.clone().child_task(async move {
                loop {
                    if let Ok((stream, _)) = listener.accept().await {
                        let acceptor = acceptor.clone();
                        let clients = clients.clone();
                        let ctx = ctx.clone();
                        tokio::spawn(async move {
                            let stream = acceptor.accept(stream).await;
                            if let Ok(stream) = stream {
                                Self::handle_stream(
                                    MaybeTlsStream::NativeTls(stream),
                                    &ctx,
                                    clients.clone(),
                                    allow_public_space,
                                )
                                .await;
                            }
                        });
                    }
                }
            });
        } else {
            ctx.clone().child_task(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    Self::handle_stream(
                        MaybeTlsStream::Plain(stream),
                        &ctx,
                        clients.clone(),
                        allow_public_space,
                    )
                    .await;
                }
            });
        }
    }

    /// WsServer is a relay adapter — it fans out Puts to all connected
    /// WebSocket clients. Must return `true` so the Router adds it to
    /// `server_peers` and relays Put messages through it.
    fn subscribe_to_everything(&self) -> bool {
        true
    }

    async fn stopping(&mut self, _context: &ActorContext) {
        info!("WsServer stopping");
    }
}
