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
use crate::metrics::Metrics;

use async_trait::async_trait;
use crate::utils::FxHashSet;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use tokio::sync::RwLock;

use log::{debug, info};
use tokio::net::TcpListener;
use tokio_native_tls::native_tls::Identity;
use tokio_websockets::ServerBuilder;

/// Shared set of connected client addresses.
type Clients = Arc<RwLock<FxHashSet<Addr>>>;

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
    /// Accepts a WebSocket upgrade on an incoming stream (plain TCP or TLS).
    ///
    /// The stream can be either a raw `TcpStream` (plain WS) or a
    /// `TlsStream<TcpStream>` (secure WSS). Both implement
    /// `AsyncRead + AsyncWrite` which `ServerBuilder::accept` requires.
    async fn handle_stream<S>(
        stream: S,
        ctx: &ActorContext,
        clients: Clients,
        allow_public_space: bool,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        // Configure with an unbounded flush_threshold so that `poll_ready`
        // never triggers an implicit `poll_flush`. Without this, the default
        // 8 KiB threshold causes `feed()` to block on TCP I/O once the
        // internal frame queue exceeds 8 KiB, which starves the actor's
        // scheduler on single-threaded runtimes and can deadlock under
        // backpressure on multi-threaded runtimes. Flushing is handled
        // explicitly by `WsConn::handle_batch` after all messages are queued.
        let ws_config = tokio_websockets::Config::default().flush_threshold(usize::MAX);
        let ws_stream = match ServerBuilder::new().config(ws_config).accept(stream).await {
            Ok((_req, s)) => s,
            Err(_e) => {
                // Suppress errors from receiving normal HTTP requests
                // (e.g. browser preflight checks).
                return;
            }
        };

        let conn = WsConn::new(ws_stream, allow_public_space);
        let addr = ctx.start_actor(Box::new(conn));
        clients.write().await.insert(addr);
    }

    /// Starts the web server for peer ID discovery.
    ///
    /// Serves on `config.port + 1`. Routes:
    /// - `/peer_id` — returns this node's peer ID
    ///
    /// When TLS is configured, the server uses `tokio_native_tls` (the same
    /// TLS stack as the WebSocket server) rather than warp's built-in TLS,
    /// which was removed in warp 0.4. This keeps one TLS implementation
    /// across the codebase (DRY).
    async fn start_web_server(config: WsServerConfig, peer_id: String, metrics: Arc<Metrics>) {
        let port = config.port + 1;

        if let Some(cert_path) = config.cert_path {
            let key_path = config.key_path.unwrap();
            let _addr = format!("https://localhost:{}", port);

            // Load TLS identity (same pattern as WebSocket TLS above)
            let cert = std::fs::read(cert_path).expect("failed to read cert file");
            let key = std::fs::read(key_path).expect("failed to read key file");
            let identity = tokio_native_tls::native_tls::Identity::from_pkcs8(&cert, &key)
                .expect("failed to create TLS identity");
            let acceptor = tokio_native_tls::TlsAcceptor::from(
                tokio_native_tls::native_tls::TlsAcceptor::new(identity).unwrap(),
            );

            let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
                .await
                .expect("failed to bind web UI port");

            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(e) => {
                        log::error!("web UI accept error: {}", e);
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let peer_id = peer_id.clone();
                let metrics_clone = metrics.clone();
                crate::tokio_spawn::spawn(async move {
                    let stream = match acceptor.accept(stream).await {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    Self::handle_http_request(stream, &peer_id, &metrics_clone).await;
                });
            }
        }

        // Plain HTTP — manual handler (no warp dependency).
        let _addr = format!("http://localhost:{}", port);
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
            .await
            .expect("failed to bind web UI port");
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    log::error!("web UI accept error: {}", e);
                    continue;
                }
            };
            let peer_id = peer_id.clone();
            let metrics_clone = metrics.clone();
            crate::tokio_spawn::spawn(async move {
                Self::handle_http_request_plain(stream, &peer_id, &metrics_clone).await;
            });
        }
    }

    /// Handle a single HTTP request over a TLS stream.
    ///
    /// Reads one HTTP/1.1 request, responds based on the path:
    /// - `/peer_id` — returns this node's peer ID as plain text
    /// - `/metrics` — returns the current metrics snapshot as JSON
    ///
    /// Minimal handler — no routing framework needed for two endpoints.
    /// Core HTTP request handler — shared between TLS and plain TCP paths.
    ///
    /// Routes: `/peer_id` → plain text, `/metrics` → JSON, else 404.
    fn build_http_response(request: &str, peer_id: &str, metrics: &Metrics) -> Option<String> {
        let request_line = request.lines().next().unwrap_or("");

        if request_line.contains("GET /peer_id") {
            Some(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                peer_id.len(),
                peer_id
            ))
        } else if request_line.contains("GET /metrics") {
            let body = serde_json::to_string_pretty(&metrics.snapshot())
                .unwrap_or_else(|_| "{}".to_string());
            Some(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            ))
        } else {
            None
        }
    }

    /// Handle a single HTTP request over a TLS stream.
    async fn handle_http_request(
        mut stream: tokio_native_tls::TlsStream<tokio::net::TcpStream>,
        peer_id: &str,
        metrics: &Metrics,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut buf = [0u8; 1024];
        let n = match stream.read(&mut buf).await {
            Ok(n) => n,
            Err(_) => return,
        };

        let request = String::from_utf8_lossy(&buf[..n]);
        let response = match Self::build_http_response(&request, peer_id, metrics) {
            Some(r) => r,
            None => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string(),
        };

        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    }

    /// Handle a single HTTP request over a plain TCP stream.
    async fn handle_http_request_plain(
        mut stream: tokio::net::TcpStream,
        peer_id: &str,
        metrics: &Metrics,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut buf = [0u8; 1024];
        let n = match stream.read(&mut buf).await {
            Ok(n) => n,
            Err(_) => return,
        };

        let request = String::from_utf8_lossy(&buf[..n]);
        let response = match Self::build_http_response(&request, peer_id, metrics) {
            Some(r) => r,
            None => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_string(),
        };

        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
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
    /// Relays wire-format messages (Put, Get, Hi, BatchPut, Flush, RtcSignal)
    /// to all connected WebSocket clients except the sender. Internal
    /// messages (CheckQuorumTimeouts, RegisterQuorum) are never relayed —
    /// they are router-internal and would produce empty or malformed frames
    /// on the wire.
    async fn handle(&mut self, msg: Arc<Message>, _ctx: &ActorContext) {
        // Only relay messages that have a valid wire representation.
        // RegisterQuorum serializes to an empty string; CheckQuorumTimeouts
        // serializes to "_tick_quorum" — neither is a valid Gun.js wire
        // message and both would cause parse errors in connected peers.
        match &*msg {
            Message::Put(_)
            | Message::Get(_)
            | Message::BatchPut(_)
            | Message::Hi { .. }
            | Message::Flush(_)
            | Message::RtcSignal(_) => {}
            Message::CheckQuorumTimeouts | Message::RegisterQuorum { .. } => return,
        }

        for conn in self.clients.read().await.iter() {
            if msg.is_from(conn) {
                continue;
            }
            if conn.send((*msg).clone()).is_err() {
                self.clients.write().await.remove(conn);
            }
        }
    }

    async fn pre_start(&mut self, ctx: &ActorContext) {
        let addr = format!("0.0.0.0:{}", self.ws_config.port).to_string();
        let ctx = ctx.clone();

        let peer_id = ctx.peer_id.read().clone();
        let config_clone = self.ws_config.clone();
        let metrics = ctx.metrics.clone();
        ctx.child_task(async move {
            Self::start_web_server(config_clone, peer_id, metrics).await;
        });

        // Create the TCP listener
        let try_socket = TcpListener::bind(&addr).await;
        let listener = try_socket.expect("Failed to bind");

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

            let mut shutdown_rx = ctx.shutdown_rx.clone();
            ctx.clone().child_task(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.changed() => {
                            debug!("WsServer TLS accept loop shutting down");
                            break;
                        }
                        result = listener.accept() => {
                            if let Ok((stream, _)) = result {
                                let acceptor = acceptor.clone();
                                let clients = clients.clone();
                                let ctx = ctx.clone();
                                crate::tokio_spawn::spawn(async move {
                                    let stream = acceptor.accept(stream).await;
                                    if let Ok(stream) = stream {
                                        Self::handle_stream(
                                            stream,
                                            &ctx,
                                            clients.clone(),
                                            allow_public_space,
                                        )
                                        .await;
                                    }
                                });
                            }
                        }
                    }
                }
            });
        } else {
            let mut shutdown_rx = ctx.shutdown_rx.clone();
            ctx.clone().child_task(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown_rx.changed() => {
                            debug!("WsServer plain accept loop shutting down");
                            break;
                        }
                        result = listener.accept() => {
                            if let Ok((stream, _)) = result {
                                Self::handle_stream(
                                    stream,
                                    &ctx,
                                    clients.clone(),
                                    allow_public_space,
                                )
                                .await;
                            }
                        }
                    }
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
        info!(
            "WsServer stopping — closing {} client connections",
            self.clients.read().await.len()
        );
        // Dropping the client Addr senders closes their channels. The WsConn
        // actors will receive stop signals via ActorContext::stop() and send
        // WebSocket Close frames in their own stopping(). Here we just clear
        // the set so no new fan-out attempts are made.
        self.clients.write().await.clear();
    }
}
