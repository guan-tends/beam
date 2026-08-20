//! BEAM — a Rust implementation of the Gun.js P2P synchronized graph database.
//!
//! This is the command-line entry point for running a BEAM node server. It
//! configures storage and network adapters, then starts the node until it
//! receives SIGINT (Ctrl-C) or SIGTERM, at which point it performs a graceful
//! shutdown: flushes storage, closes connections, and drains in-flight messages
//! before exiting.
//!
//! # Usage
//!
//! ```bash
//! # Start with defaults (redb storage, websocket server on default port)
//! cargo run --bin beam
//!
//! # Start with custom port and outgoing peers
//! cargo run --bin beam -- start --port 8080 --peers wss://peer1.example.com,wss://peer2.example.com
//!
//! # Start with TLS
//! cargo run --bin beam -- start --cert-path /path/cert.pem --key-path /path/key.pem
//!
//! # Use in-memory storage only (no persistence)
//! cargo run --bin beam -- start --memory-storage true --redb-storage false
//!
//! # Disable public space (require content-hash addressing or user signatures)
//! cargo run --bin beam -- start --allow-public-space false
//!
//! # Set a custom graceful shutdown timeout (default: 30 seconds)
//! cargo run --bin beam -- start --shutdown-timeout 10
//! ```
//!
//! # Environment Variables
//!
//! All CLI options can also be set via environment variables (uppercase, with
//! underscores). CLI flags take precedence over env vars.
//!
//! | Flag | Env Var | Default |
//! |------|---------|---------|
//! | `--port` | `PORT` | 4944 |
//! | `--ws-server` | `WS_SERVER` | true |
//! | `--peers` | `PEERS` | (none) |
//! | `--multicast` | `MULTICAST` | false |
//! | `--redb-storage` | `REDB_STORAGE` | true |
//! | `--redb-path` | `REDB_PATH` | beam.redb |
//! | `--allow-public-space` | `ALLOW_PUBLIC_SPACE` | true |
//! | `--shutdown-timeout` | `SHUTDOWN_TIMEOUT` | 30 |
//!
//! # Graceful Shutdown
//!
//! When the node receives SIGINT or SIGTERM, [`Node::shutdown`] is called
//! with the configured timeout. The shutdown sequence:
//!
//! 1. **Flush storage** — pending writes in actor mailboxes are committed.
//! 2. **Signal child tasks** — accept loops and long-running tasks stop.
//! 3. **Drain** — in-flight messages and connection close handshakes complete.
//! 4. **Force stop** — any remaining tasks are aborted as a fallback.
//!
//! If a second signal is received during shutdown, the process exits
//! immediately with exit code 1.

#[cfg(not(target_arch = "wasm32"))]
mod cli;

#[cfg(not(target_arch = "wasm32"))]
use beam::actor::Actor;
#[cfg(not(target_arch = "wasm32"))]
use beam::adapters::{
    MemoryStorage, Multicast, OutgoingWebsocketManager, RedbStorage, WsServer, WsServerConfig,
};
#[cfg(not(target_arch = "wasm32"))]
use beam::{Config, Node};
#[cfg(not(target_arch = "wasm32"))]
use clap::Parser;
#[cfg(not(target_arch = "wasm32"))]
use cli::{Cli, Command};

/// Waits for a shutdown signal (SIGINT or SIGTERM on Unix, Ctrl-C on all platforms).
///
/// This is a convenience helper used by the `Start` command to await the first
/// or second shutdown signal without embedding platform-specific code directly
/// in `tokio::select!` branches (which don't support `#[cfg]` attributes).
#[cfg(not(target_arch = "wasm32"))]
async fn wait_for_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nSIGINT received — initiating graceful shutdown...");
            }
            _ = sigterm.recv() => {
                eprintln!("SIGTERM received — initiating graceful shutdown...");
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl-C");
        eprintln!("\nCtrl-C received — initiating graceful shutdown...");
    }
}

#[tokio::main]
#[cfg(not(target_arch = "wasm32"))]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        #[cfg(any(feature = "persy", feature = "fjall"))]
        Command::Migrate(args) => {
            #[cfg(not(target_arch = "wasm32"))]
            use beam::migration::{MigrateOpts, migrate};
            #[cfg(not(target_arch = "wasm32"))]
            use std::path::PathBuf;

            let from = beam::migration::MigrateError::parse_backend(&args.from)
                .unwrap_or_else(|e| panic!("Invalid --from value: {}", e));
            let to = beam::migration::MigrateError::parse_backend(&args.to)
                .unwrap_or_else(|e| panic!("Invalid --to value: {}", e));

            let opts = MigrateOpts {
                from,
                to,
                source_path: PathBuf::from(&args.source),
                target_path: PathBuf::from(&args.target),
                batch_size: args.batch_size,
                force: args.force,
                dry_run: args.dry_run,
            };

            eprintln!(
                "Migrating {} -> {} (batch_size={}, dry_run={})",
                from.as_str(),
                to.as_str(),
                opts.batch_size,
                opts.dry_run
            );

            match migrate(&opts) {
                Ok(report) => {
                    println!(
                        "Migration complete: {} records migrated",
                        report.records_migrated
                    );
                }
                Err(e) => {
                    eprintln!("Migration failed: {:?}", e);
                    std::process::exit(1);
                }
            }
        }

        #[cfg(not(any(feature = "persy", feature = "fjall")))]
        Command::Migrate(_) => {
            eprintln!(
                "Migration requires the 'persy' or 'fjall' feature. Rebuild with: cargo run --features fjall"
            );
            std::process::exit(1);
        }

        Command::Start(args) => {
            let mut outgoing_websocket_peers = Vec::new();
            if let Some(peers) = &args.peers {
                outgoing_websocket_peers = peers.split(',').map(|s| s.to_string()).collect();
            }

            env_logger::init();

            let mut network_adapters: Vec<Box<dyn Actor>> = Vec::new();
            let mut storage_adapters: Vec<Box<dyn Actor>> = Vec::new();

            let websocket_server = args.ws_server == "true";

            let config = Config {
                allow_public_space: args.allow_public_space != "false",
                ..Config::default()
            };

            // Initialize adapters based on CLI flags
            if args.multicast == "true" {
                network_adapters.push(Box::new(Multicast::new(config.clone())));
            }
            if websocket_server {
                network_adapters.push(Box::new(WsServer::new_with_config(
                    config.clone(),
                    WsServerConfig {
                        port: args.port,
                        cert_path: args.cert_path.clone(),
                        key_path: args.key_path.clone(),
                    },
                )));
            }
            if args.redb_storage != "false" {
                storage_adapters.push(Box::new(RedbStorage::new_with_config(
                    config.clone(),
                    &args.redb_path,
                    None,
                )));
            }
            if args.memory_storage == "true" {
                storage_adapters.push(Box::new(MemoryStorage::new()));
            }
            if !outgoing_websocket_peers.is_empty() {
                network_adapters.push(Box::new(OutgoingWebsocketManager::new(
                    config.clone(),
                    outgoing_websocket_peers,
                )));
            }

            let node = Node::new_with_config(config, storage_adapters, network_adapters);

            println!("BEAM node starting...");

            // Graceful shutdown via tokio::signal.
            //
            // We listen for SIGINT (Ctrl-C) and SIGTERM (Unix). The first
            // signal triggers Node::shutdown() which flushes storage, signals
            // child tasks, drains, and force-stops. A second signal (or
            // timeout expiry) exits immediately with code 1.
            let shutdown_timeout = web_time::Duration::from_secs(args.shutdown_timeout);
            let mut node_clone = node.clone();

            // Wait for the first shutdown signal.
            wait_for_signal().await;

            // Race graceful shutdown against a second signal.
            tokio::select! {
                result = node_clone.shutdown(shutdown_timeout) => {
                    match result {
                        Ok(()) => {
                            eprintln!("Graceful shutdown complete.");
                        }
                        Err(e) => {
                            eprintln!("Graceful shutdown timed out ({}), force-stopped.", e);
                        }
                    }
                }
                _ = wait_for_signal() => {
                    eprintln!("Second signal received — forcing exit.");
                    std::process::exit(1);
                }
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {}
