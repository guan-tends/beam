//! BEAM — a Rust implementation of the Gun.js P2P synchronized graph database.
//!
//! This is the command-line entry point for running a BEAM node server. It
//! configures storage and network adapters, then starts the node until
//! interrupted with Ctrl-C.
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

mod cli;

use beam::actor::Actor;
use beam::adapters::{
    MemoryStorage, Multicast, OutgoingWebsocketManager, RedbStorage, WsServer, WsServerConfig,
};
use beam::{Config, Node};
use clap::Parser;
use cli::{Cli, Command};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        #[cfg(feature = "persy")]
        Command::Migrate(args) => {
            use beam::migration::{Backend, MigrateOpts, migrate};
            use std::path::PathBuf;

            let parse_backend = |s: &str| -> Result<Backend, String> {
                match s {
                    "redb" => Ok(Backend::Redb),
                    "persy" => Ok(Backend::Persy),
                    _ => Err(format!(
                        "Unknown backend '{}': expected 'redb' or 'persy'",
                        s
                    )),
                }
            };

            let from =
                parse_backend(&args.from).unwrap_or_else(|e| panic!("Invalid --from value: {}", e));
            let to =
                parse_backend(&args.to).unwrap_or_else(|e| panic!("Invalid --to value: {}", e));

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

        #[cfg(not(feature = "persy"))]
        Command::Migrate(_) => {
            eprintln!(
                "Migration requires the 'persy' feature. Rebuild with: cargo run --features persy"
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

            let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();

            let mut node_clone = node.clone();
            let tx_mutex = std::sync::Mutex::new(Some(cancel_tx));
            ctrlc::set_handler(move || {
                node_clone.stop();
                if let Some(tx) = tx_mutex.lock().unwrap().take() {
                    tx.send(()).unwrap();
                }
            })
            .expect("Error setting Ctrl-C handler");

            let _ = cancel_rx.await;
        }
    }
}
