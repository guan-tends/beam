//! Rod — a Rust implementation of the Gun.js P2P synchronized graph database.
//!
//! This is the command-line entry point for running a Rod node server. It
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
//! | `--port` | `PORT` | 8443 |
//! | `--ws-server` | `WS_SERVER` | true |
//! | `--peers` | `PEERS` | (none) |
//! | `--multicast` | `MULTICAST` | false |
//! | `--redb-storage` | `REDB_STORAGE` | true |
//! | `--redb-path` | `REDB_PATH` | beam.redb |
//! | `--allow-public-space` | `ALLOW_PUBLIC_SPACE` | true |
//! | `--stats` | `STATS` | true |

extern crate clap;
use clap::{App, Arg, SubCommand};
use beamdb::actor::Actor;
use beamdb::adapters::{
    MemoryStorage, Multicast, OutgoingWebsocketManager, RedbStorage, WsServer, WsServerConfig,
};
use beamdb::{Config, Node};

#[tokio::main]
async fn main() {
    let default_port = WsServerConfig::default().port.to_string();
    let matches = App::new("Rod")
        .version("1.0")
        .author("Martti Malmi")
        .about("Rod node runner")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("start")
                .about("runs the beam server")
                .arg(
                    Arg::with_name("ws-server")
                        .long("ws-server")
                        .env("WS_SERVER")
                        .value_name("BOOL")
                        .help("Run websocket server?")
                        .default_value("true")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("port")
                        .short("p")
                        .long("port")
                        .env("PORT")
                        .value_name("NUMBER")
                        .help("Websocket server port")
                        .default_value(&default_port)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("cert-path")
                        .long("cert-path")
                        .env("CERT_PATH")
                        .value_name("FILE")
                        .help("TLS certificate path")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("key-path")
                        .long("key-path")
                        .env("KEY_PATH")
                        .value_name("FILE")
                        .help("TLS key path")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("peers")
                        .long("peers")
                        .env("PEERS")
                        .value_name("URLS")
                        .help("Comma-separated outgoing websocket peers (wss://...)")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("multicast")
                        .long("multicast")
                        .env("MULTICAST")
                        .value_name("BOOL")
                        .help("Enable multicast sync?")
                        .default_value("false")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("memory-storage")
                        .long("memory-storage")
                        .env("MEMORY_STORAGE")
                        .value_name("BOOL")
                        .help("In-memory storage")
                        .default_value("false")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("redb-storage")
                        .long("redb-storage")
                        .env("REDB_STORAGE")
                        .value_name("BOOL")
                        .help("redb storage (disk+mem)")
                        .default_value("true")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("redb-path")
                        .long("redb-path")
                        .env("REDB_PATH")
                        .value_name("PATH")
                        .help("Path to the redb database file")
                        .default_value("beam.redb")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("allow-public-space")
                        .long("allow-public-space")
                        .env("ALLOW_PUBLIC_SPACE")
                        .value_name("BOOL")
                        .help("Allow writes that are not content hash addressed or user-signed")
                        .default_value("true")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("stats")
                        .long("stats")
                        .env("STATS")
                        .value_name("BOOL")
                        .help("Show stats at /stats?")
                        .default_value("true")
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("migrate")
                .about("Migrate storage between redb and persy backends")
                .arg(
                    Arg::with_name("from")
                        .long("from")
                        .value_name("BACKEND")
                        .help("Source backend: 'redb' or 'persy'")
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("to")
                        .long("to")
                        .value_name("BACKEND")
                        .help("Target backend: 'redb' or 'persy'")
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("source")
                        .long("source")
                        .value_name("PATH")
                        .help("Path to source database file")
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("target")
                        .long("target")
                        .value_name("PATH")
                        .help("Path to target database file (will be created)")
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("batch-size")
                        .long("batch-size")
                        .value_name("N")
                        .help("Records per batch (default: 1000)")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("force")
                        .long("force")
                        .help("Overwrite target if it already exists"),
                )
                .arg(
                    Arg::with_name("dry-run")
                        .long("dry-run")
                        .help("Preview the migration without writing"),
                ),
        )
        .get_matches();

    #[cfg(feature = "persy")]
    {
        if let Some(migrate_matches) = matches.subcommand_matches("migrate") {
            use beamdb::migration::{migrate, Backend, MigrateOpts};
            use std::path::PathBuf;

            // Parse backend selector
            let parse_backend = |s: &str| -> Result<Backend, String> {
                match s {
                    "redb" => Ok(Backend::Redb),
                    "persy" => Ok(Backend::Persy),
                    _ => Err(format!("Unknown backend '{}': expected 'redb' or 'persy'", s)),
                }
            };

            let from_str = migrate_matches.value_of("from").unwrap();
            let to_str = migrate_matches.value_of("to").unwrap();
            let source_path = PathBuf::from(migrate_matches.value_of("source").unwrap());
            let target_path = PathBuf::from(migrate_matches.value_of("target").unwrap());

            let from = parse_backend(from_str)
                .unwrap_or_else(|e| panic!("Invalid --from value: {}", e));
            let to = parse_backend(to_str)
                .unwrap_or_else(|e| panic!("Invalid --to value: {}", e));

            let batch_size: usize = migrate_matches
                .value_of("batch-size")
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000);

            let force = migrate_matches.is_present("force");
            let dry_run = migrate_matches.is_present("dry-run");

            let opts = MigrateOpts {
                from,
                to,
                source_path,
                target_path,
                batch_size,
                force,
                dry_run,
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
                    println!("Migration complete: {} records migrated", report.records_migrated);
                    return;
                }
                Err(e) => {
                    eprintln!("Migration failed: {:?}", e);
                    std::process::exit(1);
                }
            }
        }
    }

    if let Some(matches) = matches.subcommand_matches("start") {
        // Note: a future refactor could extract match → Config conversion into a
        // dedicated function for testability. See GitHub issue tracker.
        let mut outgoing_websocket_peers = Vec::new();
        if let Some(peers) = matches.value_of("peers") {
            outgoing_websocket_peers = peers.split(",").map(|s| s.to_string()).collect();
        }

        env_logger::init();

        let websocket_server_port: u16 = matches.value_of("port").unwrap().parse::<u16>().unwrap();

        let mut network_adapters: Vec<Box<dyn Actor>> = Vec::new();
        let mut storage_adapters: Vec<Box<dyn Actor>> = Vec::new();

        let websocket_server = matches.value_of("ws-server").unwrap() == "true";

        let config = Config {
            allow_public_space: matches.value_of("allow-public-space").unwrap() != "false",
            stats: matches.value_of("stats").unwrap() == "true",
            ..Config::default()
        };

        // Initialize adapters based on CLI flags
        if matches.value_of("multicast").unwrap() == "true" {
            network_adapters.push(Box::new(Multicast::new(config.clone())));
        }
        if websocket_server {
            let cert_path = matches.value_of("cert-path").map(|s| s.to_string());
            let key_path = matches.value_of("key-path").map(|s| s.to_string());
            network_adapters.push(Box::new(WsServer::new_with_config(
                config.clone(),
                WsServerConfig {
                    port: websocket_server_port,
                    cert_path,
                    key_path,
                },
            )));
        }
        if matches.value_of("redb-storage").unwrap() != "false" {
            let redb_path = matches.value_of("redb-path").unwrap().to_string();
            storage_adapters.push(Box::new(RedbStorage::new_with_config(
                config.clone(),
                redb_path.as_str(),
                None,
            )));
        }
        if matches.value_of("memory-storage").unwrap() == "true" {
            storage_adapters.push(Box::new(MemoryStorage::new()));
        }
        if !outgoing_websocket_peers.is_empty() {
            network_adapters.push(Box::new(OutgoingWebsocketManager::new(
                config.clone(),
                outgoing_websocket_peers,
            )));
        }

        let node = Node::new_with_config(config, storage_adapters, network_adapters);

        println!("Rod node starting...");

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
