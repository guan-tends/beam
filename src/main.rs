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
//! cargo run --bin rod
//!
//! # Start with custom port and outgoing peers
//! cargo run --bin rod -- start --port 8080 --peers wss://peer1.example.com,wss://peer2.example.com
//!
//! # Start with TLS
//! cargo run --bin rod -- start --cert-path /path/cert.pem --key-path /path/key.pem
//!
//! # Use in-memory storage only (no persistence)
//! cargo run --bin rod -- start --memory-storage true --redb-storage false
//!
//! # Disable public space (require content-hash addressing or user signatures)
//! cargo run --bin rod -- start --allow-public-space false
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
//! | `--redb-path` | `REDB_PATH` | rod.redb |
//! | `--allow-public-space` | `ALLOW_PUBLIC_SPACE` | true |
//! | `--stats` | `STATS` | true |

extern crate clap;
use clap::{App, Arg, SubCommand};
use rod::actor::Actor;
use rod::adapters::{
    MemoryStorage, Multicast, OutgoingWebsocketManager, RedbStorage, WsServer, WsServerConfig,
};
use rod::{Config, Node};

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
                .about("runs the rod server")
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
                        .default_value("rod.redb")
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
        .get_matches();

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
