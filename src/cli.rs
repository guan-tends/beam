//! CLI argument parsing for the BEAM binary.
//!
//! Uses clap 4 derive API to define [`Cli`], [`Command`], [`StartArgs`], and
//! [`MigrateArgs`]. This module is a transport adapter — it parses
//! command-line arguments and environment variables into typed structs,
//! leaving domain logic to [`Config`](beam::Config) and the binary's
//! `main()`.
//!
//! # Subcommands
//!
//! - `start` — Run a BEAM node server with configurable storage and network
//! - `migrate` — Migrate data between redb and persy storage backends
//!
//! # Environment Variables
//!
//! All `start` options support env var fallback (uppercase, with underscores).
//! CLI flags take precedence over env vars.

use clap::{Args, Parser, Subcommand};

/// BEAM — a Rust implementation of the Gun.js P2P synchronized graph database.
#[derive(Debug, Parser)]
#[command(name = "BEAM", version, about = "BEAM node runner")]
pub struct Cli {
    /// Sets a custom config file.
    #[arg(short = 'c', long = "config", value_name = "FILE")]
    pub config: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

/// Available BEAM subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the BEAM server.
    Start(StartArgs),

    /// Migrate storage between redb and persy backends.
    Migrate(MigrateArgs),
}

/// Arguments for the `start` subcommand.
#[derive(Debug, Args)]
pub struct StartArgs {
    /// Run websocket server?
    #[arg(
        long = "ws-server",
        env = "WS_SERVER",
        value_name = "BOOL",
        default_value = "true"
    )]
    pub ws_server: String,

    /// Websocket server port.
    #[arg(
        short = 'p',
        long = "port",
        env = "PORT",
        value_name = "NUMBER",
        default_value = "4944"
    )]
    pub port: u16,

    /// TLS certificate path.
    #[arg(long = "cert-path", env = "CERT_PATH", value_name = "FILE")]
    pub cert_path: Option<String>,

    /// TLS key path.
    #[arg(long = "key-path", env = "KEY_PATH", value_name = "FILE")]
    pub key_path: Option<String>,

    /// Comma-separated outgoing websocket peers (wss://...).
    #[arg(long = "peers", env = "PEERS", value_name = "URLS")]
    pub peers: Option<String>,

    /// Enable multicast sync?
    #[arg(
        long = "multicast",
        env = "MULTICAST",
        value_name = "BOOL",
        default_value = "false"
    )]
    pub multicast: String,

    /// In-memory storage.
    #[arg(
        long = "memory-storage",
        env = "MEMORY_STORAGE",
        value_name = "BOOL",
        default_value = "false"
    )]
    pub memory_storage: String,

    /// redb storage (disk+mem).
    #[arg(
        long = "redb-storage",
        env = "REDB_STORAGE",
        value_name = "BOOL",
        default_value = "true"
    )]
    pub redb_storage: String,

    /// Path to the redb database file.
    #[arg(
        long = "redb-path",
        env = "REDB_PATH",
        value_name = "PATH",
        default_value = "beam.redb"
    )]
    pub redb_path: String,

    /// Allow writes that are not content hash addressed or user-signed.
    #[arg(
        long = "allow-public-space",
        env = "ALLOW_PUBLIC_SPACE",
        value_name = "BOOL",
        default_value = "true"
    )]
    pub allow_public_space: String,
}

/// Arguments for the `migrate` subcommand.
#[derive(Debug, Args)]
pub struct MigrateArgs {
    /// Source backend: 'redb' or 'persy'.
    #[arg(long = "from", value_name = "BACKEND")]
    pub from: String,

    /// Target backend: 'redb' or 'persy'.
    #[arg(long = "to", value_name = "BACKEND")]
    pub to: String,

    /// Path to source database file.
    #[arg(long = "source", value_name = "PATH")]
    pub source: String,

    /// Path to target database file (will be created).
    #[arg(long = "target", value_name = "PATH")]
    pub target: String,

    /// Records per batch (default: 1000).
    #[arg(long = "batch-size", value_name = "N", default_value = "1000")]
    pub batch_size: usize,

    /// Overwrite target if it already exists.
    #[arg(long = "force")]
    pub force: bool,

    /// Preview the migration without writing.
    #[arg(long = "dry-run")]
    pub dry_run: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn start_with_defaults() {
        let cli = Cli::parse_from(["beam", "start"]);
        match cli.command {
            Command::Start(args) => {
                assert_eq!(args.port, 4944);
                assert_eq!(args.ws_server, "true");
                assert_eq!(args.multicast, "false");
                assert_eq!(args.redb_storage, "true");
                assert_eq!(args.redb_path, "beam.redb");
                assert_eq!(args.memory_storage, "false");
                assert_eq!(args.allow_public_space, "true");
                assert!(args.cert_path.is_none());
                assert!(args.key_path.is_none());
                assert!(args.peers.is_none());
            }
            Command::Migrate(_) => panic!("expected Start"),
        }
    }

    #[test]
    fn start_with_custom_port() {
        let cli = Cli::parse_from(["beam", "start", "--port", "8080"]);
        match cli.command {
            Command::Start(args) => assert_eq!(args.port, 8080),
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn start_with_short_port_flag() {
        let cli = Cli::parse_from(["beam", "start", "-p", "3000"]);
        match cli.command {
            Command::Start(args) => assert_eq!(args.port, 3000),
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn start_with_peers() {
        let cli = Cli::parse_from([
            "beam",
            "start",
            "--peers",
            "wss://peer1.example.com,wss://peer2.example.com",
        ]);
        match cli.command {
            Command::Start(args) => {
                let peers = args.peers.unwrap();
                assert!(peers.contains("peer1.example.com"));
                assert!(peers.contains("peer2.example.com"));
            }
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn start_with_tls() {
        let cli = Cli::parse_from([
            "beam",
            "start",
            "--cert-path",
            "/path/cert.pem",
            "--key-path",
            "/path/key.pem",
        ]);
        match cli.command {
            Command::Start(args) => {
                assert_eq!(args.cert_path.unwrap(), "/path/cert.pem");
                assert_eq!(args.key_path.unwrap(), "/path/key.pem");
            }
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn start_disable_redb() {
        let cli = Cli::parse_from([
            "beam",
            "start",
            "--redb-storage",
            "false",
            "--memory-storage",
            "true",
        ]);
        match cli.command {
            Command::Start(args) => {
                assert_eq!(args.redb_storage, "false");
                assert_eq!(args.memory_storage, "true");
            }
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn start_disable_public_space() {
        let cli = Cli::parse_from(["beam", "start", "--allow-public-space", "false"]);
        match cli.command {
            Command::Start(args) => assert_eq!(args.allow_public_space, "false"),
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn start_with_custom_redb_path() {
        let cli = Cli::parse_from(["beam", "start", "--redb-path", "/tmp/custom.beam"]);
        match cli.command {
            Command::Start(args) => assert_eq!(args.redb_path, "/tmp/custom.beam"),
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn start_with_config_flag() {
        let cli = Cli::parse_from(["beam", "-c", "config.toml", "start"]);
        assert_eq!(cli.config, Some("config.toml".to_string()));
    }

    #[test]
    fn start_with_config_long_flag() {
        let cli = Cli::parse_from(["beam", "--config", "config.toml", "start"]);
        assert_eq!(cli.config, Some("config.toml".to_string()));
    }

    #[test]
    fn migrate_basic() {
        let cli = Cli::parse_from([
            "beam",
            "migrate",
            "--from",
            "redb",
            "--to",
            "persy",
            "--source",
            "/tmp/source.redb",
            "--target",
            "/tmp/target.persy",
        ]);
        match cli.command {
            Command::Migrate(args) => {
                assert_eq!(args.from, "redb");
                assert_eq!(args.to, "persy");
                assert_eq!(args.source, "/tmp/source.redb");
                assert_eq!(args.target, "/tmp/target.persy");
                assert_eq!(args.batch_size, 1000);
                assert!(!args.force);
                assert!(!args.dry_run);
            }
            _ => panic!("expected Migrate"),
        }
    }

    #[test]
    fn migrate_with_batch_size() {
        let cli = Cli::parse_from([
            "beam",
            "migrate",
            "--from",
            "persy",
            "--to",
            "redb",
            "--source",
            "/tmp/s.persy",
            "--target",
            "/tmp/t.redb",
            "--batch-size",
            "500",
        ]);
        match cli.command {
            Command::Migrate(args) => assert_eq!(args.batch_size, 500),
            _ => panic!("expected Migrate"),
        }
    }

    #[test]
    fn migrate_with_flags() {
        let cli = Cli::parse_from([
            "beam",
            "migrate",
            "--from",
            "redb",
            "--to",
            "persy",
            "--source",
            "/tmp/s.redb",
            "--target",
            "/tmp/t.persy",
            "--force",
            "--dry-run",
        ]);
        match cli.command {
            Command::Migrate(args) => {
                assert!(args.force);
                assert!(args.dry_run);
            }
            _ => panic!("expected Migrate"),
        }
    }

    #[test]
    fn env_var_port() {
        // SAFETY: env vars are process-wide; we set/restore in the same test.
        // Other tests in this module don't check PORT, so this is safe.
        unsafe {
            std::env::set_var("PORT", "9999");
        }
        let cli = Cli::parse_from(["beam", "start"]);
        match cli.command {
            Command::Start(args) => assert_eq!(args.port, 9999),
            _ => panic!("expected Start"),
        }
        unsafe {
            std::env::remove_var("PORT");
        }
    }

    #[test]
    fn env_var_peers() {
        unsafe {
            std::env::set_var("PEERS", "wss://env-peer.example.com");
        }
        let cli = Cli::parse_from(["beam", "start"]);
        match cli.command {
            Command::Start(args) => {
                assert_eq!(args.peers.unwrap(), "wss://env-peer.example.com");
            }
            _ => panic!("expected Start"),
        }
        unsafe {
            std::env::remove_var("PEERS");
        }
    }

    #[test]
    fn cli_flag_overrides_env_var() {
        unsafe {
            std::env::set_var("PORT", "9999");
        }
        let cli = Cli::parse_from(["beam", "start", "--port", "7777"]);
        match cli.command {
            Command::Start(args) => assert_eq!(args.port, 7777),
            _ => panic!("expected Start"),
        }
        unsafe {
            std::env::remove_var("PORT");
        }
    }

    #[test]
    fn migrate_missing_from() {
        // clap exits the process on missing required args; use try_parse
        let result = Cli::try_parse_from([
            "beam", "migrate", "--to", "persy", "--source", "s", "--target", "t",
        ]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("required"),
            "error should mention required: {err}"
        );
    }

    #[test]
    fn migrate_missing_to() {
        let result = Cli::try_parse_from([
            "beam", "migrate", "--from", "redb", "--source", "s", "--target", "t",
        ]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("required"),
            "error should mention required: {err}"
        );
    }

    #[test]
    fn migrate_missing_source() {
        let result = Cli::try_parse_from([
            "beam", "migrate", "--from", "redb", "--to", "persy", "--target", "t",
        ]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("required"),
            "error should mention required: {err}"
        );
    }

    #[test]
    fn migrate_missing_target() {
        let result = Cli::try_parse_from([
            "beam", "migrate", "--from", "redb", "--to", "persy", "--source", "s",
        ]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("required"),
            "error should mention required: {err}"
        );
    }

    #[test]
    fn no_subcommand_fails() {
        // Without a subcommand, clap should error.
        let result = Cli::try_parse_from(["beam"]);
        assert!(result.is_err(), "parsing without subcommand should fail");
    }
}
