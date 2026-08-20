//! Storage backend migration: redb ↔ Persy.
//!
//! Provides one-shot batch migration between BEAM's two storage backends.
//! The translation is format-only: the inner `Children` data is byte-identical
//! between redb and Persy; only the wrapper struct ([`NodeRecord`]) differs.
//!
//! # On-disk formats
//!
//! **redb**: `TableDefinition<&str, &[u8]>` in the `beam_nodes_v1` table.
//! Key is the node_id directly; value is `postcard(Children)` (no wrapper).
//!
//! **Persy**: A segment named `beam_nodes_v1` containing opaque records.
//! Each record is `postcard(NodeRecord { node_id, children })`.
//!
//! # CLI
//!
//! ```text
//! beam migrate --from <redb|persy> --to <redb|persy> \
//!     --source <path> --target <path> \
//!     [--batch-size 1000] [--force] [--dry-run]
//! ```
//!
//! See [the migration plan](../docs/plans/PERSY-STORAGE-ADAPTER.md) for the
//! full design rationale.

use std::path::PathBuf;
use web_time::Duration;

use serde::{Deserialize, Serialize};

use crate::types::Children;

/// On-disk record format used by the Persy adapter.
///
/// Mirrors `crate::adapters::persy_storage::NodeRecord` (which lives behind the
/// `persy` feature flag). Defined here as a local copy so the migration
/// translation logic can be unit-tested without enabling the `persy` feature.
///
/// At runtime, the I/O module (gated on `persy`) uses the canonical definition
/// from `persy_storage`. The two are structurally identical and serialize to
/// the same postcard bytes, but live in separate compilation units so the
/// migration library compiles without the Persy dependency.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
pub(crate) struct NodeRecord {
    pub(crate) node_id: String,
    pub(crate) children: Children,
}

// ─────────────────────────────────────────────────────────────────────────────
// Public types
// ─────────────────────────────────────────────────────────────────────────────

/// Source/target backend selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Backend {
    /// redb backend (single-file embedded B+tree database, default)
    Redb,
    /// Persy backend (single-file embedded database with MVCC)
    Persy,
    /// fjall backend (LSM-tree, directory-based, high write throughput)
    Fjall,
}

impl Backend {
    /// Returns the canonical lowercase string used in CLI args and logs.
    pub fn as_str(&self) -> &'static str {
        match self {
            Backend::Redb => "redb",
            Backend::Persy => "persy",
            Backend::Fjall => "fjall",
        }
    }
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Migration options, typically parsed from CLI arguments.
#[derive(Debug, Clone)]
pub struct MigrateOpts {
    /// Source backend format
    pub from: Backend,
    /// Target backend format
    pub to: Backend,
    /// Path to source database (file for redb, file for Persy)
    pub source_path: PathBuf,
    /// Path to target database (will be created)
    pub target_path: PathBuf,
    /// Records per write batch (default: 1000)
    pub batch_size: usize,
    /// Overwrite target if it already exists
    pub force: bool,
    /// Preview the migration without writing
    pub dry_run: bool,
}

/// Result of a completed migration, returned to the caller for reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationReport {
    /// Number of records successfully written to target
    pub records_migrated: usize,
    /// Number of records found in source
    pub source_count: usize,
    /// Number of records in target after migration (== `records_migrated` unless partial)
    pub target_count_after: usize,
    /// Total wall-clock duration
    pub elapsed: Duration,
    /// Whether this was a dry run
    pub dry_run: bool,
}

/// All migration error variants.
///
/// Uses [`thiserror`] for idiomatic error definitions. Each variant carries
/// enough context to be useful in CLI output (path, backend, underlying error).
#[derive(thiserror::Error, Debug)]
pub enum MigrateError {
    #[error("redb error at {path}: {source}")]
    Redb {
        path: PathBuf,
        #[source]
        source: redb::Error,
    },

    #[error("redb transaction error at {path}: {source}")]
    RedbTx {
        path: PathBuf,
        #[source]
        source: redb::TransactionError,
    },

    #[error("redb table error at {path}: {source}")]
    RedbTable {
        path: PathBuf,
        #[source]
        source: redb::TableError,
    },

    #[error("redb commit error at {path}: {source}")]
    RedbCommit {
        path: PathBuf,
        #[source]
        source: redb::CommitError,
    },

    #[error("persy error: {0}")]
    Persy(String),

    #[error("fjall error: {0}")]
    Fjall(String),

    #[error("postcard error: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("target already exists at {0} (use --force to overwrite)")]
    TargetExists(PathBuf),

    #[error("unsupported migration: {from} -> {to} (use --from and --to with different backends)")]
    Unsupported { from: Backend, to: Backend },

    #[error("invalid backend string: {0} (expected 'redb', 'persy', or 'fjall')")]
    InvalidBackend(String),
}

impl MigrateError {
    /// Parse a backend name from a CLI string. Returns [`MigrateError::InvalidBackend`]
    /// if the string is not recognized.
    pub fn parse_backend(s: &str) -> Result<Backend, MigrateError> {
        match s.to_lowercase().as_str() {
            "redb" => Ok(Backend::Redb),
            "persy" => Ok(Backend::Persy),
            "fjall" => Ok(Backend::Fjall),
            _ => Err(MigrateError::InvalidBackend(s.to_string())),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pure translation functions (no I/O — trivially testable)
// ─────────────────────────────────────────────────────────────────────────────

/// Translates a redb record (node_id + raw value bytes) into a Persy record payload.
///
/// The inner `Children` data is deserialized from the redb format and re-serialized
/// inside a [`NodeRecord`] wrapper for Persy. Both formats use postcard on `Children`,
/// so the inner bytes are preserved exactly.
///
/// # Errors
///
/// Returns [`MigrateError::Postcard`] if the input bytes are not a valid postcard-encoded
/// `Children` map.
pub fn redb_to_persy_payload(key: &str, value: &[u8]) -> Result<Vec<u8>, MigrateError> {
    let children: Children = postcard::from_bytes(value)?;
    let record = NodeRecord {
        node_id: key.to_string(),
        children,
    };
    Ok(postcard::to_allocvec(&record)?)
}

/// Translates a Persy record payload into a redb (node_id, value_bytes) pair.
///
/// Counterpart to [`redb_to_persy_payload`]: unwraps the [`NodeRecord`], re-serializes
/// the `Children` map in redb's bare-bytes format.
///
/// # Errors
///
/// Returns [`MigrateError::Postcard`] if the input bytes are not a valid postcard-encoded
/// `NodeRecord`.
pub fn persy_to_redb_record(payload: &[u8]) -> Result<(String, Vec<u8>), MigrateError> {
    let record: NodeRecord = postcard::from_bytes(payload)?;
    let children_bytes = postcard::to_allocvec(&record.children)?;
    Ok((record.node_id, children_bytes))
}

// ─────────────────────────────────────────────────────────────────────────────
// Canonical intermediate format
// ─────────────────────────────────────────────────────────────────────────────

/// A single graph node record in the canonical migration format.
///
/// All BEAM storage backends store the same logical data — a `node_id`
/// mapped to a [`Children`] map. The on-disk encoding varies (bare bytes
/// vs. `NodeRecord`-wrapped, string keys vs. prefixed keys), but the
/// semantic content is identical.
///
/// This struct is the "lingua franca": every reader produces
/// `Vec<MigrationRecord>`, every writer accepts `&[MigrationRecord]`.
/// The `children_bytes` field is always `postcard(Children)` — the bare
/// serialized form that redb and fjall store directly, and that persy
/// wraps in [`NodeRecord`].
#[derive(Debug, Clone)]
pub struct MigrationRecord {
    /// The graph node identifier (e.g. `"users/alice"`, `""` for root).
    pub node_id: String,
    /// Bare `postcard(Children)` bytes — the universal value format.
    pub children_bytes: Vec<u8>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Fjall key translation — pure functions (no feature gate needed)
// ─────────────────────────────────────────────────────────────────────────────

/// Fjall key prefix (mirrors `fjall_storage::KEY_PREFIX`).
///
/// Local copy so the translation functions compile without the `fjall`
/// feature, following the same pattern as [`NodeRecord`].
const FJALL_KEY_PREFIX: u8 = 0x00;

/// Encodes a node_id string as a fjall keyspace key.
///
/// Prepends [`FJALL_KEY_PREFIX`] to avoid fjall's LSM-tree panic on empty
/// keys. The value bytes are identical between redb and fjall (both bare
/// `postcard(Children)`), so only the key needs encoding.
pub fn redb_to_fjall_key(node_id: &str) -> Vec<u8> {
    let mut key = vec![FJALL_KEY_PREFIX];
    key.extend_from_slice(node_id.as_bytes());
    key
}

/// Decodes a fjall keyspace key back to a node_id string.
///
/// Strips the [`FJALL_KEY_PREFIX`] byte and interprets the remaining bytes
/// as UTF-8. Returns [`MigrateError::Fjall`] if the key is malformed.
pub fn fjall_key_to_node_id(key: &[u8]) -> Result<String, MigrateError> {
    if key.is_empty() || key[0] != FJALL_KEY_PREFIX {
        return Err(MigrateError::Fjall(format!("invalid fjall key: {:?}", key)));
    }
    std::str::from_utf8(&key[1..])
        .map(|s| s.to_string())
        .map_err(|e| MigrateError::Fjall(format!("fjall key UTF-8 decode: {:?}", e)))
}

// ─────────────────────────────────────────────────────────────────────────────
// I/O orchestration — requires at least one non-redb backend feature
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(any(feature = "persy", feature = "fjall"))]
pub(crate) mod io {
    use super::*;
    use web_time::Instant;

    use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

    const REDB_BEAM_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("beam_nodes_v1");

    // ───────────────────────────────────────────────────────────────────────
    // Readers — one per backend, each produces Vec<MigrationRecord>
    // ───────────────────────────────────────────────────────────────────────

    /// Reads all records from a redb database file.
    ///
    /// redb stores keys as `&str` (node_id) and values as `&[u8]`
    /// (`postcard(Children)` bare bytes) — already canonical.
    fn read_redb(path: &std::path::Path) -> Result<Vec<MigrationRecord>, MigrateError> {
        let db = Database::open(path).map_err(|source| MigrateError::Redb {
            path: path.to_path_buf(),
            source: source.into(),
        })?;
        let tx = db.begin_read().map_err(|source| MigrateError::RedbTx {
            path: path.to_path_buf(),
            source,
        })?;

        // Empty source DB (no table yet) → zero records. This is valid.
        let table = match tx.open_table(REDB_BEAM_NODES) {
            Ok(t) => t,
            Err(e) => {
                use redb::TableError;
                if matches!(e, TableError::TableDoesNotExist { .. }) {
                    return Ok(Vec::new());
                }
                return Err(MigrateError::RedbTable {
                    path: path.to_path_buf(),
                    source: e,
                });
            }
        };

        let mut records = Vec::new();
        let iter = table.iter().map_err(|source| MigrateError::RedbTable {
            path: path.to_path_buf(),
            source: redb::TableError::Storage(source),
        })?;

        for entry in iter {
            let (key_guard, value_guard) = entry.map_err(|source| MigrateError::RedbTable {
                path: path.to_path_buf(),
                source: redb::TableError::Storage(source),
            })?;
            records.push(MigrationRecord {
                node_id: key_guard.value().to_string(),
                children_bytes: value_guard.value().to_vec(),
            });
        }

        Ok(records)
    }

    /// Reads all records from a Persy database file.
    ///
    /// Persy stores records as `postcard(NodeRecord { node_id, children })`
    /// — the reader unwraps each into canonical `(node_id, children_bytes)`.
    #[cfg(feature = "persy")]
    fn read_persy(path: &std::path::Path) -> Result<Vec<MigrationRecord>, MigrateError> {
        use crate::adapters::persy_storage::BEAM_NODES as PERSY_BEAM_NODES;

        let db = persy::Persy::open(path, persy::Config::new())
            .map_err(|source| MigrateError::Persy(format!("{}: {}", path.display(), source)))?;
        let segment_id = db
            .solve_segment_id(PERSY_BEAM_NODES)
            .map_err(|e| MigrateError::Persy(format!("solve_segment_id: {}", e)))?;

        let scan = db.scan(segment_id).map_err(|source| {
            MigrateError::Persy(format!("{}: scan: {}", path.display(), source))
        })?;

        let mut records = Vec::new();
        for (_id, bytes) in scan {
            let (node_id, children_bytes) = persy_to_redb_record(&bytes)?;
            records.push(MigrationRecord {
                node_id,
                children_bytes,
            });
        }

        Ok(records)
    }

    /// Reads all records from a fjall database directory.
    ///
    /// Fjall stores keys as `[0x00] ++ node_id_bytes` and values as
    /// `postcard(Children)` bare bytes. The reader strips the key prefix
    /// and yields canonical `(node_id, children_bytes)`.
    #[cfg(feature = "fjall")]
    fn read_fjall(path: &std::path::Path) -> Result<Vec<MigrationRecord>, MigrateError> {
        let db = fjall::Database::builder(path)
            .open()
            .map_err(|e| MigrateError::Fjall(format!("{}: {}", path.display(), e)))?;
        let keyspace = db
            .keyspace("beam_nodes_v1", fjall::KeyspaceCreateOptions::default)
            .map_err(|e| MigrateError::Fjall(format!("keyspace: {}", e)))?;

        let mut records = Vec::new();
        for item in keyspace.iter() {
            // Guard::into_inner() returns Result<KvPair> = Result<(Vec<u8>, Slice)>
            let (key, value) = item
                .into_inner()
                .map_err(|e| MigrateError::Fjall(format!("iter: {}", e)))?;
            let node_id = fjall_key_to_node_id(&key)?;
            records.push(MigrationRecord {
                node_id,
                children_bytes: value.to_vec(),
            });
        }

        Ok(records)
    }

    // ───────────────────────────────────────────────────────────────────────
    // Writers — one per backend, each accepts &[MigrationRecord]
    // ───────────────────────────────────────────────────────────────────────

    /// Writes records to a redb database file.
    ///
    /// redb stores keys as `&str` and values as `&[u8]` — canonical format
    /// maps directly. Commits in batches of `batch_size` to bound memory
    /// usage for large migrations.
    fn write_redb(
        path: &std::path::Path,
        records: &[MigrationRecord],
        batch_size: usize,
    ) -> Result<usize, MigrateError> {
        let db = Database::create(path).map_err(|source| MigrateError::Redb {
            path: path.to_path_buf(),
            source: source.into(),
        })?;

        let mut migrated = 0usize;
        let mut batch: Vec<(&str, &[u8])> = Vec::with_capacity(batch_size);

        for record in records {
            batch.push((&record.node_id, &record.children_bytes));

            if batch.len() >= batch_size {
                let txn = db.begin_write().map_err(|source| MigrateError::RedbTx {
                    path: path.to_path_buf(),
                    source,
                })?;
                {
                    let mut table = txn.open_table(REDB_BEAM_NODES).map_err(|source| {
                        MigrateError::RedbTable {
                            path: path.to_path_buf(),
                            source,
                        }
                    })?;
                    for (k, v) in &batch {
                        table
                            .insert(*k, *v)
                            .map_err(|source| MigrateError::RedbTable {
                                path: path.to_path_buf(),
                                source: redb::TableError::Storage(source),
                            })?;
                    }
                }
                txn.commit().map_err(|source| MigrateError::RedbCommit {
                    path: path.to_path_buf(),
                    source,
                })?;
                migrated += batch.len();
                batch.clear();
            }
        }

        // Flush remaining records
        if !batch.is_empty() {
            let txn = db.begin_write().map_err(|source| MigrateError::RedbTx {
                path: path.to_path_buf(),
                source,
            })?;
            {
                let mut table =
                    txn.open_table(REDB_BEAM_NODES)
                        .map_err(|source| MigrateError::RedbTable {
                            path: path.to_path_buf(),
                            source,
                        })?;
                for (k, v) in &batch {
                    table
                        .insert(*k, *v)
                        .map_err(|source| MigrateError::RedbTable {
                            path: path.to_path_buf(),
                            source: redb::TableError::Storage(source),
                        })?;
                }
            }
            txn.commit().map_err(|source| MigrateError::RedbCommit {
                path: path.to_path_buf(),
                source,
            })?;
            migrated += batch.len();
        }

        Ok(migrated)
    }

    /// Writes records to a Persy database file.
    ///
    /// Persy stores records as `postcard(NodeRecord { node_id, children })`.
    /// The writer wraps each canonical record via `redb_to_persy_payload()`.
    #[cfg(feature = "persy")]
    fn write_persy(
        path: &std::path::Path,
        records: &[MigrationRecord],
    ) -> Result<usize, MigrateError> {
        use crate::adapters::persy_storage::BEAM_NODES as PERSY_BEAM_NODES;

        // Materialize all payloads before opening the target — same pattern
        // as the original migrate_redb_to_persy.
        let payloads: Vec<Vec<u8>> = records
            .iter()
            .map(|r| redb_to_persy_payload(&r.node_id, &r.children_bytes))
            .collect::<Result<_, _>>()?;

        let target_db = persy::Persy::open_or_create_with(
            path.to_string_lossy().as_ref(),
            persy::Config::new(),
            |persy_db| -> Result<(), Box<dyn std::error::Error>> {
                let mut create_tx = persy_db.begin()?;
                create_tx.create_segment(PERSY_BEAM_NODES)?;
                create_tx.prepare()?.commit()?;
                Ok(())
            },
        )
        .map_err(|source| MigrateError::Persy(format!("{}: {}", path.display(), source)))?;

        let target_seg = target_db
            .solve_segment_id(PERSY_BEAM_NODES)
            .map_err(|e| MigrateError::Persy(format!("solve_segment_id: {}", e)))?;

        let mut tx = target_db
            .begin()
            .map_err(|e| MigrateError::Persy(format!("begin: {}", e)))?;

        for payload in &payloads {
            tx.insert(target_seg, payload.as_slice())
                .map_err(|e| MigrateError::Persy(format!("insert: {}", e)))?;
        }

        tx.prepare()
            .map_err(|e| MigrateError::Persy(format!("prepare: {}", e)))?
            .commit()
            .map_err(|e| MigrateError::Persy(format!("commit: {}", e)))?;

        drop(target_db);
        Ok(payloads.len())
    }

    /// Writes records to a fjall database directory.
    ///
    /// Fjall stores keys as `[0x00] ++ node_id_bytes` and values as
    /// `postcard(Children)` bare bytes. The writer encodes keys via
    /// `redb_to_fjall_key()` and passes values through unchanged.
    #[cfg(feature = "fjall")]
    fn write_fjall(
        path: &std::path::Path,
        records: &[MigrationRecord],
    ) -> Result<usize, MigrateError> {
        let db = fjall::Database::builder(path)
            .open()
            .map_err(|e| MigrateError::Fjall(format!("{}: {}", path.display(), e)))?;
        let keyspace = db
            .keyspace("beam_nodes_v1", fjall::KeyspaceCreateOptions::default)
            .map_err(|e| MigrateError::Fjall(format!("keyspace: {}", e)))?;

        for record in records {
            let key = redb_to_fjall_key(&record.node_id);
            keyspace
                .insert(key, &record.children_bytes)
                .map_err(|e| MigrateError::Fjall(format!("insert: {}", e)))?;
        }

        // Explicit fsync for durability — the target is a fresh database
        // and the migration is complete.
        db.persist(fjall::PersistMode::SyncAll)
            .map_err(|e| MigrateError::Fjall(format!("persist: {}", e)))?;

        Ok(records.len())
    }

    // ───────────────────────────────────────────────────────────────────────
    // Dispatcher — read source, write target (reader/writer pattern)
    // ───────────────────────────────────────────────────────────────────────

    /// Run a storage migration.
    ///
    /// Reads all records from the source backend into canonical
    /// [`MigrationRecord`] format, then writes them to the target backend.
    /// The reader/writer pattern means adding a new backend requires only
    /// one reader and one writer — O(N) functions, not O(N²) pairwise.
    ///
    /// # Errors
    ///
    /// Returns [`MigrateError::Unsupported`] if `from == to`, or any backend
    /// I/O error if the source can't be read or the target can't be written.
    pub fn migrate(opts: &MigrateOpts) -> Result<MigrationReport, MigrateError> {
        if opts.from == opts.to {
            return Err(MigrateError::Unsupported {
                from: opts.from,
                to: opts.to,
            });
        }

        if !opts.dry_run && opts.target_path.exists() && !opts.force {
            return Err(MigrateError::TargetExists(opts.target_path.clone()));
        }

        let start = Instant::now();

        // Read all records from source into canonical format.
        // Each arm is feature-gated via block-level cfg so the match is
        // exhaustive under any combination of backend features.
        let records = match opts.from {
            Backend::Redb => read_redb(&opts.source_path)?,
            Backend::Persy => {
                #[cfg(feature = "persy")]
                {
                    read_persy(&opts.source_path)?
                }
                #[cfg(not(feature = "persy"))]
                {
                    return Err(MigrateError::Unsupported {
                        from: opts.from,
                        to: opts.to,
                    });
                }
            }
            Backend::Fjall => {
                #[cfg(feature = "fjall")]
                {
                    read_fjall(&opts.source_path)?
                }
                #[cfg(not(feature = "fjall"))]
                {
                    return Err(MigrateError::Unsupported {
                        from: opts.from,
                        to: opts.to,
                    });
                }
            }
        };
        let source_count = records.len();

        // Dry-run: count only, no writes
        if opts.dry_run {
            return Ok(MigrationReport {
                records_migrated: source_count,
                source_count,
                target_count_after: 0,
                elapsed: start.elapsed(),
                dry_run: true,
            });
        }

        // Write all records to target
        let migrated = match opts.to {
            Backend::Redb => write_redb(&opts.target_path, &records, opts.batch_size)?,
            Backend::Persy => {
                #[cfg(feature = "persy")]
                {
                    write_persy(&opts.target_path, &records)?
                }
                #[cfg(not(feature = "persy"))]
                {
                    return Err(MigrateError::Unsupported {
                        from: opts.from,
                        to: opts.to,
                    });
                }
            }
            Backend::Fjall => {
                #[cfg(feature = "fjall")]
                {
                    write_fjall(&opts.target_path, &records)?
                }
                #[cfg(not(feature = "fjall"))]
                {
                    return Err(MigrateError::Unsupported {
                        from: opts.from,
                        to: opts.to,
                    });
                }
            }
        };

        Ok(MigrationReport {
            records_migrated: migrated,
            source_count,
            target_count_after: migrated,
            elapsed: start.elapsed(),
            dry_run: false,
        })
    }
}

#[cfg(any(feature = "persy", feature = "fjall"))]
pub use io::migrate;

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NodeData, Value};
    use arena_btreemap::BTreeMap;

    /// Helper: build a representative `Children` map using the real BEAM value types.
    fn make_test_children() -> Children {
        let mut children = BTreeMap::default();
        children.insert(
            "greeting".to_string(),
            NodeData {
                value: Value::Text("hello".to_string()),
                updated_at: 12345.0,
            },
        );
        children.insert(
            "count".to_string(),
            NodeData {
                value: Value::Number(42.0),
                updated_at: 67890.0,
            },
        );
        children.insert(
            "flag".to_string(),
            NodeData {
                value: Value::Bit(true),
                updated_at: 11111.0,
            },
        );
        children
    }

    #[test]
    fn redb_to_persy_roundtrips_children() {
        let children = make_test_children();
        let original_bytes = postcard::to_allocvec(&children).unwrap();

        let translated = redb_to_persy_payload("test-node", &original_bytes).unwrap();
        let record: NodeRecord = postcard::from_bytes(&translated).unwrap();

        assert_eq!(record.node_id, "test-node");
        assert_eq!(record.children, children);
    }

    #[test]
    fn persy_to_redb_roundtrips_children() {
        let children = make_test_children();
        let record = NodeRecord {
            node_id: "test-node".to_string(),
            children: children.clone(),
        };
        let payload = postcard::to_allocvec(&record).unwrap();

        let (key, value_bytes) = persy_to_redb_record(&payload).unwrap();

        assert_eq!(key, "test-node");
        let recovered: Children = postcard::from_bytes(&value_bytes).unwrap();
        assert_eq!(recovered, children);
    }

    #[test]
    fn translation_is_pure_and_deterministic() {
        let children = make_test_children();
        let bytes = postcard::to_allocvec(&children).unwrap();

        let result1 = redb_to_persy_payload("k", &bytes).unwrap();
        let result2 = redb_to_persy_payload("k", &bytes).unwrap();

        assert_eq!(result1, result2, "same input must produce same output");
    }

    #[test]
    fn empty_children_translates_cleanly() {
        let empty: Children = BTreeMap::default();
        let bytes = postcard::to_allocvec(&empty).unwrap();

        let translated = redb_to_persy_payload("empty-node", &bytes).unwrap();
        let record: NodeRecord = postcard::from_bytes(&translated).unwrap();

        assert_eq!(record.node_id, "empty-node");
        assert!(record.children.is_empty());
    }

    #[test]
    fn all_value_variants_preserved() {
        // Build children using every Value variant to confirm roundtrip fidelity.
        let mut children = BTreeMap::default();
        children.insert(
            "null".to_string(),
            NodeData {
                value: Value::Null,
                updated_at: 1.0,
            },
        );
        children.insert(
            "bit".to_string(),
            NodeData {
                value: Value::Bit(false),
                updated_at: 2.0,
            },
        );
        children.insert(
            "num".to_string(),
            NodeData {
                value: Value::Number(-3.15),
                updated_at: 3.0,
            },
        );
        children.insert(
            "text".to_string(),
            NodeData {
                value: Value::Text("unicode: ☃ snowman".to_string()),
                updated_at: 4.0,
            },
        );
        children.insert(
            "link".to_string(),
            NodeData {
                value: Value::Link("node/abc".to_string()),
                updated_at: 5.0,
            },
        );

        let bytes = postcard::to_allocvec(&children).unwrap();
        let translated = redb_to_persy_payload("root", &bytes).unwrap();
        let record: NodeRecord = postcard::from_bytes(&translated).unwrap();

        assert_eq!(record.children, children);
        // Spot-check the Link variant — it's the one most likely to silently corrupt.
        if let Value::Link(ref s) = record.children.get("link").unwrap().value {
            assert_eq!(s, "node/abc");
        } else {
            panic!("link value not preserved");
        }
    }

    #[test]
    fn backend_parse_accepts_lowercase() {
        assert_eq!(MigrateError::parse_backend("redb").unwrap(), Backend::Redb);
        assert_eq!(
            MigrateError::parse_backend("persy").unwrap(),
            Backend::Persy
        );
    }

    #[test]
    fn backend_parse_accepts_mixed_case() {
        assert_eq!(MigrateError::parse_backend("Redb").unwrap(), Backend::Redb);
        assert_eq!(
            MigrateError::parse_backend("PERSY").unwrap(),
            Backend::Persy
        );
    }

    #[test]
    fn backend_parse_rejects_unknown() {
        assert!(matches!(
            MigrateError::parse_backend("sqlite"),
            Err(MigrateError::InvalidBackend(_))
        ));
    }

    #[test]
    fn backend_as_str_roundtrips() {
        assert_eq!(Backend::Redb.as_str(), "redb");
        assert_eq!(Backend::Persy.as_str(), "persy");
    }

    #[test]
    fn unsorted_keys_preserved_after_roundtrip() {
        // BTreeMap sorts by key, so insertion order doesn't matter —
        // but verify the roundtrip doesn't somehow scramble the key set.
        let mut children: Children = BTreeMap::default();
        children.insert(
            "z".to_string(),
            NodeData {
                value: Value::Text("last".to_string()),
                updated_at: 1.0,
            },
        );
        children.insert(
            "a".to_string(),
            NodeData {
                value: Value::Text("first".to_string()),
                updated_at: 2.0,
            },
        );
        children.insert(
            "m".to_string(),
            NodeData {
                value: Value::Text("middle".to_string()),
                updated_at: 3.0,
            },
        );

        let bytes = postcard::to_allocvec(&children).unwrap();
        let translated = redb_to_persy_payload("k", &bytes).unwrap();
        let record: NodeRecord = postcard::from_bytes(&translated).unwrap();

        let keys: Vec<&String> = record.children.keys().collect();
        assert_eq!(keys, vec!["a", "m", "z"]); // BTreeMap ordering
    }

    // ───────────────────────────────────────────────────────────────────────
    // Fjall key translation tests — compile without any feature gate
    // ───────────────────────────────────────────────────────────────────────

    #[test]
    fn redb_to_fjall_key_adds_prefix() {
        // Empty string (root soul) gets a single prefix byte
        let key = redb_to_fjall_key("");
        assert_eq!(key, vec![0x00]);

        // Non-empty string gets prefix + bytes
        let key = redb_to_fjall_key("abc");
        assert_eq!(key, vec![0x00, b'a', b'b', b'c']);
    }

    #[test]
    fn fjall_key_to_node_id_strips_prefix() {
        assert_eq!(fjall_key_to_node_id(&[0x00]).unwrap(), "");
        assert_eq!(
            fjall_key_to_node_id(&[0x00, b'a', b'b', b'c']).unwrap(),
            "abc"
        );
    }

    #[test]
    fn fjall_key_roundtrip() {
        for node_id in &["", "root", "users/alice", "unicode/☃"] {
            let key = redb_to_fjall_key(node_id);
            let decoded = fjall_key_to_node_id(&key).unwrap();
            assert_eq!(decoded, *node_id);
        }
    }

    #[test]
    fn fjall_key_to_node_id_rejects_empty() {
        assert!(fjall_key_to_node_id(&[]).is_err());
    }

    #[test]
    fn fjall_key_to_node_id_rejects_bad_prefix() {
        assert!(fjall_key_to_node_id(&[0x01, b'a']).is_err());
        assert!(fjall_key_to_node_id(&[0xFF]).is_err());
    }

    #[test]
    fn fjall_key_to_node_id_rejects_invalid_utf8() {
        // 0xFF 0xFE is not valid UTF-8
        assert!(fjall_key_to_node_id(&[0x00, 0xFF, 0xFE]).is_err());
    }

    #[test]
    fn backend_parse_accepts_fjall() {
        assert_eq!(
            MigrateError::parse_backend("fjall").unwrap(),
            Backend::Fjall
        );
        assert_eq!(
            MigrateError::parse_backend("FJALL").unwrap(),
            Backend::Fjall
        );
    }

    #[test]
    fn backend_as_str_fjall() {
        assert_eq!(Backend::Fjall.as_str(), "fjall");
    }
}
