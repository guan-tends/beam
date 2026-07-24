//! Storage backend migration: redb ↔ Persy.
//!
//! Provides one-shot batch migration between Rod's two storage backends.
//! The translation is format-only: the inner `Children` data is byte-identical
//! between redb and Persy; only the wrapper struct ([`NodeRecord`]) differs.
//!
//! # On-disk formats
//!
//! **redb**: `TableDefinition<&str, &[u8]>` in the `rod_nodes_v1` table.
//! Key is the node_id directly; value is `bincode(Children)` (no wrapper).
//!
//! **Persy**: A segment named `rod_nodes_v1` containing opaque records.
//! Each record is `bincode(NodeRecord { node_id, children })`.
//!
//! # CLI
//!
//! ```text
//! rod migrate --from <redb|persy> --to <redb|persy> \
//!     --source <path> --target <path> \
//!     [--batch-size 1000] [--force] [--dry-run]
//! ```
//!
//! See [the migration plan](../docs/plans/PERSY-STORAGE-ADAPTER.md) for the
//! full design rationale.

use std::path::PathBuf;
use std::time::Duration;

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
/// the same bincode bytes, but live in separate compilation units so the
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
    /// redb backend (single-file embedded database)
    Redb,
    /// Persy backend (single-file embedded database with MVCC)
    Persy,
}

impl Backend {
    /// Returns the canonical lowercase string used in CLI args and logs.
    pub fn as_str(&self) -> &'static str {
        match self {
            Backend::Redb => "redb",
            Backend::Persy => "persy",
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

    #[error("bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("target already exists at {0} (use --force to overwrite)")]
    TargetExists(PathBuf),

    #[error("unsupported migration: {from} -> {to} (use --from and --to with different backends)")]
    Unsupported {
        from: Backend,
        to: Backend,
    },

    #[error("invalid backend string: {0} (expected 'redb' or 'persy')")]
    InvalidBackend(String),
}

impl MigrateError {
    /// Parse a backend name from a CLI string. Returns [`MigrateError::InvalidBackend`]
    /// if the string is not recognized.
    pub fn parse_backend(s: &str) -> Result<Backend, MigrateError> {
        match s.to_lowercase().as_str() {
            "redb" => Ok(Backend::Redb),
            "persy" => Ok(Backend::Persy),
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
/// inside a [`NodeRecord`] wrapper for Persy. Both formats use bincode on `Children`,
/// so the inner bytes are preserved exactly.
///
/// # Errors
///
/// Returns [`MigrateError::Bincode`] if the input bytes are not a valid bincode-encoded
/// `Children` map.
pub fn redb_to_persy_payload(key: &str, value: &[u8]) -> Result<Vec<u8>, MigrateError> {
    let children: Children = bincode::deserialize(value)?;
    let record = NodeRecord {
        node_id: key.to_string(),
        children,
    };
    Ok(bincode::serialize(&record)?)
}

/// Translates a Persy record payload into a redb (node_id, value_bytes) pair.
///
/// Counterpart to [`redb_to_persy_payload`]: unwraps the [`NodeRecord`], re-serializes
/// the `Children` map in redb's bare-bytes format.
///
/// # Errors
///
/// Returns [`MigrateError::Bincode`] if the input bytes are not a valid bincode-encoded
/// `NodeRecord`.
pub fn persy_to_redb_record(payload: &[u8]) -> Result<(String, Vec<u8>), MigrateError> {
    let record: NodeRecord = bincode::deserialize(payload)?;
    let children_bytes = bincode::serialize(&record.children)?;
    Ok((record.node_id, children_bytes))
}

// ─────────────────────────────────────────────────────────────────────────────
// I/O orchestration — requires the `persy` feature
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "persy")]
pub(crate) mod io {
    use super::*;
    use std::time::Instant;

    use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

    use crate::adapters::persy_storage::ROD_NODES as PERSY_ROD_NODES;

    const REDB_ROD_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("rod_nodes_v1");

    /// Run a storage migration. Dispatches on the (from, to) backend pair.
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

        if !opts.dry_run
            && opts.target_path.exists()
            && !opts.force
        {
            return Err(MigrateError::TargetExists(opts.target_path.clone()));
        }

        let start = Instant::now();

        let (source_count, migrated) = match (opts.from, opts.to) {
            (Backend::Redb, Backend::Persy) => migrate_redb_to_persy(opts)?,
            (Backend::Persy, Backend::Redb) => migrate_persy_to_redb(opts)?,
            (Backend::Redb, Backend::Redb) | (Backend::Persy, Backend::Persy) => {
                unreachable!("from == to caught above")
            }
        };

        Ok(MigrationReport {
            // In dry-run, "records_migrated" reports what WOULD have been
            // written (== source_count). This matches the test contract:
            // a dry-run preview should show the actual record count.
            records_migrated: if opts.dry_run { source_count } else { migrated },
            source_count,
            target_count_after: if opts.dry_run { 0 } else { migrated },
            elapsed: start.elapsed(),
            dry_run: opts.dry_run,
        })
    }

    fn migrate_redb_to_persy(
        opts: &MigrateOpts,
    ) -> Result<(usize, usize), MigrateError> {
        // ─── Read source records into memory ──────────────────────────────
        //
        // Source is redb, opened read-only. We materialize all records as
        // bincode-serialized NodeRecord payloads (the Persy on-disk format)
        // before opening the target. This keeps the migration flow linear
        // and avoids holding a redb read transaction open across a long
        // Persy write transaction.
        let src_db = Database::open(&opts.source_path).map_err(|source| {
            MigrateError::Redb {
                path: opts.source_path.clone(),
                source: source.into(),
            }
        })?;
        let src_tx = src_db.begin_read().map_err(|source| {
            MigrateError::RedbTx {
                path: opts.source_path.clone(),
                source,
            }
        })?;
        // An empty source DB (or one without the rod_nodes_v1 table) is a
        // valid input — treat it as zero records rather than an error.
        // This matches the e2e_migration_empty_dataset contract.
        let src_table = match src_tx.open_table(REDB_ROD_NODES) {
            Ok(t) => t,
            Err(e) => {
                use redb::TableError;
                if matches!(e, TableError::TableDoesNotExist { .. }) {
                    // Empty source — return success with zero records
                    return Ok((0, 0));
                }
                return Err(MigrateError::RedbTable {
                    path: opts.source_path.clone(),
                    source: e,
                });
            }
        };

        let mut payloads: Vec<Vec<u8>> = Vec::new();
        {
            let iter = src_table.iter().map_err(|source| MigrateError::RedbTable {
                path: opts.source_path.clone(),
                source: redb::TableError::Storage(source),
            })?;
            for entry in iter {
                let (key_guard, value_guard) = entry.map_err(|source| MigrateError::RedbTable {
                    path: opts.source_path.clone(),
                    source: redb::TableError::Storage(source),
                })?;
                let key = key_guard.value();
                let value = value_guard.value();
                payloads.push(redb_to_persy_payload(key, value)?);
            }
        }
        let source_count = payloads.len();
        drop(src_table);
        drop(src_tx);
        drop(src_db);

        // Dry-run: count only, no writes.
        if opts.dry_run {
            return Ok((source_count, 0));
        }

        // ─── Write all records in a single Persy transaction ────────────
        //
        // Mirrors the canonical Persy example pattern and our own
        // v0.5.0 PersyStorage write path (see `src/adapters/persy_storage.rs`):
        //
        //   1. Open ONE Persy handle for the entire migration
        //   2. Create the segment on first run via the open_or_create_with closure
        //   3. Resolve the segment id ONCE from that handle
        //   4. Begin ONE transaction, insert ALL payloads, commit ONCE
        //   5. Drop the handle (releases flock, flushes final state)
        //
        // Why a single transaction:
        //   * Persy's `solve_segment_id` returns the correct ID for the
        //     same in-memory address state within a single handle. Holding
        //     one handle means the segment id is stable for the whole run.
        //   * The "fresh handle per batch" pattern (prior implementation)
        //     relied on `solve_segment_id` resolving the same id across
        //     handles, which can race with the address map during recovery.
        //     Single-handle is simpler and substrate-aligned.
        //   * For our 100-record test dataset, all payloads fit in memory.
        //     For production migrations of millions of records, we can
        //     chunk within the single transaction (see TODO below).

        let target_db = persy::Persy::open_or_create_with(
            opts.target_path.to_string_lossy().as_ref(),
            persy::Config::new(),
            |persy_db| -> Result<(), Box<dyn std::error::Error>> {
                let mut create_tx = persy_db
                    .begin()
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                create_tx
                    .create_segment(PERSY_ROD_NODES)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                create_tx
                    .prepare()
                    .map_err(|e| Box::<dyn std::error::Error>::from(e))?
                    .commit()
                    .map_err(|e| Box::<dyn std::error::Error>::from(e))?;
                Ok(())
            },
        )
        .map_err(|source| MigrateError::Persy(format!("{}: {}\nhelp: ensure the target directory is writable", opts.target_path.display(), source)))?;

        let target_seg = target_db
            .solve_segment_id(PERSY_ROD_NODES)
            .map_err(|e| MigrateError::Persy(format!("{}: solve_segment_id: {}", opts.target_path.display(), e)))?;

        let mut tx = target_db.begin().map_err(|source| {
            MigrateError::Persy(format!("{}: begin: {}", opts.target_path.display(), source))
        })?;

        for payload in &payloads {
            tx.insert(target_seg, payload.as_slice()).map_err(|source| {
                MigrateError::Persy(format!(
                    "{}: insert: {}",
                    opts.target_path.display(),
                    source
                ))
            })?;
        }
        

        tx.prepare()
            .map_err(|source| MigrateError::Persy(format!("{}: prepare: {}", opts.target_path.display(), source)))?
            .commit()
            .map_err(|source| MigrateError::Persy(format!("{}: commit: {}", opts.target_path.display(), source)))?;

        // Explicit drop ensures all work completes before the caller
        // (e.g., an e2e test) opens the file for verification.
        drop(target_db);

        Ok((source_count, payloads.len()))
    }

    fn migrate_persy_to_redb(
        opts: &MigrateOpts,
    ) -> Result<(usize, usize), MigrateError> {
        let src_db = persy::Persy::open(opts.source_path.to_string_lossy().as_ref(), persy::Config::new()).map_err(
            |source| MigrateError::Persy(format!("{}: {}", opts.source_path.clone().display(), source)),
        )?;
        let src_seg = src_db.solve_segment_id(PERSY_ROD_NODES).map_err(|e| {
            MigrateError::Persy(format!("{}: solve_segment_id failed: {}", opts.source_path.display(), e))
        })?;

        let target_db = Database::create(&opts.target_path).map_err(|source| {
            MigrateError::Redb {
                path: opts.target_path.clone(),
                source: source.into(),
            }
        })?;
        let mut target_tx = target_db.begin_write().map_err(|source| {
            MigrateError::RedbTx {
                path: opts.target_path.clone(),
                source,
            }
        })?;
        let mut target_table = target_tx.open_table(REDB_ROD_NODES).map_err(|source| {
            MigrateError::RedbTable {
                path: opts.target_path.clone(),
                source,
            }
        })?;

        let scan = src_db.scan(&src_seg).map_err(|source| MigrateError::Persy(format!("{}: {}", opts.source_path.clone().display(), source)))?;

        let mut source_count = 0;
        let mut migrated = 0;
        let mut batch: Vec<(String, Vec<u8>)> = Vec::with_capacity(opts.batch_size);

        for entry in scan {
            let (_id, bytes) = entry;
            let (key, value_bytes) = persy_to_redb_record(&bytes)?;
            source_count += 1;

            if !opts.dry_run {
                batch.push((key, value_bytes));
                if batch.len() >= opts.batch_size {
                    for (k, v) in batch.drain(..) {
                        target_table
                            .insert(k.as_str(), v.as_slice())
                            .map_err(|source| MigrateError::RedbTable {
                                path: opts.target_path.clone(),
                                source: redb::TableError::Storage(source),
                            })?;
                        migrated += 1;
                    }
                }
            }
        }

        if !batch.is_empty() && !opts.dry_run {
            for (k, v) in batch.drain(..) {
                target_table
                    .insert(k.as_str(), v.as_slice())
                    .map_err(|source| MigrateError::RedbTable {
                        path: opts.target_path.clone(),
                        source: redb::TableError::Storage(source),
                    })?;
                migrated += 1;
            }
        }

        drop(target_table);
        target_tx.commit().map_err(|source| MigrateError::RedbCommit {
            path: opts.target_path.clone(),
            source,
        })?;

        // When dry_run, migrated stays 0; source_count still reflects records read.
        Ok((source_count, migrated))
    }
}
#[cfg(feature = "persy")]
pub use io::migrate;

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use crate::types::{NodeData, Value};

    /// Helper: build a representative `Children` map using the real Rod value types.
    fn make_test_children() -> Children {
        let mut children = BTreeMap::new();
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
        let original_bytes = bincode::serialize(&children).unwrap();

        let translated = redb_to_persy_payload("test-node", &original_bytes).unwrap();
        let record: NodeRecord = bincode::deserialize(&translated).unwrap();

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
        let payload = bincode::serialize(&record).unwrap();

        let (key, value_bytes) = persy_to_redb_record(&payload).unwrap();

        assert_eq!(key, "test-node");
        let recovered: Children = bincode::deserialize(&value_bytes).unwrap();
        assert_eq!(recovered, children);
    }

    #[test]
    fn translation_is_pure_and_deterministic() {
        let children = make_test_children();
        let bytes = bincode::serialize(&children).unwrap();

        let result1 = redb_to_persy_payload("k", &bytes).unwrap();
        let result2 = redb_to_persy_payload("k", &bytes).unwrap();

        assert_eq!(result1, result2, "same input must produce same output");
    }

    #[test]
    fn empty_children_translates_cleanly() {
        let empty: Children = BTreeMap::new();
        let bytes = bincode::serialize(&empty).unwrap();

        let translated = redb_to_persy_payload("empty-node", &bytes).unwrap();
        let record: NodeRecord = bincode::deserialize(&translated).unwrap();

        assert_eq!(record.node_id, "empty-node");
        assert!(record.children.is_empty());
    }

    #[test]
    fn all_value_variants_preserved() {
        // Build children using every Value variant to confirm roundtrip fidelity.
        let mut children = BTreeMap::new();
        children.insert("null".to_string(), NodeData { value: Value::Null, updated_at: 1.0 });
        children.insert("bit".to_string(), NodeData { value: Value::Bit(false), updated_at: 2.0 });
        children.insert("num".to_string(), NodeData { value: Value::Number(-3.14), updated_at: 3.0 });
        children.insert("text".to_string(), NodeData { value: Value::Text("unicode: 🪷".to_string()), updated_at: 4.0 });
        children.insert("link".to_string(), NodeData { value: Value::Link("node/abc".to_string()), updated_at: 5.0 });

        let bytes = bincode::serialize(&children).unwrap();
        let translated = redb_to_persy_payload("root", &bytes).unwrap();
        let record: NodeRecord = bincode::deserialize(&translated).unwrap();

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
        assert_eq!(MigrateError::parse_backend("persy").unwrap(), Backend::Persy);
    }

    #[test]
    fn backend_parse_accepts_mixed_case() {
        assert_eq!(MigrateError::parse_backend("Redb").unwrap(), Backend::Redb);
        assert_eq!(MigrateError::parse_backend("PERSY").unwrap(), Backend::Persy);
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
        let mut children = BTreeMap::new();
        children.insert("z".to_string(), NodeData { value: Value::Text("last".to_string()), updated_at: 1.0 });
        children.insert("a".to_string(), NodeData { value: Value::Text("first".to_string()), updated_at: 2.0 });
        children.insert("m".to_string(), NodeData { value: Value::Text("middle".to_string()), updated_at: 3.0 });

        let bytes = bincode::serialize(&children).unwrap();
        let translated = redb_to_persy_payload("k", &bytes).unwrap();
        let record: NodeRecord = bincode::deserialize(&translated).unwrap();

        let keys: Vec<&String> = record.children.keys().collect();
        assert_eq!(keys, vec!["a", "m", "z"]); // BTreeMap ordering
    }

}
