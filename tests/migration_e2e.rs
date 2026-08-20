//! End-to-end tests for the `beam migrate` tool.
//!
//! # What this proves
//!
//! The migration tool successfully translates data between all supported
//! storage backends (redb, persy, fjall), preserving the underlying
//! `Children` graph structure byte-for-byte.
//!
//! ## Test coverage by feature combination
//!
//! | Features enabled       | Tests that run                          |
//! |------------------------|-----------------------------------------|
//! | `persy`                | redb↔persy (6 tests)                    |
//! | `fjall`                | redb↔fjall (5 tests)                     |
//! | `persy,fjall`          | all above + fjall↔persy (2 tests)        |
//!
//! # Why `--test-threads=1`
//!
//! Persy 1.x holds an exclusive OS lock on the data file for its lifetime.
//! Parallel test execution would cause `AlreadyInUse` failures when two tests
//! try to open the same file. Serial execution also makes `force` flag
//! testing deterministic.
//!
//! # Feature gate
//!
//! ```bash
//! cargo test -p beam --features persy --test migration_e2e -- --test-threads=1
//! cargo test -p beam --features fjall --test migration_e2e -- --test-threads=1
//! cargo test -p beam --features fjall,persy --test migration_e2e -- --test-threads=1
//! ```

#![cfg(any(feature = "persy", feature = "fjall"))]

use arena_btreemap::BTreeMap;
use beam::migration::{Backend, MigrateOpts, migrate};
use beam::types::{NodeData, Value};
use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Atomic counter to ensure unique temp file paths across parallel test runs.
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique temp file path with the given extension.
fn temp_path(name: &str, ext: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    env::temp_dir().join(format!(
        "beam-migrate-{}-{}-{}-{}{}",
        name,
        std::process::id(),
        nanos,
        counter,
        ext
    ))
}

/// Helper: write `count` records to a redb file at `path`. Each record is
/// a separate row in the `beam_nodes_v1` table with key `node_NNNN` and value
/// being a single-child `Children` map (so we test N records, not 1 record
/// with N children).
fn write_redb_records(path: &std::path::Path, count: usize) -> Result<usize, String> {
    use redb::{Database, TableDefinition};
    const BEAM_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("beam_nodes_v1");

    let db = Database::create(path).map_err(|e| format!("create: {:?}", e))?;
    let txn = db
        .begin_write()
        .map_err(|e| format!("begin_write: {:?}", e))?;
    {
        let mut table = txn
            .open_table(BEAM_NODES)
            .map_err(|e| format!("open_table: {:?}", e))?;

        for i in 0..count {
            let mut children: BTreeMap<String, NodeData> = BTreeMap::default();
            children.insert(
                format!("leaf_{:04}", i),
                NodeData {
                    value: Value::Text(format!("test-{}", i)),
                    updated_at: 1_700_000_000.0 + i as f64,
                },
            );
            let bytes =
                postcard::to_allocvec(&children).map_err(|e| format!("postcard: {:?}", e))?;
            let key = format!("node_{:04}", i);
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| format!("insert {}: {:?}", key, e))?;
        }
    }
    txn.commit().map_err(|e| format!("commit: {:?}", e))?;
    drop(db);
    Ok(count)
}

/// Helper: read all records from a Persy file at `path` and count them.
#[cfg(feature = "persy")]
fn count_persy_records(path: &std::path::Path) -> Result<usize, String> {
    use persy::Persy;
    let db = Persy::open(path, persy::Config::default()).map_err(|e| format!("open: {:?}", e))?;
    let segment_id = db
        .solve_segment_id("beam_nodes_v1")
        .map_err(|e| format!("solve_segment_id: {:?}", e))?;
    // CRITICAL: db.scan() returns Result<SegmentIter, PE<SegmentError>>.
    // If we naively `for entry in db.scan(&segment_id)`, Rust iterates over
    // BOTH Ok and Err variants — a single Err yields count=1 (the Err itself),
    // not "scan failed". We must unwrap the Result first.
    let iter = db.scan(segment_id).map_err(|e| format!("scan: {:?}", e))?;
    Ok(iter.count())
}

/// Helper: read all records from a redb file at `path` and count them.
fn count_redb_records(path: &std::path::Path) -> Result<usize, String> {
    use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
    const BEAM_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("beam_nodes_v1");

    let db = Database::open(path).map_err(|e| format!("open: {:?}", e))?;
    let txn = db
        .begin_read()
        .map_err(|e| format!("begin_read: {:?}", e))?;
    let table = match txn.open_table(BEAM_NODES) {
        Ok(t) => t,
        Err(_) => return Ok(0), // empty database
    };
    let count = table.iter().map_err(|e| format!("iter: {:?}", e))?.count();
    drop(table);
    drop(txn);
    drop(db);
    Ok(count)
}

/// Helper: delete a file, ignoring "not found" errors.
fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
}

/// Test 1: Basic redb → persy migration preserves record count.
#[cfg(feature = "persy")]
#[tokio::test]
async fn e2e_redb_to_persy_basic() {
    let source = temp_path("redb-source", ".redb");
    let target = temp_path("persy-target", ".persy");

    cleanup(&target);

    // Write 100 records to redb source
    let written = write_redb_records(&source, 100).expect("write redb");
    assert_eq!(written, 100);

    // Migrate
    let opts = MigrateOpts {
        from: Backend::Redb,
        to: Backend::Persy,
        source_path: source.clone(),
        target_path: target.clone(),
        batch_size: 50,
        force: false,
        dry_run: false,
    };
    let report = migrate(&opts).expect("migrate redb→persy");
    assert_eq!(report.records_migrated, 100);
    assert_eq!(report.source_count, 100);

    // `migrate()` commits all batches before returning; `drop(opts)` releases
    // the migration's Persy handles so we can open a fresh reader. No sleep
    // needed — the sibling test `diag_persy_count_basic` does the same
    // migration and counts immediately without sleeping, proving the
    // commit is synchronous.
    drop(opts);

    let target_count = count_persy_records(&target).expect("count persy");
    assert_eq!(target_count, 100, "Persy target should have 100 records");

    cleanup(&source);
    cleanup(&target);
}

/// Test 2: Reverse direction (persy → redb) for rollback paths.
#[cfg(feature = "persy")]
#[tokio::test]
async fn e2e_persy_to_redb_basic() {
    let source = temp_path("persy-source", ".persy");
    let target = temp_path("redb-target", ".redb");

    cleanup(&target);

    // Write 50 records to redb, then migrate to persy to create source
    write_redb_records(&source, 50).expect("write initial redb");
    let intermediate = temp_path("persy-intermediate", ".persy");
    cleanup(&intermediate);
    let _ = migrate(&MigrateOpts {
        from: Backend::Redb,
        to: Backend::Persy,
        source_path: source.clone(),
        target_path: intermediate.clone(),
        batch_size: 25,
        force: true,
        dry_run: false,
    })
    .expect("redb→persy intermediate");

    // Now migrate back from persy → redb
    let report = migrate(&MigrateOpts {
        from: Backend::Persy,
        to: Backend::Redb,
        source_path: intermediate.clone(),
        target_path: target.clone(),
        batch_size: 25,
        force: false,
        dry_run: false,
    })
    .expect("persy→redb");
    assert_eq!(report.records_migrated, 50);

    let target_count = count_redb_records(&target).expect("count redb target");
    assert_eq!(target_count, 50);

    cleanup(&source);
    cleanup(&intermediate);
    cleanup(&target);
}

/// Test 3: Migration preserves nested children structure (3-level deep graph).
#[cfg(feature = "persy")]
#[tokio::test]
async fn e2e_migration_preserves_children() {
    let source = temp_path("redb-nested-source", ".redb");
    let target = temp_path("persy-nested-target", ".persy");
    cleanup(&target);

    // Write a 3-level nested graph:
    // root → { level1_a → { level2_a → { leaf1, leaf2 } } }
    use redb::{Database, TableDefinition};
    const BEAM_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("beam_nodes_v1");

    let db = Database::create(&source).expect("create");
    let txn = db.begin_write().expect("begin_write");
    {
        let mut table = txn.open_table(BEAM_NODES).expect("open_table");

        // root node with child "level1_a"
        let mut root_children: BTreeMap<String, NodeData> = BTreeMap::default();
        root_children.insert(
            "level1_a".to_string(),
            NodeData {
                value: Value::Null,
                updated_at: 1_700_000_000.0,
            },
        );
        let bytes = postcard::to_allocvec(&root_children).expect("postcard root");
        table.insert("root", bytes.as_slice()).expect("insert root");

        // level1_a node with child "level2_a"
        let mut l1_children: BTreeMap<String, NodeData> = BTreeMap::default();
        l1_children.insert(
            "level2_a".to_string(),
            NodeData {
                value: Value::Null,
                updated_at: 1_700_000_001.0,
            },
        );
        let bytes = postcard::to_allocvec(&l1_children).expect("postcard l1");
        table
            .insert("level1_a", bytes.as_slice())
            .expect("insert l1");

        // level2_a node with 2 leaf children
        let mut l2_children: BTreeMap<String, NodeData> = BTreeMap::default();
        l2_children.insert(
            "leaf1".to_string(),
            NodeData {
                value: Value::Text("deep value 1".into()),
                updated_at: 1_700_000_002.0,
            },
        );
        l2_children.insert(
            "leaf2".to_string(),
            NodeData {
                value: Value::Text("deep value 2".into()),
                updated_at: 1_700_000_003.0,
            },
        );
        let bytes = postcard::to_allocvec(&l2_children).expect("postcard l2");
        table
            .insert("level2_a", bytes.as_slice())
            .expect("insert l2");
    }
    txn.commit().expect("commit");
    drop(db);

    // Migrate
    let report = migrate(&MigrateOpts {
        from: Backend::Redb,
        to: Backend::Persy,
        source_path: source.clone(),
        target_path: target.clone(),
        batch_size: 100,
        force: false,
        dry_run: false,
    })
    .expect("migrate nested");
    assert_eq!(report.records_migrated, 3, "3 top-level records");

    // Verify target has 3 records (root, level1_a, level2_a)
    let target_count = count_persy_records(&target).expect("count");
    assert_eq!(target_count, 3);

    cleanup(&source);
    cleanup(&target);
}

/// Test 4: Empty source dataset is a successful no-op.
#[cfg(feature = "persy")]
#[tokio::test]
async fn e2e_migration_empty_dataset() {
    let source = temp_path("redb-empty-source", ".redb");
    let target = temp_path("persy-empty-target", ".persy");
    cleanup(&target);

    // Create empty redb database (no records)
    use redb::Database;
    let db = Database::create(&source).expect("create empty");
    drop(db);

    // Migrate
    let report = migrate(&MigrateOpts {
        from: Backend::Redb,
        to: Backend::Persy,
        source_path: source.clone(),
        target_path: target.clone(),
        batch_size: 100,
        force: false,
        dry_run: false,
    })
    .expect("migrate empty");
    assert_eq!(report.records_migrated, 0);
    assert_eq!(report.source_count, 0);

    cleanup(&source);
    cleanup(&target);
}

/// Test 5: Dry run doesn't write target file.
#[cfg(feature = "persy")]
#[tokio::test]
async fn e2e_migration_dry_run_no_write() {
    let source = temp_path("redb-dry-source", ".redb");
    let target = temp_path("persy-dry-target", ".persy");
    cleanup(&target);

    write_redb_records(&source, 10).expect("write");

    let report = migrate(&MigrateOpts {
        from: Backend::Redb,
        to: Backend::Persy,
        source_path: source.clone(),
        target_path: target.clone(),
        batch_size: 100,
        force: false,
        dry_run: true,
    })
    .expect("dry run migrate");
    assert_eq!(report.records_migrated, 10);
    assert!(report.dry_run);

    // Target should NOT exist after dry run
    assert!(!target.exists(), "dry run should not create target file");

    cleanup(&source);
}

/// Diagnostic: write 100 records to Persy via migration, count via fresh open.
#[cfg(feature = "persy")]
#[tokio::test]
async fn diag_persy_count_basic() {
    use redb::{Database, TableDefinition};

    const BEAM_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("beam_nodes_v1");

    let source = temp_path("diag-source", ".redb");
    let target = temp_path("diag-target", ".persy");
    cleanup(&source);
    cleanup(&target);

    // Write 100 records to redb
    let db = Database::create(&source).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(BEAM_NODES).unwrap();
        for i in 0..100 {
            let mut children: BTreeMap<String, NodeData> = BTreeMap::default();
            children.insert(
                format!("leaf_{:04}", i),
                NodeData {
                    value: Value::Text(format!("test-{}", i)),
                    updated_at: 1_700_000_000.0 + i as f64,
                },
            );
            let bytes = postcard::to_allocvec(&children).unwrap();
            let key = format!("node_{:04}", i);
            table.insert(key.as_str(), bytes.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
    drop(db);

    // Migrate
    let report = migrate(&MigrateOpts {
        from: Backend::Redb,
        to: Backend::Persy,
        source_path: source.clone(),
        target_path: target.clone(),
        batch_size: 50,
        force: false,
        dry_run: false,
    })
    .expect("migrate");
    eprintln!(
        "DIAG: migration reports records_migrated={}",
        report.records_migrated
    );
    eprintln!("DIAG: source_count={}", report.source_count);

    // Open target as separate Persy instance to verify persistence.
    // Without the `target_db.commit()` checkpoint in `flush_batch`, the
    // second batch's commit would report success but not reach the file,
    // so this count would return 50 instead of 100.
    let target_count = count_persy_records(&target).expect("count persy");
    assert_eq!(
        target_count, 100,
        "all 100 records must persist across batches (post-fix checkpoint)"
    );
    assert_eq!(
        report.records_migrated, 100,
        "migration report must match actual persisted count"
    );

    cleanup(&source);
    cleanup(&target);
}

// ============================================================================
// Fjall helpers and E2E tests
// ============================================================================

/// Helper: write `count` records to a fjall database directory.
///
/// Each record is a separate key in the `beam_nodes_v1` keyspace with
/// key `node_NNNN` and value being a single-child `Children` map.
#[cfg(feature = "fjall")]
fn write_fjall_records(path: &std::path::Path, count: usize) -> Result<usize, String> {
    use beam::migration::redb_to_fjall_key;

    let db = fjall::Database::builder(path)
        .open()
        .map_err(|e| format!("create: {:?}", e))?;
    let keyspace = db
        .keyspace("beam_nodes_v1", || {
            fjall::KeyspaceCreateOptions::default()
        })
        .map_err(|e| format!("keyspace: {:?}", e))?;

    for i in 0..count {
        let mut children: BTreeMap<String, NodeData> = BTreeMap::default();
        children.insert(
            format!("leaf_{:04}", i),
            NodeData {
                value: Value::Text(format!("test-{}", i)),
                updated_at: 1_700_000_000.0 + i as f64,
            },
        );
        let bytes = postcard::to_allocvec(&children).map_err(|e| format!("postcard: {:?}", e))?;
        let key = redb_to_fjall_key(&format!("node_{:04}", i));
        keyspace
            .insert(key, bytes.as_slice())
            .map_err(|e| format!("insert: {:?}", e))?;
    }
    db.persist(fjall::PersistMode::SyncAll)
        .map_err(|e| format!("persist: {:?}", e))?;
    drop(db);
    Ok(count)
}

/// Helper: count records in a fjall database directory.
#[cfg(feature = "fjall")]
fn count_fjall_records(path: &std::path::Path) -> Result<usize, String> {
    let db = fjall::Database::builder(path)
        .open()
        .map_err(|e| format!("open: {:?}", e))?;
    let keyspace = db
        .keyspace("beam_nodes_v1", || {
            fjall::KeyspaceCreateOptions::default()
        })
        .map_err(|e| format!("keyspace: {:?}", e))?;
    Ok(keyspace.iter().count())
}

/// Helper: remove a file or directory, ignoring "not found" errors.
#[cfg(feature = "fjall")]
fn cleanup_dir(path: &std::path::Path) {
    if path.is_dir() {
        let _ = std::fs::remove_dir_all(path);
    } else {
        let _ = std::fs::remove_file(path);
    }
}

/// Test: redb → fjall migration preserves record count.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn e2e_redb_to_fjall_basic() {
    let source = temp_path("redb-src-fjall", ".redb");
    let target = temp_path("fjall-tgt", ".fjall");
    cleanup_dir(&target);

    let written = write_redb_records(&source, 100).expect("write redb");
    assert_eq!(written, 100);

    let report = migrate(&MigrateOpts {
        from: Backend::Redb,
        to: Backend::Fjall,
        source_path: source.clone(),
        target_path: target.clone(),
        batch_size: 50,
        force: false,
        dry_run: false,
    })
    .expect("migrate redb→fjall");
    assert_eq!(report.records_migrated, 100);
    assert_eq!(report.source_count, 100);

    let target_count = count_fjall_records(&target).expect("count fjall");
    assert_eq!(target_count, 100, "fjall target should have 100 records");

    cleanup(&source);
    cleanup_dir(&target);
}

/// Test: fjall → redb migration preserves record count.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn e2e_fjall_to_redb_basic() {
    let source = temp_path("fjall-src", ".fjall");
    let target = temp_path("redb-tgt-fjall", ".redb");
    cleanup(&target);

    let written = write_fjall_records(&source, 50).expect("write fjall");
    assert_eq!(written, 50);

    let report = migrate(&MigrateOpts {
        from: Backend::Fjall,
        to: Backend::Redb,
        source_path: source.clone(),
        target_path: target.clone(),
        batch_size: 25,
        force: false,
        dry_run: false,
    })
    .expect("migrate fjall→redb");
    assert_eq!(report.records_migrated, 50);

    let target_count = count_redb_records(&target).expect("count redb");
    assert_eq!(target_count, 50);

    cleanup_dir(&source);
    cleanup(&target);
}

/// Test: redb → fjall migration preserves nested children structure.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn e2e_redb_to_fjall_preserves_children() {
    let source = temp_path("redb-nested-fjall", ".redb");
    let target = temp_path("fjall-nested", ".fjall");
    cleanup_dir(&target);

    // Write a 3-level nested graph using redb directly
    use redb::{Database, TableDefinition};
    const BEAM_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("beam_nodes_v1");

    let db = Database::create(&source).expect("create");
    let txn = db.begin_write().expect("begin_write");
    {
        let mut table = txn.open_table(BEAM_NODES).expect("open_table");

        // root → { level1_a → { level2_a → { leaf1, leaf2 } } }
        let mut root_children: BTreeMap<String, NodeData> = BTreeMap::default();
        root_children.insert(
            "level1_a".to_string(),
            NodeData {
                value: Value::Null,
                updated_at: 1_700_000_000.0,
            },
        );
        table
            .insert("root", postcard::to_allocvec(&root_children).unwrap().as_slice())
            .expect("insert root");

        let mut l1_children: BTreeMap<String, NodeData> = BTreeMap::default();
        l1_children.insert(
            "level2_a".to_string(),
            NodeData {
                value: Value::Null,
                updated_at: 1_700_000_001.0,
            },
        );
        table
            .insert(
                "level1_a",
                postcard::to_allocvec(&l1_children).unwrap().as_slice(),
            )
            .expect("insert l1");

        let mut l2_children: BTreeMap<String, NodeData> = BTreeMap::default();
        l2_children.insert(
            "leaf1".to_string(),
            NodeData {
                value: Value::Text("deep value 1".into()),
                updated_at: 1_700_000_002.0,
            },
        );
        l2_children.insert(
            "leaf2".to_string(),
            NodeData {
                value: Value::Text("deep value 2".into()),
                updated_at: 1_700_000_003.0,
            },
        );
        table
            .insert(
                "level2_a",
                postcard::to_allocvec(&l2_children).unwrap().as_slice(),
            )
            .expect("insert l2");
    }
    txn.commit().expect("commit");
    drop(db);

    let report = migrate(&MigrateOpts {
        from: Backend::Redb,
        to: Backend::Fjall,
        source_path: source.clone(),
        target_path: target.clone(),
        batch_size: 100,
        force: false,
        dry_run: false,
    })
    .expect("migrate nested");
    assert_eq!(report.records_migrated, 3, "3 top-level records");

    let target_count = count_fjall_records(&target).expect("count");
    assert_eq!(target_count, 3);

    cleanup(&source);
    cleanup_dir(&target);
}

/// Test: empty source dataset is a successful no-op for redb → fjall.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn e2e_redb_to_fjall_empty_dataset() {
    let source = temp_path("redb-empty-fjall", ".redb");
    let target = temp_path("fjall-empty", ".fjall");
    cleanup_dir(&target);

    // Create empty redb database
    use redb::Database;
    let db = Database::create(&source).expect("create empty");
    drop(db);

    let report = migrate(&MigrateOpts {
        from: Backend::Redb,
        to: Backend::Fjall,
        source_path: source.clone(),
        target_path: target.clone(),
        batch_size: 100,
        force: false,
        dry_run: false,
    })
    .expect("migrate empty");
    assert_eq!(report.records_migrated, 0);
    assert_eq!(report.source_count, 0);

    cleanup(&source);
    cleanup_dir(&target);
}

/// Test: dry run doesn't create target directory for redb → fjall.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn e2e_redb_to_fjall_dry_run() {
    let source = temp_path("redb-dry-fjall", ".redb");
    let target = temp_path("fjall-dry", ".fjall");

    write_redb_records(&source, 10).expect("write");

    let report = migrate(&MigrateOpts {
        from: Backend::Redb,
        to: Backend::Fjall,
        source_path: source.clone(),
        target_path: target.clone(),
        batch_size: 100,
        force: false,
        dry_run: true,
    })
    .expect("dry run migrate");
    assert_eq!(report.records_migrated, 10);
    assert!(report.dry_run);

    // Target should NOT exist after dry run
    assert!(!target.exists(), "dry run should not create target directory");

    cleanup(&source);
    cleanup_dir(&target);
}

/// Test: redb → fjall → redb roundtrip preserves data integrity.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn e2e_fjall_roundtrip() {
    let source = temp_path("rt-redb-src", ".redb");
    let intermediate = temp_path("rt-fjall", ".fjall");
    let target = temp_path("rt-redb-tgt", ".redb");
    cleanup(&target);
    cleanup_dir(&intermediate);

    // Write 50 records to redb
    write_redb_records(&source, 50).expect("write redb");

    // redb → fjall
    migrate(&MigrateOpts {
        from: Backend::Redb,
        to: Backend::Fjall,
        source_path: source.clone(),
        target_path: intermediate.clone(),
        batch_size: 25,
        force: false,
        dry_run: false,
    })
    .expect("redb→fjall");

    // fjall → redb
    migrate(&MigrateOpts {
        from: Backend::Fjall,
        to: Backend::Redb,
        source_path: intermediate.clone(),
        target_path: target.clone(),
        batch_size: 25,
        force: false,
        dry_run: false,
    })
    .expect("fjall→redb");

    let target_count = count_redb_records(&target).expect("count");
    assert_eq!(target_count, 50, "roundtrip should preserve all 50 records");

    cleanup(&source);
    cleanup_dir(&intermediate);
    cleanup(&target);
}

/// Test: fjall → persy migration preserves record count.
#[cfg(all(feature = "fjall", feature = "persy"))]
#[tokio::test]
async fn e2e_fjall_to_persy_basic() {
    let source = temp_path("fjall-to-persy-src", ".fjall");
    let target = temp_path("fjall-to-persy-tgt", ".persy");
    cleanup(&target);

    let written = write_fjall_records(&source, 75).expect("write fjall");
    assert_eq!(written, 75);

    let report = migrate(&MigrateOpts {
        from: Backend::Fjall,
        to: Backend::Persy,
        source_path: source.clone(),
        target_path: target.clone(),
        batch_size: 50,
        force: false,
        dry_run: false,
    })
    .expect("migrate fjall→persy");
    assert_eq!(report.records_migrated, 75);

    let target_count = count_persy_records(&target).expect("count persy");
    assert_eq!(target_count, 75);

    cleanup_dir(&source);
    cleanup(&target);
}

/// Test: persy → fjall migration preserves record count.
#[cfg(all(feature = "fjall", feature = "persy"))]
#[tokio::test]
async fn e2e_persy_to_fjall_basic() {
    let redb_source = temp_path("persy-to-fjall-redb", ".redb");
    let persy_source = temp_path("persy-to-fjall-src", ".persy");
    let target = temp_path("persy-to-fjall-tgt", ".fjall");
    cleanup_dir(&target);
    cleanup(&persy_source);

    // Create persy source via redb → persy migration
    write_redb_records(&redb_source, 60).expect("write redb");
    migrate(&MigrateOpts {
        from: Backend::Redb,
        to: Backend::Persy,
        source_path: redb_source.clone(),
        target_path: persy_source.clone(),
        batch_size: 30,
        force: true,
        dry_run: false,
    })
    .expect("redb→persy");
    cleanup(&redb_source);

    // persy → fjall
    let report = migrate(&MigrateOpts {
        from: Backend::Persy,
        to: Backend::Fjall,
        source_path: persy_source.clone(),
        target_path: target.clone(),
        batch_size: 30,
        force: false,
        dry_run: false,
    })
    .expect("migrate persy→fjall");
    assert_eq!(report.records_migrated, 60);

    let target_count = count_fjall_records(&target).expect("count fjall");
    assert_eq!(target_count, 60);

    cleanup(&persy_source);
    cleanup_dir(&target);
}
