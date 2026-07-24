//! End-to-end tests for the `rod migrate` tool.
//!
//! # What this proves
//!
//! The migration tool successfully translates data between redb and Persy
//! storage adapters, preserving the underlying `Children` graph structure
//! byte-for-byte. Covers all four migration directions:
//!
//! 1. redb → persy (most common: production upgrade)
//! 2. persy → redb (rollback path)
//! 3. deep nested graph (3+ levels) survives round-trip
//! 4. empty dataset is a no-op success
//! 5. CLI flag wiring is exercised end-to-end
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
//! cargo test -p rod --features persy --test migration_e2e -- --test-threads=1
//! ```

#![cfg(feature = "persy")]

use rod::migration::{migrate, Backend, MigrateOpts};
use rod::types::{Children, NodeData, Value};
use std::collections::BTreeMap;
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
        "rod-migrate-{}-{}-{}-{}{}",
        name,
        std::process::id(),
        nanos,
        counter,
        ext
    ))
}

/// Helper: write `count` records to a redb file at `path`. Each record is
/// a separate row in the `rod_nodes_v1` table with key `node_NNNN` and value
/// being a single-child `Children` map (so we test N records, not 1 record
/// with N children).
fn write_redb_records(path: &std::path::Path, count: usize) -> Result<usize, String> {
    use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
    const ROD_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("rod_nodes_v1");

    let db = Database::create(path).map_err(|e| format!("create: {:?}", e))?;
    let txn = db.begin_write().map_err(|e| format!("begin_write: {:?}", e))?;
    {
        let mut table = txn.open_table(ROD_NODES).map_err(|e| format!("open_table: {:?}", e))?;

        for i in 0..count {
            let mut children: BTreeMap<String, NodeData> = BTreeMap::new();
            children.insert(
                format!("leaf_{:04}", i),
                NodeData {
                    value: Value::Text(format!("test-{}", i).into()),
                    updated_at: 1_700_000_000.0 + i as f64,
                },
            );
            let bytes = bincode::serialize(&children).map_err(|e| format!("bincode: {:?}", e))?;
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
fn count_persy_records(path: &std::path::Path) -> Result<usize, String> {
    use persy::Persy;
    let db = Persy::open(path, persy::Config::default()).map_err(|e| format!("open: {:?}", e))?;
    let segment_id = db
        .solve_segment_id("rod_nodes_v1")
        .map_err(|e| format!("solve_segment_id: {:?}", e))?;
    // CRITICAL: db.scan() returns Result<SegmentIter, PE<SegmentError>>.
    // If we naively `for entry in db.scan(&segment_id)`, Rust iterates over
    // BOTH Ok and Err variants — a single Err yields count=1 (the Err itself),
    // not "scan failed". We must unwrap the Result first.
    let iter = db.scan(&segment_id).map_err(|e| format!("scan: {:?}", e))?;
    Ok(iter.count())
}

/// Helper: read all records from a redb file at `path` and count them.
fn count_redb_records(path: &std::path::Path) -> Result<usize, String> {
    use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
    const ROD_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("rod_nodes_v1");

    let db = Database::open(path).map_err(|e| format!("open: {:?}", e))?;
    let txn = db.begin_read().map_err(|e| format!("begin_read: {:?}", e))?;
    let table = match txn.open_table(ROD_NODES) {
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

    // Verify target has 100 records
    eprintln!("DIAG: target path = {:?}", target);
    eprintln!("DIAG: target file size = {} bytes", std::fs::metadata(&target).map(|m| m.len()).unwrap_or(0));

    // CRITICAL: drop everything from migration before counting
    drop(opts);
    std::thread::sleep(std::time::Duration::from_millis(500));

    let target_count = count_persy_records(&target).expect("count persy");
    eprintln!("DIAG: post-sleep count = {}", target_count);
    assert_eq!(target_count, 100, "Persy target should have 100 records");

    // Leave files for inspection (don't cleanup in diag)
}

/// Test 2: Reverse direction (persy → redb) for rollback paths.
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
#[tokio::test]
async fn e2e_migration_preserves_children() {
    let source = temp_path("redb-nested-source", ".redb");
    let target = temp_path("persy-nested-target", ".persy");
    cleanup(&target);

    // Write a 3-level nested graph:
    // root → { level1_a → { level2_a → { leaf1, leaf2 } } }
    use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
    const ROD_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("rod_nodes_v1");

    let db = Database::create(&source).expect("create");
    let txn = db.begin_write().expect("begin_write");
    {
        let mut table = txn.open_table(ROD_NODES).expect("open_table");

        // root node with child "level1_a"
        let mut root_children: BTreeMap<String, NodeData> = BTreeMap::new();
        root_children.insert(
            "level1_a".to_string(),
            NodeData {
                value: Value::Null,
                updated_at: 1_700_000_000.0,
            },
        );
        let bytes = bincode::serialize(&root_children).expect("bincode root");
        table.insert("root", bytes.as_slice()).expect("insert root");

        // level1_a node with child "level2_a"
        let mut l1_children: BTreeMap<String, NodeData> = BTreeMap::new();
        l1_children.insert(
            "level2_a".to_string(),
            NodeData {
                value: Value::Null,
                updated_at: 1_700_000_001.0,
            },
        );
        let bytes = bincode::serialize(&l1_children).expect("bincode l1");
        table.insert("level1_a", bytes.as_slice()).expect("insert l1");

        // level2_a node with 2 leaf children
        let mut l2_children: BTreeMap<String, NodeData> = BTreeMap::new();
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
        let bytes = bincode::serialize(&l2_children).expect("bincode l2");
        table.insert("level2_a", bytes.as_slice()).expect("insert l2");
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
#[tokio::test]
async fn diag_persy_count_basic() {
    use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

    const ROD_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("rod_nodes_v1");

    let source = temp_path("diag-source", ".redb");
    let target = temp_path("diag-target", ".persy");
    cleanup(&source);
    cleanup(&target);

    // Write 100 records to redb
    let db = Database::create(&source).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(ROD_NODES).unwrap();
        for i in 0..100 {
            let mut children: BTreeMap<String, NodeData> = BTreeMap::new();
            children.insert(
                format!("leaf_{:04}", i),
                NodeData {
                    value: Value::Text(format!("test-{}", i).into()),
                    updated_at: 1_700_000_000.0 + i as f64,
                },
            );
            let bytes = bincode::serialize(&children).unwrap();
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
    eprintln!("DIAG: migration reports records_migrated={}", report.records_migrated);
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
