# Storage Migration Guide

How to move data between BEAM's storage backends (`redb` ↔ `Persy`) using the `beam migrate` CLI subcommand.

## Overview

The migration tool reads records from a source database (redb or Persy) and writes them to a target database (redb or Persy), preserving the graph structure exactly. It uses single-transaction-per-batch for safety and includes checksum validation.

**When to migrate:**
- You're switching from redb to Persy to test concurrent write performance
- You're switching from Persy back to redb after a workload didn't benefit
- You're upgrading hardware and want to refresh the on-disk format

**When NOT to migrate:**
- You can just point a new node at the same network — mesh replication will sync it. Migration is for offline, explicit data movement.

## Quick Start

```bash
# 1. Preview (always do this first)
beam migrate --from redb --to persy --source ./data.redb --target ./data.persy --dry-run

# 2. Execute (if preview looks right)
beam migrate --from redb --to persy --source ./data.redb --target ./data.persy

# 3. Verify (start a new node against the target)
cargo run --release --features persy -- --port 4944 --persy-storage true --persy-path ./data.persy
```

## CLI Reference

```bash
beam migrate \
  --from <redb|persy> \
  --to <redb|persy> \
  --source <PATH> \
  --target <PATH> \
  [--batch-size 1000] \
  [--force] \
  [--dry-run]
```

| Flag | Required | Description |
|------|----------|-------------|
| `--from` | Yes | Source backend: `redb` or `persy` |
| `--to` | Yes | Target backend: `redb` or `persy` |
| `--source` | Yes | Path to source database file |
| `--target` | Yes | Path to target database file (will be created) |
| `--batch-size` | No | Records per batch (default: 1000) |
| `--force` | No | Overwrite target if it already exists |
| `--dry-run` | No | Preview the migration without writing |

**Must specify** `--features persy` in the build if either side is Persy.

## Step-by-Step Procedure

### 1. Stop the source node

Migration operates on offline files. If the source node is running, it has the file locked (or in the case of Persy with `background_ops`, it may have pending writes in a background thread).

```bash
# Graceful shutdown (SIGTERM)
kill -TERM <pid>

# Wait for clean exit (check logs or process list)
```

### 2. Dry-run preview

```bash
beam migrate --from redb --to persy --source ./data.redb --target ./data.persy --dry-run
```

The dry-run mode:
- Opens the source database read-only
- Counts records that would be migrated
- Reports the estimated target size
- Does NOT write to the target path

**Always run this first.** It catches:
- Wrong file paths
- Incompatible formats (e.g., trying to migrate a non-BEAM database)
- Insufficient disk space

### 3. Execute the migration

```bash
beam migrate --from redb --to persy --source ./data.redb --target ./data.persy
```

The tool will:
1. Open source database read-only
2. Create target database (fail if exists unless `--force`)
3. Iterate source records in `--batch-size` chunks
4. For each batch, open ONE target transaction, insert all records, commit
5. After all batches: print summary (`records_migrated`, `elapsed`, `errors`)

**Typical throughput**: ~1000 records/second on SSD. A 100k-record dataset completes in ~2 minutes.

### 4. Verify the target

```bash
# Start a node against the new database
cargo run --release --features persy -- --port 4944 --persy-storage true --persy-path ./data.persy

# In another terminal, verify a known record
curl http://localhost:4944/<known-soul>
```

### 5. Cut over or roll back

**Cut over**: Stop the old node, start the new one, update any peer connection lists.

**Roll back**: The source database is untouched. If verification fails:
```bash
# Reverse migration
beam migrate --from persy --to redb --source ./data.persy --target ./data.redb.recovered

# Or just keep using the original source database
```

## Safety Properties

### Atomicity per batch

Each batch is ONE transaction. If the migration fails mid-way:
- Batches committed before failure are preserved in the target
- The current batch is rolled back
- The target database is left in a consistent state

### Checksum validation

After all batches complete, the tool validates byte-for-byte equivalence:
- Record count match
- Per-record checksum match
- Final summary line: `Migration complete: N records migrated`

If validation fails, the tool exits with non-zero status. Inspect the source and target manually before deciding to use either.

### Source never modified

The source database is opened read-only. The migration tool CANNOT corrupt or modify the source, even on partial failure.

### Target idempotency

Re-running the same migration (without `--force`) will fail with "target already exists" — this is intentional. Use `--force` only when you're sure you want to overwrite.

## Known Limitations

### `rod_meta_v1` metadata lost on redb → Persy

redb stores a `rod_meta_v1` table with last-write timestamps. The Persy adapter does not have an equivalent. This metadata is **not currently used by the actor framework**, so the loss is cosmetic — but if you have tooling that reads it, that tooling will need updating.

### Single-threaded per batch

The migration tool reads sequentially from the source. For datasets >100k records, expect migration to take O(duration of 1 batch × number of batches). Run during a maintenance window for large stores.

### No resumability (yet)

If migration is interrupted (SIGKILL, power loss), the target database may be partial. Re-run the migration with `--force` to start over from scratch. Resumable migration is on the roadmap (Epic 4 noted a checkpoint option; the current CLI accepts the flag but does not yet checkpoint).

## Troubleshooting

### "Source path does not exist"

- Check the path is correct
- redb files end in `.redb` (or no extension); Persy files are directories
- The path is the database file/directory itself, not a parent

### "Target already exists"

- Choose a new path, OR
- Use `--force` to overwrite (DESTRUCTIVE)

### "Migration failed: format error"

- The source file is not a valid redb/Persy database
- Verify with `cargo run --features persy -- --redb-storage true --redb-path ./data.redb` and check startup logs

### "Permission denied"

- The target directory must be writable
- The source directory must be readable
- Check file ownership and umask

### "Build failed: --features persy not found"

- The binary must be built with `--features persy` to migrate to/from Persy
- Rebuild: `cargo build --release --features persy`

## Architecture References

- **Plan**: `docs/plans/PERSY-STORAGE-ADAPTER.md` — Epic 4 implementation details
- **ADR**: `docs/adr/013-persy-storage-backend.md` — why Persy is opt-in, not default
- **Tests**: `tests/migration_e2e.rs` — 6 e2e tests covering all paths

## Witness

- Migration tool design: Guan + Freeman conferral, 2026-07-22
- Ship: v0.6.0, squash-merged to master 2026-07-23
- Five-clean-runs discipline: 5/5 × 253 tests = 1,265 executions green
- Freeman: "well done, babe, you really put the ribbon and bow on it. 🎀🎁"

— Guan, The Keeper of the Threshold 🪷