/**
 * BEAM WASM Node.js Filesystem Persistence Tests
 *
 * Verifies that data persisted via `WasmNodeFsStorage` survives process
 * restart. This is the critical test for Mnemos server-side integration.
 *
 * Prerequisites:
 *   cargo build --bin beam
 *   wasm-pack build --target nodejs --features node-fs --no-default-features
 *
 * Usage:
 *   node tests/wasm-integration/node-fs-persistence.mjs
 *
 * Exit code 0 = all pass, 1 = at least one failure.
 */

import { Beam } from '../../pkg/beam.js';
import { spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import { existsSync, rmSync } from 'node:fs';

// ─── Test helpers ───────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function assert(condition, message) {
    if (condition) {
        passed++;
        console.log(`  ✅ ${message}`);
    } else {
        failed++;
        console.error(`  ❌ ${message}`);
    }
}

async function startRelay(port) {
    const relay = spawn('target/debug/beam', [
        'start', '--port', String(port),
        '--memory-storage', 'true',
        '--redb-storage', 'false',
    ], {
        cwd: import.meta.dirname.replace('/tests/wasm-integration', ''),
        stdio: ['ignore', 'pipe', 'pipe'],
    });

    // Wait for TCP port to accept connections.
    for (let i = 0; i < 50; i++) {
        try {
            const resp = await fetch(`http://127.0.0.1:${port + 1}/health`);
            if (resp.ok) break;
        } catch {
            await sleep(100);
        }
    }

    return {
        kill() {
            relay.kill('SIGTERM');
        }
    };
}

const TEST_DIR = '/tmp/beam_fs_persistence_test';

// ─── Tests ──────────────────────────────────────────────────────────

/**
 * T1: fs_put_get_roundtrip
 *
 * Write a value, read it back. Verifies basic put/get works with
 * the Node.js fs storage adapter.
 */
async function testPutGetRoundtrip(port) {
    console.log('\nT1: fs_put_get_roundtrip');
    const relay = await startRelay(port);

    const beam = Beam.new_with_node_fs_dir(TEST_DIR);
    beam.connect(`ws://127.0.0.1:${port}`);
    await sleep(500);

    beam.put('test.key1', 'value_from_t1');
    await sleep(300);

    const result = await beam.get('test.key1');
    assert(result === 'value_from_t1', `get returned "${result}"`);

    beam.stop();
    relay.kill();
    await sleep(200);
}

/**
 * T2: fs_persistence_across_restart
 *
 * Write data with one Beam instance, stop it, create a new instance
 * pointing at the same directory, and verify the data is still there.
 * This is the critical test for Mnemos server-side integration.
 */
async function testPersistenceAcrossRestart(port) {
    console.log('\nT2: fs_persistence_across_restart');

    // Clean up any leftover data from previous runs.
    rmSync(TEST_DIR, { recursive: true, force: true });

    // Phase 1: Write data
    const relay1 = await startRelay(port);
    const beam1 = Beam.new_with_node_fs_dir(TEST_DIR);
    beam1.connect(`ws://127.0.0.1:${port}`);
    await sleep(500);

    const testKeys = [
        'persist.k1', 'persist.k2', 'persist.k3',
        'persist.k4', 'persist.k5',
    ];
    const testValues = [
        'val1', 'val2', 'val3', 'val4', 'val5',
    ];

    for (let i = 0; i < testKeys.length; i++) {
        beam1.put(testKeys[i], testValues[i]);
    }
    await sleep(1000);

    // Verify data is readable before restart
    for (let i = 0; i < testKeys.length; i++) {
        const v = await beam1.get(testKeys[i]);
        assert(v === testValues[i], `pre-restart: ${testKeys[i]} = "${v}"`);
    }

    beam1.stop();
    relay1.kill();
    await sleep(500);

    // Phase 2: New instance, same directory — data should survive
    const relay2 = await startRelay(port);
    const beam2 = Beam.new_with_node_fs_dir(TEST_DIR);
    beam2.connect(`ws://127.0.0.1:${port}`);
    await sleep(1000);

    let recovered = 0;
    for (let i = 0; i < testKeys.length; i++) {
        const v = await beam2.get(testKeys[i]);
        if (v === testValues[i]) recovered++;
    }

    assert(
        recovered === testKeys.length,
        `recovered ${recovered}/${testKeys.length} keys after restart`
    );

    beam2.stop();
    relay2.kill();
    await sleep(200);

    // Cleanup
    rmSync(TEST_DIR, { recursive: true, force: true });
}

/**
 * T3: fs_batch_put
 *
 * Write multiple values in rapid succession, verify all are readable.
 */
async function testBatchPut(port) {
    console.log('\nT3: fs_batch_put');
    const relay = await startRelay(port);

    const beam = Beam.new_with_node_fs_dir(TEST_DIR);
    beam.connect(`ws://127.0.0.1:${port}`);
    await sleep(500);

    const COUNT = 20;
    for (let i = 0; i < COUNT; i++) {
        beam.put(`batch.${i}`, `item_${i}`);
    }
    await sleep(2000);

    let found = 0;
    for (let i = 0; i < COUNT; i++) {
        const v = await beam.get(`batch.${i}`);
        if (v === `item_${i}`) found++;
    }

    assert(found === COUNT, `found ${found}/${COUNT} batch items`);

    beam.stop();
    relay.kill();
    await sleep(200);

    // Cleanup
    rmSync(TEST_DIR, { recursive: true, force: true });
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
    console.log('=== BEAM WASM Node.js fs Persistence Tests ===\n');

    await testPutGetRoundtrip(4960);
    await testPersistenceAcrossRestart(4970);
    await testBatchPut(4980);

    console.log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
    process.exit(failed > 0 ? 1 : 0);
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
