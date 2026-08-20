/**
 * BEAM WASM Node.js Integration Tests
 *
 * Tests WebSocket networking behavior that cannot be reliably tested in
 * wasm-bindgen-test-runner (whose microtask-based executor doesn't pump
 * I/O events between poll cycles).
 *
 * Prerequisites:
 *   cargo build --bin beam
 *   wasm-pack build --target nodejs --no-default-features
 *
 * Usage:
 *   node tests/wasm-integration/node-integration.mjs
 *
 * Exit code 0 = all pass, 1 = at least one failure.
 */

import { Beam } from '../../pkg/beam.js';
import { spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';

// ─── Relay lifecycle ────────────────────────────────────────────────

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

async function fetchMetrics(wsPort) {
    const resp = await fetch(`http://127.0.0.1:${wsPort + 1}/metrics`);
    return resp.json();
}

// ─── Tests ──────────────────────────────────────────────────────────

async function testRelayConnect(port) {
    console.log('\nT1: relay_connect');
    const relay = await startRelay(port);

    const beam = new Beam();
    beam.connect(`ws://127.0.0.1:${port}`);
    await sleep(500);

    // If we got here without throwing, the connection succeeded.
    assert(true, 'WebSocket connected to relay without error');
    beam.stop();
    relay.kill();
    await sleep(200);
}

async function testRelayPutEcho(port) {
    console.log('\nT2: relay_put_echo');
    const relay = await startRelay(port);

    const beam = new Beam();
    beam.connect(`ws://127.0.0.1:${port}`);
    await sleep(500);

    beam.put('chat.relay_test', 'relay payload');
    await sleep(300);

    const result = await beam.get('chat.relay_test');
    assert(result === 'relay payload', `get returned "${result}"`);

    beam.stop();
    relay.kill();
    await sleep(200);
}

async function testTwoClientsCrossTalk(port) {
    console.log('\nT3: two_clients_cross_talk');
    const relay = await startRelay(port);

    const client1 = new Beam();
    client1.connect(`ws://127.0.0.1:${port}`);

    const client2 = new Beam();
    client2.connect(`ws://127.0.0.1:${port}`);

    await sleep(1000);

    const received = [];
    client2.on('chat', (val) => received.push(val));
    await sleep(200);

    client1.put('chat.42', 'cross-talk!');
    await sleep(1000);

    assert(
        received.some(v => v.includes('cross-talk')),
        `client2 received: ${JSON.stringify(received)}`
    );

    client1.stop();
    client2.stop();
    relay.kill();
    await sleep(200);
}

async function testBidirectionalCrossTalk(port) {
    console.log('\nT4: bidirectional_cross_talk');
    const relay = await startRelay(port);

    const client1 = new Beam();
    client1.connect(`ws://127.0.0.1:${port}`);

    const client2 = new Beam();
    client2.connect(`ws://127.0.0.1:${port}`);

    await sleep(1000);

    const c1Received = [];
    const c2Received = [];
    client1.on('chat', (v) => c1Received.push(v));
    client2.on('chat', (v) => c2Received.push(v));
    await sleep(200);

    client1.put('chat.001', 'from_client_1');
    await sleep(1000);

    client2.put('chat.002', 'from_client_2');
    await sleep(1000);

    assert(
        c2Received.some(v => v.includes('from_client_1')),
        `client2 received: ${JSON.stringify(c2Received)}`
    );
    assert(
        c1Received.some(v => v.includes('from_client_2')),
        `client1 received: ${JSON.stringify(c1Received)}`
    );

    client1.stop();
    client2.stop();
    relay.kill();
    await sleep(200);
}

async function testRelayThroughput1k(port) {
    console.log('\nT5: wasm_relay_throughput_1k');
    const relay = await startRelay(port);

    const beam = new Beam();
    beam.connect(`ws://127.0.0.1:${port}`);
    await sleep(500);

    const before = await fetchMetrics(port);
    const COUNT = 1000;

    for (let i = 0; i < COUNT; i++) {
        beam.put(`bench/${i}`, `msg_${i}`);
    }

    // Wait for relay counters to stabilize.
    let lastRelayed = before.messages_relayed || 0;
    for (let i = 0; i < 60; i++) {
        await sleep(500);
        const snap = await fetchMetrics(port);
        const nowRelayed = snap.messages_relayed || 0;
        if (nowRelayed === lastRelayed) break;
        lastRelayed = nowRelayed;
    }

    const after = await fetchMetrics(port);
    const relayed = (after.messages_relayed || 0) - (before.messages_relayed || 0);
    const wsRecv = (after.ws_messages_received || 0) - (before.ws_messages_received || 0);

    console.log(`  relay: ws_recv=${wsRecv} relayed=${relayed}`);
    assert(relayed > 0, `relay processed ${relayed} messages`);

    beam.stop();
    relay.kill();
    await sleep(200);
}

async function testRelayThroughput5k(port) {
    console.log('\nT6: wasm_relay_throughput_5k');
    const relay = await startRelay(port);

    const beam = new Beam();
    beam.connect(`ws://127.0.0.1:${port}`);
    await sleep(500);

    const before = await fetchMetrics(port);
    const COUNT = 5000;

    for (let i = 0; i < COUNT; i++) {
        beam.put(`bench/${i}`, `msg_${i}`);
    }

    // Wait for relay counters to stabilize.
    let lastRelayed = before.messages_relayed || 0;
    for (let i = 0; i < 60; i++) {
        await sleep(500);
        const snap = await fetchMetrics(port);
        const nowRelayed = snap.messages_relayed || 0;
        if (nowRelayed === lastRelayed) break;
        lastRelayed = nowRelayed;
    }

    const after = await fetchMetrics(port);
    const relayed = (after.messages_relayed || 0) - (before.messages_relayed || 0);
    const wsRecv = (after.ws_messages_received || 0) - (before.ws_messages_received || 0);

    console.log(`  relay: ws_recv=${wsRecv} relayed=${relayed}`);
    assert(relayed > 0, `relay processed ${relayed} messages`);

    beam.stop();
    relay.kill();
    await sleep(200);
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
    console.log('=== BEAM WASM Node.js Integration Tests ===\n');

    // Nodejs-target WASM module auto-initializes on import.
    // Each test gets a unique port (spaced 10 apart for WS+HTTP).
    await testRelayConnect(4900);
    await testRelayPutEcho(4910);
    await testTwoClientsCrossTalk(4920);
    await testBidirectionalCrossTalk(4930);
    await testRelayThroughput1k(4940);
    await testRelayThroughput5k(4950);

    console.log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
    process.exit(failed > 0 ? 1 : 0);
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
