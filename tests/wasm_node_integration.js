/**
 * BEAM WASM — Node.js Integration Test
 *
 * Tests cross-talk between two WASM BEAM clients through a real relay,
 * using Node's native libuv event loop (not wasm-bindgen-test-runner).
 *
 * This is the critical path for Mnemos server-side integration:
 *   import { Beam } from './pkg/beam.js';
 *
 * Run: node tests/wasm_node_integration.js
 *
 * Prerequisites:
 *   - wasm-pack build --target nodejs --no-default-features
 *   - cargo build --bin beam (for the relay binary)
 */

const { spawn } = require('node:child_process');
const { Beam } = require('../pkg/beam.js');

const RELAY_PORT = 4990;
const HTTP_PORT = RELAY_PORT + 1;

function startRelay() {
    return new Promise((resolve, reject) => {
        const child = spawn('target/debug/beam', [
            'start', '--port', String(RELAY_PORT),
            '--memory-storage', 'true', '--redb-storage', 'false',
            '--allow-public-space', 'true'
        ], {
            cwd: __dirname + '/..',
            stdio: ['ignore', 'pipe', 'pipe']
        });
        child.stdout.on('data', d => process.stderr.write('[relay] ' + d));
        child.stderr.on('data', d => process.stderr.write('[relay] ' + d));

        // Wait for TCP port to accept connections
        const net = require('node:net');
        const deadline = Date.now() + 10000;
        const tryConnect = () => {
            const sock = net.connect(RELAY_PORT, '127.0.0.1');
            sock.on('connect', () => { sock.destroy(); resolve(child); });
            sock.on('error', () => {
                if (Date.now() > deadline) reject(new Error('relay did not start'));
                else setTimeout(tryConnect, 50);
            });
        };
        tryConnect();
    });
}

function fetchMetrics() {
    return fetch(`http://127.0.0.1:${HTTP_PORT}/metrics`).then(r => r.json());
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
    console.log('=== BEAM WASM Node.js Integration Test ===\n');

    // Start relay
    console.log('Starting relay on port', RELAY_PORT, '...');
    const relay = await startRelay();
    console.log('Relay started.');

    // Two WASM clients
    const client1 = new Beam();
    client1.connect(`ws://127.0.0.1:${RELAY_PORT}`);

    const client2 = new Beam();
    client2.connect(`ws://127.0.0.1:${RELAY_PORT}`);

    // Wait for WebSocket handshakes
    console.log('Waiting for WebSocket connections...');
    await sleep(1000);

    // Register receive callbacks
    const c1Received = [];
    const c2Received = [];

    client1.on('chat', (v) => { c1Received.push(v); console.log('  client1 received:', v); });
    client2.on('chat', (v) => { c2Received.push(v); console.log('  client2 received:', v); });
    await sleep(200);

    // Direction 1: client1 → client2
    console.log('\nDirection 1: client1 → client2');
    client1.put('chat.001', 'from_client_1');
    await sleep(1000);

    // Direction 2: client2 → client1
    console.log('Direction 2: client2 → client1');
    client2.put('chat.002', 'from_client_2');
    await sleep(1000);

    client1.stop();
    client2.stop();

    // Check relay metrics
    const metrics = await fetchMetrics();
    console.log('\n--- Relay Metrics ---');
    console.log('  messages_parsed:', metrics.messages_parsed);
    console.log('  messages_relayed:', metrics.messages_relayed);
    console.log('  ws_messages_sent:', metrics.ws_messages_sent);

    // Assertions
    console.log('\n--- Results ---');
    console.log('  client1 received:', JSON.stringify(c1Received));
    console.log('  client2 received:', JSON.stringify(c2Received));

    let passed = 0;
    let failed = 0;

    // client2 should have received "from_client_1" (relayed from client1)
    if (c2Received.some(v => v.includes('from_client_1'))) {
        console.log('  ✅ client2 received from_client_1');
        passed++;
    } else {
        console.log('  ❌ client2 did NOT receive from_client_1');
        failed++;
    }

    // client1 should have received "from_client_2" (relayed from client2)
    if (c1Received.some(v => v.includes('from_client_2'))) {
        console.log('  ✅ client1 received from_client_2');
        passed++;
    } else {
        console.log('  ❌ client1 did NOT receive from_client_2');
        failed++;
    }

    // Relay should have processed messages
    if (metrics.messages_parsed >= 4) {
        console.log('  ✅ relay processed messages:', metrics.messages_parsed);
        passed++;
    } else {
        console.log('  ❌ relay did not process enough messages:', metrics.messages_parsed);
        failed++;
    }

    if (metrics.messages_relayed >= 2) {
        console.log('  ✅ relay forwarded messages:', metrics.messages_relayed);
        passed++;
    } else {
        console.log('  ❌ relay did not forward enough messages:', metrics.messages_relayed);
        failed++;
    }

    // Cleanup
    relay.kill('SIGTERM');
    await sleep(200);

    console.log(`\n=== ${passed} passed, ${failed} failed ===\n`);
    process.exit(failed > 0 ? 1 : 0);
}

main().catch(err => {
    console.error('Test error:', err);
    process.exit(1);
});
