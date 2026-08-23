/**
 * Shared PANIC test setup — eliminates boilerplate across test files.
 *
 * Provides:
 * - setupPanic(numClients): starts panic-server, spawns N node clients,
 *   returns { clients, alice, bob, carol, ... }
 * - genMsgId(prefix): generates an alphanumeric message ID (BEAM convention)
 * - teardownPanic(): stops all relays and cleans up
 *
 * @module helpers/setup
 */

const path = require('path');
const fs = require('fs');
const panic = require('panic-server');
const manager = require('panic-manager')();
const { stopAll } = require('./relay');

// ─── Static file routes (served by panic-server for browser clients) ──

const routes = {
  '/': __dirname + '/index.html',
  '/panic.js': require.resolve('panic-client'),
};

// Conditionally serve BEAM WASM if built.
const beamWasmDir = path.resolve(__dirname, '../../browser-test');
if (fs.existsSync(path.join(beamWasmDir, 'beam.js'))) {
  routes['/beam.js'] = path.join(beamWasmDir, 'beam.js');
  routes['/beam_bg.wasm'] = path.join(beamWasmDir, 'beam_bg.wasm');
}

// ─── State (single panic-server per process) ──────────────────────────

let srvStarted = false;

/**
 * Start panic-server and spawn N Node.js panic-clients.
 *
 * Allocates clients with descriptive names for convenience:
 * clients 1-4 → alice, bob, carol, dave (plus `rest` for extras).
 *
 * @param {Object} opts
 * @param {number} [opts.numClients=2]  - Number of Node.js clients to spawn.
 * @param {number} [opts.panicPort=8765] - Port for panic-server.
 * @returns {{ clients: object, alice: object, bob: object, carol?: object, dave?: object, rest: object }}
 */
function setupPanic(opts = {}) {
  const numClients = opts.numClients || 2;
  const panicPort = opts.panicPort || 8765;
  const ip = 'localhost';

  if (!srvStarted) {
    const srv = panic.server();
    srv.on('request', (req, res) => {
      const filePath = routes[req.url];
      if (filePath && !res.headersSent) {
        fs.createReadStream(filePath).pipe(res);
      }
    });
    srv.listen(panicPort, () => {
      console.log(`[panic] server on :${panicPort}`);
    });
    srvStarted = true;
  }

  manager.start({
    clients: Array.from({ length: numClients }, (_, i) => ({
      type: 'node',
      port: panicPort + i + 1,
    })),
    panic: `http://${ip}:${panicPort}`,
  });

  const clients = panic.clients;
  const alice = clients.pluck(1);
  const bob = clients.excluding(alice).pluck(1);
  const rest = clients.excluding([alice, bob]);

  const result = { clients, alice, bob, rest };

  // Allocate carol and dave if we have enough clients.
  if (numClients >= 3) {
    result.carol = rest.pluck(1);
  }
  if (numClients >= 4) {
    result.dave = rest.excluding(result.carol).pluck(1);
  }

  return result;
}

/**
 * Generate an alphanumeric message ID.
 *
 * BEAM accepts any string (Gun.js compatible), but alphanumeric is the
 * test convention for readability and consistency.
 *
 * @param {string} [prefix='msg'] - Prefix for the ID (must be alphanumeric).
 * @returns {string} Alphanumeric message ID.
 */
function genMsgId(prefix = 'msg') {
  return prefix + Math.random().toString(36).slice(2);
}

/**
 * Teardown — stop all relays and the panic manager.
 * Call in after() hooks.
 */
function teardownPanic() {
  try { manager.stop(); } catch (e) { /* ignore */ }
  stopAll();
}

module.exports = { setupPanic, genMsgId, teardownPanic };
