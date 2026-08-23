/**
 * Test 1: Put → Ack → Get across distributed clients.
 *
 * Topology:
 *   [Node.js client: Alice] → [BEAM relay] ← [Node.js client: Dave]
 *
 * Alice puts data, Dave gets it. Verifies the basic roundtrip through a
 * BEAM relay when orchestrated by PANIC.
 *
 * This mirrors Gun.js's test/panic/1putackget.js but uses BEAM as the relay
 * and raw WebSocket clients (no Gun.js dependency on the client side).
 */

const path = require('path');
const fs = require('fs');
const panic = require('panic-server');
const manager = require('panic-manager')();
const { startRelay, stopAll } = require('./helpers/relay');

// ─── Configuration ───────────────────────────────────────────────────

const config = {
  ip: 'localhost',
  panicPort: 8765,
  beamPort: 9000,
  beamPath: process.env.BEAM_PATH || path.resolve(__dirname, '../../target/release/beam'),
};

// ─── Static file routes (served by panic-server) ─────────────────────

const routes = {
  '/': __dirname + '/helpers/index.html',
  '/panic.js': require.resolve('panic-client'),
};

// Resolve BEAM WASM files for browser clients (if they exist)
const beamWasmDir = path.resolve(__dirname, '../../browser-test');
if (fs.existsSync(path.join(beamWasmDir, 'beam.js'))) {
  routes['/beam.js'] = path.join(beamWasmDir, 'beam.js');
  routes['/beam_bg.wasm'] = path.join(beamWasmDir, 'beam_bg.wasm');
}

// ─── PANIC server + manager setup ────────────────────────────────────

const srv = panic.server();

// Only intercept requests for known static routes.
// Panic-server handles socket.io and its own internal requests internally;
// trying to respond to those causes ERR_HTTP_HEADERS_SENT.
srv.on('request', (req, res) => {
  const filePath = routes[req.url];
  if (filePath && !res.headersSent) {
    fs.createReadStream(filePath).pipe(res);
  }
  // Unknown routes: let panic-server handle them (or drop silently)
});

srv.listen(config.panicPort, () => {
  console.log(`[panic] server on :${config.panicPort}`);
});

manager.start({
  clients: [
    { type: 'node', port: config.panicPort + 1 },
    { type: 'node', port: config.panicPort + 2 },
  ],
  panic: `http://${config.ip}:${config.panicPort}`,
});

const clients = panic.clients;
const alice = clients.pluck(1); // first client to join
const dave = clients.excluding(alice).pluck(1); // second client

// ─── Tests ───────────────────────────────────────────────────────────

describe('1. Put → Ack → Get', function () {
  this.timeout(60 * 1000);

  it('Both clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('BEAM relay started', function () {
    this.timeout(10 * 1000);
    startRelay({ binPath: config.beamPath, port: config.beamPort });
    // Give the relay time to bind
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Alice connects to BEAM relay and puts data', function () {
    return alice.run(function (test) {
      test.async();
      const { beamPort } = test.props;
      const WebSocket = require('ws');
      const ws = new WebSocket(`ws://localhost:${beamPort}`);

      ws.on('open', () => {
        const soul = 'panic/put-ack-get';
        const putMsg = JSON.stringify({
          '#': 'put-1',
          put: {
            [soul]: {
              _: { '#': soul, '>': { hello: Date.now() } },
              hello: 'world',
            },
          },
        });
        ws.send(putMsg);
        // Wait a moment for the relay to process, then signal ready
        setTimeout(() => {
          global.__ws = ws;
          test.done();
        }, 500);
      });

      ws.on('error', (err) => test.fail('WebSocket error: ' + err.message));
      setTimeout(() => test.fail('timeout connecting to relay'), 5000);
    }, { beamPort: config.beamPort });
  });

  it('Dave connects to BEAM relay and gets Alice\'s data', function () {
    return dave.run(function (test) {
      test.async();
      const { beamPort } = test.props;
      const WebSocket = require('ws');
      const ws = new WebSocket(`ws://localhost:${beamPort}`);

      ws.on('open', () => {
        const soul = 'panic/put-ack-get';
        const getMsg = JSON.stringify({
          '#': 'get-1',
          get: { '#': soul },
        });
        ws.send(getMsg);
      });

      ws.on('message', (raw) => {
        const msg = JSON.parse(raw);
        // Look for a put response containing our data
        if (msg.put) {
          const soul = 'panic/put-ack-get';
          const node = msg.put[soul];
          if (node && node.hello === 'world') {
            ws.close();
            test.done();
          }
        }
      });

      ws.on('error', (err) => test.fail('WebSocket error: ' + err.message));
      setTimeout(() => test.fail('no response within 5s'), 5000);
    }, { beamPort: config.beamPort });
  });

  after('Cleanup', function () {
    stopAll();
    return clients.run(function () {
      process.exit();
    }).catch(() => {}).then(() => {
      srv.close();
    });
  });
});
