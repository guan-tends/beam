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

// ─── Static file routes (served by panic-server for browser clients) ──

const routes = {
  '/': __dirname + '/helpers/index.html',
  '/panic.js': require.resolve('panic-client'),
};

const beamWasmDir = path.resolve(__dirname, '../../browser-test');
if (fs.existsSync(path.join(beamWasmDir, 'beam.js'))) {
  routes['/beam.js'] = path.join(beamWasmDir, 'beam.js');
  routes['/beam_bg.wasm'] = path.join(beamWasmDir, 'beam_bg.wasm');
}

// ─── PANIC server + manager setup ────────────────────────────────────

const srv = panic.server();

// Only intercept requests for known static routes.
// Panic-server handles socket.io internally; responding to those
// causes ERR_HTTP_HEADERS_SENT.
srv.on('request', (req, res) => {
  const filePath = routes[req.url];
  if (filePath && !res.headersSent) {
    fs.createReadStream(filePath).pipe(res);
  }
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
const alice = clients.pluck(1);
const dave = clients.excluding(alice).pluck(1);

// ─── Tests ───────────────────────────────────────────────────────────

describe('1. Put → Ack → Get', function () {
  this.timeout(60 * 1000);

  it('Both clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('BEAM relay started', function () {
    this.timeout(10 * 1000);
    startRelay({ binPath: config.beamPath, port: config.beamPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Alice puts data through BEAM relay', function () {
    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket(`ws://localhost:${test.props.beamPort}`);
      let msgs = 0;

      ws.on('open', () => {
        const soul = 'panic/put-ack-get';
        ws.send(JSON.stringify({
          '#': 'put1',
          put: {
            [soul]: {
              _: { '#': soul, '>': { hello: Date.now() } },
              hello: 'world',
            },
          },
        }));
      });

      ws.on('message', (raw) => {
        msgs++;
        const msg = JSON.parse(raw.toString());
        // Wait for the ack from storage (has @ field matching our put id)
        if (msg['@'] === 'put1') {
          console.log('[alice] ack received, closing');
          ws.close();
          test.done();
        }
      });

      ws.on('error', (err) => test.fail('WebSocket error: ' + err.message));
      setTimeout(() => test.fail('timeout — no ack in 5s'), 5000);
    }, { beamPort: config.beamPort });
  });

  it('Dave gets Alice\'s data through BEAM relay', function () {
    return dave.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket(`ws://localhost:${test.props.beamPort}`);
      let msgs = 0;

      ws.on('open', () => {
        const soul = 'panic/put-ack-get';
        ws.send(JSON.stringify({
          '#': 'get1',
          get: { '#': soul },
        }));
      });

      ws.on('message', (raw) => {
        msgs++;
        const msg = JSON.parse(raw.toString());
        // The Get response has a `put` field with the soul data
        const soul = 'panic/put-ack-get';
        if (msg.put && msg.put[soul] && msg.put[soul].hello === 'world') {
          console.log('[dave] ✅ received data: hello=world');
          ws.close();
          test.done();
        }
      });

      ws.on('error', (err) => test.fail('WebSocket error: ' + err.message));
      setTimeout(() => test.fail(`timeout — no data in 5s (received ${msgs} messages)`), 5000);
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
