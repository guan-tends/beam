/**
 * Test 2: Convergence — multi-client write/fan-out.
 *
 * Topology:
 *   [Alice] ──┐
 *   [Bob]   ──┼──→ [BEAM relay] ←── [Carol]
 *   [Dave]  ──┘
 *
 * Alice puts data. All 4 clients (including Alice) should be able
 * to Get the data back from the relay, proving storage + retrieval
 * works across multiple connected peers.
 *
 * Structure mirrors 1put-ack-get.js: separate tests for setup,
 * put, and get phases. Uses clients.atLeast(4) for connection
 * verification (idiomatic PANIC pattern).
 */
const path = require('path');
const fs = require('fs');
const panic = require('panic-server');
const manager = require('panic-manager')();
const { startRelay, stopAll } = require('./helpers/relay');
const { assert } = require('chai');

// ─── Configuration ───────────────────────────────────────────────────

const config = {
  ip: 'localhost',
  panicPort: 8765,
  // 9100 = the shared relay port convention used by every other PANIC test
  // (helpers/relay.js consumers). Avoids Penpot's permanent 9001 listener
  // and the APK server's 9000.
  beamPort: 9100,
  beamPath: process.env.BEAM_PATH || path.resolve(__dirname, '../../target/debug/beam'),
};

// ─── Static file routes ──────────────────────────────────────────────

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

srv.on('request', (req, res) => {
  const filePath = routes[req.url];
  if (filePath && !res.headersSent) {
    fs.createReadStream(filePath).pipe(res);
  }
});

srv.listen(config.panicPort, () => {
  console.log('[panic] server on :' + config.panicPort);
});

manager.start({
  clients: [
    { type: 'node', port: config.panicPort + 1 },
    { type: 'node', port: config.panicPort + 2 },
    { type: 'node', port: config.panicPort + 3 },
    { type: 'node', port: config.panicPort + 4 },
  ],
  panic: 'http://' + config.ip + ':' + config.panicPort,
});

const clients = panic.clients;
const alice = clients.pluck(1);

// ─── Tests ───────────────────────────────────────────────────────────

describe('2. Convergence: multi-client write/fan-out', function () {
  this.timeout(60 * 1000);

  const soul = 'panic/convergence/' + Date.now();

  it('All 4 clients connected to panic-server', function () {
    return clients.atLeast(4);
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
      const ws = new WebSocket('ws://localhost:' + test.props.beamPort);

      ws.on('open', () => {
        ws.send(JSON.stringify({
          '#': 'putconv',
          put: {
            [test.props.soul]: {
              _: { '#': test.props.soul, '>': { hello: Date.now() } },
              hello: 'convergence-world',
            },
          },
        }));
      });

      ws.on('message', (raw) => {
        // BEAM batches WebSocket messages into JSON arrays when several
        // are ready in the same tick — always unwrap arrays (standing rule).
        let msgs = JSON.parse(raw.toString());
        if (!Array.isArray(msgs)) msgs = [msgs];
        for (const msg of msgs) {
          if (msg['@'] === 'putconv') {
            ws.close();
            test.done();
            return;
          }
        }
      });

      ws.on('error', (err) => test.fail('WebSocket error: ' + err.message));
      setTimeout(() => test.fail('timeout — no ack in 5s'), 5000);
    }, { soul, beamPort: config.beamPort });
  });

  it('All 4 clients can Get Alice\'s data', function () {
    this.timeout(30 * 1000);

    return clients.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.beamPort);
      let gotData = null;

      ws.on('message', (raw) => {
        // BEAM batches WebSocket messages into JSON arrays when several
        // are ready in the same tick — always unwrap arrays (standing rule).
        let msgs = JSON.parse(raw.toString());
        if (!Array.isArray(msgs)) msgs = [msgs];
        for (const msg of msgs) {
          if (msg.put && msg.put[test.props.soul] && msg.put[test.props.soul].hello) {
            gotData = msg.put[test.props.soul].hello;
          }
        }
      });

      ws.on('open', () => {
        setTimeout(() => {
          ws.send(JSON.stringify({
            get: { '#': test.props.soul },
            '#': 'check' + Math.random().toString(36).slice(2),
          }));
        }, 500);
      });

      ws.on('error', (err) => test.fail('WebSocket error: ' + err.message));

      const start = Date.now();
      const check = setInterval(() => {
        if (gotData) {
          clearInterval(check);
          ws.close();
          test.done();
        } else if (Date.now() - start > 5000) {
          clearInterval(check);
          test.fail('timeout — no data in 5s');
        }
      }, 100);
    }, { soul, beamPort: config.beamPort });
  });

  after(function () {
    try { manager.stop(); } catch(e) {}
    stopAll();
  });
});
