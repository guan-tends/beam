/**
 * PANIC Test 4: Mixed Mesh Topology
 *
 * Three BEAM relays connected as peers in a mesh:
 *   Relay A ←→ Relay B ←→ Relay C
 *   (A also connects to C directly — full mesh)
 *
 * Alice puts to A. Bob (on B) and Carol (on C) should both
 * be able to Get Alice's data, proving mesh propagation.
 */

const path = require('path');
const fs = require('fs');
const panic = require('panic-server');
const manager = require('panic-manager')();
const { startRelay, stopAll } = require('./helpers/relay');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  ip: 'localhost',
  panicPort: 8765,
  relayAPort: 9300,
  relayBPort: 9400,
  relayCPort: 9500,
  soul: 'panic/mixed-mesh/' + Date.now(),
};

const routes = {
  '/': __dirname + '/helpers/index.html',
  '/panic.js': require.resolve('panic-client'),
};

const srv = panic.server();

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
    { type: 'node', port: config.panicPort + 3 },
  ],
  panic: `http://${config.ip}:${config.panicPort}`,
});

const clients = panic.clients;
const alice = clients.pluck(1);
const rest = clients.excluding(alice);
const bob = rest.pluck(1);
const carol = rest.excluding(bob).pluck(1);

describe('4. Mixed-Mesh: 3 relays peered in full mesh', function () {
  this.timeout(60000);

  it('Three clients connected to panic-server', function () {
    return clients.atLeast(3);
  });

  it('Three relays start as full mesh (A↔B, B↔C, A↔C)', function () {
    this.timeout(15000);
    // Start A first, then B (peer to A), then C (peer to A and B)
    startRelay({
      binPath,
      port: config.relayAPort,
    });
    // Wait for A to bind
    return new Promise((resolve) => setTimeout(resolve, 1000)).then(() => {
      startRelay({
        binPath,
        port: config.relayBPort,
        peers: ['ws://localhost:' + config.relayAPort],
      });
      return new Promise((resolve) => setTimeout(resolve, 1000));
    }).then(() => {
      startRelay({
        binPath,
        port: config.relayCPort,
        peers: [
          'ws://localhost:' + config.relayAPort,
          'ws://localhost:' + config.relayBPort,
        ],
      });
      return new Promise((resolve) => setTimeout(resolve, 3000));
    });
  });

  it('Alice puts data to Relay A', function () {
    this.timeout(15000);
    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayAPort);

      ws.on('open', () => {
        ws.send(JSON.stringify({
          '#': 'putmesh',
          put: {
            [test.props.soul]: {
              _: { '#': test.props.soul, '>': { msg: Date.now() } },
              msg: 'mesh-propagation-works',
            },
          },
        }));
      });

      ws.on('message', (raw) => {
        const m = JSON.parse(raw.toString());
        if (m['@'] === 'putmesh') {
          ws.close();
          test.done();
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout — no ack in 5s'), 5000);
    }, { soul: config.soul, relayAPort: config.relayAPort });
  });

  it('Bob can Get data from Relay B (1-hop propagation)', function () {
    this.timeout(15000);
    return bob.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayBPort);

      let gotData = null;

      ws.on('message', (raw) => {
        const m = JSON.parse(raw.toString());
        if (m.put && m.put[test.props.soul] && m.put[test.props.soul].msg) {
          gotData = m.put[test.props.soul].msg;
        }
      });

      ws.on('open', () => {
        setTimeout(() => {
          ws.send(JSON.stringify({
            get: { '#': test.props.soul },
            '#': 'getbob' + Math.random().toString(36).slice(2),
          }));
        }, 500);
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));

      const start = Date.now();
      const check = setInterval(() => {
        if (gotData === 'mesh-propagation-works') {
          clearInterval(check);
          ws.close();
          test.done();
        } else if (Date.now() - start > 5000) {
          clearInterval(check);
          test.fail('timeout — no data in 5s');
        }
      }, 100);
    }, { soul: config.soul, relayBPort: config.relayBPort });
  });

  it('Carol can Get data from Relay C (2-hop propagation)', function () {
    this.timeout(15000);
    return carol.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayCPort);

      let gotData = null;

      ws.on('message', (raw) => {
        const m = JSON.parse(raw.toString());
        if (m.put && m.put[test.props.soul] && m.put[test.props.soul].msg) {
          gotData = m.put[test.props.soul].msg;
        }
      });

      ws.on('open', () => {
        setTimeout(() => {
          ws.send(JSON.stringify({
            get: { '#': test.props.soul },
            '#': 'getcarol' + Math.random().toString(36).slice(2),
          }));
        }, 500);
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));

      const start = Date.now();
      const check = setInterval(() => {
        if (gotData === 'mesh-propagation-works') {
          clearInterval(check);
          ws.close();
          test.done();
        } else if (Date.now() - start > 5000) {
          clearInterval(check);
          test.fail('timeout — no data in 5s');
        }
      }, 100);
    }, { soul: config.soul, relayCPort: config.relayCPort });
  });

  after(function () {
    stopAll();
  });
});
