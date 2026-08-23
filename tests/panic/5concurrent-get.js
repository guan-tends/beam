/**
 * PANIC Test 5: Concurrent Get — No Conflict
 *
 * Maps to Gun.js: test/panic/2getget.js
 *
 * Alice saves data "offline" (no subscribers). Then Carl and Dave
 * simultaneously ask for it through the same relay. Both must receive
 * the data, and their concurrent requests must not conflict or cause
 * either to drop.
 *
 * Topology:
 *   [Alice] → [BEAM relay] ← [Carl]
 *                        ← [Dave]
 *
 * Alice puts and disconnects. Carl and Dave connect and Get
 * simultaneously (within 50ms). Both must receive Alice's data.
 */

const path = require('path');
const { startRelay } = require('./helpers/relay');
const { setupPanic, genMsgId, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayPort: 9000,
  soul: 'panic/concurrent-get/' + Date.now(),
};

const { clients, alice, rest } = setupPanic({ numClients: 3, panicPort: config.panicPort });
const carl = rest.pluck(1);
const dave = rest.excluding(carl).pluck(1);

describe('5. Concurrent Get: simultaneous requests don\'t conflict', function () {
  this.timeout(60000);

  it('Three clients connected to panic-server', function () {
    return clients.atLeast(3);
  });

  it('BEAM relay started', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Alice puts data then disconnects (offline save)', function () {
    this.timeout(15000);
    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);

      ws.on('open', () => {
        ws.send(JSON.stringify({
          '#': 'aliceput',
          put: {
            [test.props.soul]: {
              _: { '#': test.props.soul, '>': { msg: Date.now() } },
              msg: 'concurrent-get-works',
            },
          },
        }));
      });

      ws.on('message', (raw) => {
        const m = JSON.parse(raw.toString());
        if (m['@'] === 'aliceput') {
          ws.close();
          test.done();
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout — no ack in 5s'), 5000);
    }, { soul: config.soul, relayPort: config.relayPort });
  });

  it('Carl and Dave simultaneously Get — both receive data', function () {
    this.timeout(15000);

    // Run Carl and Dave concurrently — both Get at roughly the same time.
    const carlP = carl.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
      let gotData = null;

      ws.on('message', (raw) => {
        const m = JSON.parse(raw.toString());
        if (m.put && m.put[test.props.soul] && m.put[test.props.soul].msg) {
          gotData = m.put[test.props.soul].msg;
        }
      });

      ws.on('open', () => {
        // Small delay to let connection stabilize, then Get immediately.
        setTimeout(() => {
          ws.send(JSON.stringify({
            get: { '#': test.props.soul },
            '#': 'carlget' + Math.random().toString(36).slice(2),
          }));
        }, 200);
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));

      const start = Date.now();
      const check = setInterval(() => {
        if (gotData === 'concurrent-get-works') {
          clearInterval(check);
          ws.close();
          test.done();
        } else if (Date.now() - start > 5000) {
          clearInterval(check);
          test.fail('timeout — Carl got no data in 5s');
        }
      }, 100);
    }, { soul: config.soul, relayPort: config.relayPort });

    const daveP = dave.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
      let gotData = null;

      ws.on('message', (raw) => {
        const m = JSON.parse(raw.toString());
        if (m.put && m.put[test.props.soul] && m.put[test.props.soul].msg) {
          gotData = m.put[test.props.soul].msg;
        }
      });

      ws.on('open', () => {
        // Fire Get at the same time as Carl — concurrent.
        setTimeout(() => {
          ws.send(JSON.stringify({
            get: { '#': test.props.soul },
            '#': 'daveget' + Math.random().toString(36).slice(2),
          }));
        }, 200);
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));

      const start = Date.now();
      const check = setInterval(() => {
        if (gotData === 'concurrent-get-works') {
          clearInterval(check);
          ws.close();
          test.done();
        } else if (Date.now() - start > 5000) {
          clearInterval(check);
          test.fail('timeout — Dave got no data in 5s');
        }
      }, 100);
    }, { soul: config.soul, relayPort: config.relayPort });

    return Promise.all([carlP, daveP]);
  });

  after(function () {
    teardownPanic();
  });
});
