/**
 * PANIC Test 8: Server-to-Server Sync (s2s-all)
 *
 * Maps to Gun.js: test/panic/s2s-all.js
 *
 * Two BEAM relays connected as peers. Alice puts data to Relay A.
 * Bob connects to Relay B and Gets the data — proving it synced
 * from A to B via peer relay.
 *
 * Topology:
 *   [Alice] → [Relay A] ←→ [Relay B] ← [Bob]
 *
 * Gun.js tests sync of 4 souls (a1, b2, c3, d4). We test the same.
 */

const path = require('path');
const { startRelay, stopAll } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayAPort: 9100,
  relayBPort: 9200,
  souls: ['a1', 'b2', 'c3', 'd4'],
};

const { clients, alice, bob } = setupPanic({ numClients: 2, panicPort: config.panicPort });

describe('8. s2s-all: data syncs between two peered relays', function () {
  this.timeout(60000);

  it('Two clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('Relay A and Relay B start as peers', function () {
    this.timeout(15000);
    startRelay({ binPath, port: config.relayAPort });
    return new Promise((resolve) => setTimeout(resolve, 1000)).then(() => {
      startRelay({
        binPath,
        port: config.relayBPort,
        peers: ['ws://localhost:' + config.relayAPort],
      });
      return new Promise((resolve) => setTimeout(resolve, 3000));
    });
  });

  it('Alice puts 4 souls to Relay A', function () {
    this.timeout(15000);
    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayAPort);
      let acked = 0;
      const souls = test.props.souls;

      ws.on('open', () => {
        for (let i = 0; i < souls.length; i++) {
          const soul = souls[i];
          ws.send(JSON.stringify({
            '#': 's2sput' + i,
            put: {
              [soul]: {
                _: { '#': soul, '>': { val: Date.now() + i } },
                val: 'data' + i,
              },
            },
          }));
        }
      });

      ws.on('message', (raw) => {
        const m = JSON.parse(raw.toString());
        if (m['@'] && m['@'].startsWith('s2sput')) {
          acked++;
        }
        if (acked >= souls.length) {
          ws.close();
          test.done();
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout — only ' + acked + '/' + souls.length + ' acked'), 10000);
    }, { relayAPort: config.relayAPort, souls: config.souls });
  });

  it('Bob Gets all 4 souls from Relay B (cross-relay sync)', function () {
    this.timeout(30000);
    return bob.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayBPort);
      const souls = test.props.souls;
      const received = {};

      ws.on('message', (raw) => {
        const m = JSON.parse(raw.toString());
        if (m.put) {
          for (const soul of souls) {
            if (m.put[soul] && m.put[soul].val) {
              received[soul] = m.put[soul].val;
            }
          }
        }
        if (Object.keys(received).length >= souls.length) {
          ws.close();
          test.done();
        }
      });

      ws.on('open', () => {
        setTimeout(() => {
          for (const soul of souls) {
            ws.send(JSON.stringify({
              get: { '#': soul },
              '#': 's2sget' + soul + Math.random().toString(36).slice(2),
            }));
          }
        }, 500);
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));

      const start = Date.now();
      const check = setInterval(() => {
        const count = Object.keys(received).length;
        if (count >= souls.length) {
          clearInterval(check);
          ws.close();
          test.done();
        } else if (Date.now() - start > 15000) {
          clearInterval(check);
          ws.close();
          test.fail('timeout — received ' + count + '/' + souls.length + ' souls: ' + JSON.stringify(received));
        }
      }, 100);
    }, { relayBPort: config.relayBPort, souls: config.souls });
  });

  after(function () {
    teardownPanic();
  });
});
