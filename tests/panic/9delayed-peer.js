/**
 * PANIC Test 9: Delayed Peer Add
 *
 * Maps to Gun.js: test/panic/s2s-all-delayed-peer-add.js
 *
 * Relay A starts with data. Relay B starts WITHOUT a peer connection.
 * Then Relay B adds Relay A as a peer after initialization.
 * Data should sync from A to B after the delayed peer add.
 *
 * Topology:
 *   Phase 1: [Alice] → [Relay A] (standalone)   [Relay B] (standalone)
 *   Phase 2: [Alice] → [Relay A] ←→ [Relay B] (peer added later)
 *
 * This tests that BEAM can sync data when a peer is added AFTER
 * data has already been stored — not just real-time relay.
 */

const path = require('path');
const { startRelay, stopRelay, stopAll, waitForPortFree } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayAPort: 9100,
  relayBPort: 9200,
  souls: ['a1', 'b2', 'c3', 'd4'],
};

const { clients, alice, bob } = setupPanic({ numClients: 2, panicPort: config.panicPort });

describe('9. Delayed-peer: data syncs after peer added post-init', function () {
  this.timeout(60000);

  it('Two clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('Relay A starts (no peers), Relay B starts (no peers)', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayAPort });
    startRelay({ binPath, port: config.relayBPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Alice puts 4 souls to Relay A (before peer connection)', function () {
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
            '#': 'dput' + i,
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
        let msgs;
        try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
        if (!Array.isArray(msgs)) msgs = [msgs];
        for (const m of msgs) {
          if (m['@'] && m['@'].startsWith('dput')) acked++;
        }
        if (acked >= souls.length) {
          ws.close();
          test.done();
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout — ' + acked + '/' + souls.length + ' acked'), 10000);
    }, { relayAPort: config.relayAPort, souls: config.souls });
  });

  it('Relay B adds Relay A as peer (delayed)', function () {
    this.timeout(10000);
    // Stop only Relay B, keep Relay A alive (it has the data).
    stopRelay(config.relayBPort);

    // Wait for both of Relay B's ports (ws + web UI) to actually free,
    // then wait for Relay A's data to be reachable via the new peer link.
    return waitForPortFree(config.relayBPort).then(() => {
      // Restart Relay B with Relay A as peer
      startRelay({
        binPath,
        port: config.relayBPort,
        peers: ['ws://localhost:' + config.relayAPort],
      });
      return new Promise((resolve) => setTimeout(resolve, 3000));
    });
  });

  it('Bob Gets all 4 souls from Relay B (synced after delayed peer add)', function () {
    this.timeout(30000);
    return bob.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayBPort);
      const souls = test.props.souls;
      const received = {};

      ws.on('message', (raw) => {
        let msgs;
        try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
        if (!Array.isArray(msgs)) msgs = [msgs];
        for (const m of msgs) {
          if (m.put) {
            for (const soul of souls) {
              if (m.put[soul] && m.put[soul].val) {
                received[soul] = m.put[soul].val;
              }
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
              '#': 'dget' + soul + Math.random().toString(36).slice(2),
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
          test.fail('timeout — received ' + count + '/' + souls.length + ': ' + JSON.stringify(received));
        }
      }, 100);
    }, { relayBPort: config.relayBPort, souls: config.souls });
  });

  after(function () {
    teardownPanic();
  });
});
