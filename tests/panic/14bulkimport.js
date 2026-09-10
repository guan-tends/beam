/**
 * PANIC Test 14: Bulk Import
 *
 * Maps to Gun.js: test/panic/bulkimport.js
 *
 * Gun.js's bulkimport.js has Alice perform 1000 individual puts,
 * each with its own ack callback, and asserts every ack arrives.
 * The BEAM port preserves the criteria: 1000 puts, all 1000 acks
 * received, then a second client verifies all 1000 keys via Get.
 *
 * Difference from Test 6 (volume-puts): Test 6 batches puts into
 * JSON arrays (matching Gun's internal batching). This test sends
 * each put individually — 1000 separate wire messages — matching
 * Gun's bulkimport shape (one put() call per key, ack per put).
 *
 * Topology:
 *   [Alice] → [BEAM relay] ← [Bob]
 */

const path = require('path');
const { startRelay } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayPort: 9100,
  rootSoul: 'panic/bulkimport/' + Date.now(),
  count: 1000,
};

const { clients, alice, bob } = setupPanic({ numClients: 2, panicPort: config.panicPort });

describe('14. Bulk Import: 1000 individual puts, all acked + verified', function () {
  this.timeout(180000);

  it('Two clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('BEAM relay started', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Alice puts ' + config.count + ' items, all acked; Bob verifies all', function () {
    this.timeout(180000);

    const aliceP = alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
      let acked = 0;

      ws.on('open', () => {
        // One put per wire message — the bulkimport shape (no batching).
        for (let i = 0; i < test.props.count; i++) {
          const key = 'k' + i;
          const ts = Date.now() + i;
          ws.send(JSON.stringify({
            '#': 'blk' + i,
            put: {
              [test.props.rootSoul]: {
                _: { '#': test.props.rootSoul, '>': { [key]: ts } },
                [key]: 'val-' + i,
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
          if (m['@'] && m['@'].startsWith('blk')) {
            acked++;
          }
        }
        if (acked >= test.props.count) {
          setTimeout(() => { ws.close(); test.done(); }, 500);
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout — only ' + acked + '/' + test.props.count + ' acked in 150s'), 150000);
    }, { rootSoul: config.rootSoul, count: config.count, relayPort: config.relayPort });

    return aliceP.then(() => {
      return bob.run(function (test) {
        test.async();
        const WebSocket = require('ws');
        const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
        let verified = 0;

        ws.on('message', (raw) => {
          let msgs;
          try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
          if (!Array.isArray(msgs)) msgs = [msgs];

          for (const m of msgs) {
            const node = m.put && m.put[test.props.rootSoul];
            if (node) {
              // Count only this test's keys (k0..k999), skip metadata.
              for (const key of Object.keys(node)) {
                if (key !== '_' && /^k\d+$/.test(key)) {
                  verified++;
                }
              }
            }
          }
        });

        ws.on('open', () => {
          setTimeout(() => {
            ws.send(JSON.stringify({
              get: { '#': test.props.rootSoul },
              '#': 'bobget' + Math.random().toString(36).slice(2),
            }));
          }, 300);
        });

        ws.on('error', (err) => test.fail('WS error: ' + err.message));

        const start = Date.now();
        const check = setInterval(() => {
          if (verified >= test.props.count) {
            clearInterval(check);
            ws.close();
            test.done();
          } else if (Date.now() - start > 30000) {
            clearInterval(check);
            ws.close();
            test.fail('timeout — verified ' + verified + '/' + test.props.count + ' in 30s');
          }
        }, 100);
      }, { rootSoul: config.rootSoul, count: config.count, relayPort: config.relayPort });
    });
  });

  after(function () {
    teardownPanic();
  });
});
