/**
 * PANIC Test 6: Volume Puts
 *
 * Maps to Gun.js: test/panic/3puts.js
 *
 * Alice puts 500 items to the relay. Bob listens and tracks every
 * ack. Assert: all 500 items received, no drops.
 *
 * Topology:
 *   [Alice] → [BEAM relay] ← [Bob]
 *
 * Gun.js tests 1000 puts via Gun.js's put() which batches internally.
 * We batch puts into JSON arrays (BEAM's wire format supports arrays
 * of messages) to match Gun.js's batching behavior.
 */

const path = require('path');
const { startRelay } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayPort: 9100,
  rootSoul: 'panic/volume/' + Date.now(),
  count: 500,
  batchSize: 10,
};

const { clients, alice, bob } = setupPanic({ numClients: 2, panicPort: config.panicPort });

describe('6. Volume Puts: 500 puts, all received', function () {
  this.timeout(120000);

  it('Two clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('BEAM relay started', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Alice puts ' + config.count + ' items, Bob receives all', function () {
    this.timeout(120000);

    const aliceP = alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
      let acked = 0;

      ws.on('open', () => {
        const count = test.props.count;
        const batchSize = test.props.batchSize;
        const soul = test.props.rootSoul;

        for (let batchStart = 0; batchStart < count; batchStart += batchSize) {
          const batch = [];
          for (let i = batchStart; i < Math.min(batchStart + batchSize, count); i++) {
            const key = 'item' + i;
            const ts = Date.now() + i;
            batch.push({
              '#': 'put' + i,
              put: {
                [soul]: {
                  _: { '#': soul, '>': { [key]: ts } },
                  [key]: 'val-' + i,
                },
              },
            });
          }
          ws.send(JSON.stringify(batch));
        }
      });

      ws.on('message', (raw) => {
        const data = raw.toString();
        let msgs;
        try { msgs = JSON.parse(data); } catch (e) { return; }
        if (!Array.isArray(msgs)) msgs = [msgs];

        for (const m of msgs) {
          if (m['@'] && m['@'].startsWith('put')) {
            acked++;
          }
        }
        if (acked >= test.props.count) {
          setTimeout(() => { ws.close(); test.done(); }, 500);
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout — only ' + acked + '/' + test.props.count + ' acked in 90s'), 90000);
    }, { rootSoul: config.rootSoul, count: config.count, batchSize: config.batchSize, relayPort: config.relayPort });

    return aliceP.then(() => {
      return bob.run(function (test) {
        test.async();
        const WebSocket = require('ws');
        const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
        let received = 0;

        ws.on('message', (raw) => {
          const m = JSON.parse(raw.toString());
          if (m.put && m.put[test.props.rootSoul]) {
            const node = m.put[test.props.rootSoul];
            for (const key of Object.keys(node)) {
              if (key !== '_' && key.startsWith('item')) {
                received++;
              }
            }
            if (received >= test.props.count) {
              ws.close();
              test.done();
            } else {
              ws.send(JSON.stringify({
                get: { '#': test.props.rootSoul },
                '#': 'reget' + Math.random().toString(36).slice(2),
              }));
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
          if (received >= test.props.count) {
            clearInterval(check);
            ws.close();
            test.done();
          } else if (Date.now() - start > 15000) {
            clearInterval(check);
            ws.close();
            test.fail('timeout — received ' + received + '/' + test.props.count + ' in 15s');
          }
        }, 100);
      }, { rootSoul: config.rootSoul, count: config.count, relayPort: config.relayPort });
    });
  });

  after(function () {
    teardownPanic();
  });
});
