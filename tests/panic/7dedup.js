/**
 * PANIC Test 7: Dedup — Star topology, no duplicate delivery
 *
 * Maps to Gun.js: test/panic/4putackdedup.js
 *
 * In a star topology with 1 relay and 6 clients:
 *   [Alice] ──┐
 *   [Bob]   ──┤
 *   [Carol] ──┼──→ [BEAM relay]
 *   [Dave]  ──┤
 *   [Eve]   ──┤
 *   [Frank] ──┘
 *
 * Alice puts data. All other clients are subscribed (sent a Get).
 * Each client must receive the data EXACTLY ONCE — no duplicates
 * from relay fan-out.
 *
 * Gun.js tests custom ack sampling (3/N ratio). BEAM doesn't implement
 * custom ack sampling yet, so this test verifies core dedup: message
 * IDs are deduplicated so a single Put produces exactly one delivery
 * per subscriber, not multiple.
 */

const path = require('path');
const { startRelay } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayPort: 9100,
  soul: 'panic/dedup/' + Date.now(),
  numClients: 6,
};

const { clients } = setupPanic({ numClients: config.numClients, panicPort: config.panicPort });
const alice = clients.pluck(1);
const rest = clients.excluding(alice);
const bob = rest.pluck(1);
const carol = rest.excluding(bob).pluck(1);
const dave = rest.excluding([bob, carol]).pluck(1);
const eve = rest.excluding([bob, carol, dave]).pluck(1);
const frank = rest.excluding([bob, carol, dave, eve]).pluck(1);
const subscribers = [bob, carol, dave, eve, frank];

describe('7. Dedup: star topology, no duplicate delivery', function () {
  this.timeout(60000);

  it(config.numClients + ' clients connected to panic-server', function () {
    return clients.atLeast(config.numClients);
  });

  it('BEAM relay started', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('All subscribers send Get (subscribe to soul)', function () {
    this.timeout(15000);

    return Promise.all(subscribers.map(function (client) {
      return client.run(function (test) {
        test.async();
        const WebSocket = require('ws');
        const ws = new WebSocket('ws://localhost:' + test.props.relayPort);

        ws.on('open', () => {
          ws.send(JSON.stringify({
            get: { '#': test.props.soul },
            '#': 'sub' + Math.random().toString(36).slice(2),
          }));
          // Keep connection open — we're subscribing.
          setTimeout(() => { test.done(); }, 500);
        });

        ws.on('error', (err) => test.fail('WS error: ' + err.message));
        setTimeout(() => test.fail('timeout — subscribe failed in 5s'), 5000);
      }, { soul: config.soul, relayPort: config.relayPort });
    }));
  });

  it('Alice puts data — each subscriber receives exactly once', function () {
    this.timeout(30000);

    // Each subscriber counts how many times they receive the put.
    const results = subscribers.map(function (client) {
      return client.run(function (test) {
        test.async();
        const WebSocket = require('ws');
        const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
        let receiveCount = 0;

        ws.on('message', (raw) => {
          const m = JSON.parse(raw.toString());
          if (m.put && m.put[test.props.soul] && m.put[test.props.soul].hello) {
            receiveCount++;
          }
        });

        ws.on('open', () => {
          // Re-subscribe (the previous test's WS closed)
          ws.send(JSON.stringify({
            get: { '#': test.props.soul },
            '#': 'resub' + Math.random().toString(36).slice(2),
          }));
        });

        ws.on('error', (err) => test.fail('WS error: ' + err.message));

        // Wait 5 seconds after Alice's put, then check count.
        setTimeout(() => {
          ws.close();
          if (receiveCount === 0) {
            test.fail('received 0 times — data never arrived');
          } else if (receiveCount > 1) {
            test.fail('received ' + receiveCount + ' times — DUPLICATE detected!');
          } else {
            test.done();
          }
        }, 8000);
      }, { soul: config.soul, relayPort: config.relayPort });
    });

    // Give subscribers 2s to connect + subscribe, then Alice puts.
    setTimeout(() => {
      alice.run(function (test) {
        test.async();
        const WebSocket = require('ws');
        const ws = new WebSocket('ws://localhost:' + test.props.relayPort);

        ws.on('open', () => {
          ws.send(JSON.stringify({
            '#': 'alicededup',
            put: {
              [test.props.soul]: {
                _: { '#': test.props.soul, '>': { hello: Date.now() } },
                hello: 'dedup-works',
              },
            },
          }));
        });

        ws.on('message', (raw) => {
          const m = JSON.parse(raw.toString());
          if (m['@'] === 'alicededup') {
            ws.close();
            test.done();
          }
        });

        ws.on('error', (err) => test.fail('WS error: ' + err.message));
        setTimeout(() => test.fail('timeout — no ack in 5s'), 5000);
      }, { soul: config.soul, relayPort: config.relayPort });
    }, 2000);

    return Promise.all(results);
  });

  after(function () {
    teardownPanic();
  });
});
