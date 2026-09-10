/**
 * PANIC Test 12: Load
 *
 * Maps to Gun.js: test/panic/load.js
 *
 * N clients connect to one relay. Each client subscribes to a shared
 * table soul ('loadtest') and then writes `each` messages into it.
 * The test passes only when EVERY client has received EVERY client's
 * messages — N × each total records, verified by all N clients.
 *
 * This is the correctness-under-volume contract (Gun.js uses browsers
 * with map().on(); we use raw WS with parent-soul subscription, which
 * delivers every child put to topic subscribers).
 *
 * SEMANTICS (validated by standalone probe 2026-09-09): the relay
 * correctly does NOT echo a client's own puts back (Gun.js via-check)
 * and sends per-put acks. So each client seeds its OWN keys at put
 * time (local-write semantics) and counts foreign keys via fan-out.
 * Completion = every client has seen all N × each distinct keys.
 *
 * NOTE: Runs against the debug binary (suite standard, aabac39) — this
 * is a correctness-under-volume test, NOT a benchmark. Performance
 * numbers come from the release-binary bench suite.
 *
 * Topology:
 *   6 clients × 200 msgs = 1200 puts → [Relay]
 *   Every client subscribes to 'loadtest' and must verify all 1200.
 */

const path = require('path');
const { startRelay, stopAll } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayPort: 9100,
  numClients: 6,
  each: 200,
  soul: 'loadtest',
};

const { clients } = setupPanic({ numClients: config.numClients, panicPort: config.panicPort });

describe(`12. Load: ${config.numClients} clients × ${config.each} msgs, all verified by all`, function () {
  this.timeout(300000);

  it(`${config.numClients} clients connected to panic-server`, function () {
    return clients.atLeast(config.numClients);
  });

  it('Relay starts', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it(`All ${config.numClients} clients put ${config.each} msgs and receive all ${config.numClients * config.each}`, function () {
    this.timeout(240000);
    const ids = [];
    for (let i = 0; i < config.numClients; i++) ids.push('c' + i);
    const total = config.numClients * config.each;

    const tests = [];
    let idx = 0;
    // ALL clients put and verify (verbatim with 12load-gun.js — no
    // excluded observers; rest would drop alice+bob = only 4 writers).
    clients.each(function (client, id) {
      const myId = ids[idx++];
      tests.push(client.run(function (test) {
        test.async();
        const WebSocket = require('ws');
        const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
        const myId = test.props.myId;
        const each = test.props.each;
        const total = test.props.total;
        const soul = test.props.soul;

        // Distinct keys received. Own keys are seeded at put time
        // (local-write semantics: a real Gun client updates its local
        // graph on put(); the relay correctly does NOT echo own puts
        // back — Gun.js via-check). Foreign keys arrive via fan-out.
        const seen = new Set();
        let putAcked = 0;
        let finished = false;

        const finish = () => {
          if (finished) return;
          finished = true;
          ws.close();
          test.done();
        };

        const fail = (msg) => {
          if (finished) return;
          finished = true;
          try { ws.close(); } catch (e) {}
          test.fail(msg);
        };

        // Watchdog: progress must continue; absolute deadline enforced below.
        const start = Date.now();
        const check = setInterval(() => {
          if (seen.size >= total) {
            clearInterval(check);
            finish();
          } else if (Date.now() - start > 210000) {
            clearInterval(check);
            fail(`timeout — received ${seen.size}/${total} distinct keys`);
          }
        }, 200);

        ws.on('open', () => {
          // Subscribe to the shared table soul first: registers this
          // connection as a topic subscriber and replays current state.
          ws.send(JSON.stringify({
            get: { '#': soul },
            '#': 'ldsub' + myId,
          }));
          subscribed = true;

          // Put `each` messages as children of the shared soul.
          // Batch 10 puts per frame (Gun.js-style batching).
          // Seed own keys immediately (local-write semantics).
          const ts = Date.now();
          for (let batch = 0; batch < each; batch += 10) {
            const frame = [];
            for (let j = batch; j < Math.min(batch + 10, each); j++) {
              const key = myId + '_' + j;
              seen.add(key);
              frame.push({
                '#': 'ld' + myId + 'p' + j,
                put: {
                  [soul]: {
                    _: { '#': soul, '>': { [key]: ts + j } },
                    [key]: 'Hello world, ' + key + '!',
                  },
                },
              });
            }
            ws.send(JSON.stringify(frame));
          }
        });

        ws.on('message', (raw) => {
          let msgs;
          try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
          if (!Array.isArray(msgs)) msgs = [msgs];
          for (const m of msgs) {
            // Own put acks — liveness signal (acks carry @ = our msg id).
            if (m['@'] && m['@'].startsWith('ld' + myId + 'p')) {
              putAcked++;
            }
            // Fan-out deliveries — any put frame for our soul.
            if (m.put && m.put[soul]) {
              for (const k of Object.keys(m.put[soul])) {
                if (k !== '_') seen.add(k);
              }
            }
          }
        });

        ws.on('error', (err) => fail('WS error: ' + err.message));
        ws.on('close', () => { if (!finished) fail('WS closed early'); });
      }, { relayPort: config.relayPort, myId, each: config.each, total, soul: config.soul }));
    });

    return Promise.all(tests);
  });

  after(function () {
    teardownPanic();
  });
});
