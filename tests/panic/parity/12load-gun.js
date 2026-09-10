/**
 * PANIC Test 12g: Load — Gun.js parity arm (verbatim criteria)
 *
 * Project directive: "the same tests that work for gun must work for
 * beam, verbatim." This file runs the EXACT 12load.js criteria against
 * a real Gun.js relay with real Gun.js clients:
 *
 *   6 clients × 200 puts = 1200 messages on shared table 'loadtest'.
 *   Every client must receive/verify ALL 1200 keys.
 *
 * If Gun passes this, BEAM must pass the identical 12load.js. If BEAM
 * fails where Gun passes → real divergence → fix BEAM (never the test).
 *
 * Differences from 12load.js are transport-only:
 * - Relay: a PANIC node client runs Gun({web: http, rad: false,
 *   localStorage: false}) on :9100 (Gun's ws endpoint is /gun).
 * - Clients: real Gun instances (Gun({peers})) instead of raw WS.
 *   Gun clients write locally on put(), so self-receipt is natural
 *   (map().on fires for own puts) — matching 12load.js's self-seed.
 * - Gun loads via ESM dynamic import (CJS require crashes on Node 22;
 *   ESM is the proven wire-live path).
 */

const { setupPanic, teardownPanic } = require('./helpers/setup');

const config = {
  panicPort: 8765,
  relayPort: 9100,
  numClients: 6,
  each: 200,
  soul: 'loadtest',
};

const { clients } = setupPanic({ numClients: config.numClients, panicPort: config.panicPort });

describe(`12g. Load (GUN parity): ${config.numClients} clients × ${config.each} msgs, all verified by all`, function () {
  this.timeout(300000);

  it(`${config.numClients} clients connected to panic-server`, function () {
    return clients.atLeast(config.numClients);
  });

  it('Gun relay starts', function () {
    this.timeout(15000);
    return new Promise((resolve, reject) => {
      clients.atLeast(1).then(() => {
        const relay = clients.pluck(1);
        relay.run(async function (test) {
          test.async();
          try {
            const Gun = (await import('gun')).default;
            const fs = require('fs');
            // Verbatim with Gun's load.js relay: file-backed storage,
            // cleaned per run (their unlinkSync pattern).
            try { fs.rmSync('12g-data', { recursive: true, force: true }); } catch (e) {}
            const http = require('http');
            const server = http.createServer(function (req, res) {
              res.end('gun relay');
            });
            Gun({ file: '12g-data', web: server, localStorage: false });
            server.listen(test.props.relayPort, function () {
              test.done();
            });
          } catch (e) {
            test.fail('relay start: ' + e.message);
          }
        }, { relayPort: config.relayPort }).catch(reject);
        setTimeout(resolve, 3000);
      });
    });
  });

  it(`All ${config.numClients} clients put ${config.each} msgs and receive all ${config.numClients * config.each}`, function () {
    this.timeout(240000);
    const ids = [];
    for (let i = 0; i < config.numClients; i++) ids.push('c' + i);
    const total = config.numClients * config.each;

    const tests = [];
    let idx = 0;
    clients.each(function (client, id) {
      const myId = ids[idx++];
      tests.push(client.run(function (test) {
        test.async();
        const Gun = (async () => {
          const G = (await import('gun')).default;
          return G;
        })();

        Gun.then(async (G) => {
          const gun = G({
            peers: ['http://127.0.0.1:' + test.props.relayPort + '/gun'],
            localStorage: false,
          });

          const myId = test.props.myId;
          const each = test.props.each;
          const total = test.props.total;
          const soul = test.props.soul;

          const seen = new Set();
          let finished = false;

          const finish = () => {
            if (finished) return;
            finished = true;
            test.done();
          };
          const fail = (msg) => {
            if (finished) return;
            finished = true;
            test.fail(msg);
          };

          // Subscribe to the shared table BEFORE any puts (map over
          // children) — verbatim with Gun's load.js. Own puts fire the
          // callback locally (real client writes locally on put), so
          // all 1200 keys arrive through this one channel.
          gun.get(soul).map().on(function (data, key) {
            seen.add(key);
            if (seen.size >= total) finish();
          });

          // Deadline watchdog.
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

          // Put loop verbatim with Gun's load.js: setInterval drip,
          // one put per tick (wait: 1 → 1ms), clearTimeout at each.
          var i = 0;
          var to = setInterval(function go() {
            if (each <= i) { clearTimeout(to); return; }
            i += 1;
            var p = myId + '_' + i;
            gun.get(soul).get(p).put('Hello world, ' + p + '!');
          }, 1);
        }).catch((e) => test.fail('gun init: ' + e.message));
      }, { relayPort: config.relayPort, myId, each: config.each, total, soul: config.soul }));
    });

    return Promise.all(tests);
  });

  after(function () {
    teardownPanic();
  });
});
