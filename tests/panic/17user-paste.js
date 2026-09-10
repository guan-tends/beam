/**
 * PANIC Test 17: User Paste (vanishing property, headless)
 *
 * Maps to Gun.js: test/panic/user-paste.js
 *
 * Gun's user-paste.js exercises the shared-credentials use case:
 * Alice creates + auths a user; Bob logs into the SAME account from
 * a different client session; Alice pastes data into her user space;
 * Bob (as the same user, different session) reads it back.
 *
 * Headless port preserves every criterion via Node Gun+SEA clients
 * against the BEAM relay:
 *   1. create + auth (chained, Gun's own pattern)
 *   2. second client auths with the same credentials (session independence)
 *   3. put with chained .on() self-verification
 *   4. cross-session read of user-space data through the relay
 *
 * Topology:
 *   [Alice (Gun+SEA)] → [BEAM relay] ← [Bob (Gun+SEA, same creds)]
 */

const path = require('path');
const { startRelay } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayPort: 9100,
  // Unique alias per run — SEA alias registration on a persistent
  // relay must never collide across runs. Credentials pattern unchanged.
  runId: Date.now().toString(36),
};

const { clients, alice, bob } = setupPanic({ numClients: 2, panicPort: config.panicPort });

describe('17. User Paste: shared credentials across sessions (headless)', function () {
  this.timeout(300000);

  it('Two clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('BEAM relay started', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Alice creates user + auths (chained)', function () {
    return alice.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      const alias = 'pasteAlice' + test.props.runId;
      user.create(alias, 'password', function () {
        user.auth(alias, 'password', function (ack) {
          if (ack.err) { test.fail('bad login: ' + ack.err); return; }
          test.done();
        });
      });
      setTimeout(() => test.fail('timeout — create+auth in 90s'), 90000);
    }, { runId: config.runId, relayPort: config.relayPort });
  });

  it('Bob logs into the same user (session independence)', function () {
    return bob.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      const alias = 'pasteAlice' + test.props.runId;
      user.auth(alias, 'password', function (ack) {
        if (ack.err) { test.fail('bad login: ' + ack.err); return; }
        test.done();
      });
      setTimeout(() => test.fail('timeout — bob-as-alice auth in 60s'), 60000);
    }, { runId: config.runId, relayPort: config.relayPort });
  });

  it('Alice pastes (put + chained .on() self-verify)', function () {
    return alice.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      user.get('paste').put('hello world!').on(function (data) {
        if (this.stop) { return; }
        this.stop = 1; // Gun's own one-shot hack
        if ('hello world!' !== data) { test.fail('bad data: ' + JSON.stringify(data)); return; }
        setTimeout(function () { test.done(); }, 500);
      });
      setTimeout(() => test.fail('timeout — paste self-verify in 30s'), 30000);
    }, { relayPort: config.relayPort });
  });

  it('Bob reads (cross-session user-space read via once)', function () {
    return bob.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      user.once(function (data) {
        if (!data || 'hello world!' !== data.paste) {
          test.fail('bad data: ' + JSON.stringify(data));
          return;
        }
        test.done();
      });
      setTimeout(() => test.fail('timeout — cross-session read in 30s'), 30000);
    }, { relayPort: config.relayPort });
  });

  after(function () {
    teardownPanic();
  });
});
