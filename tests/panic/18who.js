/**
 * PANIC Test 18: Who — SEA sync with set() + re-auth sessions (headless)
 *
 * Maps to Gun.js: test/panic/who.js
 *
 * Gun's who.js proves SEA user-space sync across sessions:
 *   1. Alice creates + auths, writes a set() entry into
 *      user.get('who').get('said') — a dynamic list node.
 *   2. Bob discovers Alice through the ~@alias reverse index
 *      (gun.get('~@alias').map().once()) and subscribes to her
 *      user space, collecting set items.
 *   3. Alice's session reloads (browser page reload in Gun's test;
 *      re-auth in a fresh session here — the reload is browser
 *      mechanics, the criterion is a NEW session seeing + writing
 *      existing user data) and writes a second set entry.
 *   4. Bob's existing subscription receives the second entry —
 *      user-space set updates propagate across sessions via the relay.
 *
 * Headless port preserves every criterion via Node Gun+SEA clients
 * against the BEAM relay. New wire surface exercised: '~@alias'
 * reverse-index souls through BEAM's parser.
 *
 * Topology:
 *   [Alice (Gun+SEA)] → [BEAM relay] ← [Bob (Gun+SEA)]
 */

const path = require('path');
const { startRelay } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayPort: 9100,
  runId: Date.now().toString(36),
};

const { clients, alice, bob } = setupPanic({ numClients: 2, panicPort: config.panicPort });

describe('18. Who: SEA set() sync across sessions (headless)', function () {
  this.timeout(300000);

  it('Two clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('BEAM relay started', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Create Alice: auth + write first set entry', function () {
    return alice.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      const alias = 'whoAlice' + test.props.runId;
      user.create(alias, 'xyzabcmnopq', function (ack) {
        if (ack.err || !ack.pub) { test.fail('create failed: ' + (ack.err || 'no pub')); return; }
        globalThis.alicePub = ack.pub;
        user.auth(alias, 'xyzabcmnopq', function (ack) {
          if (ack.err || !ack.pub) { test.fail('auth failed: ' + (ack.err || 'no pub')); return; }
          user.get('who').get('said').set({
            what: 'Hello world!',
          }, function (ack) {
            if (ack.err) { test.fail('set failed: ' + ack.err); return; }
            test.done();
          });
        });
      });
      setTimeout(() => test.fail('timeout — create+auth+set in 90s'), 90000);
    }, { runId: config.runId, relayPort: config.relayPort });
  });

  it('Create Bob: auth only', function () {
    return bob.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      const alias = 'whoBob' + test.props.runId;
      user.create(alias, 'zyxcbaqponm', function (ack) {
        if (ack.err || !ack.pub) { test.fail('create failed: ' + (ack.err || 'no pub')); return; }
        user.auth(alias, 'zyxcbaqponm', function (ack) {
          if (ack.err || !ack.pub) { test.fail('auth failed: ' + (ack.err || 'no pub')); return; }
          test.done();
        });
      });
      setTimeout(() => test.fail('timeout — create+auth bob in 90s'), 90000);
    }, { runId: config.runId, relayPort: config.relayPort });
  });

  it('Bob finds Alice via ~@alias reverse index', function () {
    return bob.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      const alias = 'whoAlice' + test.props.runId;
      gun.get('~@' + alias).map().once(function (data) {
        if (!data || !data.pub) { return; }
        globalThis.ref = gun.get('~' + data.pub);
        test.done();
      });
      setTimeout(() => test.fail('timeout — ~@alias discovery in 45s'), 45000);
    }, { runId: config.runId, relayPort: config.relayPort });
  });

  it('Bob listens: collects first set item', function () {
    return bob.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      globalThis.count = [];
      globalThis.ref.get('who').get('said').map().once(function (data) {
        if (!data) { return; }
        globalThis.count.push(data);
        if (globalThis.count.length - 1) { return; }
        test.done();
      });
      setTimeout(() => test.fail('timeout — first set item in 30s'), 30000);
    }, { relayPort: config.relayPort });
  });

  it('Alice re-auths (new session) + writes second set entry', function () {
    return alice.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      const alias = 'whoAlice' + test.props.runId;
      // Fresh session: re-auth (Gun's browser test reloads the page;
      // the criterion is a new session recovering the user + writing).
      user.auth(alias, 'xyzabcmnopq', function (ack) {
        if (ack.err || !ack.pub) { test.fail('re-auth failed: ' + (ack.err || 'no pub')); return; }
        user.get('who').get('said').set({
          what: 'AAA',
        }, function (ack) {
          if (ack.err) { test.fail('set failed: ' + ack.err); return; }
          test.done();
        });
      });
      setTimeout(() => test.fail('timeout — re-auth + second set in 60s'), 60000);
    }, { runId: config.runId, relayPort: config.relayPort });
  });

  it("Bob's existing subscription receives the second entry (AAA)", function () {
    return bob.run(async function (test) {
      test.async();
      const start = Date.now();
      const check = setInterval(() => {
        const items = globalThis.count || [];
        if (items.length >= 2 && items[1] && items[1].what === 'AAA') {
          clearInterval(check);
          test.done();
        } else if (Date.now() - start > 30000) {
          clearInterval(check);
          test.fail('timeout — collected ' + JSON.stringify(items));
        }
      }, 200);
    });
  });

  after(function () {
    teardownPanic();
  });
});
