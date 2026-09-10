/**
 * PANIC Test 16: SEA User Accounts (headless)
 *
 * Maps to Gun.js: test/panic/users.js
 *
 * Gun's users.js exercises the full SEA user lifecycle through two
 * browser clients: create → auth → user-space put → self-subscribe →
 * cross-user discovery (alias → pub) → cross-user write attempt must
 * not corrupt the target's space.
 *
 * The headless port preserves every criterion, swapping Gun's browser
 * clients for Node.js panic-clients running the same Gun.js user
 * library (SEA works identically in Node via ESM). The server under
 * test is the BEAM relay — Gun's signed ~pubkey souls, alias/pub
 * indirection, and user-space subscription semantics must all survive
 * BEAM's wire parser, storage, and fan-out.
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
  // Unique per run so SEA alias registration never collides with a
  // previous relay's stored graph.
  runId: Date.now().toString(36),
};

const { clients, alice, bob } = setupPanic({ numClients: 2, panicPort: config.panicPort });

/**
 * One-line client bootstrap used at the top of every Gun step.
 *
 * Delegates to helpers/gunboot.js (dynamic import() inside a real
 * async helper — the only import form that works in PANIC's eval'd
 * callbacks, after the eval-of-string approach failed with
 * "Unexpected token 'import'"). Idempotent per client process.
 */
const BOOT = 'if (!globalThis.user) { await require("./helpers/gunboot").bootGun(test.props.relayPort); }';

describe('16. SEA User Accounts: create/auth/save/subscribe against BEAM (headless)', function () {
  this.timeout(300000);

  it('Two clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('BEAM relay started', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Create Alice', function () {
    return alice.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      const alias = 'panicAlice' + test.props.runId;
      user.create(alias, 'xyzabcmnopq', function (ack) {
        if (ack.err || !ack.pub) { test.fail('create failed: ' + (ack.err || 'no pub')); return; }
        globalThis.alicePub = ack.pub;
        test.done();
      });
      setTimeout(() => test.fail('timeout — create Alice in 60s'), 60000);
    }, { runId: config.runId, relayPort: config.relayPort });
  });

  it('Create Bob', function () {
    return bob.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      const alias = 'panicBob' + test.props.runId;
      user.create(alias, 'zyxcbaqponm', function (ack) {
        if (ack.err || !ack.pub) { test.fail('create failed: ' + (ack.err || 'no pub')); return; }
        globalThis.bobPub = ack.pub;
        test.done();
      });
      setTimeout(() => test.fail('timeout — create Bob in 60s'), 60000);
    }, { runId: config.runId, relayPort: config.relayPort });
  });

  it('Auth Alice', function () {
    return alice.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      const alias = 'panicAlice' + test.props.runId;
      user.auth(alias, 'xyzabcmnopq', function (ack) {
        if (ack.err || !ack.pub) { test.fail('auth failed: ' + (ack.err || 'no pub')); return; }
        test.done();
      });
      setTimeout(() => test.fail('timeout — auth Alice in 60s'), 60000);
    }, { runId: config.runId, relayPort: config.relayPort });
  });

  it('Auth Bob', function () {
    return bob.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      const alias = 'panicBob' + test.props.runId;
      user.auth(alias, 'zyxcbaqponm', function (ack) {
        if (ack.err || !ack.pub) { test.fail('auth failed: ' + (ack.err || 'no pub')); return; }
        test.done();
      });
      setTimeout(() => test.fail('timeout — auth Bob in 60s'), 60000);
    }, { runId: config.runId, relayPort: config.relayPort });
  });

  it('Alice: self-subscribe + user-space put (hello=world lands via BEAM)', function () {
    return alice.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      user.on(function (aliceData) {
        if (aliceData && aliceData.hello === 'world') {
          test.done();
        }
      });
      setTimeout(function () {
        user.get('hello').put('world');
      }, 500);
      setTimeout(() => test.fail('timeout — self put/subscribe in 30s'), 30000);
    }, { relayPort: config.relayPort });
  });

  it('Alice subscribes to Bob via alias discovery; Bob puts mars', function () {
    // Alice: discover Bob's pub through the alias index, subscribe to
    // his public user space. Bob: user-space put hello=mars.
    const aliceP = alice.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      gun.get('alias/panicBob' + test.props.runId).map().on(function (data) {
        if (data && data.pub) {
          globalThis.discoveredBobPub = data.pub;
        }
      });
      gun.get('pub/' + globalThis.bobPub).get('hello').on(function (value) {
        if (value === 'mars') {
          test.done();
        }
      });
      setTimeout(() => test.fail('timeout — never saw Bob mars in 45s'), 45000);
    }, { runId: config.runId, relayPort: config.relayPort });

    return aliceP.then(() => {
      return bob.run(async function (test) {
        test.async();
        /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
        user.on(function (bobData) {
          if (bobData && bobData.hello === 'mars') {
            test.done();
          }
        });
        setTimeout(function () {
          user.get('hello').put('mars');
        }, 500);
        setTimeout(() => test.fail('timeout — bob put/subscribe in 30s'), 30000);
      }, { relayPort: config.relayPort });
    });
  });

  it('Alice received Bob mars through BEAM', function () {
    return alice.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      const start = Date.now();
      const check = setInterval(() => {
        if (globalThis.discoveredBobPub === globalThis.bobPub) {
          clearInterval(check);
          test.done();
        } else if (Date.now() - start > 20000) {
          clearInterval(check);
          test.fail('alias discovery incomplete: discovered=' + globalThis.discoveredBobPub);
        }
      }, 200);
    }, { relayPort: config.relayPort });
  });

  it('Alice tries to crack Bob; Bob space stays intact', function () {
    const crackP = alice.run(async function (test) {
      test.async();
      /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
      gun.get('pub/' + globalThis.bobPub).get('crackers').put('gonna crack');
      setTimeout(() => test.done(), 1500);
    }, { relayPort: config.relayPort });

    return crackP.then(() => {
      return alice.run(async function (test) {
        test.async();
        /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
        gun.get('pub/' + globalThis.bobPub).val(function (data) {
          if (data.pub === globalThis.bobPub
            && data.hello === 'mars'
            && data.crackers !== 'gonna crack'
            && undefined === data.crackers) {
            test.done();
          }
        });
        setTimeout(() => test.fail('timeout — pub-space read in 30s'), 30000);
      }, { relayPort: config.relayPort });
    }).then(() => {
      return bob.run(async function (test) {
        test.async();
        /*BOOT*/ if (!globalThis.user) { await require('./helpers/gunboot').bootGun(test.props.relayPort); }
        user.val(function (data) {
          if (data.hello === 'mars'
            && data.crackers !== 'gonna crack'
            && undefined === data.crackers) {
            test.done();
          }
        });
        setTimeout(() => test.fail('timeout — bob user-space read in 30s'), 30000);
      }, { relayPort: config.relayPort });
    });
  });

  after(function () {
    teardownPanic();
  });
});
