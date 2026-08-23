/**
 * PANIC Test 3: Partition and Recovery
 *
 * Two BEAM relays start independently (partitioned).
 * Alice puts to relay A, Bob puts to relay B.
 * Then cross-Get: Bob gets Alice's data from relay A, Alice gets Bob's from relay B.
 *
 * Tests that data persists on each relay and can be retrieved cross-relay.
 * (Full partition-heal with auto-convergence requires connecting relays as peers,
 *  which is a future test. This verifies data is available cross-relay.)
 */

const path = require('path');
const fs = require('fs');
const panic = require('panic-server');
const manager = require('panic-manager')();
const { startRelay, stopAll } = require('./helpers/relay');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  ip: 'localhost',
  panicPort: 8765,
  relayAPort: 9100,
  relayBPort: 9200,
  soulAlice: 'panic/partition/alice/' + Date.now(),
  soulBob: 'panic/partition/bob/' + Date.now(),
};

const routes = {
  '/': __dirname + '/helpers/index.html',
  '/panic.js': require.resolve('panic-client'),
};

const srv = panic.server();

srv.on('request', (req, res) => {
  const filePath = routes[req.url];
  if (filePath && !res.headersSent) {
    fs.createReadStream(filePath).pipe(res);
  }
});

srv.listen(config.panicPort, () => {
  console.log(`[panic] server on :${config.panicPort}`);
});

manager.start({
  clients: [
    { type: 'node', port: config.panicPort + 1 },
    { type: 'node', port: config.panicPort + 2 },
  ],
  panic: `http://${config.ip}:${config.panicPort}`,
});

const clients = panic.clients;
const alice = clients.pluck(1);
const bob = clients.excluding(alice).pluck(1);

describe('3. Partition-Recovery: data persists per relay, cross-Get works', function () {
  this.timeout(60000);

  it('Two clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('Relay A and Relay B start (independent, no peers)', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayAPort });
    startRelay({ binPath, port: config.relayBPort });
    return new Promise((resolve) => setTimeout(resolve, 3000));
  });

  it('Alice puts to Relay A', function () {
    this.timeout(15000);
    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayAPort);

      ws.on('open', () => {
        ws.send(JSON.stringify({
          '#': 'palice',
          put: {
            [test.props.soulAlice]: {
              _: { '#': test.props.soulAlice, '>': { data: Date.now() } },
              data: 'alice-was-here',
            },
          },
        }));
      });

      ws.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        if (msg['@'] === 'palice') {
          ws.close();
          test.done();
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout — no ack in 5s'), 5000);
    }, {
      soulAlice: config.soulAlice,
      relayAPort: config.relayAPort,
    });
  });

  it('Bob puts to Relay B', function () {
    this.timeout(15000);
    return bob.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayBPort);

      ws.on('open', () => {
        ws.send(JSON.stringify({
          '#': 'pbob',
          put: {
            [test.props.soulBob]: {
              _: { '#': test.props.soulBob, '>': { data: Date.now() } },
              data: 'bob-was-here',
            },
          },
        }));
      });

      ws.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        if (msg['@'] === 'pbob') {
          ws.close();
          test.done();
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout — no ack in 5s'), 5000);
    }, {
      soulBob: config.soulBob,
      relayBPort: config.relayBPort,
    });
  });

  it('Bob can Get Alice data from Relay A', function () {
    this.timeout(15000);
    return bob.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayAPort);

      let gotData = null;

      ws.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        if (msg.put && msg.put[test.props.soulAlice] && msg.put[test.props.soulAlice].data) {
          gotData = msg.put[test.props.soulAlice].data;
        }
      });

      ws.on('open', () => {
        setTimeout(() => {
          ws.send(JSON.stringify({
            get: { '#': test.props.soulAlice },
            '#': 'checkalice' + Math.random().toString(36).slice(2),
          }));
        }, 500);
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));

      const start = Date.now();
      const check = setInterval(() => {
        if (gotData === 'alice-was-here') {
          clearInterval(check);
          ws.close();
          test.done();
        } else if (Date.now() - start > 5000) {
          clearInterval(check);
          test.fail('timeout — no data in 5s');
        }
      }, 100);
    }, {
      soulAlice: config.soulAlice,
      relayAPort: config.relayAPort,
    });
  });

  it('Alice can Get Bob data from Relay B', function () {
    this.timeout(15000);
    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayBPort);

      let gotData = null;

      ws.on('message', (raw) => {
        const msg = JSON.parse(raw.toString());
        if (msg.put && msg.put[test.props.soulBob] && msg.put[test.props.soulBob].data) {
          gotData = msg.put[test.props.soulBob].data;
        }
      });

      ws.on('open', () => {
        setTimeout(() => {
          ws.send(JSON.stringify({
            get: { '#': test.props.soulBob },
            '#': 'checkbob' + Math.random().toString(36).slice(2),
          }));
        }, 500);
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));

      const start = Date.now();
      const check = setInterval(() => {
        if (gotData === 'bob-was-here') {
          clearInterval(check);
          ws.close();
          test.done();
        } else if (Date.now() - start > 5000) {
          clearInterval(check);
          test.fail('timeout — no data in 5s');
        }
      }, 100);
    }, {
      soulBob: config.soulBob,
      relayBPort: config.relayBPort,
    });
  });

  after(function () {
    stopAll();
  });
});
