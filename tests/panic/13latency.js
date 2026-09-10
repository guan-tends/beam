/**
 * PANIC Test 13: Latency (read under write flood)
 *
 * Maps to Gun.js: test/panic/latency.js
 *
 * One client floods the relay with random puts (bursts, sustained).
 * A reader joins DURING the flood, subscribes to a known soul, and
 * measures how long it takes to receive the target value. The flood
 * must not starve reads: data must arrive within a bounded time even
 * while the relay is being hammered with writes.
 *
 * Gun.js measures time-to-first-data on a key that keeps being re-put
 * during the flood; we do the same — the flooder re-puts the target
 * every second amid random bursts, the reader connects mid-flood, and
 * the assertion is: target received, latency under bound. The measured
 * latency is reported in the failure message for visibility.
 *
 * NOTE: debug binary (suite standard). This is a correctness/latency
 * bound under load, not a benchmark number.
 */

const path = require('path');
const { startRelay, stopRelay, stopAll } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayPort: 9100,
  soul: 'lattest',
  target: 'target',
  floodSeconds: 15,
  latencyBoundMs: 10000,
};

const { clients, alice, bob } = setupPanic({ numClients: 2, panicPort: config.panicPort });

describe('13. Latency: reader receives target during sustained write flood', function () {
  this.timeout(120000);

  it('Two clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('Relay starts', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Flooder floods random puts and re-puts target for ' + config.floodSeconds + 's', function () {
    this.timeout(config.floodSeconds * 1000 + 20000);
    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
      const soul = test.props.soul;
      const target = test.props.target;
      const floodSeconds = test.props.floodSeconds;

      ws.on('open', () => {
        // Subscribe so the relay treats us as a peer (and so our puts
        // fan out to any other subscriber, e.g. the late-joining reader).
        ws.send(JSON.stringify({ get: { '#': soul }, '#': 'latfsub' }));

        const rand = (n) => Math.floor(Math.random() * n);
        const putOne = (key, val, id) => {
          ws.send(JSON.stringify({
            '#': id,
            put: {
              [soul]: {
                _: { '#': soul, '>': { [key]: Date.now() } },
                [key]: val,
              },
            },
          }));
        };

        // Target put immediately, then every second amid flood bursts.
        putOne(target, 'hello world', 'lattgt0');
        let t = 1;
        const tgt = setInterval(() => putOne(target, 'hello world', 'lattgt' + t++), 1000);

        // Flood: burst of random puts every 50ms.
        const flood = setInterval(() => {
          for (let b = 0; b < 10; b++) {
            putOne(
              'r' + rand(1e6).toString(36) + rand(100).toString(36),
              rand(1e6).toString(36),
              'latf' + rand(1e9).toString(36)
            );
          }
        }, 50);

        setTimeout(() => {
          clearInterval(tgt);
          clearInterval(flood);
          ws.close();
          test.done();
        }, floodSeconds * 1000);
      });

      ws.on('error', () => { /* flood client errors are non-fatal */ });
    }, { relayPort: config.relayPort, soul: config.soul, target: config.target, floodSeconds: config.floodSeconds });
  });

  it('Reader joins mid-flood and receives target within bound', function () {
    this.timeout(60000);
    return bob.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      // Small delay so the reader genuinely joins after the flood is underway.
      setTimeout(() => {
        const t0 = Date.now();
        const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
        const soul = test.props.soul;
        const target = test.props.target;
        const bound = test.props.bound;
        let done = false;

        ws.on('open', () => {
          ws.send(JSON.stringify({
            get: { '#': soul },
            '#': 'latrsub' + Math.random().toString(36).slice(2),
          }));
        });

        ws.on('message', (raw) => {
          let msgs;
          try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
          if (!Array.isArray(msgs)) msgs = [msgs];
          for (const m of msgs) {
            if (m.put && m.put[soul] && m.put[soul][target] !== undefined && !done) {
              done = true;
              const latency = Date.now() - t0;
              ws.close();
              if (latency <= bound) {
                test.done();
              } else {
                test.fail(`latency ${latency}ms > bound ${bound}ms`);
              }
            }
          }
        });

        ws.on('error', (err) => { if (!done) { done = true; test.fail('WS error: ' + err.message); } });
        setTimeout(() => {
          if (!done) {
            done = true;
            ws.close();
            test.fail('timeout — target never received within ' + bound + 'ms');
          }
        }, bound + 2000);
      }, 2000);
    }, { relayPort: config.relayPort, soul: config.soul, target: config.target, bound: config.latencyBoundMs });
  });

  after(function () {
    teardownPanic();
  });
});
