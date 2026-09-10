/**
 * PANIC Test 11: No Override (merge, not replace)
 *
 * Maps to Gun.js: test/panic/no-override.js
 *
 * Gun.js nodes merge at the FIELD level: writing field `x` on a node and
 * then writing field `y` on the SAME node must leave both fields present.
 * A Get of the node must return both — an implementation that replaces
 * the node object on each put would fail ("overwrote old object!").
 *
 * BEAM stores nodes as Children maps and apply_put merges per-child by
 * timestamp, so this should pass. This test pins the contract.
 *
 * Topology:
 *   [Alice] → [Relay]
 *   1. Put { x: 1 } on soul 'survey'
 *   2. Put { y: 1 } on the SAME soul 'survey'
 *   3. Get 'survey' → response must contain BOTH x and y.
 */

const path = require('path');
const { startRelay, stopAll } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayPort: 9100,
  soul: 'survey',
};

const { clients, alice } = setupPanic({ numClients: 1, panicPort: config.panicPort });

describe('11. No-override: field-level merge on same soul', function () {
  this.timeout(60000);

  it('One client connected to panic-server', function () {
    return clients.atLeast(1);
  });

  it('Relay starts', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Alice puts {x:1} then {y:1} on the same soul', function () {
    this.timeout(20000);
    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
      const acks = new Set();

      ws.on('open', () => {
        const now = Date.now();
        // First put: field x on soul 'survey'
        ws.send(JSON.stringify({
          '#': 'noput1',
          put: {
            survey: {
              _: { '#': 'survey', '>': { x: now } },
              x: 1,
            },
          },
        }));
        // Second put: field y on the SAME soul (separate frame, like a
        // real client doing two writes a moment apart)
        setTimeout(() => {
          ws.send(JSON.stringify({
            '#': 'noput2',
            put: {
              survey: {
                _: { '#': 'survey', '>': { y: now + 1 } },
                y: 1,
              },
            },
          }));
        }, 200);
      });

      ws.on('message', (raw) => {
        let msgs;
        try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
        if (!Array.isArray(msgs)) msgs = [msgs];
        for (const m of msgs) {
          if (m['@'] === 'noput1') acks.add('noput1');
          if (m['@'] === 'noput2') acks.add('noput2');
        }
        if (acks.size >= 2) {
          ws.close();
          test.done();
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout — got ' + acks.size + '/2 acks'), 15000);
    }, { relayPort: config.relayPort });
  });

  it('Get returns BOTH x and y (no override)', function () {
    this.timeout(15000);
    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
      let done = false;

      ws.on('open', () => {
        setTimeout(() => {
          ws.send(JSON.stringify({
            get: { '#': test.props.soul },
            '#': 'noget1' + Math.random().toString(36).slice(2),
          }));
        }, 300);
      });

      ws.on('message', (raw) => {
        let msgs;
        try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
        if (!Array.isArray(msgs)) msgs = [msgs];
        for (const m of msgs) {
          if (m.put && m.put.survey && !done) {
            const node = m.put.survey;
            const hasX = node.x !== undefined;
            const hasY = node.y !== undefined;
            if (hasX && hasY) {
              done = true;
              ws.close();
              test.done();
            } else {
              done = true;
              ws.close();
              test.fail('overwrote old object! got: ' + JSON.stringify(node));
            }
          }
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => { if (!done) test.fail('timeout — survey node never returned'); }, 10000);
    }, { relayPort: config.relayPort, soul: config.soul });
  });

  after(function () {
    teardownPanic();
  });
});
