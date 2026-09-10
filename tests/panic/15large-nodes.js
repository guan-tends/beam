/**
 * PANIC Test 15: Large Nodes
 *
 * Maps to Gun.js: test/panic/large-nodes.js
 *
 * Gun.js's large-nodes.js has Alice put a SINGLE node containing
 * 25,000 fields in one put() call, then verifies retrieval. The
 * Get half of Gun's file is disabled (`return;`) — the active
 * criteria are: one massive node, put ack, retrieval works.
 *
 * The BEAM port preserves the shape exactly: one put wire message
 * whose node carries 25,000 random-keyed fields (Gun uses
 * Gun.text.random(9) keys; we use the same 9-char alphanumeric
 * charset). Bob then Gets the node and verifies the field count
 * plus spot-checks values — stressing single-node wire size,
 * storage, and fan-out.
 *
 * Topology:
 *   [Alice] → [BEAM relay] ← [Bob]
 */

const path = require('path');
const { startRelay } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayPort: 9100,
  rootSoul: 'panic/largenode/' + Date.now(),
  fieldCount: 25000, // Gun.js's exact default (test.props.each || 25000)
};

// Same charset as Gun.text.random: alphanumeric, 9 chars.
const GUN_RANDOM_CHARS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXZabcdefghijklmnopqrstuvwxyz';
function gunRandom9(seed) {
  // Deterministic 9-char keys from an index so Bob can spot-check
  // the same keys Alice wrote (Gun's test uses random keys but only
  // counts acks; determinism lets us verify values, a strict superset
  // of Gun's criteria).
  let h = seed * 2654435761 % 4294967296;
  let s = '';
  for (let j = 0; j < 9; j++) {
    h = (h * 1103515245 + 12345) % 4294967296;
    s += GUN_RANDOM_CHARS[h % GUN_RANDOM_CHARS.length];
  }
  return s;
}

// Build the field map ONCE in the parent so both clients verify
// against the same key/value set. Passed to clients via props.
const fields = {};
for (let i = 1; i <= config.fieldCount; i++) {
  fields[gunRandom9(i)] = i;
}
const spotKeys = Object.keys(fields).filter((_, idx) => idx % 5000 === 0);

const { clients, alice, bob } = setupPanic({ numClients: 2, panicPort: config.panicPort });

describe('15. Large Nodes: single ' + config.fieldCount + '-field node, ack + retrieval', function () {
  this.timeout(240000);

  it('Two clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('BEAM relay started', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Alice puts a ' + config.fieldCount + '-field node in ONE put; ack received', function () {
    this.timeout(180000);

    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
      const nodeChildren = {};
      for (const [k, v] of Object.entries(test.props.fields)) {
        nodeChildren[k] = v;
      }

      ws.on('open', () => {
        const ts = Date.now();
        const stateMap = {};
        for (const k of Object.keys(nodeChildren)) {
          stateMap[k] = ts + 1;
        }
        // ONE wire message, ONE node, 25K fields — Gun's shape.
        ws.send(JSON.stringify({
          '#': 'bignode',
          put: {
            [test.props.rootSoul]: {
              _: { '#': test.props.rootSoul, '>': stateMap },
              ...nodeChildren,
            },
          },
        }));
      });

      ws.on('message', (raw) => {
        let msgs;
        try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
        if (!Array.isArray(msgs)) msgs = [msgs];

        for (const m of msgs) {
          if (m['@'] === 'bignode') {
            setTimeout(() => { ws.close(); test.done(); }, 500);
            return;
          }
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout — no ack for 25K-field node in 150s'), 150000);
    }, {
      rootSoul: config.rootSoul,
      fields,
      relayPort: config.relayPort,
    });
  });

  it('Bob Gets the node: all ' + config.fieldCount + ' fields + spot values verified', function () {
    this.timeout(60000);

    return bob.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
      const fields = test.props.fields;
      const expected = Object.keys(fields).length;
      let fieldCount = 0;
      let spotChecked = 0;

      ws.on('message', (raw) => {
        let msgs;
        try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
        if (!Array.isArray(msgs)) msgs = [msgs];

        for (const m of msgs) {
          const node = m.put && m.put[test.props.rootSoul];
          if (node) {
            for (const key of Object.keys(node)) {
              if (key === '_') continue;
              fieldCount++;
              if (key in fields) {
                if (node[key] !== fields[key]) {
                  test.fail('value mismatch at ' + key + ': got ' + node[key] + ', want ' + fields[key]);
                  return;
                }
                spotChecked++;
              }
            }
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
        if (fieldCount >= expected) {
          clearInterval(check);
          ws.close();
          test.done();
        } else if (Date.now() - start > 45000) {
          clearInterval(check);
          ws.close();
          test.fail('timeout — received ' + fieldCount + '/' + expected + ' fields in 45s');
        }
      }, 100);
    }, {
      rootSoul: config.rootSoul,
      fields,
      relayPort: config.relayPort,
    });
  });

  after(function () {
    teardownPanic();
  });
});
