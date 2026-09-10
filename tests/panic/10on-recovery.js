/**
 * PANIC Test 10: On-Recovery
 *
 * Maps to Gun.js: test/panic/on-recovery.js
 *
 * A client subscribed to a soul must continue receiving updates after the
 * relay peer it is connected to crashes and comes back online. In Gun.js
 * the browser's WebSocket transparently reconnects; with raw WebSocket
 * clients the reconnection is explicit, so the equivalent contract under
 * test is: after the relay restarts, a fresh connection + subscription
 * still receives subsequent puts (the "recovered" relay delivers).
 *
 * Topology:
 *   [Alice] → [Relay A]
 *   Phase 1: Alice puts hello=world. Client D (fresh connection) Gets and
 *            verifies it received the data (baseline sanity).
 *   Phase 2: Relay A is killed and restarted (same port, storage WIPED —
 *            memory storage does not persist).
 *   Phase 3: Alice puts foo=bar to the restarted relay.
 *   Phase 4: A fresh subscriber connects and receives foo=bar — the
 *            restarted relay accepts puts and serves Gets normally
 *            (recovery works end-to-end).
 */

const path = require('path');
const { startRelay, stopRelay, stopAll, waitForPortFree } = require('./helpers/relay');
const { setupPanic, teardownPanic } = require('./helpers/setup');

const binPath = path.resolve(__dirname, '../../target/debug/beam');

const config = {
  panicPort: 8765,
  relayPort: 9100,
};

const { clients, alice, bob } = setupPanic({ numClients: 2, panicPort: config.panicPort });

describe('10. On-recovery: relay accepts puts and serves gets after restart', function () {
  this.timeout(60000);

  it('Two clients connected to panic-server', function () {
    return clients.atLeast(2);
  });

  it('Relay A starts', function () {
    this.timeout(10000);
    startRelay({ binPath, port: config.relayPort });
    return new Promise((resolve) => setTimeout(resolve, 2000));
  });

  it('Alice puts hello=world', function () {
    this.timeout(15000);
    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);

      ws.on('open', () => {
        ws.send(JSON.stringify({
          '#': 'recput1',
          put: {
            g1: {
              _: { '#': 'g1', '>': { hello: Date.now() } },
              hello: 'world',
            },
          },
        }));
      });

      ws.on('message', (raw) => {
        let msgs;
        try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
        if (!Array.isArray(msgs)) msgs = [msgs];
        for (const m of msgs) {
          if (m['@'] === 'recput1') {
            ws.close();
            test.done();
          }
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout waiting for put ack'), 10000);
    }, { relayPort: config.relayPort });
  });

  it('Fresh subscriber Gets hello=world (baseline)', function () {
    this.timeout(15000);
    return bob.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
      let done = false;

      ws.on('open', () => {
        // Subscribe (Get) — this also registers us for future updates.
        ws.send(JSON.stringify({
          get: { '#': 'g1' },
          '#': 'recget1' + Math.random().toString(36).slice(2),
        }));
      });

      ws.on('message', (raw) => {
        let msgs;
        try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
        if (!Array.isArray(msgs)) msgs = [msgs];
        for (const m of msgs) {
          if (m.put && m.put.g1 && m.put.g1.hello === 'world' && !done) {
            done = true;
            ws.close();
            test.done();
          }
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => { if (!done) test.fail('timeout — hello=world never received'); }, 10000);
    }, { relayPort: config.relayPort });
  });

  it('Relay is killed and restarted (memory storage wiped)', function () {
    this.timeout(20000);
    stopRelay(config.relayPort);
    return waitForPortFree(config.relayPort).then(() => {
      startRelay({ binPath, port: config.relayPort });
      return new Promise((resolve) => setTimeout(resolve, 2000));
    });
  });

  it('Alice puts foo=bar to the restarted relay', function () {
    this.timeout(15000);
    return alice.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);

      ws.on('open', () => {
        ws.send(JSON.stringify({
          '#': 'recput2',
          put: {
            g2: {
              _: { '#': 'g2', '>': { foo: Date.now() } },
              foo: 'bar',
            },
          },
        }));
      });

      ws.on('message', (raw) => {
        let msgs;
        try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
        if (!Array.isArray(msgs)) msgs = [msgs];
        for (const m of msgs) {
          if (m['@'] === 'recput2') {
            ws.close();
            test.done();
          }
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => test.fail('timeout waiting for put ack on restarted relay'), 10000);
    }, { relayPort: config.relayPort });
  });

  it('Fresh subscriber receives foo=bar from recovered relay', function () {
    this.timeout(15000);
    return bob.run(function (test) {
      test.async();
      const WebSocket = require('ws');
      const ws = new WebSocket('ws://localhost:' + test.props.relayPort);
      let done = false;

      ws.on('open', () => {
        ws.send(JSON.stringify({
          get: { '#': 'g2' },
          '#': 'recget2' + Math.random().toString(36).slice(2),
        }));
      });

      ws.on('message', (raw) => {
        let msgs;
        try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
        if (!Array.isArray(msgs)) msgs = [msgs];
        for (const m of msgs) {
          if (m.put && m.put.g2 && m.put.g2.foo === 'bar' && !done) {
            done = true;
            ws.close();
            test.done();
          }
        }
      });

      ws.on('error', (err) => test.fail('WS error: ' + err.message));
      setTimeout(() => { if (!done) test.fail('timeout — foo=bar never received after recovery'); }, 10000);
    }, { relayPort: config.relayPort });
  });

  after(function () {
    teardownPanic();
  });
});
