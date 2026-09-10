/**
 * Standalone load-probe — isolates relay fan-out from PANIC/mocha.
 *
 * Replicates test 12's exact wire pattern with raw WebSockets:
 * - N clients subscribe to soul 'loadtest' (Get on parent)
 * - Each client puts `each` messages as children (batched 10/frame)
 * - Every client counts distinct keys received
 *
 * Exits with a summary. No PANIC, no mocha, no test framework.
 * Usage: node standalone-load-probe.js [numClients] [each]
 */

const N = parseInt(process.argv[2] || '3', 10);
const EACH = parseInt(process.argv[3] || '50', 10);
const RELAY = 'ws://127.0.0.1:9100';
const SOUL = 'loadtest';

const { spawn } = require('child_process');

// ── start relay ──
const beam = spawn('/home/guan/src/beam/target/debug/beam',
  ['start', '--port', '9100', '--memory-storage', 'true', '--redb-storage', 'false'],
  { stdio: ['ignore', 'ignore', 'ignore'] });

setTimeout(() => run(), 2000);

function run() {
  const clients = [];
  const results = [];

  for (let i = 0; i < N; i++) {
    const myId = 'p' + i;
    const seen = new Set();
    let acked = 0;

    const ws = new (require('ws'))(RELAY);
    clients.push(ws);

    ws.on('open', () => {
      // Subscribe to the shared table soul.
      ws.send(JSON.stringify({ get: { '#': SOUL }, '#': 'probe-sub-' + myId }));

      // Put EACH messages, batched 10 per frame.
      const ts = Date.now();
      for (let batch = 0; batch < EACH; batch += 10) {
        const frame = [];
        for (let j = batch; j < Math.min(batch + 10, EACH); j++) {
          const key = myId + '_' + j;
          frame.push({
            '#': 'probe-' + myId + '-' + j,
            put: {
              [SOUL]: {
                _: { '#': SOUL, '>': { [key]: ts + j } },
                [key]: 'Hello world, ' + key + '!',
              },
            },
          });
        }
        ws.send(JSON.stringify(frame));
      }
    });

    const originCount = {}; // sender prefix → count of distinct keys
    const sampleAcks = [];
    let rawFrames = 0;

    ws.on('message', (raw) => {
      rawFrames++;
      let msgs;
      try { msgs = JSON.parse(raw.toString()); } catch (e) { return; }
      if (!Array.isArray(msgs)) msgs = [msgs];
      for (const m of msgs) {
        if (m['@'] && sampleAcks.length < 3) {
          sampleAcks.push(JSON.stringify(m).slice(0, 200));
        }
        if (m['@'] && m['@'].startsWith('probe-' + myId + '-')) acked++;
        if (m.put && m.put[SOUL]) {
          for (const k of Object.keys(m.put[SOUL])) {
            if (k === '_') continue;
            const isNew = !seen.has(k);
            if (isNew) {
              seen.add(k);
              const origin = k.split('_')[0];
              originCount[origin] = (originCount[origin] || 0) + 1;
            }
          }
        }
      }
    });

    results.push({ myId, seen, acked, ws, originCount, sampleAcks, get rawFrames() { return rawFrames; } });

    ws.on('error', (e) => console.error(`[${myId}] WS error:`, e.message));
    results.push({ myId, seen, acked, ws });
  }

  const total = N * EACH;
  const t0 = Date.now();

  const iv = setInterval(() => {
    const line = results
      .map(r => `${r.myId}: seen=${r.seen.size}/${total} acked=${r.acked}/${EACH} origins=${JSON.stringify(r.originCount)}`)
      .join('  |  ');
    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    console.log(`[${elapsed}s] ${line}`);

    const allDone = results.every(r => r.seen.size >= total);
    if (allDone) {
      clearInterval(iv);
      console.log(`\n✅ ALL CLIENTS RECEIVED ALL ${total} KEYS in ${elapsed}s`);
      console.log('Sample acks p0:', JSON.stringify(results[0].sampleAcks, null, 1));
      cleanup();
    } else if (Date.now() - t0 > 30000) {
      clearInterval(iv);
      console.log(`\n❌ TIMEOUT — final state: ${line}`);
      console.log('Sample acks per client:', results.map(r => `${r.myId}:${JSON.stringify(r.sampleAcks)}`).join('\n'));
      cleanup();
    }
  }, 2000);

  function cleanup() {
    results.forEach(r => { try { r.ws.close(); } catch (e) {} });
    setTimeout(() => { beam.kill('SIGTERM'); setTimeout(() => process.exit(0), 500); }, 500);
  }
}
