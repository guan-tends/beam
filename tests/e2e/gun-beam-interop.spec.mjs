import { test, expect } from '@playwright/test';
import { execSync } from "child_process";
import { spawn } from "child_process";
import { writeFileSync } from 'fs';

const RELAY_URL = 'ws://127.0.0.1:4944';
const BASE_URL = 'http://127.0.0.1:8080';

const GUN_PAGE = `<!DOCTYPE html><html><head><meta charset="utf-8"></head><body>
<div id="status">idle</div><div id="data">{}</div>
<script src="gun.js"></script>
<script>
window.gunReady = false; window.receivedData = {};
const gun = Gun('${RELAY_URL}');
function subscribe(soul) {
  gun.get(soul).map().on((value, key) => {
    if (typeof value === 'string') {
      window.receivedData[key] = value;
      document.getElementById('data').textContent = JSON.stringify(window.receivedData);
    }
  });
}
window.gunSubscribe = subscribe;
setTimeout(() => { window.gunReady = true; }, 5000);
window.gunPut = (soul, key, value) => gun.get(soul).get(key).put(value);
</script></body></html>`;

const BEAM_PAGE = `<!DOCTYPE html><html><head><meta charset="utf-8"></head><body>
<div id="status">idle</div><div id="data"></div>
<script type="module">
import init, { Beam } from "./beam.js";
await init();
const beam = new Beam();
beam.connect('${RELAY_URL}');
window.beamReceived = {};
window.beamSubscribe = (soul) => {
  beam.on(soul, (value) => {
    window.beamReceived[value] = true;
    document.getElementById('data').textContent = JSON.stringify(Object.keys(window.beamReceived));
    document.getElementById('status').textContent = 'received';
  });
};
window.beamReady = false;
setTimeout(() => { window.beamReady = true; }, 5000);
window.beamPut = (soul, key, value) => beam.put(soul + '.' + key, value);
</script></body></html>`;

test.beforeAll(async () => {
  writeFileSync('browser-test/_gun_test.html', GUN_PAGE);
  writeFileSync('browser-test/_beam_test.html', BEAM_PAGE);
});


test.beforeEach(async () => {
  // Kill any existing relay
  try { execSync('pkill -f "target/debug/beam"', { stdio: 'ignore' }); } catch {}
  await new Promise(r => setTimeout(r, 1000));
  // Start fresh relay (memory-only, no redb persistence)
  const relay = spawn('./target/debug/beam', [
    'start', '--port', '4944',
    '--memory-storage', 'true',
    '--redb-storage', 'false',
    '--allow-public-space', 'true'
  ], {
    cwd: '/home/guan/src/beam',
    env: { ...process.env, RUST_LOG: 'beam=info' },
    stdio: 'ignore',
  });
  // Wait for relay to bind
  await new Promise(r => setTimeout(r, 3000));
});

test.afterEach(async () => {
  try { execSync('pkill -f "target/debug/beam"', { stdio: 'ignore' }); } catch {}
  await new Promise(r => setTimeout(r, 500));
});

test('gun puts, beam receives', async ({ context }) => {
  test.setTimeout(120000);
  const gunPage = await context.newPage();
  const beamPage = await context.newPage();
  await gunPage.goto(`${BASE_URL}/_gun_test.html`);
  await beamPage.goto(`${BASE_URL}/_beam_test.html`);
  await gunPage.waitForFunction(() => window.gunReady === true, { timeout: 20000 });
  await beamPage.waitForFunction(() => window.beamReady === true, { timeout: 20000 });
  await gunPage.evaluate(() => window.gunSubscribe('e2etest'));
  await beamPage.evaluate(() => window.beamSubscribe('e2etest'));
  await gunPage.evaluate(() => window.gunPut('e2etest', 'msg1', 'hello from gun'));
  await beamPage.waitForFunction(
    () => document.getElementById('data').textContent.includes('hello from gun'),
    { timeout: 15000 }
  );
  expect(await beamPage.textContent('#data')).toContain('hello from gun');
  await gunPage.close(); await beamPage.close();
});

test('beam puts, gun receives', async ({ context }) => {
  test.setTimeout(120000);
  const gunPage = await context.newPage();
  const beamPage = await context.newPage();
  await gunPage.goto(`${BASE_URL}/_gun_test.html`);
  await beamPage.goto(`${BASE_URL}/_beam_test.html`);
  await gunPage.waitForFunction(() => window.gunReady === true, { timeout: 20000 });
  await beamPage.waitForFunction(() => window.beamReady === true, { timeout: 20000 });
  await gunPage.evaluate(() => window.gunSubscribe('e2etest'));
  await beamPage.evaluate(() => window.beamSubscribe('e2etest'));
  await beamPage.evaluate(() => window.beamPut('e2etest', 'msg2', 'hello from beam'));
  await gunPage.waitForFunction(
    () => document.getElementById('data').textContent.includes('hello from beam'),
    { timeout: 15000 }
  );
  expect(await gunPage.textContent('#data')).toContain('hello from beam');
  await gunPage.close(); await beamPage.close();
});

test('bidirectional convergence', async ({ context }) => {
  test.setTimeout(120000);
  const gunPage = await context.newPage();
  const beamPage = await context.newPage();
  await gunPage.goto(`${BASE_URL}/_gun_test.html`);
  await beamPage.goto(`${BASE_URL}/_beam_test.html`);
  // Wait for both pages to signal ready
  await gunPage.waitForFunction(() => window.gunReady === true, { timeout: 20000 });
  await beamPage.waitForFunction(() => window.beamReady === true, { timeout: 20000 });

  // Subscribe to bidir soul, then wait for subscriptions to settle
  await gunPage.evaluate(() => window.gunSubscribe('bidir'));
  await beamPage.evaluate(() => window.beamSubscribe('bidir'));
  await gunPage.waitForTimeout(1000);

  // Gun.js puts first, wait for BEAM to receive it
  await gunPage.evaluate(() => window.gunPut('bidir', 'from_gun', 'gun_value'));
  await beamPage.waitForFunction(
    () => document.getElementById('data').textContent.includes('gun_value'),
    { timeout: 20000 }
  );

  // Small delay before BEAM puts
  await beamPage.waitForTimeout(500);

  // Now BEAM puts, wait for Gun.js to receive it
  await beamPage.evaluate(() => window.beamPut('bidir', 'from_beam', 'beam_value'));
  await gunPage.waitForFunction(
    () => document.getElementById('data').textContent.includes('beam_value'),
    { timeout: 20000 }
  );

  expect(await gunPage.textContent('#data')).toContain('beam_value');
  expect(await beamPage.textContent('#data')).toContain('gun_value');
  await gunPage.close(); await beamPage.close();
});
