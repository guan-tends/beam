import { test, expect } from '@playwright/test';
import { copyFileSync, existsSync, mkdirSync } from 'fs';

const BASE_URL = 'http://127.0.0.1:8080';

test.beforeAll(async () => {
  // Copy WASM assets into the test serving directory (browser-test/)
  const pkgDir = 'pkg';
  const testDir = 'browser-test';
  const files = ['beam.js', 'beam_bg.wasm'];
  for (const f of files) {
    const src = `${pkgDir}/${f}`;
    const dst = `${testDir}/${f}`;
    if (existsSync(src)) {
      copyFileSync(src, dst);
    }
  }
  // Also copy the OPFS test HTML
  if (existsSync('tests/e2e/opfs-test.html')) {
    copyFileSync('tests/e2e/opfs-test.html', `${testDir}/opfs-test.html`);
  }
});

test('OPFS: data persists across page reload', async ({ page, context }) => {
  test.setTimeout(60000);

  // Navigate to the OPFS test page
  await page.goto(`${BASE_URL}/opfs-test.html`);

  // Wait for BEAM to be ready
  await page.waitForFunction(() => window.beamReady === true, { timeout: 15000 });

  // Write test data
  await page.evaluate(() => window.beamPut('opfsTest.key1', 'hello_opfs'));
  await page.evaluate(() => window.beamPut('opfsTest.key2', 'world_opfs'));

  // Give OPFS time to flush (async writes via spawn_local)
  await page.waitForTimeout(2000);

  // Read back before reload (verify write worked)
  const beforeReload = await page.evaluate(() => window.beamGet('opfsTest.key1'));
  expect(beforeReload).toBe('hello_opfs');

  // Reload the page — OPFS data should persist
  await page.reload();
  await page.waitForFunction(() => window.beamReady === true, { timeout: 15000 });

  // Wait for OPFS to initialize and read back
  await page.waitForTimeout(2000);

  // Verify data survived the reload
  const afterReload1 = await page.evaluate(() => window.beamGet('opfsTest.key1'));
  expect(afterReload1).toBe('hello_opfs');

  const afterReload2 = await page.evaluate(() => window.beamGet('opfsTest.key2'));
  expect(afterReload2).toBe('world_opfs');
});

test('OPFS: data survives full browser context close', async ({ context }) => {
  test.setTimeout(60000);

  // First page: write data
  const page1 = await context.newPage();
  await page1.goto(`${BASE_URL}/opfs-test.html`);
  await page1.waitForFunction(() => window.beamReady === true, { timeout: 15000 });
  await page1.evaluate(() => window.beamPut('persistTest.key', 'survives_close'));
  await page1.waitForTimeout(2000);
  await page1.close();

  // New page in same context: read data back
  const page2 = await context.newPage();
  await page2.goto(`${BASE_URL}/opfs-test.html`);
  await page2.waitForFunction(() => window.beamReady === true, { timeout: 15000 });
  await page2.waitForTimeout(2000);

  const result = await page2.evaluate(() => window.beamGet('persistTest.key'));
  expect(result).toBe('survives_close');
  await page2.close();
});
