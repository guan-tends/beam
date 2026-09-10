import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  timeout: 60000,
  retries: 0,
  use: {
    headless: true,
    // CI runs under Xvfb + dbus-run-session (see justfile test-wasm).
    // executablePath pins Playwright's chromium build 1148 — the version
    // this suite was green with at v0.17.0. The chrome-headless-shell that
    // Playwright 1.62 downloads by default (CFT 151) silently kills pages
    // running wasm async runtimes ~1s after init(); build 1148 does not.
    // --disable-gpu keeps the run deterministic (software compositing).
    launchOptions: {
      executablePath: '/home/guan/.cache/ms-playwright/chromium-1148/chrome-linux/chrome',
      args: ['--disable-gpu', '--no-sandbox'],
    },
  },
  projects: [
    {
      name: 'chromium',
      use: { browserName: 'chromium' },
    },
  ],
  webServer: {
    command: 'npx http-server browser-test -p 8080 --cors',
    port: 8080,
    reuseExistingServer: true,
    timeout: 15000,
  },
});
