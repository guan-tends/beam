import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  timeout: 60000,
  retries: 0,
  use: {
    headless: true,
    // Headless CI over SSH: no X display, no /dev/dri GPU nodes.
    // Playwright 1.62's default GPU fallback (--enable-unsafe-swiftshader)
    // fails EGL/Vulkan init on such boxes and the browser process dies
    // mid-test ("Target page, context or browser has been closed").
    // --disable-gpu skips GPU init entirely; software compositing only.
    launchOptions: {
      args: ['--disable-gpu'],
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
