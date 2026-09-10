/**
 * Browser management for PANIC tests.
 *
 * Uses Puppeteer to launch browser clients that auto-connect to the panic-server.
 * Mirrors the pattern from Gun.js's test/panic/util/open.js but with cleaner
 * lifecycle management and error handling.
 *
 * @module helpers/browser
 */

const puppeteer = require('puppeteer');

/** @type {import('puppeteer').Browser[]} */
let browsers = [];

/**
 * Open N browser tabs pointing at the panic-server URL.
 *
 * The served HTML page must include panic-client and call `panic.server(url)`.
 *
 * @param {number} count  - Number of browser tabs to open.
 * @param {string} url    - URL of the panic-server (serves the client HTML).
 */
async function openBrowsers(count, url) {
  const browser = await puppeteer.launch({ headless: 'new' });
  browsers.push(browser);

  for (let i = 0; i < count; i++) {
    const page = await browser.newPage();
    page.on('console', (msg) => {
      const text = msg.text();
      if (text !== 'JSHandle@object') {
        console.log(`  [browser:${i}] ${msg.type()}: ${text}`);
      }
    });
    page.on('pageerror', (err) => console.error(`  [browser:${i}!] ${err.message}`));
    await page.goto(url);
    console.log(`  Browser tab ${i} open at ${url}`);
  }
}

/**
 * Close all tracked browser instances.
 */
async function cleanup() {
  if (browsers.length === 0) return;
  await Promise.all(browsers.map((b) => b.close()));
  browsers = [];
  console.log('  All browsers closed');
}

module.exports = { openBrowsers, cleanup };
