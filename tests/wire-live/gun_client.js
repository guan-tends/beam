/**
 * Gun.js client for BEAM live integration tests.
 *
 * Usage:  node gun_client.js <relay_url> <api_port>
 *
 * In Node.js, Gun.js needs Gun.window.WebSocket set to enable the
 * embedded WebSocket client module (checked in gun.js core).
 *
 * HTTP API (same shape as gun_relay.js):
 *   GET  /health           → 200 "ok"
 *   GET  /get?soul=X&key=Y → JSON { "value": ... }
 *   POST /put              → body: {"soul":"X","key":"Y","value":"Z"}
 */

import http from 'node:http';
import WebSocket from 'ws';

const Gun = (await import('gun')).default;

// Provide WebSocket implementation — Gun.js checks Gun.window.WebSocket
Gun.window = Gun.window || {};
Gun.window.WebSocket = WebSocket;

const RELAY_URL = process.argv[2] || 'ws://127.0.0.1:4944/gun';
const API_PORT = parseInt(process.argv[3] || '8766', 10);

// ---------------------------------------------------------------------------
// Gun.js client
// ---------------------------------------------------------------------------

const gun = Gun({
  peers: [RELAY_URL],
  axe: false,
  multicast: false,
  file: null,
  localStorage: false,
  radisk: false,
});

// Force mesh to open WebSocket connections to all peers.
// In Node.js ESM, Gun's websocket module may not auto-trigger the
// initial connection because the setTimeout in websocket.js's
// `Gun.on('opt')` handler can fire before the mesh is ready.
// Manually calling mesh.hi() for each peer ensures the WebSocket
// is opened regardless of module loading order.
function forceConnect() {
  const mesh = gun._.opt.mesh;
  if (!mesh) { return; }
  for (const url of Object.keys(gun._.opt.peers)) {
    const peer = gun._.opt.peers[url];
    if (!peer.wire || peer.wire.readyState === 3) {
      try { mesh.hi(peer); } catch(e) {}
    }
  }
}
setTimeout(forceConnect, 50);
setTimeout(forceConnect, 500);
setTimeout(forceConnect, 1000);
setInterval(forceConnect, 2000);

console.log(`Gun.js client connecting to ${RELAY_URL}`);

// ---------------------------------------------------------------------------
// HTTP API server
// ---------------------------------------------------------------------------

const apiServer = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://localhost:${API_PORT}`);

  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }

  if (url.pathname === '/get' && req.method === 'GET') {
    const soul = url.searchParams.get('soul');
    const key = url.searchParams.get('key');
    if (!soul || !key) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'missing soul or key' }));
      return;
    }
    gun.get(soul).once((node) => {
      if (!node) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'not found' }));
        return;
      }
      const val = node[key];
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ value: val }));
    });
    return;
  }

  if (url.pathname === '/put' && req.method === 'POST') {
    let body = '';
    for await (const chunk of req) body += chunk;
    try {
      const { soul, key, value } = JSON.parse(body);
      if (!soul || !key) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'missing soul or key' }));
        return;
      }
      const data = {};
      if (value && typeof value === 'object' && value['#']) {
        data[key] = value;
      } else {
        data[key] = value;
      }
      gun.get(soul).put(data);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    } catch (e) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
});

apiServer.listen(API_PORT, () => {
  console.log(`HTTP API on port ${API_PORT}`);
});
