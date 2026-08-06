/**
 * Minimal Gun.js relay server for BEAM live integration tests.
 *
 * Usage:  node gun_relay.js <ws_port> <api_port>
 *
 * Two servers:
 *   1. Gun.js WebSocket relay on <ws_port> — BEAM connects here
 *   2. HTTP API on <api_port> — Rust tests use this for verification
 *
 * HTTP API:
 *   GET  /health          → 200 "ok"
 *   GET  /get?soul=X&key=Y → JSON { "value": ... }
 *   POST /put             → body: {"soul":"X","key":"Y","value":"Z"}
 *
 * WebSocket: ws://localhost:<ws_port>/gun
 */

import http from 'node:http';

const Gun = (await import('gun')).default;

const WS_PORT = parseInt(process.argv[2] || '8765', 10);
const API_PORT = parseInt(process.argv[3] || '8766', 10);

// ---------------------------------------------------------------------------
// Gun.js WebSocket relay server
// ---------------------------------------------------------------------------

const wsServer = http.createServer((_req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Gun relay WebSocket server. Connect via WebSocket.');
});

const gun = Gun({
  web: wsServer,
  file: null,
  localStorage: false,
  radisk: false,
});

wsServer.listen(WS_PORT, () => {
  console.log(`Gun relay WebSocket on port ${WS_PORT} (path /gun)`);
});

// ---------------------------------------------------------------------------
// HTTP API server (separate port — Gun.js doesn't intercept this one)
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

  // Health check
  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }

  // Get data from Gun
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

  // Put data into Gun
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
  console.log(`Health: http://localhost:${API_PORT}/health`);
});
