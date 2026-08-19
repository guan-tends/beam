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

// Custom Get response handler — sends Put responses directly to the requesting
// peer's WebSocket, bypassing mesh.say's self-check (peer === meta.via) which
// drops Get responses for the originating peer when F=true (in-memory graph).
//
// Root cause: Gun.on.get.ack creates a `faith` object when F=true (node === root.graph[soul]).
// During the async mesh.raw/mesh.hash pipeline, faith.via gets set to the requesting peer.
// mesh.say's self-check (peer === meta.via) then drops the response.
//
// This handler intercepts Gets AFTER Gun.on.get has processed them (via root.on('get')),
// and sends the response directly via peer.wire.send(), bypassing mesh.say entirely.
// This mimics what a storage adapter does — the response comes from a different code
// path that doesn't go through the faith/mesh.say pipeline.
const gunRoot = gun._.root || gun._;
gunRoot.on('get', function(msg) {
  this.to.next(msg); // pass to other adapters
  const lex = msg.get;
  const soul = lex && lex['#'];
  if (!soul) return;
  const node = gunRoot.graph[soul];
  if (!node) return;

  // Build a Put response message (matching Gun.js wire format)
  const soulObj = {};
  const meta = { '#': soul, '>': {} };
  for (const k in node) {
    if (k === '_') { Object.assign(meta, node[k]); continue; }
    soulObj[k] = node[k];
    if (node._ && node._['>'] && node._['>'][k]) meta['>'][k] = node._['>'][k];
  }
  soulObj['_'] = meta;
  const put = {}; put[soul] = soulObj;

  const response = {
    '#': (Gun.text && Gun.text.random) ? Gun.text.random(9) : Math.random().toString(36).slice(2, 11),
    '@': msg['#'],
    'put': put,
  };

  // Find the peer that sent the Get
  let peer = (msg._ && msg._.via) ? msg._.via : null;
  if (!peer) {
    const dup = gunRoot.dup;
    if (dup && dup.s && dup.s[msg['#']]) peer = dup.s[msg['#']].via;
  }

  if (peer && peer.wire) {
    const raw = JSON.stringify(response);
    if (peer.say) peer.say(raw);
    else if (peer.wire.send) peer.wire.send(raw);
  }
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

  if (url.pathname === '/debug' && req.method === 'GET') {
    const graphKeys = Object.keys(gun._.root.graph || {});
    const reconNode = gun._.root.graph && gun._.root.graph['beamtest/recon'];
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      graphKeys,
      reconNode: reconNode ? JSON.stringify(reconNode).slice(0,500) : null,
    }));
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
});

apiServer.listen(API_PORT, () => {
  console.log(`HTTP API on port ${API_PORT}`);
  console.log(`Health: http://localhost:${API_PORT}/health`);
});
