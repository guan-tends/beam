import http from 'node:http';
import WebSocket from 'ws';

const Gun = (await import('gun')).default;

const RELAY_URL = process.argv[2] || 'ws://127.0.0.1:9920/gun';
const API_PORT = parseInt(process.argv[3] || '9922', 10);

// Intercept WebSocket to log all messages
const OriginalWebSocket = global.WebSocket || WebSocket;
class LoggingWebSocket extends OriginalWebSocket {
    constructor(url, protocols) {
        super(url, protocols);
        console.log(`[WS] connecting to ${url}`);
        this.addEventListener('open', () => console.log('[WS] connected'));
        this.addEventListener('close', () => console.log('[WS] closed'));
        this.addEventListener('error', (e) => console.log('[WS] error', e));
    }
    send(data) {
        console.log('[WS] SEND:', data);
        return super.send(data);
    }
}
global.WebSocket = LoggingWebSocket;
if (typeof globalThis.WebSocket === 'undefined') globalThis.WebSocket = LoggingWebSocket;

const gun = Gun({
    peers: [RELAY_URL],
    file: null,
    localStorage: false,
    radisk: false,
});

// Log incoming messages via gun.on
gun.on('hi', (peer) => console.log('[GUN] hi from peer', peer));
gun.on('bye', (peer) => console.log('[GUN] bye from peer', peer));

console.log(`Gun.js client connecting to ${RELAY_URL}`);

const apiServer = http.createServer(async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
    const url = new URL(req.url, `http://localhost:${API_PORT}`);
    if (url.pathname === '/health') { res.writeHead(200); res.end('ok'); return; }
    if (url.pathname === '/get' && req.method === 'GET') {
        const soul = url.searchParams.get('soul');
        const key = url.searchParams.get('key');
        gun.get(soul).once((node) => {
            if (!node) { res.writeHead(404); res.end(JSON.stringify({error:'not found'})); return; }
            res.writeHead(200); res.end(JSON.stringify({value: node[key]}));
        });
        return;
    }
    if (url.pathname === '/put' && req.method === 'POST') {
        let body = '';
        for await (const chunk of req) body += chunk;
        const { soul, key, value } = JSON.parse(body);
        const data = {}; data[key] = value;
        console.log(`[PUT] gun.get(${soul}).put(${JSON.stringify(data)})`);
        gun.get(soul).put(data);
        res.writeHead(200); res.end(JSON.stringify({ok:true}));
        return;
    }
    res.writeHead(404); res.end('not found');
});
apiServer.listen(API_PORT, () => console.log(`HTTP API on port ${API_PORT}`));
