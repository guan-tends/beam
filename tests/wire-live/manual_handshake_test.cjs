// Manual test: Start Gun.js relay, connect a raw WebSocket, 
// send dam:"?" handshake, then send a Get, and log ALL messages received.

const http = require('http');
const WebSocket = require('ws');
const Gun = require('/home/guan/src/gun-js/index.js');

// Create HTTP server for Gun.js
const server = http.createServer();
const gun = Gun({ web: server });
server.listen(9999, () => {
    console.log('Gun.js relay started on port 9999');
    
    // Wait for relay to be ready, then connect
    setTimeout(() => {
        const ws = new WebSocket('ws://127.0.0.1:9999/gun');
        
        ws.on('open', () => {
            console.log('[WS] Connected to Gun.js relay');
            
            // Send dam:"?" handshake (initial contact, no @)
            const hi = JSON.stringify({
                dam: '?',
                pid: 'beam-test-pid-123',
                '#': 'msg001'
            });
            console.log('[WS] Sending:', hi);
            ws.send(hi);
        });
        
        ws.on('message', (data) => {
            const msg = data.toString();
            console.log('[WS] Received:', msg);
            
            // If we receive dam:"?" with @ (ack from Gun.js), send a Get
            try {
                const parsed = JSON.parse(msg);
                if (parsed.dam === '?' && parsed['@']) {
                    console.log('[WS] Handshake ack received! Sending Get...');
                    
                    // Send a Get for a soul
                    const get = JSON.stringify({
                        get: { '#': 'beamtest/recon' },
                        '#': 'get001'
                    });
                    console.log('[WS] Sending Get:', get);
                    ws.send(get);
                }
            } catch (e) {
                // Not JSON, ignore
            }
        });
        
        ws.on('error', (err) => {
            console.error('[WS] Error:', err.message);
        });
        
        // Keep alive for 10 seconds
        setTimeout(() => {
            console.log('[WS] Done');
            ws.close();
            process.exit(0);
        }, 10000);
    }, 1000);
});
