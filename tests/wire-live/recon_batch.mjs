import http from 'node:http';
import WebSocket from 'ws';
import Gun from 'gun';

const wsServer = http.createServer((_req, res) => { res.writeHead(200); res.end('ok'); });
const gun = Gun({ web: wsServer, file: null, localStorage: false, radisk: false });
wsServer.listen(9871, async () => {
  console.log('relay up');
  gun.get('beamtest/recon').put({ phase2: 'second' });
  console.log('data stored');
  await new Promise(r => setTimeout(r, 500));

  const ws = new WebSocket('ws://127.0.0.1:9871/gun');
  const received = [];

  ws.on('open', () => {
    console.log('ws connected — sending dam:?');
    ws.send(JSON.stringify({ dam: '?', pid: 'beam_test_001', '#': 'init001' }));
  });

  ws.on('message', (d) => {
    const msg = d.toString();
    const parsed = JSON.parse(msg);
    received.push(parsed);
    console.log('RECV:', msg.substring(0, 400));

    if (parsed.dam === '?' && !parsed['@']) {
      console.log('sending ack');
      ws.send(JSON.stringify({
        dam: '?', pid: 'beam_test_001',
        '@': parsed['#'],
        '#': 'ack' + Math.random().toString(36).substr(2, 6)
      }));
    }

    if (parsed.dam === '?' && parsed['@']) {
      console.log('handshake done — sending BATCH with ack + get');
      setTimeout(() => {
        // Simulate what BEAM does: batch Hi ack + Get in a JSON array
        const batch = [
          { dam: '?', pid: 'beam_test_001', '@': parsed['@'], '#': 'batchack001' },
          { get: { '#': 'beamtest/recon', '.': 'phase2' }, '#': 'batchget001' }
        ];
        ws.send(JSON.stringify(batch));
        console.log('batch sent:', JSON.stringify(batch));
      }, 1000);
    }
  });

  setTimeout(() => {
    console.log('\n=== RESULT ===');
    const gotPut = received.some(m => m.put);
    console.log('Got put response:', gotPut);
    process.exit(gotPut ? 0 : 1);
  }, 8000);
});
