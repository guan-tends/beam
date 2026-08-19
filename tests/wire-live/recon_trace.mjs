import http from 'node:http';
import WebSocket from 'ws';
import Gun from 'gun';

// Start Gun.js relay on port 9871
const wsServer = http.createServer((_req, res) => { res.writeHead(200); res.end('ok'); });
const gun = Gun({ web: wsServer, file: null, localStorage: false, radisk: false });
wsServer.listen(9871, async () => {
  console.log('1. relay up');
  
  // Put data directly via Gun.js
  gun.get('beamtest/recon').put({ phase2: 'second' });
  console.log('2. data stored');
  await new Promise(r => setTimeout(r, 500));
  
  // Now simulate what BEAM does: connect, send dam:?, wait, send get
  const ws = new WebSocket('ws://127.0.0.1:9871/gun');
  const received = [];
  
  ws.on('open', () => {
    console.log('3. ws connected — sending dam:? with pid');
    // This is what BEAM's WsConn::pre_start sends
    ws.send(JSON.stringify({ dam: '?', pid: 'beam_recon_test', '#': 'init001' }));
  });
  
  ws.on('message', (d) => {
    const msg = d.toString();
    const parsed = JSON.parse(msg);
    received.push(parsed);
    console.log('RECV:', msg.substring(0, 400));
    
    // If we receive dam:? with no @, this is Gun.js's initial contact.
    // BEAM's Router should respond with an ack. Simulate that here.
    if (parsed.dam === '?' && !parsed['@']) {
      console.log('4. Got initial dam:? — sending ack with @');
      ws.send(JSON.stringify({ 
        dam: '?', 
        pid: 'beam_recon_test', 
        '@': parsed['#'],
        '#': 'ack' + Math.random().toString(36).substr(2, 6)
      }));
    }
    
    // If we receive dam:? with @, handshake complete
    if (parsed.dam === '?' && parsed['@']) {
      console.log('5. Handshake complete — sending Get in 1s');
      setTimeout(() => {
        console.log('6. Sending Get for beamtest/recon/phase2');
        ws.send(JSON.stringify({ 
          get: { '#': 'beamtest/recon' }, 
          '#': 'gettest002' 
        }));
      }, 1000);
    }
  });
  
  // Safety timeout
  setTimeout(() => {
    console.log('\n=== RESULT ===');
    const gotPut = received.some(m => m.put && m.put['beamtest/recon']);
    console.log('Received put response:', gotPut);
    if (gotPut) {
      const putMsg = received.find(m => m.put && m.put['beamtest/recon']);
      console.log('Data:', JSON.stringify(putMsg.put['beamtest/recon']));
    }
    process.exit(gotPut ? 0 : 1);
  }, 8000);
});
