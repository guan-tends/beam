import http from 'node:http';
import WebSocket from 'ws';
import Gun from 'gun';

const wsServer = http.createServer((_req, res) => { res.writeHead(200); res.end('ok'); });
const gun = Gun({ web: wsServer, file: null, localStorage: false, radisk: false });
wsServer.listen(9871, () => {
  console.log('relay up');
  gun.get('beamtest/recon').put({ phase2: 'second' });
  console.log('data stored');

  // Simulate BEAM connecting: send dam: "?" with our pid
  const ws = new WebSocket('ws://127.0.0.1:9871/gun');
  ws.on('open', () => {
    console.log('ws connected, sending dam:?');
    ws.send(JSON.stringify({ dam: '?', pid: 'beamtest_pid_001', '#': 'msg001' }));
    // After handshake, send a Get
    setTimeout(() => {
      ws.send(JSON.stringify({ get: { '#': 'beamtest/recon' }, '#': 'gettest001' }));
      console.log('get sent');
    }, 1000);
  });
  ws.on('message', (d) => {
    const msg = d.toString();
    console.log('RECV:', msg.substring(0, 400));
  });
});
setTimeout(() => process.exit(0), 8000);
