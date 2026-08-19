import http from 'node:http';
import WebSocket from 'ws';
import Gun from 'gun';

const wsServer = http.createServer((_req, res) => { res.writeHead(200); res.end('ok'); });
const gun = Gun({ web: wsServer, file: null, localStorage: false, radisk: false });
wsServer.listen(9871, () => {
  console.log('relay up');
  gun.get('beamtest/recon').put({ phase2: 'second' });
  console.log('data stored');

  const ws = new WebSocket('ws://127.0.0.1:9871/gun');
  ws.on('open', () => {
    console.log('ws connected');
    ws.send(JSON.stringify({ dam: 'hi' }));
    setTimeout(() => {
      ws.send(JSON.stringify({ get: { '#': 'beamtest/recon' }, '#': 'gettest001' }));
      console.log('get sent');
    }, 2000);
  });
  ws.on('message', (d) => {
    console.log('RECV:', d.toString().substring(0, 300));
  });
});
setTimeout(() => process.exit(0), 10000);
