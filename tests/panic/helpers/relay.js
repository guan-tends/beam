/**
 * BEAM relay manager for PANIC tests.
 *
 * Spawns BEAM relay subprocesses and provides clean lifecycle management.
 * Each relay gets unique ports to avoid collisions.
 *
 * @module helpers/relay
 */

const { spawn } = require('child_process');

/** @type {Map<string, import('child_process').ChildProcess>} */
const relays = new Map();

/**
 * Spawn a BEAM relay subprocess.
 *
 * @param {Object} opts
 * @param {string} opts.binPath   - Path to the `beam` binary.
 * @param {number} opts.port      - WebSocket port for the relay.
 * @param {string[]} [opts.peers] - Peer URLs to connect to (ws://host:port).
 * @returns {{ process: import('child_process').ChildProcess, port: number }}
 */
function startRelay({ binPath, port, peers = [] }) {
  const args = ['start', '--port', String(port), '--memory-storage', 'true', '--redb-storage', 'false'];
  if (peers.length > 0) {
    args.push('--peers', peers.join(','));
  }

  const proc = spawn(binPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });
  proc.stdout.on('data', (d) => process.stdout.write(`[beam:${port}] ${d}`));
  proc.stderr.on('data', (d) => process.stderr.write(`[beam:${port}!] ${d}`));

  relays.set(String(port), proc);
  return { process: proc, port };
}

/**
 * Stop a specific relay by port.
 * @param {number} port
 */
function stopRelay(port) {
  const proc = relays.get(String(port));
  if (proc) {
    proc.kill('SIGTERM');
    relays.delete(String(port));
  }
}

/**
 * Wait until a TCP port is free (no listener).
 *
 * BEAM binds two ports per relay (ws on `port`, web UI on `port + 1`).
 * After stopRelay(), the kernel may keep the socket in a lingering state
 * briefly; restarting too fast causes an AddrInUse panic in the web UI
 * thread. This helper polls both ports until neither accepts connections.
 *
 * @param {number} port - WebSocket port (port and port+1 are both checked).
 * @param {number} [timeoutMs] - Max wait (default 10000).
 * @returns {Promise<void>} Resolves when both ports are free; rejects on timeout.
 */
function waitForPortFree(port, timeoutMs = 10000) {
  const net = require('net');
  const deadline = Date.now() + timeoutMs;

  function portOpen(p) {
    return new Promise((resolve) => {
      const sock = net.connect({ port: p, host: '127.0.0.1' });
      const done = (open) => {
        sock.destroy();
        resolve(open);
      };
      sock.once('connect', () => done(true));
      sock.once('error', () => done(false));
    });
  }

  return new Promise((resolve, reject) => {
    const attempt = async () => {
      const wsOpen = await portOpen(port);
      const uiOpen = await portOpen(port + 1);
      if (!wsOpen && !uiOpen) return resolve();
      if (Date.now() > deadline) {
        return reject(new Error(`waitForPortFree: ports ${port}/${port + 1} still in use after ${timeoutMs}ms`));
      }
      setTimeout(attempt, 250);
    };
    attempt();
  });
}

/**
 * Stop all tracked relays.
 */
function stopAll() {
  for (const [port, proc] of relays) {
    proc.kill('SIGTERM');
  }
  relays.clear();
}

module.exports = { startRelay, stopRelay, stopAll, waitForPortFree };
