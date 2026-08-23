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
 * Stop all tracked relays.
 */
function stopAll() {
  for (const [port, proc] of relays) {
    proc.kill('SIGTERM');
  }
  relays.clear();
}

module.exports = { startRelay, stopRelay, stopAll };
