/**
 * Headless Gun.js + SEA bootstrap for PANIC Node clients.
 *
 * Loads Gun + SEA via dynamic import() (the only import form that
 * parses inside PANIC's eval'd .run() callbacks), wires the Node
 * WebSocket implementation, connects to the BEAM relay, and forces
 * mesh.hi() (Gun's ESM/Node mode does not auto-open peer sockets).
 *
 * Idempotent: sets globalThis.gun / globalThis.user once per client
 * process; subsequent calls are no-ops. Returns { gun, user }.
 *
 * Usage inside a .run() callback:
 *   if (!globalThis.user) {
 *     await require('./helpers/gunboot').bootGun(test.props.relayPort);
 *   }
 *
 * @module helpers/gunboot
 */

/** Track in-flight boot to avoid double-init on concurrent steps. */
let booting = null;

async function bootGun(relayPort) {
  if (globalThis.user && globalThis.gun) {
    return { gun: globalThis.gun, user: globalThis.user };
  }
  if (booting) return booting;

  booting = (async () => {
    const Gun = (await import('gun')).default;
    await import('gun/sea.js');
    const { default: WebSocket } = await import('ws');

    Gun.window = Gun.window || {};
    Gun.window.WebSocket = WebSocket;

    const gun = Gun({
      peers: ['ws://localhost:' + relayPort + '/gun'],
      localStorage: false,
      radisk: false,
    });

    // Gun's ESM/Node mode does not auto-open peer sockets — force mesh.hi.
    const mesh = gun._.opt.mesh;
    if (mesh) {
      for (const url of Object.keys(gun._.opt.peers)) {
        mesh.hi(gun._.opt.peers[url]);
      }
    }

    globalThis.gun = gun;
    globalThis.user = gun.user();
    return { gun, user: globalThis.user };
  })();

  return booting;
}

module.exports = { bootGun };
