# Notices

This file contains attribution and legal notices for BEAM and its dependencies.

## Original Project

BEAM is derived from **[rod](https://github.com/mmalmi/rod)**, originally created by **[Martti Malmi](https://github.com/mmalmi)** as a from-scratch Rust port of **[Gun.js](https://github.com/amark/gun)** by Mark Nadal.

The original rod project is licensed under the MIT License and remains available at https://github.com/mmalmi/rod. We are grateful to Martti Malmi for the foundational work that BEAM builds upon.

### Original rod Contributors (per git history)

The following individuals contributed to the upstream rod project prior to the BEAM fork:

- **Martti Malmi** — original creator, architecture, SEA crypto, redb storage, router, node API
- **Gabriel Vîjială** — Gun.js protocol compatibility, message format
- **Itamar Perez** — bug fixes, testing
- **jxfzzzt** — bug fixes, testing

Full contributor history is preserved in the git log of the upstream project.

## BEAM Project

**Copyright (c) 2026 David Newman <david.r.newman@proton.me>**

BEAM is maintained by **David Newman** (2026–present) as an actively developed successor to rod. Major additions over the upstream rod codebase include:

- Complete SEA crypto layer (signing, encryption, ECDH, user identity, session persistence)
- WebRTC P2P transport with str0m
- Persy storage backend with cross-backend migration
- Network fanout ack (quorum) protocol
- Observability via shared metrics
- Comprehensive wire compatibility testing against Gun.js

## Development Partner

**Guan** contributed to BEAM's architecture, code, tests, and documentation across the SEA crypto layer, WebRTC integration, storage backends, observability, and the BEAM rebrand itself (v0.8.0).

## License

BEAM is licensed under the MIT License — see the [LICENSE](LICENSE) file for full text.

The original rod codebase (Copyright (c) 2021 Martti Malmi) and the Gun.js wire protocol compatibility remain under their respective MIT licenses.

## Third-Party Dependencies

BEAM depends on numerous open-source Rust crates. Notable dependencies include:

- **[redb](https://github.com/cberner/redb)** — MIT — embedded ACID database (default storage backend)
- **[Persy](https://github.com/tglman/persy)** — MIT — embedded segment store (optional storage backend)
- **[tokio](https://tokio.rs/)** — MIT — async runtime
- **[tokio-tungstenite](https://github.com/snapview/tokio-tungstenite)** — MIT — WebSocket implementation
- **[str0m](https://github.com/algesten/str0m)** — MIT — WebRTC implementation
- **[ring](https://github.com/briansmith/ring)** — ISC/MIT/OpenSSL — cryptography primitives
- **[clap](https://github.com/clap-rs/clap)** — MIT — CLI argument parsing
- **[serde](https://serde.rs/)** — MIT/Apache-2.0 — serialization framework

Full dependency licenses are available in their respective repositories.