# Deploying BEAM

> *Production deployment guide. Every command, port, and configuration option below is verified against the actual source code.*

---

## Building

```bash
# Debug build (faster compile, slower runtime)
cargo build

# Release build (optimized, production-ready)
cargo build --release

# With WebRTC support
cargo build --release --features webrtc

# Binary locations after build:
#   target/release/beam                — main server binary
#   target/release/beam-sea-keygen   — session key generator utility
```

### Cross-Compilation

BEAM uses `ring` (for ECDSA), which requires a C compiler for the target platform. For cross-compilation, ensure you have the appropriate target toolchain installed.

---

## Running a Relay Node

A relay node accepts WebSocket connections from peers and synchronizes data between them.

### Basic Relay

```bash
# Start with default config: redb storage, WebSocket on port 4944
./target/release/beam -- start --port 4944
```

This starts:
- **WebSocket server** on port 4944 (accepts peer connections at `ws://your-host:4944/ws`)
- **Web UI** on port 4945 (serves `/peer_id` and `/stats/*`)
- **redb persistent storage** at `./beam.redb`

### With WebRTC (Direct P2P)

```bash
# Enable WebRTC data channels for direct peer connections
./target/release/beam --features webrtc -- start --port 4944
```

This adds:
- WebRTC peer connection capability via `str0m`
- STUN discovery using Google's public STUN server (`stun:stun.l.google.com:19302`)
- TURN relay allocation support

### With TLS

```bash
# Enable WSS (WebSocket Secure) and HTTPS web UI
./target/release/beam -- start --port 4944 \
  --cert-path /etc/beam/cert.pem \
  --key-path /etc/beam/key.pem
```

TLS is handled natively by the `WsServer` adapter via `tokio-native-tls`. The certificate must be in PEM/PKCS8 format.

### With Peers (Mesh Joining)

```bash
# Connect to existing relay peers on startup
./target/release/beam -- start --port 4944 \
  --peers wss://relay1.example.com:8443/ws,wss://relay2.example.com:8443/ws
```

The `OutgoingWebsocketManager` connects to each peer URL and maintains the connection with retry. All peers in the `--peers` list become relay peers (`subscribe_to_everything = true`), receiving all messages for forwarding.

### With Multicast (LAN Discovery)

```bash
# Enable UDP multicast for local network peer discovery
./target/release/beam -- start --port 4944 --multicast true
```

Uses multicast group `224.0.0.123:6969`. Peers on the same LAN automatically discover and sync with each other. **Disable on public-facing servers** — multicast is for trusted local networks only.

### In-Memory Only (No Persistence)

```bash
# Use ephemeral in-memory storage (data lost on restart)
./target/release/beam -- start --port 4944 \
  --memory-storage true \
  --redb-storage false
```

### Restricted Mode (Signed Data Only)

```bash
# Reject unsigned writes to public space
# Only user-signed (~{pub}) and content-addressed (#) data accepted
./target/release/beam -- start --port 4944 \
  --allow-public-space false
```

This matches Gun.js `opt.enforce` semantics. Useful for relay nodes that should only propagate authenticated data.

---

## All CLI Flags

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `PORT` | 4944 | WebSocket server port |
| `--ws-server` | `WS_SERVER` | true | Enable WebSocket server |
| `--cert-path` | `CERT_PATH` | — | TLS certificate file (PEM/PKCS8) |
| `--key-path` | `KEY_PATH` | — | TLS private key file |
| `--peers` | `PEERS` | — | Comma-separated WebSocket peer URLs |
| `--multicast` | `MULTICAST` | false | Enable UDP multicast LAN discovery |
| `--memory-storage` | `MEMORY_STORAGE` | false | Enable in-memory storage |
| `--redb-storage` | `REDB_STORAGE` | true | Enable redb persistent storage |
| `--redb-path` | `REDB_PATH` | `beam.redb` | Path to redb database file |
| `--allow-public-space` | `ALLOW_PUBLIC_SPACE` | true | Accept unsigned writes to public nodes |
| `--stats` | `STATS` | true | Expose stats endpoint on web UI |

All flags can be set via environment variables (uppercase, underscore-separated). CLI flags take precedence over env vars.

---

## Ports

| Port | Service | Protocol |
|------|---------|----------|
| 4944 (configurable) | WebSocket server (peer connections) | WS/WSS |
| 4945 (port + 1) | Web UI (peer ID, stats) | HTTP/HTTPS |
| 6969 | Multicast discovery (fixed) | UDP multicast (224.0.0.123) |

---

## Systemd Service

```ini
# /etc/systemd/system/beam.service
[Unit]
Description=BEAM P2P Graph Database Relay
After=network.target

[Service]
Type=simple
User=beam
Group=beam
ExecStart=/usr/local/bin/beam -- start \
  --port 4944 \
  --redb-path /var/lib/beam/data.redb \
  --peers wss://relay1.example.com:8443/ws
WorkingDirectory=/var/lib/beam
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

# Environment variables (alternative to CLI flags)
Environment="RUST_LOG=info"
Environment="ALLOW_PUBLIC_SPACE=false"

[Install]
WantedBy=multi-user.target
```

### Setup

```bash
# Create user and data directory
sudo useradd -r -s /usr/sbin/nologin -d /var/lib/beam beam
sudo mkdir -p /var/lib/beam
sudo chown beam:beam /var/lib/beam

# Install binary
sudo cp target/release/beam /usr/local/bin/beam
sudo cp target/release/beam-sea-keygen /usr/local/bin/

# Install service
sudo cp beam.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable beam
sudo systemctl start beam

# Check status
sudo systemctl status beam
journalctl -u beam -f
```

---

## Docker

### Dockerfile

```dockerfile
FROM rust:1.85-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/beam /usr/local/bin/beam
COPY --from=builder /app/target/release/beam-sea-keygen /usr/local/bin/beam-sea-keygen

# Default port
EXPOSE 4944
EXPOSE 4945

# Data volume
VOLUME ["/data"]

ENV RUST_LOG=info
ENV REDB_PATH=/data/beam.redb

ENTRYPOINT ["beam"]
CMD ["--", "start", "--port", "4944", "--redb-path", "/data/beam.redb"]
```

### docker-compose.yml

```yaml
version: "3.8"
services:
  beam:
    build: .
    ports:
      - "4944:4944"
      - "4945:4945"
    volumes:
      - beam-data:/data
    environment:
      - RUST_LOG=info
      - ALLOW_PUBLIC_SPACE=false
      - PEERS=wss://relay.example.com:8443/ws
    restart: unless-stopped

volumes:
  beam-data:
```

### Running

```bash
# Build and start
docker-compose up -d

# View logs
docker-compose logs -f

# Generate a session key
docker-compose exec beam beam-sea-keygen
```

---

## Security Considerations

### Network Security

| Concern | Recommendation |
|---------|---------------|
| **WebSocket traffic** | Plaintext by default. Use `--cert-path` / `--key-path` for WSS, or put behind a TLS-terminating reverse proxy (nginx, Caddy, Traefik). |
| **Multicast** | Local network only. Disable on public-facing servers (`--multicast false`). |
| **WebRTC** | DTLS encryption is built-in (via `str0m`). Data channels are always encrypted. |
| **Public space** | Default allows anyone to write to public nodes. Set `--allow-public-space false` for relay nodes that should only propagate signed data. |

### SEA Key Management

```bash
# Generate a session encryption key for EncryptedFileSessionStorage
export BEAM_SEA_SESSION_KEY=$(beam-sea-keygen)

# Store in secrets manager (examples)
# systemd credentials:
echo "$BEAM_SEA_SESSION_KEY" | sudo systemd-creds encrypt - beam-session.key

# Docker secrets:
echo "$BEAM_SEA_SESSION_KEY" | docker secret create beam_session_key -
```

**Session files** contain encrypted private keys. Protect the session directory (`~/.config/beam/sessions/`) with filesystem permissions:

```bash
chmod 700 ~/.config/beam/sessions/
```

### Storage Security

| Storage | Security |
|---------|----------|
| `MemoryStorage` | Ephemeral — data in RAM only, lost on restart. No at-rest encryption needed. |
| `RedbStorage` | Persistent to disk. No built-in encryption at rest. Use OS-level disk encryption (LUKS, ZFS encryption) for sensitive data. |

### Certificate Management

SEA certificates enable delegated trust — an authority can issue time-limited certificates authorizing specific public keys to write to specific paths.

```rust
// Issue a certificate
let cert = beam::sea::certify(
    &["alice_pub_key", "bob_pub_key"],     // authorized certificants
    Some(&json!({"e": 9999999999999.0,     // expiry timestamp
                  "r": ".*",                // read regex
                  "w": "skills/"})),         // write path prefix
    &authority_pair,
).await?;

// Verify a certificate
let payload = beam::sea::verify_certificate(&cert, &authority_pub_key)?;
let is_authorized = beam::sea::is_pubkey_certified(&payload, &alice_pub_key);
```

---

## Health Checks

```bash
# Check if WebSocket server is accepting connections
curl -s http://localhost:4945/peer_id
# Returns the node's peer ID (16-char random string)

# Check if WebSocket is up
websocat ws://localhost:4944/ws

# Check systemd service
systemctl status beam

# Check logs
journalctl -u beam -f --since "1 hour ago"
```

---

## Logging

BEAM uses `env_logger` (initialized via `env_logger::init()` in `main.rs`). Control verbosity with `RUST_LOG`:

```bash
# Error only
RUST_LOG=error ./beam -- start

# Info (default)
RUST_LOG=info ./beam -- start

# Debug (verbose — includes message routing, dedup decisions, peer management)
RUST_LOG=debug ./beam -- start

# Trace (everything — includes every message received)
RUST_LOG=trace ./beam -- start

# Filter by module
RUST_LOG=beam::router=debug,beam::node=info ./beam -- start
```

---

## Scaling

### Multi-Relay Topology

Multiple relay nodes can be chained. Each peer connects to one relay, and relays forward to each other:

```
Peer A ──→ Relay 1 ←──→ Relay 2 ←──→ Peer B
                         ↑
                       Peer C
```

- The `Dup` table (999 entries, 9s TTL) prevents message loops in multi-relay topologies
- The `peer_hop_list` (`><` field in wire format) tracks which peers have already seen a message
- Relays with `subscribe_to_everything = true` receive all messages for forwarding

### Performance Considerations

| Component | Tuning |
|-----------|--------|
| `broadcast_buffer_size` | Default 4096. Increase for high-throughput scenarios with many subscribers. Decrease to save memory. |
| `redb` cache | The `redb` database uses an in-memory B-tree page cache. Ensure adequate RAM for your data set. |
| `Dup` table | 999 entries / 9s TTL. For high-throughput networks, consider increasing (requires code change in `router.rs`). |
| WebSocket connections | Each connection spawns a `WsConn` actor with its own Tokio task. Use `LimitNOFILE=65536` in systemd for high connection counts. |
| WebRTC | Direct P2P connections reduce relay bandwidth. Enable with `--features webrtc` for peer-to-peer data transfer. |

### Storage Sizing

- `MemoryStorage`: bounded only by available RAM
- `RedbStorage`: the `beam.redb` file grows with data. Monitor disk usage. The file does not auto-shrink on deletes — run `redb` compaction periodically (requires a maintenance window).

---

## Monitoring

### Stats Endpoint

When `--stats true` (default), the web UI server (port 4945) serves:
- `/peer_id` — returns this node's peer ID
- `/stats/*` — static files from `./assets/stats/` directory

The `WsServer` periodically reports WebSocket connection count to the node's stats graph (`node_stats/{peer_id}/ws_server_connections`).

### Prometheus / Grafana

BEAM does not have built-in Prometheus metrics. The `msg_counter` atomic in `Router` tracks total messages processed but is not yet exposed. A future implementation could expose this via the web UI server.

For now, use log-based monitoring:
```bash
# Count messages per second
journalctl -u beam -f --since "1 min ago" | grep "incoming message" | wc -l
```

---

## Backup and Recovery

### Redb Storage Backup

```bash
# Stop the node gracefully (flushes pending writes)
systemctl stop beam

# Copy the database file
cp /var/lib/beam/data.redb /backup/beam-$(date +%Y%m%d).redb

# Restart
systemctl start beam
```

### Session Key Backup

The `BEAM_SEA_SESSION_KEY` environment variable encrypts session files. **If this key is lost, all encrypted session files become unrecoverable.** Store it in a secrets manager (Vault, AWS Secrets Manager, systemd credentials).

---

## Troubleshooting

### "Failed to bind" on startup

Port is already in use. Check `lsof -i :4944` and kill the conflicting process, or use `--port` to choose a different port.

### "TableDoesNotExist" on first read

This was a known bug, fixed in commit `979139b` (redb schema warm at startup). If you encounter this, ensure you're running a build from after that commit.

### WebSocket connection refused

- Check firewall rules: ports 4944 (WS) and 4945 (web UI) must be open
- Check `--ws-server true` is not set to `false`
- Check TLS configuration: if `--cert-path` is set, `--key-path` must also be set

### Peers not syncing

1. Check `RUST_LOG=debug` for routing decisions
2. Verify `--peers` URLs are reachable: `websocat wss://peer-url/ws`
3. Check that both peers have the same `allow_public_space` setting — mismatched settings can cause data rejection
4. Verify the `Dup` table isn't too aggressive — messages expire after 9s by default

### WebRTC connection failures

1. Ensure `--features webrtc` was used during build
2. Check STUN server reachability: `stun:stun.l.google.com:19302` must be accessible
3. Check firewall allows UDP — WebRTC uses UDP for data channels
4. Check `RUST_LOG=debug` for ICE candidate exchange and DTLS handshake logs
