# Rod

Rust Object Database.

The decentralized social networking application [Iris-messenger](https://github.com/irislib/iris-messenger) syncs over Rod peers by default.

## Use

Install [Rust](https://doc.rust-lang.org/book/ch01-01-installation.html) first.

### Install & run

```
cargo install rod
rod start
```

### Library

```rust
use rod::{Node, Config, Value};
use rod::adapters::*;

#[tokio::main]
async fn main() {
    let config = Config::default();
    let ws_client = OutgoingWebsocketManager::new(
        config.clone(),
        vec!["ws://localhost:4944/ws".to_string()],
    );
    let mut db = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_client)]);

    let mut sub = db.get("greeting").on();
    db.get("greeting").put("Hello World!".into());
    if let Value::Text(str) = sub.recv().await.unwrap() {
        assert_eq!(&str, "Hello World!");
        println!("{}", &str);
    }
    db.stop();
}
```

## Status

15/5/2022:

- [x] Basic 
- [x] CLI for running the server
- [x] Incoming websockets
- [x] Outgoing websockets (env PEERS=wss://some-server-url.herokuapp.com/ws)
- [x] Multicast (currently size limited to 65KB — large photos in messages will not sync over it)
- [x] In-memory storage
- [x] TLS support (env CERT_PATH and KEY_PATH)
- [x] Advanced deduplication of network messages
- [x] Publish & subscribe (network messages only relayed to relevant peers)
- [x] Disk storage ([sled.rs](https://sled.rs))
- [x] Hash verification for content-addressed data (`db.get('#').get(data_hash).put(data)`)
- [x] Signature verification of user data (`db.get('~' + pubkey).get('profile') ...`)
- [ ] Encryption & decryption (usually not needed on the server, but used on the client side in js, like [iris](https://github.com/iris-lib/iris-messenger) private messaging)

### Issues

- Multicast doesn't relay large messages like Iris posts with photos

## Develop

```
cargo install cargo-watch
RUST_LOG=debug cargo watch -x 'run -- start'
```

```
cargo test
```

Watch for code changes and re-run tests that contain the word "stats":
```
RUST_LOG=debug cargo watch -x 'test stats'
```

```
cargo bench
```

## Run on Heroku

```
heroku create --buildpack emk/rust
git push heroku master
```

or:

[![Deploy](assets/herokubutton.svg)](https://heroku.com/deploy?template=https://github.com/mmalmi/rod)

---

## BEAM SEA: Authentication, Encryption & Authorization

BEAM SEA provides Gun.js-compatible user authentication with production-grade session security for Rust server environments.

### Quick Start

```rust
use rod::{Node, Value};
use rod::sea::session::{EncryptedFileSessionStorage, InMemorySessionStorage};
use rod::sea::User;

#[tokio::main]
async fn main() {
    let mut db = Node::new();

    // Create a user
    let user = db.user().create("alice", "secret123").await.unwrap();
    println!("Created: {}", user.pub_key());

    // Authenticate later
    let auth = db.user().auth("alice", "secret123").await.unwrap();
    assert!(auth.is_authenticated());

    // Log out (invalidates all clones)
    auth.leave();
    assert!(!auth.is_authenticated());
}
```

### Session Persistence (Recall)

Unlike Gun.js SEA's plaintext `sessionStorage`, BEAM encrypts session files with AES-256-GCM using a master key from your environment:

```rust
use rod::sea::session::EncryptedFileSessionStorage;
use rod::sea::User;

#[tokio::main]
async fn main() {
    let storage = EncryptedFileSessionStorage::new().unwrap();

    // After authenticating, save the session
    let user = db.user().auth("alice", "secret123").await.unwrap();
    user.save_to(&storage).await.unwrap();

    // Later, recall without re-typing password
    let recalled = User::recall("alice", &storage).await.unwrap();
    assert!(recalled.is_authenticated());
}
```

### Deployment: Generating the Session Key

**Required for production deployments.** The session key is never hardcoded or committed to git.

```bash
# Generate a fresh 32-byte key
cargo run --quiet --bin beam-sea-keygen
# → qGmUkJ5mZSg45XVzMHOKH9IxiamPI5wmqIAnwzASr/M=
```

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BEAM_SEA_SESSION_KEY` | **Yes** (for file storage) | — | Base64-encoded 32-byte AES master key. Generate with `beam-sea-keygen`. |
| `BEAM_SEA_SESSION_EXPIRY_DAYS` | No | 30 | Session file lifetime in days. Expired files are reaped on load. |

**Safe fallback:** If `BEAM_SEA_SESSION_KEY` is unset, `EncryptedFileSessionStorage` silently skips persistence — no panic, no crash. This is intentional for development and test environments.

### DevOps Examples

#### systemd (with `ProtectHome`)

```ini
[Service]
Environment="BEAM_SEA_SESSION_KEY=$(/usr/local/bin/beam-sea-keygen)"
Environment="BEAM_SEA_SESSION_EXPIRY_DAYS=7"
ExecStart=/usr/local/bin/my-beam-agent
ReadWritePaths=%h/.config/beam/sessions
```

#### Docker Compose

```yaml
services:
  agent:
    image: my-beam-agent
    environment:
      BEAM_SEA_SESSION_KEY: ${BEAM_SEA_SESSION_KEY}
    volumes:
      - beam-sessions:/root/.config/beam/sessions
volumes:
  beam-sessions:
```

#### Kubernetes (secret + deployment)

```bash
# Generate and create secret
kubectl create secret generic beam-session-key \
  --from-literal=BEAM_SEA_SESSION_KEY="$(cargo run --quiet --bin beam-sea-keygen)"
```

```yaml
# deployment.yaml snippet
spec:
  containers:
  - name: beam-agent
    envFrom:
    - secretRef:
        name: beam-session-key
```

### Security Model

| Feature | Implementation |
|---------|---------------|
| **At rest** | AES-256-GCM encrypted JSON files in `~/.config/beam/sessions/` |
| **In memory** | `zeroize` crate scrubs private keys on `User::leave()` and `Drop` |
| **Clone invalidation** | `Arc<RwLock<SessionState>>` — all clones die when `leave()` is called |
| **Expiry** | Automatic reaping on `load()`; no background daemon needed |
| **Permissions** | Session directory created with `0700` (owner-only) |

For full architecture details, see `docs/adr/004-session-storage.md`.

### Architecture

```
rod::sea::User          ← Arc<RwLock<SessionState>> ← shared invalidation
       │
       ├── save_to(storage) ──→ SessionStorage::save(alias, KeyPair)
       │
       ├── recall(alias, storage) ←── SessionStorage::load(alias)
       │
       └── leave() ──→ zeroize priv/epriv keys → is_authenticated = false

SessionStorage trait:
  ├── InMemorySessionStorage      (ephemeral, test-safe)
  └── EncryptedFileSessionStorage (production, AES-256-GCM)
```
