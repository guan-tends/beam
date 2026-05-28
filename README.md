# Rod

> **BEAM Maintained Fork** — v0.2.5 | Rust 2024 | MSRV 1.85

Rust Object Database.

Rod powers decentralized graph synchronization. For an example application built on Rod, see [Iris-messenger](https://github.com/irislib/iris-messenger) (upstream demo, not included in BEAM).

## Use

Install [Rust](https://doc.rust-lang.org/book/ch01-01-installation.html) first.

### Install & run

```
cargo install rod
rod start --redb-storage --redb-path my-node.redb
```

With memory (ephemeral, for testing):
```
rod start --memory-storage
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

With disk-backed (`RedbStorage`) and flush for durability:
```rust
use rod::{Node, Config, Value};
use rod::adapters::RedbStorage;

#[tokio::main]
async fn main() {
    let config = Config::default();
    let redb = RedbStorage::new_with_config(config.clone(), "rod.redb");
    let mut db = Node::new_with_config(config, vec![Box::new(redb)], vec![]);

    db.get("greeting").put("Hello World!".into());
    db.flush_storage(Some(Duration::from_secs(5))).await.unwrap();
    
    if let Value::Text(str) = db.get("greeting").once(None).await.unwrap() {
        println!("{}", &str);
    }
    db.stop();
}
```

## Status

**BEAM Maintained Fork** (2026-05-18): This is a maintained fork for the Mnemos agent memory system. Key divergences from upstream: redb replaces sled as default storage, flush protocol guarantees durability, edition 2024 / rust-version 1.85.

Original status (15/5/2022):

- [x] Basic graph primitives (get/put/on/map/set)
- [x] CLI for running the server (`rod start`)
- [x] Incoming websockets
- [x] Outgoing websockets (`PEERS=wss://...`)
- [x] Multicast (65KB size limit)
- [x] In-memory storage
- [x] **Disk storage via [redb](https://github.com/cberner/redb)** (ACID, MVCC, default)
- [x] Disk storage via [sled.rs](https://sled.rs) (legacy, deprecated)
- [x] TLS support (`CERT_PATH`, `KEY_PATH`)
- [x] Advanced deduplication of network messages
- [x] Publish & subscribe
- [x] Hash verification (`#` namespace)
- [x] Signature verification (`~` namespace)
- [x] **Flush protocol** — `db.flush_storage()` guarantees fsync durability
- [x] **BEAM SEA** — Encryption, session storage, keypair management
- [ ] Encryption for P2P message relay (not yet; client-side only)

### SEA.certify — Capability Certificates

SEA now supports capability certificates for delegated authorization.
An authority signs a certificate naming specific certificants and optional
policies (read, write, expiry). Others verify the certificate against the
authority's public key before trusting the delegation.

```rust
use rod::sea::{certify, verify_certificate, generate_pair};
use serde_json::json;

#[tokio::main]
async fn main() {
    let authority = generate_pair().await.unwrap();
    let alice = generate_pair().await.unwrap();

    let policies = Some(json!({"e": 9999999999999.0, "r": ".*", "w": "skills/"}));
    let certificants = vec![alice.pub_key.clone()];
    let signed = certify(&certificants, policies.as_ref(), &authority).await.unwrap();

    // Verifier side
    let payload = verify_certificate(&signed, &authority.pub_key).unwrap();
    assert!(payload["c"].as_array().unwrap().contains(&alice.pub_key.into());
}
```

### Requirements

- Rust ≥ 1.85 (edition 2024)
- `BEAM_SEA_SESSION_KEY` for production session encryption

### Issues

- Multicast doesn't relay large messages like social posts with photos

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

### User API Reference

`rod::sea::User` is the authenticated user handle. It wraps `Arc<RwLock<SessionState>>` so all clones share the same session — `leave()` on any clone invalidates all of them.

| Method | Async | Description |
|--------|-------|-------------|
| `User::create(alias, pass, node)` | ✅ | Generates P-256 keypair, encrypts auth data, stores at `~@{alias}` in Rod |
| `User::auth(alias, pass, node)` | ✅ | Derives proof from alias+password, decrypts stored auth, returns User |
| `User::recall(alias, storage)` | ✅ | Loads cached keypair from session storage (no network, no password) |
| `User::from_pair(pair, alias?)` | ❌ | Constructs User directly from an existing `KeyPair` |
| `user.save_to(storage)` | ✅ | Encrypts and persists current keypair to session storage |
| `user.pub_key()` | ❌ | Clone of signing pubkey string (`~`-prefixed base64) |
| `user.pair()` | ❌ | Clone of full `KeyPair` struct (pub, priv, epub, epriv) |
| `user.alias()` | ❌ | `Some(alias)` or `None` |
| `user.is_authenticated()` | ❌ | `true` until `leave()` called |
| `user.leave()` | ❌ | Zeroizes keys and marks unauthenticated (all clones die) |

**Builder-style (via `Node`):**
```rust
// Create:  db.user().create("alice", "pass").await
// Auth:    db.user().auth("alice", "pass").await
```

### Crypto Primitives

| Function | Async | Description |
|----------|-------|-------------|
| `rod::sea::generate_pair()` | ✅ | Fresh P-256 signing + encryption keypair |
| `rod::sea::sign(data, pair)` | ✅ | ECDSA-P256-SHA256 sign a JSON value |
| `rod::sea::verify(signed, pub_key)` | ✅ | Verify ECDSA signature (sync wrapper) |
| `rod::sea::verify_async(signed, pub_key)` | ✅ | Verify without blocking async executor |
| `rod::sea::encrypt(data, pair, their_epub?)` | ✅ | AES-GCM encrypt (self-encrypt if `their_epub` is `None`) |
| `rod::sea::decrypt(encrypted, pair, their_epub?)` | ✅ | AES-GCM decrypt |
| `rod::sea::secret(their_epub, pair)` | ✅ | ECDH shared secret derivation |
| `rod::sea::work(data, salt?, opts)` | ✅ | PBKDF2 (100K iters, SHA-256) or SHA-256 hash |

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
  ├── InMemorySessionStorage         (ephemeral, test-safe)
  ├── EncryptedFileSessionStorage    (production, AES-256-GCM, default path)
  └── EncryptedFileSessionStorage::with_session_dir(path)  (production, explicit path)
```
