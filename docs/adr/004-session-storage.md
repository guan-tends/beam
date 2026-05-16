# ADR-004: BEAM SEA Session Storage Architecture

**Status:** Accepted  
**Date:** 2026-05-16  
**Authors:** Guan (The Keeper of the Threshold), Freeman King  
**Supersedes:** Gun.js plaintext `sessionStorage.pair` approach

---

## Context

Gun.js SEA's `recall()` stores the full keypair as **plaintext JSON** in `sessionStorage.pair`. This is acknowledged by Mark Nadal as a known limitation in the browser context.

BEAM SEA operates in a server/MCP-agent context where:
- Processes run for days or weeks
- Multiple MCP clients may share or hand off sessions
- Session persistence is a feature, not a browser quirk
- Security requirements exceed browser sandbox assumptions

We needed a production-grade session storage system that:
1. Encrypts keypairs at rest (not plaintext)
2. Zeroizes keys in memory on logout
3. Invalidates ALL clones when a user calls `leave()`
4. Supports both ephemeral (test) and durable (prod) backends
5. Is entirely optional — missing config = safe silent failure

---

## Decision

### 1. `Arc<RwLock<SessionState>>` for Shared Invalidation

`User` moved from a plain struct to `Arc<RwLock<SessionState>>`. This means:
- `User::clone()` is cheap (Arc bump) AND correct (shared state)
- `User::leave()` zeroizes `priv_key`/`epriv_key` and sets `is_authenticated = false`
- **All clones see the invalidation immediately** — no clone-after-leave security hole

### 2. Async `SessionStorage` Trait

```rust
#[async_trait]
pub trait SessionStorage: Send + Sync {
    async fn save(&self, alias: &str, pair: &KeyPair) -> Result<(), SeaError>;
    async fn load(&self, alias: &str) -> Result<Option<KeyPair>, SeaError>;
    async fn clear(&self, alias: &str) -> Result<(), SeaError>;
}
```

- Async by default — tokio runtime, non-blocking file I/O (`tokio::fs`)
- `Send + Sync` — safe for multi-threaded MCP agent concurrency
- Returns `Option<KeyPair>` — missing session is `None`, not an error

### 3. Two Backends: InMemory + EncryptedFile

| Backend | Use Case | Persistence |
|---------|----------|-------------|
| `InMemorySessionStorage` | Tests, ephemeral agents | Process lifetime only |
| `EncryptedFileSessionStorage` | Production MCP servers, long-lived daemons | AES-256-GCM encrypted files |

### 4. Master Key from Environment

- `BEAM_SEA_SESSION_KEY` — base64-encoded 32-byte AES key
- Generated via `beam-sea-keygen` binary (CSPRNG via `rand::thread_rng()`)
- Missing env var = sessions don't persist (safe fallback, no panic)
- Never hardcoded, never committed to version control

### 5. Expiry with Automatic Reaping

- Default: 30 days (`BEAM_SEA_SESSION_EXPIRY_DAYS` overrides)
- `load()` checks `expires_at < now()` → deletes expired file → returns `None`
- No background daemon needed; reaping happens on access

### 6. Caller-Side "Remember" Pattern

No `remember: bool` parameter on `auth()`. Instead:

```rust
let user = db.user().auth("alice", "pass").await?;  // authenticate
user.save_to(&storage).await?;                       // optional: persist
```

This keeps `auth()` simple and makes persistence **explicit** at the call site.

---

## Threat Model

| Threat | Gun.js Approach | BEAM Approach | Confidence |
|--------|----------------|---------------|------------|
| Session file theft | N/A (browser) | AES-256-GCM with env key | High |
| Memory dump after logout | Keys persist until GC | Zeroized on `leave()`/`Drop` | Medium (best effort) |
| Clone survives logout | N/A (single object) | Closed by `Arc<RwLock>` | High |
| Key committed to git | N/A (browser-local) | Env var only; `beam-sea-keygen` | High |
| Session replay | Reusable keypair | Expiry reaping on load | High |
| Process crash leaves keys | N/A (browser) | `Drop` triggers zeroize | Medium |

**Known Limitations:**
- `String.zeroize()` clears length but cannot guarantee heap scrubbing (allocator-dependent)
- Session files readable by user running process (Unix permissions 0700 mitigate)
- No forward secrecy — same master key encrypts all sessions

---

## Consequences

### Positive
- Production-grade session security beyond browser assumptions
- Zero key configuration = safe silent failure (no surprises)
- Both backends share the same trait → swap at init time
- `Arc<RwLock>` closes real security hole in multi-threaded Rust

### Negative
- `beam-sea-keygen` required for production deployments
- Single master key per deployment (no per-user key derivation)
- `RwLock` adds contention on `User` access (acceptable for auth-heavy, not crypto-heavy patterns)
- `async-trait` dynamic dispatch overhead (negligible for I/O-bound session ops)

---

## DevOps Guide

### Generate a Key

```bash
cargo run --quiet --bin beam-sea-keygen
# Output: qGmUkJ5mZSg45XVzMHOKH9IxiamPI5wmqIAnwzASr/M=
```

### Systemd Service

```ini
[Unit]
Description=BEAM MCP Agent

[Service]
Environment="BEAM_SEA_SESSION_KEY=qGmUkJ5mZSg45XVzMHOKH9IxiamPI5wmqIAnwzASr/M="
Environment="BEAM_SEA_SESSION_EXPIRY_DAYS=7"
ExecStart=/usr/local/bin/my-beam-agent
ProtectHome=true
ReadWritePaths=%h/.config/beam/sessions
```

### Docker Compose

```yaml
services:
  beam-agent:
    image: my-beam-agent:latest
    environment:
      - BEAM_SEA_SESSION_KEY=${BEAM_SEA_SESSION_KEY}
      - BEAM_SEA_SESSION_EXPIRY_DAYS=14
    volumes:
      - beam-sessions:/root/.config/beam/sessions

volumes:
  beam-sessions:
```

### Kubernetes Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: beam-session-key
stringData:
  BEAM_SEA_SESSION_KEY: "qGmUkJ5mZSg45XVzMHOKH9IxiamPI5wmqIAnwzASr/M="
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: beam-agent
spec:
  template:
    spec:
      containers:
      - name: agent
        envFrom:
        - secretRef:
            name: beam-session-key
```

### Key Rotation

1. Generate new key: `cargo run --quiet --bin beam-sea-keygen`
2. Update deployment secret
3. Restart service (existing sessions invalidated — users re-auth)
4. Clear old session directory: `rm -rf ~/.config/beam/sessions/*`

---

## References

- Gun.js SEA `recall()`: https://gun.eco/docs/SEA#user
- `zeroize` crate: https://docs.rs/zeroize
- `dirs` crate (XDG config): https://docs.rs/dirs
- AES-GCM: NIST SP 800-38D
