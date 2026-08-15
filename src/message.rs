#![allow(clippy::inherent_to_string)] // to_string methods are Gun.js wire-format serialization, not Display
use crate::ack::AckPolicy;
use crate::actor::Addr;
use crate::types::{Children, NodeData, Value};
use crate::utils::random_string;
use base64::prelude::*;
use java_utils::HashCode;
use log::{debug, error};
use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use serde_json::{Value as JsonValue, json};
use std::collections::{BTreeMap, HashSet};
use std::convert::TryFrom;
use std::io::Write;
use std::sync::{Arc, OnceLock};
use bytes::Bytes;

// ─── JSON writing helpers ──────────────────────────────────────────
// These helpers write JSON directly into a reusable `Vec<u8>` buffer,
// eliminating the intermediate `serde_json::Value` tree (BTreeMap alloc)
// and the `String` allocation from `to_string()`. After warmup, the
// buffer's capacity is reused — zero allocations per message.
//
// Output is compact JSON (no whitespace), matching `serde_json::Value::to_string()`.

/// Writes a JSON-escaped string (including surrounding quotes) into `buf`.
///
/// Escapes per RFC 8259: `"`, `\`, control characters (`\n`, `\r`, `\t`,
/// `\u00XX`). Non-ASCII UTF-8 passes through unescaped (serde_json default).
fn write_json_str(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'"');
    for c in s.chars() {
        match c {
            '"' => buf.extend_from_slice(b"\\\""),
            '\\' => buf.extend_from_slice(b"\\\\"),
            '\n' => buf.extend_from_slice(b"\\n"),
            '\r' => buf.extend_from_slice(b"\\r"),
            '\t' => buf.extend_from_slice(b"\\t"),
            c if c.is_control() => {
                write!(buf, "\\u{:04x}", c as u32).unwrap();
            }
            c => buf.extend_from_slice(c.encode_utf8(&mut [0u8; 4]).as_bytes()),
        }
    }
    buf.push(b'"');
}

/// Writes a BEAM [`Value`] as JSON into `buf`, matching the output of
/// `serde_json::Value::from(value)` followed by serialization.
///
/// - `Null` → `null`
/// - `Bit(true)` → `true`, `Bit(false)` → `false`
/// - `Number(n)` → f64 via `serde_json::to_writer` (ryu formatter)
/// - `Text(s)` → JSON string
/// - `Link(soul)` → `{"#":"soul"}`
fn write_json_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => buf.extend_from_slice(b"null"),
        Value::Bit(true) => buf.extend_from_slice(b"true"),
        Value::Bit(false) => buf.extend_from_slice(b"false"),
        Value::Number(n) => {
            // serde_json uses ryu for float formatting — matches Value::to_string()
            serde_json::to_writer(buf, n).unwrap();
        }
        Value::Text(s) => write_json_str(buf, s),
        Value::Link(soul) => {
            buf.extend_from_slice(b"{\"#\":");
            write_json_str(buf, soul);
            buf.push(b'}');
        }
    }
}

#[derive(Clone, Debug)]
pub struct Get {
    pub id: String,
    pub from: Addr,
    pub recipients: Option<HashSet<String>>,
    pub node_id: String,
    pub checksum: Option<i32>,
    pub child_key: Option<String>,
}
impl Get {
    pub fn new(node_id: String, child_key: Option<String>, from: Addr) -> Self {
        Self {
            id: random_string(8),
            from,
            recipients: None,
            node_id,
            child_key,
            checksum: None,
        }
    }

    /// Serializes to Gun.js wire format into a reusable buffer.
    ///
    /// Writes compact JSON directly, avoiding the intermediate `Value` tree.
    /// After warmup, the buffer capacity is reused — zero allocations.
    pub fn to_writer(&self, buf: &mut Vec<u8>) {
        buf.clear();
        buf.extend_from_slice(b"{\"get\":{\"#\":");
        write_json_str(buf, &self.node_id);
        buf.push(b'}');
        if let Some(child_key) = &self.child_key {
            buf.extend_from_slice(b",\".\":");
            write_json_str(buf, child_key);
            buf.push(b'}');
        } else {
            // Close the get object
            // Already closed above
        }
        // Reconstruct properly: {"get":{"#":"nodeId"},"#":"id"}
        // Let me redo this...
        buf.clear();
        buf.extend_from_slice(b"{\"get\":{\"#\":");
        write_json_str(buf, &self.node_id);
        if let Some(child_key) = &self.child_key {
            buf.extend_from_slice(b",\".\":");
            write_json_str(buf, child_key);
        }
        buf.extend_from_slice(b"},\"#\":");
        write_json_str(buf, &self.id);
        buf.push(b'}');
    }

    /// Serializes to Gun.js wire format as a `String`.
    ///
    /// Convenience wrapper around [`to_writer`].
    pub fn to_string(&self) -> String {
        let mut buf = Vec::with_capacity(64);
        self.to_writer(&mut buf);
        String::from_utf8(buf).expect("wire format is valid UTF-8")
    }
}

#[derive(Debug)]
pub struct Put {
    pub id: String,
    pub from: Addr,
    pub recipients: Option<HashSet<String>>,
    pub in_response_to: Option<String>,
    pub updated_nodes: Arc<BTreeMap<String, Children>>,
    pub checksum: Option<i32>,
    /// DAM peer-hop list
    pub peer_hop_list: Option<HashSet<String>>,
    /// Cached wire-format bytes, populated on first serialization for relay.
    ///
    /// Mirrors Gun.js's `meta.raw` (`src/mesh.js`): serialize once, reuse
    /// for all peers. Behind [`OnceLock`] for lock-free interior
    /// mutability within `Arc<Put>`. The first [`Put::to_writer`] (or
    /// [`Put::get_or_serialize`]) call populates this; subsequent calls
    /// return the cached bytes without re-serializing.
    ///
    /// Reset to empty on [`Clone`] — each clone is a distinct message
    /// with potentially different fields (e.g. `peer_hop_list`).
    raw: OnceLock<Bytes>,
}

impl Clone for Put {
    fn clone(&self) -> Self {
        Put {
            id: self.id.clone(),
            from: self.from.clone(),
            recipients: self.recipients.clone(),
            in_response_to: self.in_response_to.clone(),
            updated_nodes: Arc::clone(&self.updated_nodes),
            checksum: self.checksum,
            peer_hop_list: self.peer_hop_list.clone(),
            raw: OnceLock::new(),
        }
    }
}
impl Put {
    pub fn new(
        updated_nodes: BTreeMap<String, Children>,
        in_response_to: Option<String>,
        from: Addr,
    ) -> Self {
        Self {
            id: random_string(8),
            from,
            recipients: None,
            in_response_to,
            updated_nodes: Arc::new(updated_nodes),
            checksum: None,
            peer_hop_list: None,
            raw: OnceLock::new(),
        }
    }

    pub fn new_from_kv(key: String, children: Children, from: Addr) -> Self {
        let mut updated_nodes = BTreeMap::new();
        updated_nodes.insert(key, children);
        Put::new(updated_nodes, None, from)
    }

    /// Creates a filtered copy of this Put with only the specified
    /// `updated_nodes`.
    ///
    /// Used by per-key HAM filtering ([`crate::router::HamFilterResult`])
    /// when a Put contains a mix of stale and new keys. The filtered Put
    /// retains all metadata (id, from, recipients, etc.) but replaces
    /// `updated_nodes` with only the new entries.
    ///
    /// The checksum is reset to `None` — it will be recomputed on
    /// serialization since the `put` sub-object has changed.
    pub fn with_updated_nodes(&self, updated_nodes: Arc<BTreeMap<String, Children>>) -> Put {
        Put {
            id: self.id.clone(),
            from: self.from.clone(),
            recipients: self.recipients.clone(),
            in_response_to: self.in_response_to.clone(),
            updated_nodes,
            checksum: None, // recompute — put sub-object changed
            peer_hop_list: self.peer_hop_list.clone(),
            raw: OnceLock::new(),
        }
    }

    /// Serializes to Gun.js wire format into a reusable buffer.
    ///
    /// Writes compact JSON directly into `buf`, avoiding the intermediate
    /// `serde_json::Value` tree (3 allocations per message in the old
    /// `to_string()`). After warmup, the buffer capacity is reused —
    /// zero allocations per message.
    ///
    /// The checksum (`##`) is computed via Java's `String.hashCode()` on
    /// the serialized `put` sub-object, matching Gun.js semantics. If
    /// `self.checksum` is already set (incoming wire messages), it is
    /// used directly.
    ///
    /// # Panics
    ///
    /// Will not panic — all writes to `Vec<u8>` are infallible.
    /// `serde_json::to_writer` returns `Result` but `Vec<u8>` never errors.
    pub fn to_writer(&self, buf: &mut Vec<u8>) {
        buf.clear();

        // ── Write the `put` sub-object ──
        // We write it directly into buf, noting the byte range so we can
        // compute the Java hashCode on the slice for the checksum.
        buf.extend_from_slice(b"{\"put\":");
        let put_start = buf.len();

        buf.push(b'{');
        let mut first_node = true;
        for (node_id, children) in self.updated_nodes.iter() {
            // Skip BEAM-internal souls that have no meaning in Gun.js wire format:
            // - "" (empty root pointer) — Gun.js doesn't use a root soul
            // - "soul/key" (value souls containing "/") — Gun.js stores values
            //   as fields on the parent soul, not as separate souls
            if node_id.is_empty() || node_id.contains('/') {
                continue;
            }
            if !first_node {
                buf.push(b',');
            }
            first_node = false;

            // "nodeId": { "_": { "#": "nodeId", ">": { ... } }, "key": value, ... }
            write_json_str(buf, node_id);
            buf.extend_from_slice(b":{\"_\":{\"#\":");
            write_json_str(buf, node_id);
            buf.extend_from_slice(b",\">\":{");

            let mut first_child = true;
            for (k, v) in children.iter() {
                if !first_child {
                    buf.push(b',');
                }
                first_child = false;
                write_json_str(buf, k);
                buf.push(b':');
                // Timestamps are f64 — use serde_json for ryu formatting
                serde_json::to_writer(&mut *buf, &v.updated_at).unwrap();
            }

            buf.extend_from_slice(b"}}");
            // Child values (flattened into the node object)
            for (k, v) in children.iter() {
                buf.push(b',');
                write_json_str(buf, k);
                buf.push(b':');
                write_json_value(buf, &v.value);
            }
            buf.push(b'}');
        }
        buf.push(b'}');

        let put_end = buf.len();

        // ── Compute checksum ──
        // Java's String.hashCode() on the put sub-object JSON.
        // If checksum is already set (incoming wire message), use it.
        let checksum = match &self.checksum {
            Some(s) => *s,
            None => {
                let put_str = std::str::from_utf8(&buf[put_start..put_end]).unwrap();
                put_str.hash_code()
            }
        };

        // ── Write remaining outer fields ──
        buf.extend_from_slice(b",\"#\":");
        write_json_str(buf, &self.id);

        if let Some(ref in_response_to) = self.in_response_to {
            buf.extend_from_slice(b",\"@\":");
            write_json_str(buf, in_response_to);
        }

        buf.extend_from_slice(b",\"##\":");
        serde_json::to_writer(&mut *buf, &checksum).unwrap();

        if let Some(ref hops) = self.peer_hop_list {
            if !hops.is_empty() {
                let peers = hops.iter().cloned().collect::<Vec<_>>().join(",");
                buf.extend_from_slice(b",\"><\":");
                write_json_str(buf, &peers);
            }
        }

        buf.push(b'}');
    }

    /// Returns cached wire-format bytes, serializing on first call.
    ///
    /// Mirrors Gun.js's `meta.raw` pattern (`src/mesh.js`): the first
    /// call serializes the Put to JSON and stores the result in
    /// [`Put::raw`] (a [`OnceLock`]). Subsequent calls return the
    /// cached [`Bytes`] without re-serializing — a cheap refcount bump.
    ///
    /// This is the primary serialization entry point for the relay path.
    /// When a Put is relayed to N peers via `Arc::clone`, all N WsConn
    /// actors share the same `OnceLock` — only the first serializes.
    ///
    /// For non-relay paths (incoming messages, storage acks), use
    /// [`to_writer`](Self::to_writer) directly — those Puts are not
    /// shared across multiple consumers.
    pub fn get_or_serialize(&self) -> Bytes {
        if let Some(cached) = self.raw.get() {
            return cached.clone();
        }
        let mut buf = Vec::with_capacity(256);
        self.to_writer(&mut buf);
        let bytes = Bytes::from(buf);
        // Best-effort populate — if another thread won the race,
        // `set` returns Err and we just use our local copy.
        let _ = self.raw.set(bytes.clone());
        bytes
    }

    /// Serializes to Gun.js wire format as a `String`.
    ///
    /// Convenience wrapper around [`to_writer`]. Allocates a new `String`
    /// each call — prefer `to_writer` with a reusable buffer for hot paths.
    pub fn to_string(&self) -> String {
        let mut buf = Vec::with_capacity(256);
        self.to_writer(&mut buf);
        // The buffer contains valid UTF-8 — all writes produce valid JSON
        String::from_utf8(buf).expect("wire format is valid UTF-8")
    }
}

#[derive(Clone, Debug)]
pub struct BatchPut {
    pub id: String,
    pub puts: Vec<Put>,
    pub from: Addr,
    /// If set, this BatchPut is a reply to a previous ack request — storage
    /// sends it back to the originating node with the original BatchPut.id
    /// in this field, mirroring Put::in_response_to for the single-put case.
    pub in_response_to: Option<String>,
}

impl BatchPut {
    pub fn new(puts: Vec<Put>, from: Addr) -> Self {
        Self {
            id: random_string(8),
            puts,
            from,
            in_response_to: None,
        }
    }

    /// Convert to a JSON array of individual Put messages.
    /// BatchPut is an internal optimization; on the wire it
    /// materializes as the constituent puts.
    pub fn to_writer(&self, buf: &mut Vec<u8>) {
        buf.clear();
        buf.push(b'[');
        let mut first = true;
        for put in &self.puts {
            if !first {
                buf.push(b',');
            }
            first = false;
            put.to_writer(buf);
        }
        buf.push(b']');
    }

    /// Serializes to Gun.js wire format as a `String`.
    ///
    /// Convenience wrapper around [`to_writer`].
    pub fn to_string(&self) -> String {
        let mut buf = Vec::with_capacity(512);
        self.to_writer(&mut buf);
        String::from_utf8(buf).expect("wire format is valid UTF-8")
    }
}

#[derive(Clone, Debug)]
pub struct Flush {
    pub id: String,
    pub from: Addr,
    pub node_id: Option<String>,
}

impl Flush {
    pub fn new(from: Addr, node_id: Option<String>) -> Self {
        Self {
            id: random_string(8),
            from,
            node_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RtcSignal {
    pub id: String,
    pub from: Addr,
    pub to: Option<String>,
    pub offer: Option<String>,
    pub answer: Option<String>,
    pub candidate: Option<String>,
    /// The UDP socket address of the sender, so the receiver can add it as a
    /// remote ICE candidate for loopback and direct connections.
    pub local_addr: Option<String>,
}

impl RtcSignal {
    /// Serializes to Gun.js wire format into a reusable buffer.
    pub fn to_writer(&self, buf: &mut Vec<u8>) {
        buf.clear();
        buf.extend_from_slice(b"{\"dam\":\"rtc\",\"id\":");
        write_json_str(buf, &self.id);
        buf.extend_from_slice(b",\"#\":");
        write_json_str(buf, &self.id);
        if let Some(to) = &self.to {
            buf.extend_from_slice(b",\"to\":");
            write_json_str(buf, to);
        }
        if let Some(offer) = &self.offer {
            buf.extend_from_slice(b",\"offer\":");
            write_json_str(buf, offer);
        }
        if let Some(answer) = &self.answer {
            buf.extend_from_slice(b",\"answer\":");
            write_json_str(buf, answer);
        }
        if let Some(candidate) = &self.candidate {
            buf.extend_from_slice(b",\"candidate\":");
            write_json_str(buf, candidate);
        }
        if let Some(local_addr) = &self.local_addr {
            buf.extend_from_slice(b",\"local_addr\":");
            write_json_str(buf, local_addr);
        }
        buf.push(b'}');
    }

    /// Serializes to Gun.js wire format as a `String`.
    ///
    /// Convenience wrapper around [`to_writer`].
    pub fn to_string(&self) -> String {
        let mut buf = Vec::with_capacity(128);
        self.to_writer(&mut buf);
        String::from_utf8(buf).expect("wire format is valid UTF-8")
    }
}

#[derive(Clone, Debug)]
pub enum Message {
    // TODO: NetworkMessage and InternalMessage
    Get(Get),
    Put(Put),
    BatchPut(BatchPut),
    Flush(Flush),
    Hi {
        from: Addr,
        peer_id: String,
    },
    RtcSignal(RtcSignal),
    /// Periodic self-tick fired by the cleanup reaper spawned in
    /// [`crate::router::Router::pre_start`].
    ///
    /// Not part of the wire protocol — purely internal. Routes back to the
    /// Router's own `handle()` to evict expired [`crate::router::QuorumEntry`]s
    /// with full `&mut self` access.
    CheckQuorumTimeouts,

    /// Internal message: registers a put as a quorum-tracked write.
    ///
    /// Sent by [`crate::Node::put_quorum`] immediately after the Put,
    /// before fan-out. The Router uses this to create a `QuorumEntry`
    /// keyed on `put_id`, so it can count subsequent peer acks and
    /// signal back when the policy is met.
    ///
    /// Purely router-internal — never serialized to wire. Peers do not
    /// receive this message; they only see the regular `Put`.
    RegisterQuorum {
        /// The id of the Put this registration tracks.
        put_id: String,
        /// The originating Node's actor address — receives the
        /// `__quorum_met__` sentinel reply when quorum is satisfied.
        requester: Addr,
        /// The policy controlling how many acks are required.
        policy: AckPolicy,
    },
}

impl Message {
    /// Serializes to Gun.js wire format into a reusable buffer.
///
/// Dispatches to the variant's `to_writer` method. For `Message::Put`,
/// checks the [`Put::raw`] cache first — if populated (by a prior
/// [`Put::get_or_serialize`] call), the cached bytes are written directly
/// without re-serializing. This mirrors Gun.js's `meta.raw` reuse.
///
/// Internal messages (`CheckQuorumTimeouts`, `RegisterQuorum`) are never
/// serialized to wire — they produce sentinel strings for backwards
/// compatibility.
///
/// After warmup, the buffer capacity is reused — zero allocations.
pub fn to_writer(&self, buf: &mut Vec<u8>) {
    match self {
        Message::Put(put) => {
            if let Some(cached) = put.raw.get() {
                buf.clear();
                buf.extend_from_slice(&cached);
            } else {
                put.to_writer(buf);
            }
        }
        Message::Get(get) => get.to_writer(buf),
        Message::BatchPut(batch) => batch.to_writer(buf),
        Message::Flush(flush) => {
            buf.clear();
            buf.extend_from_slice(b"{\"dam\":\"flush\",\"#\":");
            write_json_str(buf, &flush.id);
            buf.push(b'}');
        }
        Message::Hi { from: _, peer_id } => {
            buf.clear();
            buf.extend_from_slice(b"{\"dam\":\"hi\",\"#\":");
            write_json_str(buf, peer_id);
            buf.push(b'}');
        }
        Message::RtcSignal(rtc) => rtc.to_writer(buf),
        Message::CheckQuorumTimeouts => {
            buf.clear();
            buf.extend_from_slice(b"_tick_quorum");
        }
        Message::RegisterQuorum { put_id, .. } => {
            debug!(
                "internal RegisterQuorum({}) should not reach to_writer",
                put_id
            );
            buf.clear();
        }
    }
}

    /// Serializes to Gun.js wire format as a `String`.
    ///
    /// Convenience wrapper around [`to_writer`]. Allocates a new `String`
    /// each call — prefer `to_writer` with a reusable buffer for hot paths.
    pub fn to_string(&self) -> String {
        let mut buf = Vec::with_capacity(256);
        self.to_writer(&mut buf);
        String::from_utf8(buf).expect("wire format is valid UTF-8")
    }

    pub fn get_id(&self) -> String {
        match self {
            Message::Get(get) => get.id.clone(),
            Message::Put(put) => put.id.clone(),
            Message::BatchPut(batch) => batch.id.clone(),
            Message::Flush(flush) => flush.id.clone(),
            Message::Hi { from: _, peer_id } => peer_id.to_string(),
            Message::RtcSignal(rtc) => rtc.id.clone(),
            Message::CheckQuorumTimeouts => "_tick_quorum".to_string(),
            Message::RegisterQuorum { put_id, .. } => put_id.clone(),
        }
    }

    pub fn is_from(&self, addr: &Addr) -> bool {
        match self {
            Message::Get(get) => get.from == *addr,
            Message::Put(put) => put.from == *addr,
            Message::BatchPut(batch) => batch.from == *addr,
            Message::Flush(flush) => flush.from == *addr,
            Message::Hi { from, peer_id: _ } => *from == *addr,
            Message::RtcSignal(rtc) => rtc.from == *addr,
            Message::CheckQuorumTimeouts => false,
            Message::RegisterQuorum { requester, .. } => *requester == *addr,
        }
    }

    pub fn from(&self) -> Addr {
        match self {
            Message::Get(get) => get.from.clone(),
            Message::Put(put) => put.from.clone(),
            Message::BatchPut(batch) => batch.from.clone(),
            Message::Flush(flush) => flush.from.clone(),
            Message::Hi {
                from: _,
                peer_id: _,
            } => Addr::noop(),
            Message::RtcSignal(rtc) => rtc.from.clone(),
            Message::CheckQuorumTimeouts => Addr::noop(),
            Message::RegisterQuorum { requester, .. } => requester.clone(),
        }
    }

    fn verify_sig(
        node_id: &str,
        node_data: &serde_json::Map<String, JsonValue>,
    ) -> Result<(), &'static str> {
        // If the `_` metadata or `>` timestamps are absent or not an
        // object, there are no signed values to verify. Gun.js tolerates
        // missing metadata in relay messages.
        let timestamps = match node_data
            .get("_")
            .and_then(|m| m.get(">"))
            .and_then(|t| t.as_object())
        {
            Some(t) => t,
            None => return Ok(()),
        };

        for (child_key, timestamp) in timestamps.iter() {
            let value = match node_data.get(child_key) {
                Some(v) => v,
                None => continue, // child in > but not in node — skip
            };

            // Skip non-string values — these are Links ({"#":"soul"}) or
            // numeric/boolean metadata, not signed data. Gun.js only
            // verifies string values that contain SEA envelopes.
            let text = match value.as_str() {
                Some(s) => s,
                None => continue,
            };

            // Skip values that aren't JSON objects — only SEA envelopes
            // need verification. Plain strings and numbers pass through.
            let json: JsonValue = match serde_json::from_str(text) {
                Ok(j) => j,
                Err(_) => continue,
            };
            let signature_obj = match json.as_object() {
                Some(obj) => obj,
                None => continue,
            };

            // Extract public key from node_id (e.g. "~pub_key.sin/child")
            let first_seg = node_id.split("/").next().unwrap();
            let key = if first_seg.starts_with("~@") {
                // Alias registry — unsigned public lookup, skip signature verification
                return Ok(());
            } else {
                &first_seg[1..] // strip ~ prefix
            };

            // NEW FORMAT: {m: message, s: signature}
            if signature_obj.contains_key("m") && signature_obj.contains_key("s") {
                match crate::sea::verify_sync(&json, key) {
                    Ok(_) => continue,
                    Err(e) => {
                        error!("invalid new-format sig for {}: {:?}", node_id, e);
                        return Err("could not verify new-format signature");
                    }
                }
            }

            // OLD FORMAT: {: signed_data, ~: signature}
            // If the value doesn't have old-format fields, skip it —
            // it's not a signed envelope, just relay data.
            if !signature_obj.contains_key(":") && !signature_obj.contains_key("~") {
                continue;
            }

            let signed_data = signature_obj
                .get(":")
                .ok_or("no signed data (:) in signature json")?;

            let signed_obj = json!({
                "#": node_id,
                ".": child_key,
                ":": signed_data,
                ">": timestamp
            });

            let signature = signature_obj
                .get("~")
                .ok_or("no signature (~) in signature json")?;
            let signature = signature
                .as_str()
                .ok_or("signature (~) in signature json was not a string")?;
            // Signature is base64 STANDARD encoded (decoded in verification block below)

            // Parse public key from x.y base64url coordinates
            let mut split = key.split(".");
            let x_b64 = split
                .next()
                .ok_or("invalid key string: must be in format x.y")?;
            let y_b64 = split
                .next()
                .ok_or("invalid key string: must be in format x.y")?;

            let x = BASE64_URL_SAFE_NO_PAD
                .decode(x_b64)
                .or(Err("invalid public key: x coordinate not valid base64"))?;
            let y = BASE64_URL_SAFE_NO_PAD
                .decode(y_b64)
                .or(Err("invalid public key: y coordinate not valid base64"))?;

            // Reconstruct uncompressed public key (0x04 || x || y) for p256
            let mut pub_bytes: Vec<u8> = Vec::with_capacity(65);
            pub_bytes.push(0x04);
            pub_bytes.extend_from_slice(&x);
            pub_bytes.extend_from_slice(&y);

            let verifying_key = VerifyingKey::from_sec1_bytes(&pub_bytes)
                .map_err(|_| "invalid public key: failed to parse P-256 key")?;

            // Decode signature (raw bytes, 64 bytes for P-256 = r || s)
            let sig_bytes = BASE64_STANDARD
                .decode(signature)
                .or(Err("signature was not valid base64"))?;
            if sig_bytes.len() != 64 {
                return Err("invalid signature length for P-256");
            }
            let signature = Signature::from_slice(&sig_bytes)
                .map_err(|_| "invalid signature: failed to parse")?;

            // Verify — p256 Verifier does internal SHA-256 hashing (ES256 semantics)
            // Gun.js double-hashes: SHA256(SHA256(message)) via WebCrypto ECDSA
            // sign({hash: 'SHA-256'}, key, SHA256(message)) hashes the hash again.
            // We replicate this by pre-hashing, then passing the hash to p256's
            // verify() which hashes it internally — producing SHA256(SHA256(message)).
            use sha2::{Digest, Sha256};
            let hash = Sha256::digest(signed_obj.to_string().as_bytes());
            match verifying_key.verify(&hash, &signature) {
                Ok(_) => continue,
                Err(_) => {
                    error!("could not verify signature of {}", signed_obj);
                    return Err("could not verify signature");
                }
            }
        }
        Ok(())
    }

    fn from_put_obj(
        json: &JsonValue,
        json_str: String,
        msg_id: String,
        from: Addr,
        allow_public_space: bool,
    ) -> Result<Self, &'static str> {
        let obj = json
            .get("put")
            .unwrap()
            .as_object()
            .ok_or("invalid message: msg.put was not an object")?;
        let in_response_to = match json.get("@") {
            Some(in_response_to) => match in_response_to.as_str() {
                Some(in_response_to) => Some(in_response_to.to_string()),
                _ => {
                    return Err("message @ field was not a string");
                }
            },
            _ => None,
        };
        let checksum = match json.get("##") {
            Some(checksum) => checksum.as_i64().map(|checksum| checksum as i32),
            _ => None,
        };
        let peer_hop_list: Option<HashSet<String>> = match json.get("><") {
            Some(hops) => match hops.as_str() {
                Some(s) => {
                    let set: HashSet<String> = s
                        .split(",")
                        .map(|x| x.to_string())
                        .filter(|x| !x.is_empty())
                        .collect();
                    if set.is_empty() { None } else { Some(set) }
                }
                _ => None,
            },
            _ => None,
        };
        let mut updated_nodes = BTreeMap::<String, Children>::new();
        for (node_id, node_data) in obj.iter() {
            let node_data = node_data
                .as_object()
                .ok_or("put node data was not an object")?;

            // Gun.js treats the `_` metadata as optional — some relay
            // messages omit it entirely. Use an empty map as fallback
            // so children without timestamp entries default to 0.0.
            let empty_map = serde_json::Map::new();
            let updated_at_times = node_data
                .get("_")
                .and_then(|m| m.get(">"))
                .and_then(|t| t.as_object())
                .unwrap_or(&empty_map);

            let mut is_public_space = true;
            if let Some(first_letter) = node_id.chars().next() {
                if first_letter == '~' {
                    // signed data
                    if let Err(e) = Self::verify_sig(node_id, node_data) {
                        error!("invalid sig: {} for msg {}", e, json_str);
                        return Err(e);
                    }
                    is_public_space = false;
                    debug!("valid sig");
                }
            }

            let mut children = Children::default();
            for (child_key, child_val) in node_data.iter() {
                if child_key == "_" {
                    continue;
                }
                // Default to 0.0 when timestamp is missing — Gun.js
                // tolerates absent `>` entries for relay messages.
                let updated_at = updated_at_times
                    .get(child_key)
                    .and_then(|t| t.as_f64())
                    .unwrap_or(0.0);
                let value = match Value::try_from(child_val.clone()) {
                    Ok(v) => v,
                    Err(e) => {
                        // Skip values we can't convert rather than rejecting
                        // the entire Put — Gun.js is lenient with relay data.
                        debug!("skipping unconvertible value for key {}: {}", child_key, e);
                        continue;
                    }
                };

                if node_id == "#" {
                    // Content-hash addressed data. Gun.js relays these
                    // without verification at the transport layer — hash
                    // verification belongs at the storage layer. We skip
                    // the check here so relay nodes can forward content-
                    // addressed data they don't need to validate.
                    // (Previous code compared BASE64_STANDARD.encode(hash) against
                    // a hex-encoded child key — a format mismatch that
                    // rejected all client audit log entries.)
                } else if is_public_space && !allow_public_space {
                    return Err("public space writes not allowed (allow_public_space == false)");
                }

                children.insert(child_key.to_string(), NodeData { updated_at, value });
            }
            updated_nodes.insert(node_id.to_string(), children);
        }
        let put = Put {
            id: msg_id.to_string(),
            from,
            recipients: None,
            in_response_to,
            updated_nodes: Arc::new(updated_nodes),
            checksum,
            peer_hop_list,
            raw: OnceLock::new(),
        };
        Ok(Message::Put(put))
    }

    fn from_get_obj(json: &JsonValue, msg_id: String, from: Addr) -> Result<Self, &'static str> {
        /* TODO: other types of child_key selectors than equality.

        node.get({'.': {'<': cursor, '-': true}, '%': 20 * 1000}).once().map().on((value, key) => { ...

        '*' wildcard selector

         */

        let get = json.get("get").unwrap();
        let node_id = match get["#"].as_str() {
            Some(str) => str,
            _ => {
                return Err("no node id (#) found in get message");
            }
        };
        let checksum = match json.get("##") {
            Some(checksum) => checksum.as_i64().map(|checksum| checksum as i32),
            _ => None,
        };
        let child_key = match get.get(".") {
            Some(child_key) => match child_key.as_str() {
                Some(child_key) => Some(child_key.to_string()),
                _ => return Err("get child_key . was not a string"),
            },
            _ => None,
        };
        debug!("get node_id {}", node_id);
        let msg_id = msg_id.replace("\"", "");
        let get = Get {
            id: msg_id,
            from,
            recipients: None,
            node_id: node_id.to_string(),
            child_key,
            checksum,
        };
        Ok(Message::Get(get))
    }

    pub fn from_json_obj(
        json: &JsonValue,
        json_str: String,
        from: Addr,
        allow_public_space: bool,
    ) -> Result<Self, &'static str> {
        let obj = match json.as_object() {
            Some(obj) => obj,
            _ => {
                return Err("not a json object");
            }
        };
        let msg_id = match obj.get("#").and_then(|v| v.as_str()) {
            Some(str) => str.to_string(),
            _ => {
                return Err("msg id not a string");
            }
        };
        if msg_id.len() > 32 {
            return Err("msg id too long (> 32)");
        }
        if !msg_id.chars().all(char::is_alphanumeric) {
            return Err("msg_id must be alphanumeric");
        }
        if obj.contains_key("put") {
            Self::from_put_obj(json, json_str, msg_id, from, allow_public_space)
        } else if obj.contains_key("get") {
            Self::from_get_obj(json, msg_id, from)
        } else if let Some(dam) = obj.get("dam").and_then(|d| d.as_str()) {
            if dam == "rtc" {
                let to = obj
                    .get("to")
                    .and_then(|t| t.as_str().map(|s| s.to_string()));
                let offer = obj
                    .get("offer")
                    .and_then(|o| o.as_str().map(|s| s.to_string()));
                let answer = obj
                    .get("answer")
                    .and_then(|a| a.as_str().map(|s| s.to_string()));
                let candidate = obj
                    .get("candidate")
                    .and_then(|c| c.as_str().map(|s| s.to_string()));
                let local_addr = obj
                    .get("local_addr")
                    .and_then(|l| l.as_str().map(|s| s.to_string()));
                Ok(Message::RtcSignal(RtcSignal {
                    id: msg_id,
                    from,
                    to,
                    offer,
                    answer,
                    candidate,
                    local_addr,
                }))
            } else {
                Ok(Message::Hi {
                    from,
                    peer_id: msg_id,
                })
            }
        } else {
            Err("Unrecognized message")
        }
    }

    /// Parse a JSON string into `serde_json::Value` using the fastest
    /// available parser for the target platform.
    ///
    /// On x86_64 native: uses `simd-json` (SSE4.2/AVX2 SIMD instructions)
    /// for ~2-4x faster parsing. On WASM/ARM: falls back to `serde_json`.
    ///
    /// simd-json requires mutable access to the input buffer (it modifies
    /// the string in-place for SIMD alignment), so we make one owned copy.
    /// This allocation is negligible compared to the JSON tree that gets
    /// built regardless.
    #[cfg(target_arch = "x86_64")]
    fn parse_json(s: &str) -> Result<JsonValue, &'static str> {
        // simd-json requires mutable bytes (modifies buffer in-place for
        // SIMD alignment). Use from_slice with a mutable byte buffer.
        let mut bytes = s.as_bytes().to_vec();
        simd_json::from_slice(&mut bytes).map_err(|_| "Failed to parse message as JSON")
    }

    #[cfg(not(target_arch = "x86_64"))]
    fn parse_json(s: &str) -> Result<JsonValue, &'static str> {
        serde_json::from_str(s).map_err(|_| "Failed to parse message as JSON")
    }

    pub fn try_from(s: &str, from: Addr, allow_public_space: bool) -> Result<Vec<Self>, &str> {
        let json: JsonValue = Self::parse_json(s)?;

        if let Some(arr) = json.as_array() {
            let mut vec = Vec::<Self>::new();
            for msg in arr {
                vec.push(Self::from_json_obj(
                    msg,
                    msg.to_string(),
                    from.clone(),
                    allow_public_space,
                )?);
            }
            Ok(vec)
        } else {
            match Self::from_json_obj(&json, s.to_string(), from, allow_public_space) {
                Ok(msg) => Ok(vec![msg]),
                Err(e) => Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::actor::Addr;
    use crate::message::Message;

    #[test]
    fn public_space_write_allowed() {
        Message::try_from(r##"
        [
          {
            "put": {
              "something": {
                "_": {
                  "#": "something",
                  ">": {
                    "else": 1653465227430
                  }
                },
                "else": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
              }
            },
            "#": "yvd2vk4338i"
          }
        ]
        "##, Addr::noop(), true).unwrap();
    }

    #[test]
    fn public_space_write_disallowed() {
        let res = Message::try_from(
            r##"
        [
          {
            "put": {
              "something": {
                "_": {
                  "#": "something",
                  ">": {
                    "else": 1653465227430
                  }
                },
                "else": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
              }
            },
            "#": "yvd2vk4338i"
          }
        ]
        "##,
            Addr::noop(),
            false,
        );
        assert!(res.is_err());
    }

    #[test]
    fn valid_content_addressed_data() {
        Message::try_from(r##"
        [
          {
            "put": {
              "#": {
                "_": {
                  "#": "#",
                  ">": {
                    "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": 1653465227430
                  }
                },
                "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
              }
            },
            "#": "yvd2vk4338i"
          }
        ]
        "##, Addr::noop(), false).unwrap();
    }

    #[test]
    fn invalid_content_addressed_data() {
        let res = Message::try_from(
            r##"
        [
          {
            "put": {
              "#": {
                "_": {
                  "#": "#",
                  ">": {
                    "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": 1653465227430
                  }
                },
                "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"invalid test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
              }
            },
            "#": "yvd2vk4338i"
          }
        ]
        "##,
            Addr::noop(),
            false,
        );
        // Content-hash verification now happens at the storage layer, not
        // the transport layer. The relay accepts content-addressed data
        // without hash verification (matching Gun.js behavior). This test
        // previously asserted is_err() — now the relay accepts it.
        assert!(res.is_ok());
    }

    #[test]
    fn valid_user_signed_data() {
        Message::try_from(r##"
        {
          "put": {
            "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8": {
              "_": {
                "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8",
                ">": {
                  "profile": 1653463165115
                }
              },
              "profile": "{\":\":{\"#\":\"~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile\"},\"~\":\"JW+tFHHVBaY+zm/uzUoGVlogvXXQIA3vFNT0f0uX6tnnPGrRevDWzEmnVYy+ChxS6AJi5THiPyOc2HorIIM5wg==\"}"
            },
            "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile": {
              "_": {
                ">": {
                  "name": 1653463165115
                },
                "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile"
              },
              "name": "{\":\":\"Arja Koriseva\",\"~\":\"KCq2D/T0mMenizxiVMso8FO5JIv9ZJLA0Q67DFa9qssPSKCmmieC1Nl5+nRpOX29C6A2/kLaJgphN/X7kUQjww==\"}"
            }
          },
          "#": "issWkzotF"
        }
        "##, Addr::noop(), false).unwrap();
    }

    #[test]
    fn invalid_user_signed_data() {
        let res = Message::try_from(
            r##"
        {
          "put": {
            "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8": {
              "_": {
                "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8",
                ">": {
                  "profile": 1653463165115
                }
              },
              "profile": "{\":\":{\"#\":\"~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile\"},\"~\":\"JW+tFHHVBaY+zm/uzUoGVlogvXXQIA3vFNT0f0uX6tnnPGrRevDWzEmnVYy+ChxS6AJi5THiPyOc2HorIIM5wg==\"}"
            },
            "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile": {
              "_": {
                ">": {
                  "name": 1653463165115
                },
                "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile"
              },
              "name": "{\":\":\"Fake Arja Koriseva\",\"~\":\"KCq2D/T0mMenizxiVMso8FO5JIv9ZJLA0Q67DFa9qssPSKCmmieC1Nl5+nRpOX29C6A2/kLaJgphN/X7kUQjww==\"}"
            }
          },
          "#": "issWkzotF"
        }
        "##,
            Addr::noop(),
            false,
        );
        assert!(res.is_err());
    }

    #[test]
    fn alias_registry_accepted_unsigned() {
        // ~@alias is the public alias registry — unsigned lookup data.
        // verify_sig should skip validation and return Ok immediately.
        let res = Message::try_from(
            r##"
        {
          "put": {
            "~@alice": {
              "_": {
                "#": "~@alice",
                ">": {
                  "pub": 1716460800000
                }
              },
              "pub": "{\"pub\":\"BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8\",\"epub\":\"UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U.U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM\"}"
            }
          },
          "#": "aliasmsg01"
        }
        "##,
            Addr::noop(),
            false,
        );
        assert!(
            res.is_ok(),
            "~@alias registry should be accepted without sig verification"
        );
    }

    // ── Sprint 1: Serialized Message Caching ──────────────────────

    use crate::message::{Get, Put};
    use crate::types::{Children, NodeData, Value};
    use std::collections::BTreeMap;

    /// Build a Put with a single soul/key/value for cache tests.
    fn make_test_put() -> Put {
        let mut children = Children::new();
        children.insert(
            "key".to_string(),
            NodeData {
                value: Value::Text("hello".to_string()),
                updated_at: 1000.0,
            },
        );
        let mut nodes = BTreeMap::new();
        nodes.insert("soul1".to_string(), children);
        Put::new(nodes, None, Addr::noop())
    }

    #[test]
    fn raw_cache_starts_empty() {
        let put = make_test_put();
        assert!(put.raw.get().is_none(), "raw cache must start as None");
    }

    #[test]
    fn raw_cache_resets_on_clone() {
        let put = make_test_put();
        // Populate cache
        let _bytes = put.get_or_serialize();
        assert!(put.raw.get().is_some(), "cache must be populated after get_or_serialize");

        // Clone — cache should reset
        let cloned = put.clone();
        assert!(cloned.raw.get().is_none(), "cache must reset to None on clone");
    }

    #[test]
    fn get_or_serialize_populates_cache() {
        let put = make_test_put();
        // First call serializes and populates cache
        let bytes1 = put.get_or_serialize();
        assert!(!bytes1.is_empty(), "serialized bytes must not be empty");
        assert!(put.raw.get().is_some(), "cache must be populated after first call");

        // Second call returns cached bytes — same content
        let bytes2 = put.get_or_serialize();
        assert_eq!(bytes1.as_ref(), bytes2.as_ref(), "cached bytes must match first serialization");
    }

    #[test]
    fn message_to_writer_uses_put_cache() {
        let put = make_test_put();
        let msg = Message::Put(put);

        // Serialize via Message::to_writer (populates cache via Put path)
        let mut buf1 = Vec::new();
        msg.to_writer(&mut buf1);

        // Serialize again — should use cache, same output
        let mut buf2 = Vec::new();
        msg.to_writer(&mut buf2);

        assert_eq!(buf1, buf2, "second to_writer must produce identical bytes via cache");
    }

    #[test]
    fn non_put_messages_bypass_cache() {
        // Get messages don't have a cache — verify to_writer still works
        let get = Get::new("test_soul".to_string(), None, Addr::noop());
        let msg = Message::Get(get);

        let mut buf1 = Vec::new();
        msg.to_writer(&mut buf1);

        let mut buf2 = Vec::new();
        msg.to_writer(&mut buf2);

        assert_eq!(buf1, buf2, "non-Put messages must serialize identically");
    }

    #[test]
    fn cached_bytes_match_direct_serialization() {
        let put = make_test_put();
        let msg = Message::Put(put);

        // Direct serialization (populates cache)
        let mut direct_buf = Vec::new();
        msg.to_writer(&mut direct_buf);
        let direct = String::from_utf8(direct_buf).unwrap();

        // get_or_serialize should return same bytes
        if let Message::Put(ref put) = msg {
            let cached = put.get_or_serialize();
            let cached_str = String::from_utf8(cached.to_vec()).unwrap();
            assert_eq!(direct, cached_str, "cached bytes must match direct serialization");
        }
    }
}
