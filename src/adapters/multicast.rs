//! UDP multicast LAN discovery and sync adapter with message chunking.
//!
//! [`Multicast`] uses UDP multicast to discover and sync with BEAM peers on
//! the local network. It broadcasts `Put` and `Get` messages to a multicast
//! group, enabling zero-config peer discovery on LANs.
//!
//! # Configuration
//!
//! - Multicast group: `233.255.255.255:7654`
//! - Buffer size: 64 KB
//! - Interfaces: all IPv4 interfaces
//!
//! # Behavior
//!
//! - `pre_start`: Joins the multicast group and starts a blocking receive
//!   loop in a `blocking_child_task`
//! - `handle`: Broadcasts outgoing `Put` and `Get` messages to the group
//! - Incoming messages are parsed and forwarded to the [`crate::router::Router`]
//! - Marks itself as `subscribe_to_everything` (receives all messages)
//!
//! # Message Chunking
//!
//! UDP datagrams are limited by the Ethernet MTU (~1500 bytes, ~1472 bytes
//! safe payload after IP + UDP headers). Messages exceeding this limit would
//! trigger IP fragmentation, which is unreliable — if any fragment is lost,
//! the entire datagram is dropped.
//!
//! BEAM solves this with application-layer chunking entirely within the
//! multicast adapter:
//!
//! - Messages ≤ [`MAX_DATAGRAM_SIZE`] are sent as raw JSON (backward compatible)
//! - Messages > [`MAX_DATAGRAM_SIZE`] are split into chunks, each wrapped in a
//!   JSON envelope: `{"beam_chunk":{"id","seq","total","data":"<base64>"}}`
//! - The [`ReassemblyBuffer`] collects chunks by `id` and reassembles the
//!   complete message when all chunks arrive
//! - Incomplete reassemblies time out after [`CHUNK_TIMEOUT`] seconds
//! - At most [`MAX_REASSEMBLY_SLOTS`] messages can be in-flight simultaneously
//!
//! This is transparent to the rest of the actor system — the router, message
//! types, and node logic are unaware of chunking.
//!
//! # Limitations
//!
//! The receive loop uses `blocking_child_task` — the `MulticastSocket::receive`
//! call is synchronous and blocks. This is not optimal for async contexts
//! but is required by the `multicast_socket` crate's API.
//!
//! There is no retransmission — UDP is unreliable by design. If a chunk is
//! lost, the message will not be reassembled and will time out. This is
//! acceptable for multicast's best-effort LAN sync use case.

use base64::prelude::*;
use oko_multicast_socket::{MulticastOptions, MulticastSocket, all_ipv4_interfaces};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddrV4;
use web_time::{Duration, Instant};

use crate::Config;
use crate::actor::{Actor, ActorContext};
use crate::message::Message;
use async_trait::async_trait;
use log::{debug, error, info, warn};
use std::sync::Arc;
use tokio::sync::RwLock;

// ─── Chunking constants ─────────────────────────────────────────────────────

/// Maximum safe UDP datagram payload size.
///
/// Ethernet MTU is 1500 bytes. Subtracting IP header (20) and UDP header (8)
/// gives 1472 bytes. We use 1400 to leave room for the JSON chunk envelope
/// overhead and any lower-layer encapsulation (e.g. VPN tunnels).
const MAX_DATAGRAM_SIZE: usize = 1400;

/// Maximum payload bytes per chunk, accounting for base64 expansion (~33%)
/// and the JSON envelope overhead (~80 bytes for the wrapper structure).
///
/// `1200` base64-encodes to ~1600 bytes — but we're encoding *fragments* of
/// the original message, not the whole thing. The envelope + encoded fragment
/// must fit under [`MAX_DATAGRAM_SIZE`]. A 900-byte fragment base64-encodes
/// to ~1200 bytes, plus ~80 bytes of envelope = ~1280 bytes total. Safe.
const CHUNK_PAYLOAD_SIZE: usize = 900;

/// How long to keep incomplete reassembly slots before evicting them.
const CHUNK_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of concurrent incomplete reassemblies.
///
/// Prevents memory exhaustion from malformed or hostile chunk floods.
/// Once this limit is reached, the oldest incomplete slot is evicted.
const MAX_REASSEMBLY_SLOTS: usize = 64;

// ─── Wire format ────────────────────────────────────────────────────────────

/// Wire-format envelope for a single chunk of a fragmented message.
///
/// Serialized as JSON:
/// ```json
/// {"beam_chunk":{"id":"abc123","seq":0,"total":3,"data":"<base64>"}}
/// ```
///
/// The receiver collects all `total` chunks for a given `id`, concatenates
/// them in `seq` order, base64-decodes the result, and parses the reassembled
/// string as a normal BEAM [`Message`].
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChunkEnvelope {
    beam_chunk: ChunkFields,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChunkFields {
    /// Message ID shared by all chunks of the same message.
    id: String,
    /// Zero-indexed sequence number.
    seq: usize,
    /// Total number of chunks for this message.
    total: usize,
    /// Base64-encoded fragment of the original serialized message.
    data: String,
}

// ─── Reassembly buffer (T1) ─────────────────────────────────────────────────

/// Reassembly buffer for collecting chunked multicast messages.
///
/// Tracks partial messages keyed by chunk `id`. When all `total` chunks for
/// a message have arrived, the reassembled payload is returned to the caller.
///
/// Incomplete slots are evicted after [`CHUNK_TIMEOUT`] and when the
/// [`MAX_REASSEMBLY_SLOTS`] cap is exceeded (oldest first).
///
/// This struct is not `Send` — it's intended to be owned by the `Multicast`
/// actor's blocking receive loop, which runs on a dedicated thread.
#[derive(Debug)]
struct ReassemblyBuffer {
    /// Active reassembly slots, keyed by message ID.
    slots: HashMap<String, PartialMessage>,
}

/// A partially-reassembled message awaiting remaining chunks.
#[derive(Debug)]
struct PartialMessage {
    /// Total expected chunk count.
    total: usize,
    /// Received fragments, indexed by `seq`. `None` = not yet received.
    received: Vec<Option<Vec<u8>>>,
    /// Deadline for eviction. Set when the first chunk arrives.
    deadline: Instant,
}

impl ReassemblyBuffer {
    /// Creates a new empty reassembly buffer.
    fn new() -> Self {
        Self {
            slots: HashMap::new(),
        }
    }

    /// Inserts a chunk and returns the reassembled payload if this was the
    /// final missing piece.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(data))` — all chunks received; `data` is the reassembled
    ///   byte string (the original serialized message).
    /// - `Ok(None)` — chunk stored, more chunks needed.
    /// - `Err(msg)` — invalid chunk (bad seq, bad total, duplicate completion).
    fn insert(&mut self, chunk: &ChunkFields) -> Result<Option<Vec<u8>>, &'static str> {
        self.evict_expired();

        // If this is a duplicate of an already-received chunk, ignore it.
        if let Some(partial) = self.slots.get(&chunk.id) {
            if chunk.seq < partial.received.len() && partial.received[chunk.seq].is_some() {
                debug!("duplicate chunk {} seq {} — ignoring", chunk.id, chunk.seq);
                return Ok(None);
            }
        }

        // Validate seq bounds.
        if chunk.seq >= chunk.total {
            return Err("chunk seq >= total");
        }

        // Evict oldest if at capacity (before borrowing via entry).
        if !self.slots.contains_key(&chunk.id) && self.slots.len() >= MAX_REASSEMBLY_SLOTS {
            self.evict_oldest();
        }

        // Get or create the partial message slot.
        let partial = self
            .slots
            .entry(chunk.id.clone())
            .or_insert_with(|| PartialMessage {
                total: chunk.total,
                received: vec![None; chunk.total],
                deadline: Instant::now() + CHUNK_TIMEOUT,
            });

        // If the slot already exists with a different total, something is wrong.
        if partial.total != chunk.total {
            warn!(
                "chunk total mismatch for {}: expected {}, got {}",
                chunk.id, partial.total, chunk.total
            );
            return Err("chunk total mismatch");
        }

        // Ensure the received vector is large enough.
        if chunk.seq >= partial.received.len() {
            // This shouldn't happen if total is consistent, but guard anyway.
            partial.received.resize(chunk.total, None);
        }

        // Store the decoded fragment.
        let fragment = BASE64_STANDARD
            .decode(&chunk.data)
            .map_err(|_| "invalid base64 in chunk data")?;
        partial.received[chunk.seq] = Some(fragment);

        // Check if all chunks have arrived.
        if partial.received.iter().all(|f| f.is_some()) {
            // Reassemble: concatenate all fragments in order.
            let mut assembled = Vec::with_capacity(
                partial
                    .received
                    .iter()
                    .map(|f| f.as_ref().map(|d| d.len()).unwrap_or(0))
                    .sum(),
            );
            for data in partial.received.iter().flatten() {
                assembled.extend_from_slice(data);
            }
            self.slots.remove(&chunk.id);
            Ok(Some(assembled))
        } else {
            Ok(None)
        }
    }

    /// Evicts slots whose deadline has passed.
    fn evict_expired(&mut self) {
        let now = Instant::now();
        self.slots.retain(|id, partial| {
            if partial.deadline <= now {
                warn!("evicting expired incomplete chunk: {}", id);
                false
            } else {
                true
            }
        });
    }

    /// Evicts the slot with the earliest deadline.
    fn evict_oldest(&mut self) {
        if let Some((oldest_id, _)) = self
            .slots
            .iter()
            .min_by_key(|(_, partial)| partial.deadline)
            .map(|(id, p)| (id.clone(), p.deadline))
        {
            warn!("evicting oldest chunk to make room: {}", oldest_id);
            self.slots.remove(&oldest_id);
        }
    }

    /// Returns the number of active reassembly slots.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.slots.len()
    }
}

// ─── Chunk sender (T2) ──────────────────────────────────────────────────────

/// Splits a serialized message into chunk envelopes for multicast broadcast.
///
/// Messages ≤ [`MAX_DATAGRAM_SIZE`] are returned as a single raw JSON string
/// (no envelope) for backward compatibility.
///
/// Messages > [`MAX_DATAGRAM_SIZE`] are split into [`CHUNK_PAYLOAD_SIZE`]-byte
/// fragments, each wrapped in a [`ChunkEnvelope`] and serialized to JSON.
///
/// # Arguments
///
/// - `data` — the serialized wire-format message string
/// - `msg_id` — the message ID used for reassembly grouping
///
/// # Returns
///
/// A vector of strings, each ≤ [`MAX_DATAGRAM_SIZE`] bytes, ready for
/// `socket.broadcast()`.
fn chunk_message(data: &str, msg_id: &str) -> Vec<String> {
    // If the message fits in a single datagram, send it raw.
    if data.len() <= MAX_DATAGRAM_SIZE {
        return vec![data.to_string()];
    }

    let data_bytes = data.as_bytes();
    let total = data_bytes.len().div_ceil(CHUNK_PAYLOAD_SIZE);

    let mut chunks = Vec::with_capacity(total);
    for (seq, fragment) in data_bytes.chunks(CHUNK_PAYLOAD_SIZE).enumerate() {
        let envelope = ChunkEnvelope {
            beam_chunk: ChunkFields {
                id: msg_id.to_string(),
                seq,
                total,
                data: BASE64_STANDARD.encode(fragment),
            },
        };
        let serialized = serde_json::to_string(&envelope)
            .expect("chunk envelope serialization should never fail");
        debug_assert!(
            serialized.len() <= MAX_DATAGRAM_SIZE,
            "chunk envelope {} bytes exceeds MAX_DATAGRAM_SIZE ({}): total={}, seq={}",
            serialized.len(),
            MAX_DATAGRAM_SIZE,
            total,
            seq
        );
        chunks.push(serialized);
    }

    chunks
}

// ─── Multicast adapter (T3) ─────────────────────────────────────────────────

/// UDP multicast adapter for LAN peer discovery and sync.
///
/// Broadcasts Gun protocol messages to the multicast group `233.255.255.255:7654`
/// and receives messages from other peers on the same LAN. Messages exceeding
/// the safe UDP datagram size are transparently chunked and reassembled.
pub struct Multicast {
    socket: Arc<RwLock<MulticastSocket>>,
    config: Config,
}

impl Multicast {
    /// Creates a new multicast adapter bound to the default group.
    ///
    /// # Panics
    ///
    /// Panics if the multicast socket cannot be created (e.g. no network
    /// interfaces available, or port 7654 is in use).
    pub fn new(config: Config) -> Self {
        let bind_address = SocketAddrV4::new([233, 255, 255, 255].into(), 7654);
        let options = MulticastOptions {
            buffer_size: 64 * 1024,
            ..MulticastOptions::default()
        };
        let interfaces = all_ipv4_interfaces().expect("could not list multicast interfaces");
        let socket = MulticastSocket::with_options(bind_address, interfaces, options)
            .expect("could not create and bind multicast socket");
        let socket = Arc::new(RwLock::new(socket));
        Multicast { socket, config }
    }

    /// Parses an incoming multicast datagram and forwards it to the router.
    ///
    /// Handles three cases:
    /// 1. Normal BEAM message — parse and forward immediately
    /// 2. Chunk envelope — store in reassembly buffer; forward when complete
    /// 3. Neither — log and discard
    ///
    /// Only `Put` and `Get` messages are forwarded — other message types
    /// (Hi, Flush, RtcSignal) are not meaningful over multicast.
    fn handle_incoming_message(
        data: &str,
        ctx: &ActorContext,
        allow_public_space: bool,
        reassembly: &mut ReassemblyBuffer,
    ) {
        debug!("in {} bytes", data.len());

        // Try parsing as a normal BEAM message first (backward compat).
        match Message::try_from(data, ctx.addr.clone(), allow_public_space) {
            Ok(msgs) => {
                for msg in msgs.into_iter() {
                    Self::forward_message(msg, ctx);
                }
                return;
            }
            Err(_) => {
                // Not a normal message — might be a chunk envelope.
            }
        }

        // Try parsing as a chunk envelope.
        match serde_json::from_str::<ChunkEnvelope>(data) {
            Ok(envelope) => {
                let chunk = &envelope.beam_chunk;
                debug!("chunk id={} seq={}/{}", chunk.id, chunk.seq, chunk.total);

                match reassembly.insert(chunk) {
                    Ok(Some(reassembled)) => {
                        // All chunks received — parse the reassembled message.
                        match String::from_utf8(reassembled) {
                            Ok(json_str) => {
                                debug!(
                                    "reassembled message {} ({} bytes)",
                                    chunk.id,
                                    json_str.len()
                                );
                                match Message::try_from(
                                    &json_str,
                                    ctx.addr.clone(),
                                    allow_public_space,
                                ) {
                                    Ok(msgs) => {
                                        for msg in msgs.into_iter() {
                                            Self::forward_message(msg, ctx);
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "reassembled message parse failed: {} (id={})",
                                            e, chunk.id
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "reassembled payload not valid UTF-8: {} (id={})",
                                    e, chunk.id
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        // Chunk stored, waiting for more.
                    }
                    Err(e) => {
                        warn!("chunk insert failed: {} (id={})", e, chunk.id);
                    }
                }
            }
            Err(_) => {
                // Neither a normal message nor a chunk envelope — discard.
                debug!("discarding unrecognizable multicast datagram");
            }
        }
    }

    /// Forwards a parsed message to the router, filtering to Put/Get only.
    fn forward_message(msg: Message, ctx: &ActorContext) {
        match msg {
            Message::Put(put) => {
                let put = put.clone();
                if let Err(e) = ctx.router.send(Message::Put(put)) {
                    error!("failed to send message to node: {:?}", e);
                }
            }
            Message::Get(get) => {
                let get = get.clone();
                if let Err(e) = ctx.router.send(Message::Get(get)) {
                    error!("failed to send message to node: {:?}", e);
                }
            }
            _ => {}
        }
    }

    /// Broadcasts a serialized message over multicast, chunking if necessary.
    async fn broadcast_message(&self, serialized: String, msg_id: String) {
        let chunks = chunk_message(&serialized, &msg_id);
        let socket = self.socket.read().await;
        for chunk in chunks {
            if let Err(e) = socket.broadcast(chunk.as_bytes()) {
                error!("multicast send error: {}", e);
            }
        }
    }
}

#[async_trait]
impl Actor for Multicast {
    async fn handle(&mut self, msg: Arc<Message>, ctx: &ActorContext) {
        debug!("out {}", msg.get_id());
        if msg.is_from(&ctx.addr) {
            return;
        }
        match &*msg {
            Message::Put(put) => {
                let msg_id = put.id.clone();
                let serialized = put.to_string();
                self.broadcast_message(serialized, msg_id).await;
            }
            Message::Get(get) => {
                let msg_id = get.id.clone();
                let serialized = get.to_string();
                self.broadcast_message(serialized, msg_id).await;
            }
            _ => {
                debug!("not sending");
            }
        }
    }

    /// Returns `true` — multicast subscribes to all messages.
    fn subscribe_to_everything(&self) -> bool {
        true
    }

    async fn pre_start(&mut self, ctx: &ActorContext) {
        info!("Syncing over multicast\n");

        let ctx_clone = ctx.clone();

        let bind_address = SocketAddrV4::new([233, 255, 255, 255].into(), 7654);
        let options = MulticastOptions {
            buffer_size: 64 * 1024,
            ..MulticastOptions::default()
        };
        let interfaces = all_ipv4_interfaces().expect("could not list multicast interfaces");
        let socket = MulticastSocket::with_options(bind_address, interfaces, options)
            .expect("could not create and bind multicast socket");

        let allow_public_space = self.config.allow_public_space;
        ctx.blocking_child_task(move || {
            let mut reassembly = ReassemblyBuffer::new();
            loop {
                if let Ok(message) = socket.receive() {
                    // TODO: if message.from == multicast_[interface], don't resend to [interface]
                    if let Ok(data) = std::str::from_utf8(&message.data) {
                        Self::handle_incoming_message(
                            data,
                            &ctx_clone,
                            allow_public_space,
                            &mut reassembly,
                        );
                    }
                }
                if *ctx_clone.is_stopped.read() {
                    break;
                }
            }
        });
    }

    async fn stopping(&mut self, _ctx: &ActorContext) {
        // The blocking child task checks is_stopped and will break on the
        // next iteration. The multicast socket is dropped when the task
        // completes. No additional cleanup needed.
        info!("Multicast stopping");
    }
}

// ─── Tests (T4) ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── ReassemblyBuffer tests ──

    #[test]
    fn test_reassembly_single_chunk() {
        let mut buf = ReassemblyBuffer::new();
        let data = b"hello world";
        let encoded = BASE64_STANDARD.encode(data);

        let chunk = ChunkFields {
            id: "msg1".to_string(),
            seq: 0,
            total: 1,
            data: encoded,
        };

        let result = buf.insert(&chunk).expect("insert should succeed");
        assert_eq!(result, Some(b"hello world".to_vec()));
        assert_eq!(buf.len(), 0); // slot was consumed
    }

    #[test]
    fn test_reassembly_multiple_chunks_in_order() {
        let mut buf = ReassemblyBuffer::new();
        let fragments: Vec<Vec<u8>> = vec![b"AAA".to_vec(), b"BBB".to_vec(), b"CCC".to_vec()];

        for (seq, frag) in fragments.iter().enumerate() {
            let chunk = ChunkFields {
                id: "msg2".to_string(),
                seq,
                total: 3,
                data: BASE64_STANDARD.encode(frag),
            };
            let result = buf.insert(&chunk).expect("insert should succeed");
            if seq < 2 {
                assert!(result.is_none(), "should not be complete at seq {}", seq);
            } else {
                assert_eq!(
                    result,
                    Some(b"AAABBBCCC".to_vec()),
                    "should reassemble on final chunk"
                );
            }
        }
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_reassembly_multiple_chunks_out_of_order() {
        let mut buf = ReassemblyBuffer::new();
        let fragments: Vec<Vec<u8>> = vec![b"AAA".to_vec(), b"BBB".to_vec(), b"CCC".to_vec()];

        // Insert in reverse order: seq 2, then 0, then 1.
        let order = [2, 0, 1];
        for &seq in &order {
            let chunk = ChunkFields {
                id: "msg3".to_string(),
                seq,
                total: 3,
                data: BASE64_STANDARD.encode(&fragments[seq]),
            };
            let result = buf.insert(&chunk).expect("insert should succeed");
            if seq != 1 {
                assert!(result.is_none(), "should not be complete yet");
            } else {
                assert_eq!(
                    result,
                    Some(b"AAABBBCCC".to_vec()),
                    "should reassemble when last chunk arrives"
                );
            }
        }
    }

    #[test]
    fn test_reassembly_duplicate_chunk_ignored() {
        let mut buf = ReassemblyBuffer::new();

        let chunk = ChunkFields {
            id: "msg4".to_string(),
            seq: 0,
            total: 2,
            data: BASE64_STANDARD.encode(b"AAA"),
        };
        buf.insert(&chunk).expect("first insert");

        // Insert same chunk again.
        let result = buf.insert(&chunk).expect("duplicate insert");
        assert!(result.is_none(), "duplicate should return None");
        assert_eq!(buf.len(), 1, "slot should still exist");
    }

    #[test]
    fn test_reassembly_total_mismatch_rejected() {
        let mut buf = ReassemblyBuffer::new();

        let chunk1 = ChunkFields {
            id: "msg5".to_string(),
            seq: 0,
            total: 3,
            data: BASE64_STANDARD.encode(b"AAA"),
        };
        buf.insert(&chunk1).expect("first insert");

        let chunk2 = ChunkFields {
            id: "msg5".to_string(),
            seq: 1,
            total: 2, // different total!
            data: BASE64_STANDARD.encode(b"BBB"),
        };
        let result = buf.insert(&chunk2);
        assert!(result.is_err(), "total mismatch should be rejected");
    }

    #[test]
    fn test_reassembly_seq_out_of_bounds_rejected() {
        let mut buf = ReassemblyBuffer::new();

        let chunk = ChunkFields {
            id: "msg6".to_string(),
            seq: 5,
            total: 3,
            data: BASE64_STANDARD.encode(b"AAA"),
        };
        let result = buf.insert(&chunk);
        assert!(result.is_err(), "seq >= total should be rejected");
    }

    #[test]
    fn test_reassembly_invalid_base64_rejected() {
        let mut buf = ReassemblyBuffer::new();

        let chunk = ChunkFields {
            id: "msg7".to_string(),
            seq: 0,
            total: 1,
            data: "not valid base64!!!".to_string(),
        };
        let result = buf.insert(&chunk);
        assert!(result.is_err(), "invalid base64 should be rejected");
    }

    #[test]
    fn test_reassembly_concurrent_messages() {
        let mut buf = ReassemblyBuffer::new();

        // Interleave chunks from two messages.
        let chunks = [
            ChunkFields {
                id: "a".to_string(),
                seq: 0,
                total: 2,
                data: BASE64_STANDARD.encode(b"A0"),
            },
            ChunkFields {
                id: "b".to_string(),
                seq: 0,
                total: 2,
                data: BASE64_STANDARD.encode(b"B0"),
            },
            ChunkFields {
                id: "a".to_string(),
                seq: 1,
                total: 2,
                data: BASE64_STANDARD.encode(b"A1"),
            },
            ChunkFields {
                id: "b".to_string(),
                seq: 1,
                total: 2,
                data: BASE64_STANDARD.encode(b"B1"),
            },
        ];

        let results: Vec<_> = chunks.iter().map(|c| buf.insert(c).unwrap()).collect();

        assert!(results[0].is_none()); // a:0
        assert!(results[1].is_none()); // b:0
        assert_eq!(results[2], Some(b"A0A1".to_vec())); // a:1 → complete
        assert_eq!(results[3], Some(b"B0B1".to_vec())); // b:1 → complete
    }

    #[test]
    fn test_reassembly_max_slots_eviction() {
        let mut buf = ReassemblyBuffer::new();

        // Fill up to MAX_REASSEMBLY_SLOTS with single-chunk-pending messages.
        for i in 0..MAX_REASSEMBLY_SLOTS {
            let chunk = ChunkFields {
                id: format!("fill{}", i),
                seq: 0,
                total: 2, // leave incomplete
                data: BASE64_STANDARD.encode(b"x"),
            };
            buf.insert(&chunk).expect("fill insert");
        }
        assert_eq!(buf.len(), MAX_REASSEMBLY_SLOTS);

        // Insert one more — should evict the oldest.
        let chunk = ChunkFields {
            id: "new".to_string(),
            seq: 0,
            total: 2,
            data: BASE64_STANDARD.encode(b"y"),
        };
        buf.insert(&chunk).expect("overflow insert");
        assert_eq!(
            buf.len(),
            MAX_REASSEMBLY_SLOTS,
            "should evict oldest to maintain cap"
        );
        // The "new" slot should exist.
        assert!(buf.slots.contains_key("new"));
        // The oldest ("fill0") should have been evicted.
        assert!(!buf.slots.contains_key("fill0"));
    }

    #[test]
    fn test_reassembly_empty_data() {
        let mut buf = ReassemblyBuffer::new();
        let chunk = ChunkFields {
            id: "empty".to_string(),
            seq: 0,
            total: 1,
            data: BASE64_STANDARD.encode(b""),
        };
        let result = buf.insert(&chunk).expect("empty insert");
        assert_eq!(result, Some(Vec::new()));
    }

    // ── chunk_message tests ──

    #[test]
    fn test_chunk_small_message_passthrough() {
        let msg =
            r##"{"put":{"~test":{"_":{"#":"~test",">":{"name":1}},"name":"hello"}},"#":"abc123"}"##;
        let chunks = chunk_message(msg, "abc123");
        assert_eq!(chunks.len(), 1, "small message should not be chunked");
        assert_eq!(chunks[0], msg, "passthrough should be identical");
    }

    #[test]
    fn test_chunk_exactly_at_threshold() {
        // A message exactly MAX_DATAGRAM_SIZE bytes should pass through.
        let msg = "x".repeat(MAX_DATAGRAM_SIZE);
        let chunks = chunk_message(&msg, "threshold");
        assert_eq!(
            chunks.len(),
            1,
            "message at threshold should not be chunked"
        );
    }

    #[test]
    fn test_chunk_one_byte_over_threshold() {
        let msg = "x".repeat(MAX_DATAGRAM_SIZE + 1);
        let chunks = chunk_message(&msg, "over");
        assert!(chunks.len() > 1, "message over threshold should be chunked");

        // Verify each chunk fits within MAX_DATAGRAM_SIZE.
        for chunk in &chunks {
            assert!(
                chunk.len() <= MAX_DATAGRAM_SIZE,
                "chunk of {} bytes exceeds MAX_DATAGRAM_SIZE",
                chunk.len()
            );
        }
    }

    #[test]
    fn test_chunk_round_trip_reassembly() {
        let mut buf = ReassemblyBuffer::new();
        // Create a message large enough to require multiple chunks.
        let payload = "Z".repeat(MAX_DATAGRAM_SIZE * 3 + 42);
        let msg_id = "roundtrip";

        let chunks = chunk_message(&payload, msg_id);
        assert!(chunks.len() > 1, "should produce multiple chunks");

        // The first chunk is the raw message if it fit — but this one doesn't,
        // so all chunks should be envelopes.
        for (i, chunk) in chunks.iter().enumerate() {
            let envelope: ChunkEnvelope =
                serde_json::from_str(chunk).expect("chunk should be valid envelope");
            assert_eq!(envelope.beam_chunk.id, msg_id);
            assert_eq!(envelope.beam_chunk.seq, i);
            assert_eq!(envelope.beam_chunk.total, chunks.len());

            let result = buf
                .insert(&envelope.beam_chunk)
                .expect("insert should succeed");
            if i < chunks.len() - 1 {
                assert!(result.is_none(), "should not be complete at chunk {}", i);
            } else {
                let reassembled = result.expect("should be complete on last chunk");
                let reassembled_str =
                    String::from_utf8(reassembled).expect("reassembled should be UTF-8");
                assert_eq!(
                    reassembled_str, payload,
                    "reassembled payload should match original"
                );
            }
        }
    }

    #[test]
    fn test_chunk_each_envelope_under_max() {
        // Test with realistic Gun.js-style Put message content.
        let large_value = "A".repeat(5000);
        let msg = format!(
            r##"{{"put":{{"~test":{{"_":{{"#":"~test",">":{{"data":1}}}},"data":"{}"}}}},"#":"bigmsg"}}"##,
            large_value
        );
        let chunks = chunk_message(&msg, "bigmsg");
        assert!(chunks.len() > 1, "large message should be chunked");

        for (i, chunk) in chunks.iter().enumerate() {
            assert!(
                chunk.len() <= MAX_DATAGRAM_SIZE,
                "chunk {} is {} bytes, exceeds MAX_DATAGRAM_SIZE ({})",
                i,
                chunk.len(),
                MAX_DATAGRAM_SIZE
            );
        }
    }

    #[test]
    fn test_chunk_empty_message() {
        let chunks = chunk_message("", "empty");
        assert_eq!(
            chunks.len(),
            1,
            "empty message should be single passthrough"
        );
        assert_eq!(chunks[0], "");
    }

    #[test]
    fn test_chunk_preserves_message_id() {
        let msg = "x".repeat(MAX_DATAGRAM_SIZE + 100);
        let chunks = chunk_message(&msg, "myID123");

        for chunk in &chunks {
            let envelope: ChunkEnvelope =
                serde_json::from_str(chunk).expect("chunk should be valid envelope");
            assert_eq!(envelope.beam_chunk.id, "myID123");
        }
    }

    #[test]
    fn test_chunk_total_is_consistent() {
        let msg = "y".repeat(CHUNK_PAYLOAD_SIZE * 5 + 1);
        let chunks = chunk_message(&msg, "consistency");

        let total = chunks
            .first()
            .map(|c| {
                let env: ChunkEnvelope = serde_json::from_str(c).expect("first chunk is envelope");
                env.beam_chunk.total
            })
            .expect("should have at least one chunk");

        assert_eq!(chunks.len(), total, "chunk count should match total field");

        for (i, chunk) in chunks.iter().enumerate() {
            let env: ChunkEnvelope = serde_json::from_str(chunk).expect("chunk is valid envelope");
            assert_eq!(env.beam_chunk.total, total);
            assert_eq!(env.beam_chunk.seq, i);
        }
    }
}
